package deckers.thibault.aves.cloud

import android.content.Context
import android.content.Intent
import android.net.Uri
import android.os.Build
import android.os.Handler
import android.os.Looper
import android.provider.DocumentsContract
import android.util.Log
import io.flutter.plugin.common.EventChannel
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import kotlin.coroutines.coroutineContext
import org.json.JSONObject
import java.io.BufferedInputStream
import java.io.EOFException
import java.io.File
import java.io.FileOutputStream
import java.io.InputStream
import java.nio.charset.StandardCharsets
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import kotlin.math.ceil
import kotlin.math.max
import kotlin.math.min

/**
 * CloudGallery 双轨缩略图管道核心管理器 (Dual-Track Thumbnail Pipeline)
 *
 * - Track 1 (P0 前台视口秒出):
 *   本地未命中时，通过 Targeted Query (精准按名称查询单文件) 直接通过 SAF 单点拉取 20~30KB WebP (50~150ms)；
 *   杜绝 9000+ 文件全量 Cursor 扫描与全局锁互斥。
 *
 * - Track 2 (P2 后台全量流式解包):
 *   消费 thumbs_pack.tar，边解包边原子落盘；批量推流 (120ms / 30张) 唤醒 Flutter 局部重绘。
 *
 * - Track 3 (P0 原图渐进分级加载):
 *   通过 getDocumentIdByPath 精准定位 Google 云端硬盘真实大图，异步拉取并落盘至本地缓存；
 *   就绪后广播 original_ready 事件驱动前端无缝渐变替换。
 */

object AppLogger {
    private val logLock = Any()
    private val sdf = java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS", java.util.Locale.US)

    fun log(context: Context?, msg: String) {
        android.util.Log.i("CloudDebug", msg)
        if (context == null) return
        try {
            val logFile = File(context.filesDir, "cloud_debug.log")
            synchronized(logLock) {
                val time = sdf.format(java.util.Date())
                val line = "[$time] $msg\n"
                logFile.appendText(line, java.nio.charset.StandardCharsets.UTF_8)
                // 控制文件大小不超过 2MB
                if (logFile.length() > 2 * 1024 * 1024L) {
                    val lines = logFile.readLines()
                    if (lines.size > 2000) {
                        logFile.writeText(lines.takeLast(1000).joinToString("\n") + "\n")
                    }
                }
            }
        } catch (_: Exception) {}
    }
}

object GoogleDriveSafManager : EventChannel.StreamHandler {
    private const val TAG = "GoogleDriveSafManager"
    private const val CLOUD_DIR_NAME = ".cloudgallery"
    private const val THUMBS_DIR_NAME = "thumbs"
    private const val THUMBS_PACK_NAME = "thumbs_pack.tar"
    private const val SNAPSHOT_DB_NAME = "master_index_snapshot.db"
    private const val META_FILE_NAME = ".pack_meta"
    private const val COMPLETE_MARKER_NAME = ".unpack_complete"

    private const val READ_CHUNK_SIZE = 32 * 1024 // 32KB 读取边界
    private const val TAR_BLOCK_SIZE = 512
    private const val DEFAULT_RATE_LIMIT_BYTES_PER_SEC = 0L // 默认 0 表示全速下载，不人为限速
    private const val EMIT_INTERVAL_MS = 120L
    private const val EMIT_BATCH_COUNT = 30

    private val WEBP_NAME_REGEX = Regex("^[a-fA-F0-9]{16,64}\\.webp$", RegexOption.IGNORE_CASE)

    private val ioScope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
    private val singleUnpackDispatcher = Dispatchers.IO.limitedParallelism(1)
    private val mainHandler = Handler(Looper.getMainLooper())

    @Volatile
    private var eventSink: EventChannel.EventSink? = null

    @Volatile
    var appContext: Context? = null

    // 状态原子追踪
    private val unpacking = AtomicBoolean(false)
    private val activeForegroundCount = AtomicInteger(0)
    private val downloadedBytes = AtomicLong(0L)
    private val totalBytes = AtomicLong(0L)
    private val extractedCounter = AtomicInteger(0)
    private val totalExpectedCount = AtomicInteger(9007)

    @Volatile
    private var isCompleted = false

    @Volatile
    private var isPreempted = false

    @Volatile
    private var rateLimitBytesPerSec = DEFAULT_RATE_LIMIT_BYTES_PER_SEC

    private var unpackJob: Job? = null

    // 路径与 DocumentId 高速缓存 (彻底根除 9000 文件遍历死锁)
    private val pathCache = ConcurrentHashMap<String, String>()
    private val readyHashes = ConcurrentHashMap.newKeySet<String>()
    private val waiters = ConcurrentHashMap<String, CompletableFuture<File?>>()
    private val downloadingOriginals = ConcurrentHashMap.newKeySet<String>()

    // 令牌桶平滑限速
    private val bucketLock = Any()
    private var availableTokens: Double = 10L * 1024L * 1024L.toDouble()
    private var lastRefillNano: Long = System.nanoTime()

    // 批量事件发射缓冲
    private val pendingEmitLock = Any()
    private val pendingRecentHashes = LinkedHashSet<String>()
    private var lastEmitTimeMs: Long = 0L

    fun isUnpacking(): Boolean = unpacking.get()

    fun createOpenTreeIntent(initialUri: Uri? = null): Intent {
        return Intent(Intent.ACTION_OPEN_DOCUMENT_TREE).apply {
            addFlags(
                Intent.FLAG_GRANT_READ_URI_PERMISSION or
                    Intent.FLAG_GRANT_PERSISTABLE_URI_PERMISSION or
                    Intent.FLAG_GRANT_PREFIX_URI_PERMISSION
            )
            if (initialUri != null && Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
                putExtra(DocumentsContract.EXTRA_INITIAL_URI, initialUri)
            }
        }
    }

    fun persistTreePermission(context: Context, treeUri: Uri, intentFlags: Int) {
        val readFlag = intentFlags and Intent.FLAG_GRANT_READ_URI_PERMISSION
        val takeFlags = if (readFlag != 0) readFlag else Intent.FLAG_GRANT_READ_URI_PERMISSION
        try {
            context.contentResolver.takePersistableUriPermission(treeUri, takeFlags)
        } catch (_: Throwable) {
            context.contentResolver.takePersistableUriPermission(treeUri, Intent.FLAG_GRANT_READ_URI_PERMISSION)
        }
    }

    fun getThumbRootDir(context: Context): File {
        val dir = File(context.noBackupFilesDir, "cloudgallery_thumbs")
        if (!dir.exists()) dir.mkdirs()
        return dir
    }

    fun getOriginalRootDir(context: Context): File {
        val dir = File(context.cacheDir, "cloud_originals")
        if (!dir.exists()) dir.mkdirs()
        return dir
    }

    fun getShardedThumbFile(root: File, sha256: String): File {
        val normalized = sha256.lowercase()
        val shard = if (normalized.length >= 2) normalized.substring(0, 2) else "00"
        return File(File(root, shard), "$normalized.webp")
    }

    /**
     * 0ms 本地缩略图命中检查（优先查分片路径，同时兼容平铺路径）
     */
    fun getLocalThumbIfPresent(context: Context, sha256: String): File? {
        if (appContext == null) appContext = context.applicationContext
        if (sha256.length !in 16..64) return null
        val root = getThumbRootDir(context)
        val sharded = getShardedThumbFile(root, sha256)
        if (sharded.isFile && sharded.length() > 0L) return sharded

        val flat = File(root, "${sha256.lowercase()}.webp")
        if (flat.isFile && flat.length() > 0L) return flat
        return null
    }

    /**
     * 0ms 本地原图命中检查
     */
    fun getLocalOriginalIfPresent(context: Context, sha256: String): File? {
        if (sha256.length !in 16..64) return null
        val root = getOriginalRootDir(context)
        val file = File(root, sha256.lowercase().trim())
        return if (file.isFile && file.length() > 0L) file else null
    }

    /**
     * EventChannel.StreamHandler 接入
     */
    override fun onListen(arguments: Any?, events: EventChannel.EventSink?) {
        eventSink = events
        emitProgressSnapshot(force = true)
    }

    override fun onCancel(arguments: Any?) {
        eventSink = null
    }

    fun setRateLimitBytesPerSec(bytesPerSec: Long) {
        synchronized(bucketLock) {
            rateLimitBytesPerSec = max(0L, bytesPerSec)
            if (rateLimitBytesPerSec > 0L) {
                refillTokensLocked(System.nanoTime())
                availableTokens = min(availableTokens, rateLimitBytesPerSec.toDouble())
            }
        }
    }

    fun getStatusMap(): Map<String, Any> = buildProgressPayload(emptyList())

    /**
     * 单次拉取 .cloudgallery/master_index_snapshot.db 到本地 filesDir
     */
    suspend fun syncMasterSnapshot(context: Context, treeUri: Uri): File = withContext(Dispatchers.IO) {
        val resolver = context.contentResolver
        val rootDocId = DocumentsContract.getTreeDocumentId(treeUri)
        val cloudDir = findChildDocument(resolver, treeUri, rootDocId, CLOUD_DIR_NAME)
            ?: throw IllegalStateException("未在所选目录找到 $CLOUD_DIR_NAME")

        val dbDoc = findChildDocument(resolver, treeUri, cloudDir.documentId, SNAPSHOT_DB_NAME)
            ?: throw IllegalStateException("未在 $CLOUD_DIR_NAME 中找到 $SNAPSHOT_DB_NAME")

        val destFile = File(context.filesDir, "cloudgallery/$SNAPSHOT_DB_NAME")
        destFile.parentFile?.mkdirs()

        val dbUri = DocumentsContract.buildDocumentUriUsingTree(treeUri, dbDoc.documentId)
        val tmp = File(destFile.parentFile, "${destFile.name}.${Thread.currentThread().id}.tmp")
        resolver.openInputStream(dbUri)?.use { input ->
            FileOutputStream(tmp).use { output ->
                input.copyTo(output, bufferSize = 64 * 1024)
            }
        } ?: throw IllegalStateException("无法打开输入流: $dbUri")

        if (!tmp.renameTo(destFile)) {
            tmp.copyTo(destFile, overwrite = true)
            tmp.delete()
        }
        destFile
    }

    /**
     * 根据相对路径精准获取云端 DocumentId（Targeted Query，彻底根除 9000 文件遍历死锁）
     */
    fun getDocumentIdByPath(context: Context, rootUri: Uri, relativePath: String): String? {
        val cleanPath = relativePath.trim().replace('\\', '/').trimStart('/')
        if (cleanPath.isEmpty()) return null

        pathCache[cleanPath]?.let { return it }

        val rootDocId = DocumentsContract.getTreeDocumentId(rootUri)
        val segments = cleanPath.split("/").filter { it.isNotEmpty() }
        var currentParentId = rootDocId

        for (i in segments.indices) {
            val segment = segments[i]
            val subPathKey = segments.subList(0, i + 1).joinToString("/")

            val cachedSubId = pathCache[subPathKey]
            if (cachedSubId != null) {
                currentParentId = cachedSubId
                continue
            }

            val foundId = queryChildDocumentId(context, rootUri, currentParentId, segment)
            if (foundId == null) {
                Log.w(TAG, "SAF item not found: $subPathKey (parent: $currentParentId)")
                return null
            }

            pathCache[subPathKey] = foundId
            currentParentId = foundId
        }

        return currentParentId
    }

    private fun queryChildDocumentId(
        context: Context,
        rootUri: Uri,
        parentDocId: String,
        displayName: String
    ): String? {
        val childrenUri = DocumentsContract.buildChildDocumentsUriUsingTree(rootUri, parentDocId)
        val projection = arrayOf(
            DocumentsContract.Document.COLUMN_DOCUMENT_ID,
            DocumentsContract.Document.COLUMN_DISPLAY_NAME
        )
        val selection = "${DocumentsContract.Document.COLUMN_DISPLAY_NAME} = ?"
        val selectionArgs = arrayOf(displayName)

        try {
            context.contentResolver.query(childrenUri, projection, selection, selectionArgs, null)?.use { cursor ->
                val idIdx = cursor.getColumnIndex(DocumentsContract.Document.COLUMN_DOCUMENT_ID)
                val nameIdx = cursor.getColumnIndex(DocumentsContract.Document.COLUMN_DISPLAY_NAME)
                while (cursor.moveToNext()) {
                    val name = if (nameIdx >= 0) cursor.getString(nameIdx) else null
                    if (name == null || name.equals(displayName, ignoreCase = true)) {
                        if (idIdx >= 0) {
                            return cursor.getString(idIdx)
                        }
                    }
                }
            }
        } catch (e: Exception) {
            Log.e(TAG, "Error querying SAF child: $displayName", e)
        }
        return null
    }

    /**
     * Track 1 (P0 前台视口秒出):
     * 1. 0ms 本地磁盘缓存检查；
     * 2. 若未命中：通过 Targeted Query 精准定位 .cloudgallery/thumbs/{sha256}.webp 单点拉取 (50~150ms)；
     * 3. 成功后落盘并推流通知 Flutter 局部重绘。
     */
    suspend fun fetchForegroundThumbnail(context: Context, treeUri: Uri?, sha256: String): File? = withContext(Dispatchers.IO) {
        val normalized = sha256.lowercase().trim()
        if (normalized.length !in 16..64) return@withContext null

        getLocalThumbIfPresent(context, normalized)?.let {
            readyHashes.add(normalized)
            return@withContext it
        }

        if (treeUri == null) return@withContext null

        val rootDir = getThumbRootDir(context)
        val targetFile = getShardedThumbFile(rootDir, normalized)

        // 精准单项查询：直接定位 .cloudgallery/thumbs/{sha256}.webp
        val docId = getDocumentIdByPath(context, treeUri, "$CLOUD_DIR_NAME/$THUMBS_DIR_NAME/$normalized.webp")
        if (docId != null) {
            val docUri = DocumentsContract.buildDocumentUriUsingTree(treeUri, docId)
            val tmpFile = File(targetFile.parentFile ?: rootDir, "$normalized.tmp.${Thread.currentThread().id}")
            tmpFile.parentFile?.mkdirs()

            try {
                context.contentResolver.openInputStream(docUri)?.use { input ->
                    FileOutputStream(tmpFile).use { output ->
                        input.copyTo(output, bufferSize = 16 * 1024)
                    }
                }

                if (tmpFile.isFile && tmpFile.length() > 0L) {
                    if (targetFile.exists()) targetFile.delete()
                    if (!tmpFile.renameTo(targetFile)) {
                        tmpFile.copyTo(targetFile, overwrite = true)
                        tmpFile.delete()
                    }
                    readyHashes.add(normalized)
                    notifyWaiter(normalized, targetFile)
                    enqueueRecentHash(normalized, forceNow = true)
                    return@withContext targetFile
                } else {
                    tmpFile.delete()
                }
            } catch (e: Exception) {
                tmpFile.delete()
                Log.w(TAG, "Track 1 single fetch failed for $normalized: ${e.message}")
            }
        }

        return@withContext null
    }

    /**
     * Track 3: 按需从 Google Drive SAF 点对点拉取真实高清原图到本地缓存：cacheDir/cloud_originals/{sha256}
     */
    suspend fun fetchOriginalToCache(
        context: Context,
        rootUri: Uri,
        relPath: String,
        sha256: String
    ): File? = withContext(Dispatchers.IO) {
        AppLogger.log(context, "[OrigFetch] Start relPath=$relPath sha=$sha256")
        val normalized = sha256.lowercase().trim()
        if (normalized.length !in 16..64) return@withContext null

        val cached = getLocalOriginalIfPresent(context, normalized)
        if (cached != null) return@withContext cached

        val newFuture = CompletableFuture<File?>()
        val existing = waiters.putIfAbsent(normalized, newFuture)
        if (existing != null) {
            return@withContext runCatching { existing.get(60, TimeUnit.SECONDS) }.getOrNull()
        }

        var resultFile: File? = null
        try {
            val docId = getDocumentIdByPath(context, rootUri, relPath)
            if (docId == null) {
                Log.w(TAG, "Cannot find original DocumentId for relPath: $relPath")
                return@withContext null
            }

            val docUri = DocumentsContract.buildDocumentUriUsingTree(rootUri, docId)
            val rootDir = getOriginalRootDir(context)
            val targetFile = File(rootDir, normalized)
            val tmpFile = File(rootDir, "$normalized.tmp.${Thread.currentThread().id}")

            context.contentResolver.openInputStream(docUri)?.use { input ->
                FileOutputStream(tmpFile).use { output ->
                    input.copyTo(output, bufferSize = 64 * 1024)
                }
            }

            if (tmpFile.isFile && tmpFile.length() > 0L) {
                if (targetFile.exists()) targetFile.delete()
                if (!tmpFile.renameTo(targetFile)) {
                    tmpFile.copyTo(targetFile, overwrite = true)
                    tmpFile.delete()
                }
                notifyOriginalReady(normalized)
                resultFile = targetFile
                return@withContext targetFile
            } else {
                tmpFile.delete()
            }
        } catch (e: Exception) {
            Log.e(TAG, "Failed downloading original for $normalized ($relPath)", e)
        } finally {
            newFuture.complete(resultFile)
            waiters.remove(normalized, newFuture)
        }
        return@withContext null
    }

    private fun notifyOriginalReady(sha256: String) {
        AppLogger.log(appContext, "[OrigReady] Broadcast event for sha=$sha256")
        mainHandler.post {
            val payload = mapOf(
                "type" to "original_ready",
                "sha256" to sha256.lowercase().trim()
            )
            eventSink?.success(payload)
        }
    }

    private fun hasSufficientLocalThumbs(context: Context): Boolean {
        val root = getThumbRootDir(context)
        var count = 0
        root.listFiles()?.forEach { sub ->
            if (sub.isDirectory) {
                count += sub.listFiles { f -> f.extension.equals("webp", ignoreCase = true) }?.size ?: 0
            } else if (sub.extension.equals("webp", ignoreCase = true)) {
                count++
            }
            if (count > 2000) return true
        }
        return count > 2000
    }

    fun startThumbPackUnpack(context: Context, treeUri: Uri, forceRestart: Boolean = false): Boolean {
        if (unpacking.get()) return true

        val rootDir = getThumbRootDir(context)
        val completeMarker = File(rootDir, COMPLETE_MARKER_NAME)
        if (!forceRestart && (completeMarker.exists() || hasSufficientLocalThumbs(context))) {
            if (!completeMarker.exists()) {
                completeMarker.writeText(System.currentTimeMillis().toString())
            }
            isCompleted = true
            emitProgressSnapshot(force = true)
            return false
        }

        unpackJob?.cancel()
        unpackJob = CoroutineScope(singleUnpackDispatcher).launch {
            try {
                untarThumbsPack(context, treeUri, force = forceRestart)
            } catch (e: CancellationException) {
                Log.i(TAG, "untarThumbsPack cancelled")
            } catch (e: Exception) {
                Log.e(TAG, "untarThumbsPack error", e)
            } finally {
                unpacking.set(false)
                isPreempted = false
                emitProgressSnapshot(force = true)
            }
        }
        return true
    }

    /**
     * Track 2 (P2 后台全量流式解包)
     */
    suspend fun untarThumbsPack(
        context: Context,
        treeUri: Uri,
        force: Boolean = false
    ): Boolean = withContext(singleUnpackDispatcher) {
        if (!unpacking.compareAndSet(false, true)) {
            Log.i(TAG, "untarThumbsPack already running, skip duplicate call")
            return@withContext true
        }

        val rootDir = getThumbRootDir(context)

        try {
            isCompleted = false
            downloadedBytes.set(0L)
            extractedCounter.set(0)

            val resolver = context.contentResolver
            val rootDocId = DocumentsContract.getTreeDocumentId(treeUri)

            val cloudDir = findChildDocument(resolver, treeUri, rootDocId, CLOUD_DIR_NAME)
            if (cloudDir == null) {
                Log.w(TAG, "Missing $CLOUD_DIR_NAME in treeUri")
                emitProgressSnapshot(force = true)
                return@withContext false
            }

            val packDoc = findChildDocument(resolver, treeUri, cloudDir.documentId, THUMBS_PACK_NAME)
            if (packDoc == null) {
                Log.w(TAG, "Missing $THUMBS_PACK_NAME in $CLOUD_DIR_NAME")
                emitProgressSnapshot(force = true)
                return@withContext false
            }

            totalBytes.set(packDoc.size)
            emitProgressSnapshot(force = true)

            val packUri = DocumentsContract.buildDocumentUriUsingTree(treeUri, packDoc.documentId)
            val rawStream = resolver.openInputStream(packUri)
                ?: throw IllegalStateException("Cannot open thumbs_pack.tar stream")

            val headerBuf = ByteArray(TAR_BLOCK_SIZE)

            BufferedInputStream(rawStream, 256 * 1024).use { input ->
                var pendingLongName: String? = null

                while (coroutineContext.isActive) {
                    val got = readFullyOrEof(input, headerBuf, 0, TAR_BLOCK_SIZE)
                    if (got < TAR_BLOCK_SIZE) break
                    downloadedBytes.addAndGet(got.toLong())

                    if (isAllZero(headerBuf)) {
                        drainRemainingStream(input)
                        break
                    }

                    val typeFlag = headerBuf[156].toInt().toChar()
                    val entrySize = parseTarSize(headerBuf, 124, 12)
                    val paddedSize = (entrySize + 511L) and 511L.inv()
                    val paddingBytes = paddedSize - entrySize

                    if (typeFlag == 'L') {
                        val nameBytes = ByteArray(entrySize.toInt())
                        readFully(input, nameBytes, 0, nameBytes.size)
                        skipPaced(input, paddingBytes)
                        downloadedBytes.addAndGet(entrySize)
                        pendingLongName = extractNullTerminatedString(nameBytes, 0, nameBytes.size)
                        continue
                    }

                    val rawName = pendingLongName ?: buildUstarName(headerBuf)
                    pendingLongName = null

                    val baseName = rawName.substringAfterLast('/')
                    val isWebp = WEBP_NAME_REGEX.matches(baseName)
                    val isRegular = (typeFlag == '0' || typeFlag == 0.toChar())

                    if (isRegular && isWebp && entrySize in 1L..10_000_000L) {
                        val sha256 = baseName.substringBeforeLast('.').lowercase()
                        val targetFile = getShardedThumbFile(rootDir, sha256)
                        targetFile.parentFile?.mkdirs()

                        if (targetFile.isFile && targetFile.length() == entrySize && !force) {
                            skipPaced(input, entrySize + paddingBytes)
                            readyHashes.add(sha256)
                            extractedCounter.incrementAndGet()
                            enqueueRecentHash(sha256, forceNow = false)
                        } else {
                            writeEntryPaced(input, targetFile, sha256, entrySize.toInt(), paddingBytes)
                        }
                    } else {
                        skipPaced(input, entrySize + paddingBytes)
                    }
                }
            }

            File(rootDir, COMPLETE_MARKER_NAME).writeText(System.currentTimeMillis().toString())
            isCompleted = true
            flushPendingEvents(force = true)
            return@withContext true
        } catch (e: Exception) {
            Log.e(TAG, "untarThumbsPack failed: ${e.message}", e)
            return@withContext false
        } finally {
            unpacking.set(false)
            isPreempted = false
            emitProgressSnapshot(force = true)
        }
    }

    private suspend fun writeEntryPaced(
        input: InputStream,
        targetFile: File,
        sha256: String,
        entrySize: Int,
        paddingBytes: Long
    ) {
        val buffer = ByteArray(entrySize)
        var remaining = entrySize
        var offset = 0

        while (remaining > 0) {
            val chunk = min(remaining, READ_CHUNK_SIZE)
            consumeTokensSmoothly(chunk)
            val n = input.read(buffer, offset, chunk)
            if (n <= 0) throw EOFException("Unexpected EOF while reading entry $sha256")
            downloadedBytes.addAndGet(n.toLong())
            remaining -= n
            offset += n
        }

        if (paddingBytes > 0L) {
            skipPaced(input, paddingBytes)
        }

        val tmpFile = File(targetFile.parentFile, "$sha256.webp.tmp.${Thread.currentThread().id}")
        FileOutputStream(tmpFile).use { fos ->
            fos.write(buffer, 0, entrySize)
        }

        if (targetFile.exists()) targetFile.delete()
        if (!tmpFile.renameTo(targetFile)) {
            tmpFile.copyTo(targetFile, overwrite = true)
            tmpFile.delete()
        }

        readyHashes.add(sha256)
        notifyWaiter(sha256, targetFile)
        extractedCounter.incrementAndGet()
        enqueueRecentHash(sha256, forceNow = false)
    }

    private suspend fun skipPaced(input: InputStream, bytesToSkip: Long) {
        var remaining = bytesToSkip
        val scratch = ByteArray(READ_CHUNK_SIZE)
        while (remaining > 0L) {
            val chunk = min(remaining, READ_CHUNK_SIZE.toLong()).toInt()
            consumeTokensSmoothly(chunk)
            val n = input.read(scratch, 0, chunk)
            if (n <= 0) break
            downloadedBytes.addAndGet(n.toLong())
            remaining -= n
        }
    }

    private suspend fun drainRemainingStream(input: InputStream) {
        val scratch = ByteArray(READ_CHUNK_SIZE)
        while (coroutineContext.isActive) {
            consumeTokensSmoothly(READ_CHUNK_SIZE)
            val n = input.read(scratch, 0, READ_CHUNK_SIZE)
            if (n <= 0) break
            downloadedBytes.addAndGet(n.toLong())
        }
    }

    /**
     * 平滑令牌桶限速（rateLimitBytesPerSec <= 0L 时全速通行）
     */
    private suspend fun consumeTokensSmoothly(bytesNeeded: Int) {
        if (bytesNeeded <= 0 || rateLimitBytesPerSec <= 0L) return

        while (coroutineContext.isActive) {
            var waitMs = 0L
            synchronized(bucketLock) {
                val now = System.nanoTime()
                refillTokensLocked(now)
                if (availableTokens >= bytesNeeded) {
                    availableTokens -= bytesNeeded
                    return
                }
                val deficit = bytesNeeded - availableTokens
                waitMs = ceil((deficit / rateLimitBytesPerSec.toDouble()) * 1000.0).toLong().coerceIn(5L, 200L)
            }
            delay(waitMs)
        }
    }

    private fun refillTokensLocked(nowNano: Long) {
        if (rateLimitBytesPerSec <= 0L) return
        val elapsedSec = (nowNano - lastRefillNano).coerceAtLeast(0L) / 1_000_000_000.0
        lastRefillNano = nowNano
        availableTokens = min(rateLimitBytesPerSec.toDouble(), availableTokens + elapsedSec * rateLimitBytesPerSec)
    }

    private fun enqueueRecentHash(sha256: String, forceNow: Boolean) {
        val count: Int
        synchronized(pendingEmitLock) {
            pendingRecentHashes.add(sha256)
            count = pendingRecentHashes.size
        }
        val now = System.currentTimeMillis()
        if (forceNow || count >= EMIT_BATCH_COUNT || (now - lastEmitTimeMs) >= EMIT_INTERVAL_MS) {
            flushPendingEvents(force = forceNow)
        }
    }

    private fun emitProgressSnapshot(force: Boolean) {
        flushPendingEvents(force)
    }

    private fun flushPendingEvents(force: Boolean) {
        val batch: List<String>
        synchronized(pendingEmitLock) {
            if (pendingRecentHashes.isEmpty() && !force) return
            batch = pendingRecentHashes.toList()
            pendingRecentHashes.clear()
            lastEmitTimeMs = System.currentTimeMillis()
        }

        val payload = buildProgressPayload(batch)
        mainHandler.post {
            eventSink?.success(payload)
        }
    }

    private fun buildProgressPayload(recentHashes: List<String>): Map<String, Any> {
        val dl = downloadedBytes.get()
        val tot = totalBytes.get()
        val ext = extractedCounter.get()
        val exp = totalExpectedCount.get()

        val progress = if (tot > 0L) {
            (dl.toDouble() / tot.toDouble()).coerceIn(0.0, 1.0)
        } else if (exp > 0) {
            (ext.toDouble() / exp.toDouble()).coerceIn(0.0, 1.0)
        } else 0.0

        return mapOf(
            "downloadedBytes" to dl,
            "totalBytes" to tot,
            "extractedCount" to ext,
            "totalCount" to exp,
            "progress" to progress,
            "isSyncing" to unpacking.get(),
            "isCompleted" to isCompleted,
            "isPreempted" to isPreempted,
            "rateLimitBytesPerSec" to rateLimitBytesPerSec,
            "recentHashes" to recentHashes
        )
    }

    private fun notifyWaiter(sha256: String, file: File) {
        waiters.remove(sha256)?.complete(file)
    }

    data class SafChildDoc(val documentId: String, val size: Long, val lastModified: Long)

    fun findChildDocument(
        resolver: android.content.ContentResolver,
        treeUri: Uri,
        parentDocId: String,
        targetName: String
    ): SafChildDoc? {
        val childrenUri = DocumentsContract.buildChildDocumentsUriUsingTree(treeUri, parentDocId)
        val projection = arrayOf(
            DocumentsContract.Document.COLUMN_DOCUMENT_ID,
            DocumentsContract.Document.COLUMN_DISPLAY_NAME,
            DocumentsContract.Document.COLUMN_SIZE,
            DocumentsContract.Document.COLUMN_LAST_MODIFIED
        )
        val selection = "${DocumentsContract.Document.COLUMN_DISPLAY_NAME} = ?"
        val selectionArgs = arrayOf(targetName)

        resolver.query(childrenUri, projection, selection, selectionArgs, null)?.use { cursor ->
            val idIdx = cursor.getColumnIndex(DocumentsContract.Document.COLUMN_DOCUMENT_ID)
            val nameIdx = cursor.getColumnIndex(DocumentsContract.Document.COLUMN_DISPLAY_NAME)
            val sizeIdx = cursor.getColumnIndex(DocumentsContract.Document.COLUMN_SIZE)
            val modIdx = cursor.getColumnIndex(DocumentsContract.Document.COLUMN_LAST_MODIFIED)
            while (cursor.moveToNext()) {
                val name = if (nameIdx >= 0) cursor.getString(nameIdx) else null
                if (name == null || name.equals(targetName, ignoreCase = true)) {
                    val id = if (idIdx >= 0) cursor.getString(idIdx) else null
                    if (id != null) {
                        return SafChildDoc(
                            documentId = id,
                            size = if (sizeIdx >= 0 && !cursor.isNull(sizeIdx)) cursor.getLong(sizeIdx) else 0L,
                            lastModified = if (modIdx >= 0 && !cursor.isNull(modIdx)) cursor.getLong(modIdx) else 0L
                        )
                    }
                }
            }
        }
        return null
    }

    private fun isAllZero(block: ByteArray): Boolean {
        for (b in block) if (b.toInt() != 0) return false
        return true
    }

    private fun buildUstarName(header: ByteArray): String {
        val name = extractNullTerminatedString(header, 0, 100)
        val prefix = extractNullTerminatedString(header, 345, 155)
        return if (prefix.isNotEmpty()) "$prefix/$name" else name
    }

    private fun extractNullTerminatedString(buf: ByteArray, offset: Int, length: Int): String {
        var end = offset
        val limit = offset + length
        while (end < limit && buf[end].toInt() != 0) end++
        return String(buf, offset, end - offset, StandardCharsets.UTF_8)
    }

    private fun parseTarSize(buf: ByteArray, offset: Int, length: Int): Long {
        if ((buf[offset].toInt() and 0x80) != 0) {
            var value = 0L
            for (i in 1 until length) {
                value = (value shl 8) or (buf[offset + i].toLong() and 0xFFL)
            }
            return value
        }
        val str = extractNullTerminatedString(buf, offset, length).trim()
        if (str.isEmpty()) return 0L
        return str.toLong(8)
    }

    private fun readFullyOrEof(input: InputStream, buf: ByteArray, off: Int, len: Int): Int {
        var total = 0
        while (total < len) {
            val n = input.read(buf, off + total, len - total)
            if (n < 0) return total
            total += n
        }
        return total
    }

    private fun readFully(input: InputStream, buf: ByteArray, off: Int, len: Int) {
        val got = readFullyOrEof(input, buf, off, len)
        if (got < len) throw EOFException("Unexpected EOF in tar stream (expected $len, got $got)")
    }
}
