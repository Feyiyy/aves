package deckers.thibault.aves.cloud

import android.app.Activity
import android.content.Context
import android.content.Intent
import android.net.Uri
import android.os.Handler
import android.os.Looper
import android.util.Base64
import android.util.Log
import io.flutter.plugin.common.EventChannel
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext
import org.json.JSONArray
import org.json.JSONObject
import java.io.BufferedInputStream
import java.io.BufferedOutputStream
import java.io.BufferedReader
import java.io.File
import java.io.FileInputStream
import java.io.FileOutputStream
import java.io.InputStream
import java.io.InputStreamReader
import java.net.HttpURLConnection
import java.net.InetAddress
import java.net.ServerSocket
import java.net.Socket
import java.net.SocketException
import java.net.SocketTimeoutException
import java.net.URL
import java.net.URLEncoder
import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.security.SecureRandom
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

/**
 * CloudGallery v3.0 统一云端引擎（单文件、零外部依赖，兼容 Aves libre 变体）
 *
 * 1. 动态双通道选路：450ms 探测局域网 PC Daemon，不可达自动无缝降级 Google Drive REST API v3；
 * 2. OAuth 2.0 + PKCE (127.0.0.1 本地回环，兼容 Desktop Client + Personal 免审核长期授权) + Single-Flight 刷新锁；
 * 3. 不可变分卷 (packs pack_id.pack) HTTP Range 精准拉取单张 20KB WebP 缩略图，彻底告别 SAF 与 thumbs_pack.tar；
 * 4. 原图 Single-Flight 并发合并下载 + 1.5GB LRU 本地缓存 + original_ready 事件推流；
 * 5. 内置本地 HTTP 206 流式回环代理 (127.0.0.1)，让 mpv 在家/出门均可流式播放 4K 视频与拖拽进度条；
 * 6. Outbox 投递与流式上传（LAN 固定长度流式 / Drive Resumable，4K 视频恒定 64KB 内存）。
 */
object CloudDriveApiClient : EventChannel.StreamHandler {
    private const val TAG = "CloudDriveApiClient"
    private const val PREFS_NAME = "cloudgallery_drive_v3_prefs"

    private const val KEY_LAN_URL = "lan_base_url"
    private const val KEY_LAN_TOKEN = "lan_api_token"
    private const val KEY_CLIENT_ID = "oauth_client_id"
    private const val KEY_CLIENT_SECRET = "oauth_client_secret"
    private const val KEY_ACCESS_TOKEN = "oauth_access_token"
    private const val KEY_REFRESH_TOKEN = "oauth_refresh_token"
    private const val KEY_TOKEN_EXPIRY_MS = "oauth_token_expiry_ms"
    private const val KEY_DRIVE_ROOT_ID = "drive_root_folder_id_v2"
    private const val KEY_DRIVE_ROOT_NAME = "drive_root_folder_name"

    private const val GOOGLE_AUTH_URL = "https://accounts.google.com/o/oauth2/v2/auth"
    private const val GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token"
    private const val DRIVE_API_BASE = "https://www.googleapis.com/drive/v3"
    private const val DRIVE_UPLOAD_BASE = "https://www.googleapis.com/upload/drive/v3"
    private const val DRIVE_SCOPE = "https://www.googleapis.com/auth/drive"

    private const val CLOUD_DIR = ".cloudgallery"
    private const val SNAPSHOTS_DIR = "snapshots"
    private const val PACKS_DIR = "packs"
    private const val THUMBS_DIR = "thumbs"
    private const val SNAPSHOT_DB_NAME = "master_index_snapshot.db"
    private const val MANIFEST_NAME = "manifest.json"
    private const val DEFAULT_LAN_URL = "http://192.168.5.3:8088"
    private const val MAX_ORIGINAL_CACHE_BYTES = 1500L * 1024L * 1024L
    private const val LAN_PROBE_TTL_MS = 25_000L

    val ioScope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
    private val mainHandler = Handler(Looper.getMainLooper())

    @Volatile private var eventSink: EventChannel.EventSink? = null
    @Volatile private var cachedLanReachable: Boolean = false
    @Volatile private var lastLanProbeMs: Long = 0L

    private val authInProgress = AtomicBoolean(false)
    private val tokenRefreshMutex = Mutex()
    private val pathToFileIdCache = ConcurrentHashMap<String, String>()
    private val packFileIdCache = ConcurrentHashMap<String, String>()
    private val inFlightThumbs = ConcurrentHashMap<String, CompletableFuture<File?>>()
    private val inFlightOriginals = ConcurrentHashMap<String, CompletableFuture<File?>>()
    private val lastOriginalErrors = ConcurrentHashMap<String, String>()

    fun getLastOriginalError(itemId: String): String? =
        lastOriginalErrors[itemId.lowercase().trim()]

    fun setLastOriginalError(itemId: String, error: String) {
        val clean = itemId.lowercase().trim()
        lastOriginalErrors[clean] = error
        Log.e("CloudDebug", "[OrigError] itemId=$clean error=$error")
    }

    private val proxyServer = LocalMediaProxyServer()

    data class SnapshotSyncResult(
        val snapshotFile: File,
        val snapshotVersion: Long,
        val channel: String,
    )

    private data class DriveDownloadOutcome(
        val success: Boolean,
        val httpCode: Int,
    )

    // =========================================================================
    // 1. EventChannel 推流（原图就绪通知）
    // =========================================================================

    override fun onListen(arguments: Any?, events: EventChannel.EventSink?) {
        eventSink = events
    }

    override fun onCancel(arguments: Any?) {
        eventSink = null
    }

    private fun notifyOriginalReady(itemId: String) {
        mainHandler.post {
            eventSink?.success(
                mapOf(
                    "type" to "original_ready",
                    "sha256" to itemId.lowercase().trim(),
                )
            )
        }
    }

    // =========================================================================
    // 2. 配置与状态管理
    // =========================================================================

    private fun prefs(context: Context) =
        context.applicationContext.getSharedPreferences(PREFS_NAME, Context.MODE_PRIVATE)

    fun purgeGlideDiskCacheIfNeeded(context: Context) {
        val prefs = context.applicationContext.getSharedPreferences("cloudgallery_prefs", Context.MODE_PRIVATE)
        if (!prefs.getBoolean("glide_cache_purged_v3", false)) {
            ioScope.launch {
                try {
                    val glideCacheDir = File(context.applicationContext.cacheDir, "image_manager_disk_cache")
                    if (glideCacheDir.exists()) {
                        glideCacheDir.deleteRecursively()
                        Log.i("CloudDebug", "[GlideCache] Successfully purged dirty Glide disk cache: ${glideCacheDir.absolutePath}")
                    }
                    prefs.edit().putBoolean("glide_cache_purged_v3", true).apply()
                } catch (e: Exception) {
                    Log.w("CloudDebug", "[GlideCache] Failed to purge Glide disk cache", e)
                }
            }
        }
    }

    fun configure(
        context: Context,
        lanBaseUrl: String? = null,
        lanApiToken: String? = null,
        oauthClientId: String? = null,
        oauthClientSecret: String? = null,
        driveRootFolderName: String? = null,
        driveRootFolderId: String? = null,
    ) {
        purgeGlideDiskCacheIfNeeded(context)
        val editor = prefs(context).edit()
        lanBaseUrl?.let { editor.putString(KEY_LAN_URL, it.trim().trimEnd('/')) }
        lanApiToken?.let { editor.putString(KEY_LAN_TOKEN, it.trim()) }
        oauthClientId?.let { editor.putString(KEY_CLIENT_ID, it.trim()) }
        oauthClientSecret?.let { editor.putString(KEY_CLIENT_SECRET, it.trim()) }
        driveRootFolderName?.let {
            editor.putString(KEY_DRIVE_ROOT_NAME, it.trim())
            if (driveRootFolderId == null) {
                editor.remove(KEY_DRIVE_ROOT_ID)
                editor.remove("drive_root_folder_id")
            }
        }
        driveRootFolderId?.let { editor.putString(KEY_DRIVE_ROOT_ID, it.trim()) }
        editor.apply()
        lastLanProbeMs = 0L
        pathToFileIdCache.clear()
        packFileIdCache.clear()
    }

    fun getLanBaseUrl(context: Context): String =
        prefs(context).getString(KEY_LAN_URL, DEFAULT_LAN_URL)?.trim()?.trimEnd('/').orEmpty()
            .ifEmpty { DEFAULT_LAN_URL }

    fun getLanApiToken(context: Context): String =
        prefs(context).getString(KEY_LAN_TOKEN, "")?.trim().orEmpty()

    fun isDriveAuthorized(context: Context): Boolean =
        !prefs(context).getString(KEY_REFRESH_TOKEN, null).isNullOrBlank()

    fun clearDriveAuth(context: Context) {
        prefs(context).edit()
            .remove(KEY_ACCESS_TOKEN)
            .remove(KEY_REFRESH_TOKEN)
            .remove(KEY_TOKEN_EXPIRY_MS)
            .remove(KEY_DRIVE_ROOT_ID)
            .remove("drive_root_folder_id")
            .apply()
        pathToFileIdCache.clear()
        packFileIdCache.clear()
    }

    fun getStatusMap(context: Context): Map<String, Any?> = mapOf(
        "lanBaseUrl" to getLanBaseUrl(context),
        "lanReachable" to cachedLanReachable,
        "driveAuthorized" to isDriveAuthorized(context),
        "oauthClientId" to prefs(context).getString(KEY_CLIENT_ID, ""),
        "driveRootFolderId" to prefs(context).getString(KEY_DRIVE_ROOT_ID, null),
        "driveRootFolderName" to prefs(context).getString(KEY_DRIVE_ROOT_NAME, "图片"),
        "proxyPort" to proxyServer.ensureStarted(context.applicationContext),
    )

    // =========================================================================
    // 3. 动态双通道探活
    // =========================================================================

    fun isLanAvailable(context: Context, forceProbe: Boolean = false): Boolean {
        val now = System.currentTimeMillis()
        if (!forceProbe && (now - lastLanProbeMs) < LAN_PROBE_TTL_MS) return cachedLanReachable
        val baseUrl = getLanBaseUrl(context)
        if (baseUrl.isBlank()) {
            cachedLanReachable = false
            lastLanProbeMs = now
            return false
        }
        val ok = try {
            val conn = (URL("$baseUrl/api/ping").openConnection() as HttpURLConnection).apply {
                requestMethod = "GET"
                connectTimeout = 450
                readTimeout = 600
                useCaches = false
            }
            try {
                conn.responseCode == 200
            } finally {
                conn.disconnect()
            }
        } catch (_: Exception) {
            false
        }
        cachedLanReachable = ok
        lastLanProbeMs = now
        return ok
    }

    fun invalidateLanProbe() {
        lastLanProbeMs = 0L
    }

    // =========================================================================
    // 4. Google OAuth 2.0 + PKCE (127.0.0.1 回环，抗预连接/favicon 干扰)
    // =========================================================================

    suspend fun authorizeGoogleDrive(
        activity: Activity,
        clientId: String? = null,
        clientSecret: String? = null,
        timeoutMs: Long = 180_000L,
    ): Boolean = withContext(Dispatchers.IO) {
        if (!authInProgress.compareAndSet(false, true)) {
            Log.w(TAG, "OAuth flow already in progress")
            return@withContext false
        }
        try {
            val sp = prefs(activity)
            val effectiveClientId = (clientId ?: sp.getString(KEY_CLIENT_ID, null))?.trim().orEmpty()
            val effectiveSecret = (clientSecret ?: sp.getString(KEY_CLIENT_SECRET, null))?.trim().orEmpty()
            require(effectiveClientId.isNotEmpty()) { "OAuth Client ID 不能为空" }

            configure(activity, oauthClientId = effectiveClientId, oauthClientSecret = effectiveSecret)

            val codeVerifier = generatePkceVerifier()
            val codeChallenge = generatePkceChallenge(codeVerifier)
            val expectedState = generateRandomHex(16)

            val callbackServer = ServerSocket(0, 8, InetAddress.getByName("127.0.0.1"))
            val redirectUri = "http://127.0.0.1:${callbackServer.localPort}/oauth2callback"

            val authUri = Uri.parse(GOOGLE_AUTH_URL).buildUpon()
                .appendQueryParameter("client_id", effectiveClientId)
                .appendQueryParameter("redirect_uri", redirectUri)
                .appendQueryParameter("response_type", "code")
                .appendQueryParameter("scope", DRIVE_SCOPE)
                .appendQueryParameter("access_type", "offline")
                .appendQueryParameter("prompt", "consent")
                .appendQueryParameter("code_challenge", codeChallenge)
                .appendQueryParameter("code_challenge_method", "S256")
                .appendQueryParameter("state", expectedState)
                .build()

            withContext(Dispatchers.Main) {
                activity.startActivity(Intent(Intent.ACTION_VIEW, authUri).addFlags(Intent.FLAG_ACTIVITY_NEW_TASK))
            }

            val authCode = waitForOAuthLoopbackCode(callbackServer, expectedState, timeoutMs)
                ?: return@withContext false

            val params = linkedMapOf(
                "client_id" to effectiveClientId,
                "code" to authCode,
                "code_verifier" to codeVerifier,
                "grant_type" to "authorization_code",
                "redirect_uri" to redirectUri,
            )
            if (effectiveSecret.isNotEmpty()) params["client_secret"] = effectiveSecret

            val tokenJson = postFormJson(GOOGLE_TOKEN_URL, params) ?: return@withContext false
            saveTokenResponse(activity, tokenJson)
            Log.i(TAG, "Google Drive OAuth authorized successfully")
            true
        } finally {
            authInProgress.set(false)
        }
    }

    private fun waitForOAuthLoopbackCode(
        server: ServerSocket,
        expectedState: String,
        timeoutMs: Long,
    ): String? {
        val deadline = System.currentTimeMillis() + timeoutMs
        server.use { srv ->
            while (System.currentTimeMillis() < deadline) {
                val remaining = (deadline - System.currentTimeMillis()).coerceAtLeast(1000L)
                srv.soTimeout = remaining.toInt()
                val socket = try {
                    srv.accept()
                } catch (_: SocketTimeoutException) {
                    return null
                } catch (e: Exception) {
                    Log.e(TAG, "OAuth accept failed: ${e.message}")
                    return null
                }

                socket.use { client ->
                    client.soTimeout = 5000
                    val requestLine = try {
                        BufferedReader(InputStreamReader(client.getInputStream(), StandardCharsets.UTF_8)).readLine()
                    } catch (_: Exception) {
                        null
                    }
                    // 忽略浏览器的空预连接
                    if (requestLine.isNullOrBlank()) return@use

                    val pathWithQuery = requestLine.split(" ").getOrNull(1).orEmpty()
                    if (!pathWithQuery.startsWith("/oauth2callback")) {
                        // 忽略 /favicon.ico 等附属请求，继续等待真正的回调
                        writeHtmlResponse(client, 404, "Not Found")
                        return@use
                    }

                    val parsed = Uri.parse("http://127.0.0.1$pathWithQuery")
                    val code = parsed.getQueryParameter("code")
                    val returnedState = parsed.getQueryParameter("state")
                    val err = parsed.getQueryParameter("error")

                    val ok = !code.isNullOrBlank() && returnedState == expectedState
                    val html = if (ok) {
                        "<html><meta charset='utf-8'><body style='font-family:sans-serif;text-align:center;padding:48px;'><h2>✅ Google Drive 授权成功</h2><p>请关闭此页面并返回 Aves 相册。</p></body></html>"
                    } else {
                        "<html><meta charset='utf-8'><body style='font-family:sans-serif;text-align:center;padding:48px;'><h2>❌ 授权未完成</h2><p>原因：${err ?: "state 校验失败或已取消"}</p></body></html>"
                    }
                    writeHtmlResponse(client, 200, html)
                    return if (ok) code else null
                }
            }
        }
        return null
    }

    private fun writeHtmlResponse(client: Socket, code: Int, html: String) {
        runCatching {
            val body = html.toByteArray(StandardCharsets.UTF_8)
            val status = if (code == 200) "OK" else "Not Found"
            val hdr = "HTTP/1.1 $code $status\r\nContent-Type: text/html; charset=utf-8\r\nContent-Length: ${body.size}\r\nConnection: close\r\n\r\n"
            client.getOutputStream().apply {
                write(hdr.toByteArray(StandardCharsets.UTF_8))
                write(body)
                flush()
            }
        }
    }

    suspend fun getValidAccessToken(context: Context, forceRefresh: Boolean = false): String? {
        val sp = prefs(context)
        val cached = sp.getString(KEY_ACCESS_TOKEN, null)
        val expiry = sp.getLong(KEY_TOKEN_EXPIRY_MS, 0L)
        if (!forceRefresh && !cached.isNullOrBlank() && System.currentTimeMillis() < expiry - 90_000L) {
            return cached
        }
        return tokenRefreshMutex.withLock {
            val latest = sp.getString(KEY_ACCESS_TOKEN, null)
            val latestExp = sp.getLong(KEY_TOKEN_EXPIRY_MS, 0L)
            if (!forceRefresh && !latest.isNullOrBlank() && System.currentTimeMillis() < latestExp - 90_000L) {
                return@withLock latest
            }
            val refreshToken = sp.getString(KEY_REFRESH_TOKEN, null) ?: return@withLock null
            val clientId = sp.getString(KEY_CLIENT_ID, null)?.trim().orEmpty()
            val clientSecret = sp.getString(KEY_CLIENT_SECRET, null)?.trim().orEmpty()
            if (clientId.isEmpty()) return@withLock null

            withContext(Dispatchers.IO) {
                val params = linkedMapOf(
                    "client_id" to clientId,
                    "refresh_token" to refreshToken,
                    "grant_type" to "refresh_token",
                )
                if (clientSecret.isNotEmpty()) params["client_secret"] = clientSecret
                val json = postFormJson(GOOGLE_TOKEN_URL, params) ?: return@withContext null
                saveTokenResponse(context, json)
                json.optString("access_token").takeIf { it.isNotBlank() }
            }
        }
    }

    private fun getValidAccessTokenBlocking(context: Context, forceRefresh: Boolean = false): String? =
        runBlocking { getValidAccessToken(context, forceRefresh) }

    private fun saveTokenResponse(context: Context, json: JSONObject) {
        val access = json.optString("access_token")
        val refresh = json.optString("refresh_token")
        val expiresIn = json.optLong("expires_in", 3600L)
        val ed = prefs(context).edit()
        if (access.isNotBlank()) {
            ed.putString(KEY_ACCESS_TOKEN, access)
            ed.putLong(KEY_TOKEN_EXPIRY_MS, System.currentTimeMillis() + expiresIn * 1000L)
        }
        if (refresh.isNotBlank()) ed.putString(KEY_REFRESH_TOKEN, refresh)
        ed.apply()
    }

    // =========================================================================
    // 5. Drive API v3 精准路径解析与自愈缓存
    // =========================================================================

    private fun escapeQ(v: String): String = v.replace("\\", "\\\\").replace("'", "\\'")

    private val lastPathDiagnostics = ConcurrentHashMap<String, String>()

    fun getLastPathDiagnostic(cleanPath: String): String? = lastPathDiagnostics[cleanPath]

    fun resolveDriveRootFolderId(context: Context, token: String, bypassCache: Boolean = false): String? {
        val sp = prefs(context)
        if (sp.contains("drive_root_folder_id")) {
            sp.edit().remove("drive_root_folder_id").apply()
            Log.i("CloudDebug", "[DriveRoot] Cleared legacy drive_root_folder_id from prefs")
        }
        if (!bypassCache) {
            sp.getString(KEY_DRIVE_ROOT_ID, null)?.takeIf { it.isNotBlank() }?.let { return it }
        }

        // 1. 查询所有名为 .cloudgallery 且 trashed = false 的文件夹（按最近修改排序）
        val qCloud = "name = '$CLOUD_DIR' and mimeType = 'application/vnd.google-apps.folder' and trashed = false"
        val cloudUrl = "$DRIVE_API_BASE/files?q=${urlEncode(qCloud)}&fields=${urlEncode("files(id,parents,modifiedTime)")}&pageSize=20&orderBy=${urlEncode("modifiedTime desc")}&supportsAllDrives=true&includeItemsFromAllDrives=true"
        val cloudFiles = driveApiGetJson(context, token, cloudUrl)?.optJSONArray("files")

        var bestParentId: String? = null
        var bestCloudId: String? = null
        var firstCandidateParentId: String? = null
        var firstCandidateCloudId: String? = null

        if (cloudFiles != null && cloudFiles.length() > 0) {
            for (idx in 0 until cloudFiles.length()) {
                val cObj = cloudFiles.optJSONObject(idx) ?: continue
                val cId = cObj.optString("id")
                val pId = cObj.optJSONArray("parents")?.optString(0).orEmpty()
                val modTime = cObj.optString("modifiedTime")
                if (pId.isBlank()) continue
                if (firstCandidateParentId == null) {
                    firstCandidateParentId = pId
                    firstCandidateCloudId = cId
                }

                // 检查该父目录下是否还存在除 .cloudgallery 之外的其他子文件夹（说明是真正存放照片的图库根目录）
                val qSiblings = "'$pId' in parents and mimeType = 'application/vnd.google-apps.folder' and trashed = false"
                val sibUrl = "$DRIVE_API_BASE/files?q=${urlEncode(qSiblings)}&fields=${urlEncode("files(id,name)")}&pageSize=10&supportsAllDrives=true&includeItemsFromAllDrives=true"
                val sibFiles = driveApiGetJson(context, token, sibUrl)?.optJSONArray("files")
                val siblingNames = mutableListOf<String>()
                if (sibFiles != null) {
                    for (sIdx in 0 until sibFiles.length()) {
                        val sName = sibFiles.optJSONObject(sIdx)?.optString("name").orEmpty()
                        if (sName.isNotEmpty() && !sName.equals(CLOUD_DIR, ignoreCase = true)) {
                            siblingNames.add(sName)
                        }
                    }
                }
                Log.i("CloudDebug", "[DriveRoot] candidate #$idx cId=$cId pId=$pId modTime=$modTime siblingFolders=$siblingNames")
                if (siblingNames.isNotEmpty()) {
                    bestParentId = pId
                    bestCloudId = cId
                    Log.i("CloudDebug", "[DriveRoot] Selected parentId=$pId as root (has sibling folders: $siblingNames, modTime=$modTime)")
                    break
                }
            }
        }

        val selectedParentId = bestParentId ?: firstCandidateParentId
        val selectedCloudId = bestCloudId ?: firstCandidateCloudId
        if (!selectedParentId.isNullOrBlank()) {
            sp.edit().putString(KEY_DRIVE_ROOT_ID, selectedParentId).apply()
            if (!selectedCloudId.isNullOrBlank()) {
                pathToFileIdCache[CLOUD_DIR] = selectedCloudId
            }
            logTopLevelItems(context, token, selectedParentId)
            return selectedParentId
        }

        // 2. 兜底尝试按配置的 rootName 查找
        val rootName = sp.getString(KEY_DRIVE_ROOT_NAME, "图片")?.trim().orEmpty().ifEmpty { "图片" }
        val qRoot = "name = '${escapeQ(rootName)}' and mimeType = 'application/vnd.google-apps.folder' and trashed = false"
        val rootUrl = "$DRIVE_API_BASE/files?q=${urlEncode(qRoot)}&fields=${urlEncode("files(id)")}&pageSize=5&supportsAllDrives=true&includeItemsFromAllDrives=true"
        val rootId = driveApiGetJson(context, token, rootUrl)?.optJSONArray("files")?.optJSONObject(0)?.optString("id")
        Log.i("CloudDebug", "[DriveRoot] resolved rootName='$rootName' -> rootId=$rootId")
        if (!rootId.isNullOrBlank()) {
            sp.edit().putString(KEY_DRIVE_ROOT_ID, rootId).apply()
            logTopLevelItems(context, token, rootId)
            return rootId
        }
        Log.w("CloudDebug", "[DriveRoot] failed to resolve rootId on Google Drive!")
        return null
    }

    private fun logTopLevelItems(context: Context, token: String, rootId: String) {
        try {
            val qTop = "'$rootId' in parents and trashed = false"
            val topUrl = "$DRIVE_API_BASE/files?q=${urlEncode(qTop)}&fields=${urlEncode("files(name,mimeType)")}&pageSize=20&supportsAllDrives=true&includeItemsFromAllDrives=true"
            val files = driveApiGetJson(context, token, topUrl)?.optJSONArray("files")
            val names = mutableListOf<String>()
            if (files != null) {
                for (i in 0 until files.length()) {
                    files.optJSONObject(i)?.optString("name")?.let { if (it.isNotEmpty()) names.add(it) }
                }
            }
            Log.i("CloudDebug", "[DriveRoot] rootId=$rootId top-level items: $names")
        } catch (e: Exception) {
            Log.w("CloudDebug", "[DriveRoot] logTopLevelItems failed: ${e.message}")
        }
    }

    fun resolveDriveFileIdByPath(
        context: Context,
        relativePath: String,
        token: String,
        bypassCache: Boolean = false,
    ): String? {
        var clean = relativePath.trim().replace('\\', '/').trimStart('/')
        while (clean.contains("%2F", ignoreCase = true) || clean.contains("%2f") || clean.contains("%20")) {
            val decoded = try {
                java.net.URLDecoder.decode(clean, "UTF-8")
            } catch (_: Exception) {
                break
            }
            if (decoded == clean) break
            clean = decoded
        }
        if (clean.isEmpty()) return null
        if (!bypassCache) pathToFileIdCache[clean]?.let { return it }

        val rootId = resolveDriveRootFolderId(context, token, bypassCache = bypassCache) ?: return null
        val segments = clean.split('/').filter { it.isNotEmpty() }
        if (segments.isEmpty()) return null

        return searchSegmentCandidates(context, token, listOf(rootId), 0, segments, bypassCache, clean, rootId)
    }

    private fun searchSegmentCandidates(
        context: Context,
        token: String,
        parentIds: List<String>,
        segmentIndex: Int,
        segments: List<String>,
        bypassCache: Boolean,
        cleanPath: String,
        rootId: String,
    ): String? {
        if (segmentIndex >= segments.size) return null
        val seg = segments[segmentIndex]
        val isLast = (segmentIndex == segments.lastIndex)
        val subKey = segments.subList(0, segmentIndex + 1).joinToString("/")

        if (!bypassCache && pathToFileIdCache.containsKey(subKey)) {
            val cachedId = pathToFileIdCache[subKey]!!
            if (isLast) return cachedId
            val found = searchSegmentCandidates(
                context, token, listOf(cachedId), segmentIndex + 1, segments, bypassCache, cleanPath, rootId
            )
            if (found != null) return found
            pathToFileIdCache.remove(subKey)
        }

        val mimeFilter = if (isLast) "" else " and mimeType = 'application/vnd.google-apps.folder'"
        for (pId in parentIds) {
            val q = "'$pId' in parents and name = '${escapeQ(seg)}'$mimeFilter and trashed = false"
            val queryUrl = "$DRIVE_API_BASE/files?q=${urlEncode(q)}&fields=${urlEncode("files(id,name,mimeType)")}&pageSize=10&supportsAllDrives=true&includeItemsFromAllDrives=true"
            val fileArr = driveApiGetJson(context, token, queryUrl)?.optJSONArray("files")
            val matches = mutableListOf<String>()
            if (fileArr != null) {
                for (j in 0 until fileArr.length()) {
                    val id = fileArr.optJSONObject(j)?.optString("id").orEmpty()
                    if (id.isNotBlank()) matches.add(id)
                }
            }
            Log.i("CloudDebug", "[DrivePath] pId=$pId seg='$seg' (depth=$segmentIndex/${segments.size}) matches=${matches.size}")

            if (matches.isEmpty()) {
                try {
                    val qActual = "'$pId' in parents and trashed = false"
                    val actualUrl = "$DRIVE_API_BASE/files?q=${urlEncode(qActual)}&fields=${urlEncode("files(name)")}&pageSize=5&supportsAllDrives=true&includeItemsFromAllDrives=true"
                    val actualArr = driveApiGetJson(context, token, actualUrl)?.optJSONArray("files")
                    val actualNames = mutableListOf<String>()
                    if (actualArr != null) {
                        for (k in 0 until actualArr.length()) {
                            actualArr.optJSONObject(k)?.optString("name")?.let { if (it.isNotEmpty()) actualNames.add(it) }
                        }
                    }
                    val diag = "断在[$seg] rootId=$rootId 实际子项=$actualNames"
                    lastPathDiagnostics[cleanPath] = diag
                    Log.w("CloudDebug", "[DrivePath] $diag")
                } catch (_: Exception) {}
                continue
            }

            if (isLast) {
                val chosenId = matches[0]
                pathToFileIdCache[cleanPath] = chosenId
                return chosenId
            }

            for (candidateId in matches) {
                val found = searchSegmentCandidates(
                    context, token, listOf(candidateId), segmentIndex + 1, segments, bypassCache, cleanPath, rootId
                )
                if (found != null) {
                    pathToFileIdCache[subKey] = candidateId
                    return found
                }
            }
        }
        return null
    }

    fun fallbackFindDriveFileId(
        context: Context,
        relPath: String,
        token: String,
        rootId: String?,
        expectedSizeBytes: Long? = null,
    ): String? {
        val clean = relPath.trim().replace('\\', '/').trimStart('/')
        val fileName = clean.substringAfterLast('/')
        if (fileName.isBlank()) return null

        Log.i("CloudDebug", "[OrigFallback] Searching by fileName='$fileName' expectedSize=$expectedSizeBytes")
        val q = "name = '${escapeQ(fileName)}' and trashed = false"
        val url = "$DRIVE_API_BASE/files?q=${urlEncode(q)}&fields=${urlEncode("files(id,name,size,parents,modifiedTime)")}&pageSize=10&supportsAllDrives=true&includeItemsFromAllDrives=true"
        val json = driveApiGetJson(context, token, url)
        val files = json?.optJSONArray("files")
        if (files == null || files.length() == 0) {
            Log.w("CloudDebug", "[OrigFallback] No files found matching name='$fileName'")
            return null
        }

        if (files.length() == 1) {
            val fObj = files.getJSONObject(0)
            val fileId = fObj.optString("id")
            if (fileId.isNotBlank()) {
                pathToFileIdCache[clean] = fileId
                Log.i("CloudDebug", "[OrigFallback] Exactly 1 candidate found: id=$fileId name='$fileName'")
                return fileId
            }
        }

        data class Candidate(val id: String, val size: Long, val parents: List<String>, val modifiedTime: String, var score: Int = 0)
        val candidates = mutableListOf<Candidate>()
        for (i in 0 until files.length()) {
            val obj = files.optJSONObject(i) ?: continue
            val id = obj.optString("id")
            if (id.isBlank()) continue
            val size = obj.optLong("size", -1L)
            val pList = mutableListOf<String>()
            val pArr = obj.optJSONArray("parents")
            if (pArr != null) {
                for (p in 0 until pArr.length()) {
                    val pStr = pArr.optString(p)
                    if (pStr.isNotEmpty()) pList.add(pStr)
                }
            }
            val modTime = obj.optString("modifiedTime")
            candidates.add(Candidate(id, size, pList, modTime))
        }

        if (candidates.isEmpty()) return null

        for (cand in candidates) {
            if (expectedSizeBytes != null && expectedSizeBytes > 0L && cand.size == expectedSizeBytes) {
                cand.score += 100
            }
            if (rootId != null && cand.parents.contains(rootId)) {
                cand.score += 50
            }
        }

        candidates.sortWith(compareByDescending<Candidate> { it.score }.thenByDescending { it.modifiedTime })

        val chosen = candidates[0]
        Log.i("CloudDebug", "[OrigFallback] Picked candidate id=${chosen.id} score=${chosen.score} size=${chosen.size} mod=${chosen.modifiedTime} (total=${candidates.size})")
        pathToFileIdCache[clean] = chosen.id
        return chosen.id
    }

    private fun resolvePackFileId(
        context: Context,
        packId: String,
        token: String,
        bypassCache: Boolean = false,
    ): String? {
        if (!bypassCache) packFileIdCache[packId]?.let { return it }
        val packsDirId = resolveDriveFileIdByPath(context, "$CLOUD_DIR/$PACKS_DIR", token) ?: return null
        val q = "'$packsDirId' in parents and trashed = false"
        val url = "$DRIVE_API_BASE/files?q=${urlEncode(q)}&fields=${urlEncode("files(id,name)")}&pageSize=200&supportsAllDrives=true&includeItemsFromAllDrives=true"
        val arr = driveApiGetJson(context, token, url)?.optJSONArray("files") ?: return null
        for (i in 0 until arr.length()) {
            val obj = arr.optJSONObject(i) ?: continue
            val name = obj.optString("name")
            val id = obj.optString("id")
            if (name.isNotBlank() && id.isNotBlank()) {
                packFileIdCache[name] = id
                pathToFileIdCache["$CLOUD_DIR/$PACKS_DIR/$name"] = id
            }
        }
        return packFileIdCache[packId]
    }

    /**
     * 在 .cloudgallery/snapshots/ 下查找最新已上传完成的不可变快照（规避 manifest 先传完导致的瞬时 404）
     */
    private fun findLatestDriveSnapshot(context: Context, token: String): Pair<String, Long>? {
        val snapDirId = resolveDriveFileIdByPath(context, "$CLOUD_DIR/$SNAPSHOTS_DIR", token) ?: return null
        val q = "'$snapDirId' in parents and name contains 'snapshot_v' and trashed = false"
        val arr = driveApiGetJson(
            context,
            token,
            "$DRIVE_API_BASE/files?q=${urlEncode(q)}&orderBy=${urlEncode("modifiedTime desc")}&fields=${urlEncode("files(id,name)")}&pageSize=10"
        )?.optJSONArray("files") ?: return null

        var bestId: String? = null
        var bestVer = -1L
        for (i in 0 until arr.length()) {
            val obj = arr.optJSONObject(i) ?: continue
            val name = obj.optString("name")
            val id = obj.optString("id")
            val ver = name.removePrefix("snapshot_v").removeSuffix(".db").toLongOrNull() ?: continue
            if (ver > bestVer && id.isNotBlank()) {
                bestVer = ver
                bestId = id
            }
        }
        return if (bestId != null && bestVer > 0L) bestId to bestVer else null
    }

    private fun ensureDriveFolderPath(context: Context, folderRelPath: String, token: String): String? {
        val clean = folderRelPath.trim().replace('\\', '/').trim('/')
        var parentId = resolveDriveRootFolderId(context, token) ?: return null
        if (clean.isEmpty()) return parentId

        val segments = clean.split('/').filter { it.isNotEmpty() }
        for (i in segments.indices) {
            val seg = segments[i]
            val subKey = segments.subList(0, i + 1).joinToString("/")
            val existing = pathToFileIdCache[subKey] ?: run {
                val q = "'$parentId' in parents and name = '${escapeQ(seg)}' and mimeType = 'application/vnd.google-apps.folder' and trashed = false"
                driveApiGetJson(context, token, "$DRIVE_API_BASE/files?q=${urlEncode(q)}&fields=${urlEncode("files(id)")}&pageSize=1")
                    ?.optJSONArray("files")?.optJSONObject(0)?.optString("id")?.takeIf { it.isNotBlank() }
            }
            parentId = if (existing != null) {
                pathToFileIdCache[subKey] = existing
                existing
            } else {
                val created = createDriveFolder(context, parentId, seg, token) ?: return null
                pathToFileIdCache[subKey] = created
                created
            }
        }
        return parentId
    }

    private fun createDriveFolder(context: Context, parentId: String, name: String, token: String): String? {
        val body = JSONObject().apply {
            put("name", name)
            put("mimeType", "application/vnd.google-apps.folder")
            put("parents", JSONArray().put(parentId))
        }.toString().toByteArray(StandardCharsets.UTF_8)

        val conn = openDriveConnection(context, "$DRIVE_API_BASE/files?fields=id", token, "POST", "application/json; charset=UTF-8") ?: return null
        return try {
            conn.doOutput = true
            conn.outputStream.use { it.write(body) }
            if (conn.responseCode in 200..299) {
                JSONObject(conn.inputStream.bufferedReader().use { it.readText() }).optString("id").takeIf { it.isNotBlank() }
            } else null
        } catch (_: Exception) {
            null
        } finally {
            conn.disconnect()
        }
    }

    // =========================================================================
    // 6. 主库快照同步（LAN 优先 -> Drive API 自动降级 + 防竞态回退）
    // =========================================================================

    suspend fun syncMasterSnapshot(
        context: Context,
        localSnapshotVersion: Long = 0L,
    ): SnapshotSyncResult? = withContext(Dispatchers.IO) {
        val destFile = File(context.filesDir, "cloudgallery/$SNAPSHOT_DB_NAME")
        destFile.parentFile?.mkdirs()

        // 通道 1：局域网 PC Daemon
        if (isLanAvailable(context, forceProbe = true)) {
            val baseUrl = getLanBaseUrl(context)
            val remoteVer = lanGetJson(context, "$baseUrl/api/version")?.optLong("snapshot_version", 0L) ?: 0L
            if (remoteVer > 0L && remoteVer == localSnapshotVersion && destFile.isFile && destFile.length() > 0L) {
                return@withContext SnapshotSyncResult(destFile, remoteVer, "lan")
            }
            if (downloadLanToFile(context, "$baseUrl/api/database", destFile, null)) {
                return@withContext SnapshotSyncResult(destFile, remoteVer.coerceAtLeast(1L), "lan")
            }
        }

        // 通道 2：Google Drive REST API v3
        val token = getValidAccessToken(context) ?: return@withContext null
        var manifestVer = 0L
        resolveDriveFileIdByPath(context, "$CLOUD_DIR/$MANIFEST_NAME", token, bypassCache = true)?.let { mId ->
            driveDownloadText(context, mId, token)?.let { txt ->
                runCatching { manifestVer = JSONObject(txt).optLong("snapshot_version", 0L) }
            }
        }
        if (manifestVer > 0L && manifestVer == localSnapshotVersion && destFile.isFile && destFile.length() > 0L) {
            return@withContext SnapshotSyncResult(destFile, manifestVer, "drive")
        }

        // 依次查找：manifest 对应版本 -> snapshots 目录下最新可用版本 -> 兼容旧 master_index_snapshot.db
        var chosenId: String? = null
        var chosenVer = manifestVer
        if (manifestVer > 0L) {
            chosenId = resolveDriveFileIdByPath(context, "$CLOUD_DIR/$SNAPSHOTS_DIR/snapshot_v$manifestVer.db", token, bypassCache = true)
        }
        if (chosenId == null) {
            findLatestDriveSnapshot(context, token)?.let { (latestId, latestVer) ->
                chosenId = latestId
                chosenVer = latestVer
            }
        }
        if (chosenId == null) {
            chosenId = resolveDriveFileIdByPath(context, "$CLOUD_DIR/$SNAPSHOT_DB_NAME", token, bypassCache = true)
        }
        val finalId = chosenId ?: return@withContext null

        if (chosenVer > 0L && chosenVer == localSnapshotVersion && destFile.isFile && destFile.length() > 0L) {
            return@withContext SnapshotSyncResult(destFile, chosenVer, "drive")
        }

        val outcome = downloadDriveToFile(context, finalId, destFile, token, null)
        if (outcome.success) SnapshotSyncResult(destFile, chosenVer.coerceAtLeast(1L), "drive") else null
    }

    // =========================================================================
    // 7. 不可变分卷 Range 秒拉缩略图（双通道通用，含 404 缓存自愈）
    // =========================================================================

    fun getThumbRootDir(context: Context): File =
        File(context.noBackupFilesDir, "cloudgallery_thumbs").apply { if (!exists()) mkdirs() }

    fun getLocalThumbIfPresent(context: Context, key: String): File? {
        val clean = key.lowercase().trim().removeSuffix(".webp")
        if (clean.length < 8) return null
        val file = File(File(getThumbRootDir(context), clean.substring(0, 2)), "$clean.webp")
        return if (file.isFile && file.length() > 0L) file else null
    }

    fun thumbTargetFile(context: Context, key: String): File {
        val clean = key.lowercase().trim().removeSuffix(".webp")
        val dir = File(getThumbRootDir(context), clean.take(2).padEnd(2, '0')).apply { if (!exists()) mkdirs() }
        return File(dir, "$clean.webp")
    }

    suspend fun fetchThumbnail(context: Context, uri: Uri): File? = withContext(Dispatchers.IO) {
        val itemId = (uri.host ?: uri.authority)?.trim()?.lowercase().orEmpty()
        if (itemId.isEmpty()) return@withContext null

        val thumbParam = uri.getQueryParameter("thumb")?.trim()?.lowercase().orEmpty()
        val thumbName = when {
            thumbParam.endsWith(".webp") -> thumbParam
            thumbParam.isNotEmpty() -> "$thumbParam.webp"
            else -> "$itemId.webp"
        }
        val cacheKey = thumbName.removeSuffix(".webp")
        getLocalThumbIfPresent(context, cacheKey)?.let { return@withContext it }

        val future = CompletableFuture<File?>()
        val existing = inFlightThumbs.putIfAbsent(cacheKey, future)
        if (existing != null) return@withContext runCatching { existing.get(30, TimeUnit.SECONDS) }.getOrNull()

        var result: File? = null
        try {
            val target = thumbTargetFile(context, cacheKey)
            val packId = uri.getQueryParameter("pack")?.trim()?.takeIf { it.isNotEmpty() }
            val off = uri.getQueryParameter("off")?.toLongOrNull()?.takeIf { it >= 0L }
            val len = uri.getQueryParameter("len")?.toLongOrNull()?.takeIf { it > 0L }
            val rangeHeader = if (packId != null && off != null && len != null) {
                "bytes=$off-${off + len - 1L}"
            } else null

            var ok = false
            if (isLanAvailable(context)) {
                val baseUrl = getLanBaseUrl(context)
                ok = if (packId != null && rangeHeader != null) {
                    downloadLanToFile(context, "$baseUrl/api/packs/${urlEncode(packId)}", target, rangeHeader)
                } else {
                    downloadLanToFile(context, "$baseUrl/api/thumbs/${urlEncode(thumbName)}", target, null)
                }
            }

            if (!ok) {
                val token = getValidAccessToken(context)
                if (token != null) {
                    if (packId != null && rangeHeader != null) {
                        ok = downloadDrivePackWithRetry(context, packId, rangeHeader, target, token)
                    }
                    if (!ok) {
                        ok = downloadDrivePathWithRetry(context, "$CLOUD_DIR/$THUMBS_DIR/$thumbName", target, token, null)
                    }
                }
            }

            if (ok && target.isFile && target.length() > 0L) result = target
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            Log.w(TAG, "fetchThumbnail failed ($cacheKey): ${e.message}")
        } finally {
            future.complete(result)
            inFlightThumbs.remove(cacheKey, future)
        }
        result
    }

    private fun downloadDrivePackWithRetry(
        context: Context,
        packId: String,
        rangeHeader: String,
        target: File,
        token: String,
    ): Boolean {
        val fileId = resolvePackFileId(context, packId, token, bypassCache = false) ?: return false
        val first = downloadDriveToFile(context, fileId, target, token, rangeHeader)
        if (first.success) return true
        if (first.httpCode == 404) {
            packFileIdCache.remove(packId)
            val refreshedId = resolvePackFileId(context, packId, token, bypassCache = true) ?: return false
            return downloadDriveToFile(context, refreshedId, target, token, rangeHeader).success
        }
        return false
    }

    private fun downloadDrivePathWithRetry(
        context: Context,
        relPath: String,
        target: File,
        token: String,
        rangeHeader: String?,
        itemId: String = "",
        expectedSizeBytes: Long? = null,
    ): Boolean {
        val rootId = resolveDriveRootFolderId(context, token)
        if (rootId == null) {
            val rootName = prefs(context).getString(KEY_DRIVE_ROOT_NAME, "图片")?.trim().orEmpty().ifEmpty { "图片" }
            if (itemId.isNotEmpty()) setLastOriginalError(itemId, "Drive未找到图库根目录 (rootName='$rootName')")
            return false
        }

        val clean = relPath.trim().replace('\\', '/').trimStart('/')
        var fileId = resolveDriveFileIdByPath(context, relPath, token, bypassCache = false)
        if (fileId == null) {
            Log.i("CloudDebug", "[Orig] Path resolution returned null for $relPath, trying fallback...")
            fileId = fallbackFindDriveFileId(context, relPath, token, rootId, expectedSizeBytes)
        }
        if (fileId == null) {
            val diag = lastPathDiagnostics[clean] ?: "断在未知段 rootId=$rootId"
            if (itemId.isNotEmpty()) setLastOriginalError(itemId, diag)
            return false
        }

        val first = downloadDriveToFile(context, fileId, target, token, rangeHeader)
        if (first.success) return true
        if (first.httpCode == 404) {
            pathToFileIdCache.remove(clean)
            var refreshedId = resolveDriveFileIdByPath(context, relPath, token, bypassCache = true)
            if (refreshedId == null) {
                refreshedId = fallbackFindDriveFileId(context, relPath, token, rootId, expectedSizeBytes)
            }
            if (refreshedId == null) {
                val diag = lastPathDiagnostics[clean] ?: "重试未找到 rootId=$rootId"
                if (itemId.isNotEmpty()) setLastOriginalError(itemId, diag)
                return false
            }
            val second = downloadDriveToFile(context, refreshedId, target, token, rangeHeader)
            if (!second.success && itemId.isNotEmpty()) {
                setLastOriginalError(itemId, "Drive下载HTTP ${second.httpCode} (fileId=$refreshedId)")
            }
            return second.success
        } else {
            if (itemId.isNotEmpty()) setLastOriginalError(itemId, "Drive下载HTTP ${first.httpCode} (fileId=$fileId)")
        }
        return false
    }

    // =========================================================================
    // 8. 原图 Single-Flight 下载 + 1.5GB LRU 缓存
    // =========================================================================

    fun getOriginalRootDir(context: Context): File =
        File(context.cacheDir, "cloud_originals").apply { if (!exists()) mkdirs() }

    fun getLocalOriginalIfPresent(context: Context, itemId: String): File? {
        val clean = itemId.lowercase().trim()
        if (clean.length < 8) return null
        val file = File(getOriginalRootDir(context), clean)
        if (file.isFile && file.length() > 0L) {
            file.setLastModified(System.currentTimeMillis())
            return file
        }
        return null
    }

    suspend fun fetchOriginal(
        context: Context,
        uri: Uri,
        expectedSize: Long? = null,
    ): File? = withContext(Dispatchers.IO) {
        val itemId = (uri.host ?: uri.authority)?.trim()?.lowercase().orEmpty()
        var relPath = uri.getQueryParameter("rel")?.trim().orEmpty()
        val sizeParam = uri.getQueryParameter("size")?.toLongOrNull() ?: uri.getQueryParameter("len")?.toLongOrNull()
        val effectiveExpectedSize = expectedSize ?: sizeParam
        if (itemId.isEmpty() || relPath.isEmpty()) {
            setLastOriginalError(itemId, "无效云端URI参数 (itemId=$itemId, rel=$relPath)")
            return@withContext null
        }

        while (relPath.contains("%2F", ignoreCase = true) || relPath.contains("%2f") || relPath.contains("%20")) {
            val decoded = try {
                java.net.URLDecoder.decode(relPath, "UTF-8")
            } catch (_: Exception) {
                break
            }
            if (decoded == relPath) break
            relPath = decoded
        }

        getLocalOriginalIfPresent(context, itemId)?.let {
            lastOriginalErrors.remove(itemId)
            Log.i("CloudDebug", "[Orig] cache hit itemId=$itemId size=${it.length()} rel=$relPath")
            return@withContext it
        }

        val lanAvailable = isLanAvailable(context)
        Log.i("CloudDebug", "[Orig] start rel=$relPath lan=$lanAvailable itemId=$itemId")

        val future = CompletableFuture<File?>()
        val existing = inFlightOriginals.putIfAbsent(itemId, future)
        if (existing != null) return@withContext runCatching { existing.get(90, TimeUnit.SECONDS) }.getOrNull()

        var result: File? = null
        try {
            val target = File(getOriginalRootDir(context), itemId)
            var ok = false
            var lanFailedReason: String? = null
            if (lanAvailable) {
                val lanUrl = "${getLanBaseUrl(context)}/api/original?path=${urlEncode(relPath)}"
                ok = downloadLanToFile(context, lanUrl, target, null)
                Log.i("CloudDebug", "[Orig] LAN download finished ok=$ok targetLen=${if (target.exists()) target.length() else -1}")
                if (!ok) {
                    lanFailedReason = "LAN下载失败 ($lanUrl)"
                }
            } else {
                lanFailedReason = "LAN不可达(${getLanBaseUrl(context)})"
            }

            if (!ok) {
                val token = getValidAccessToken(context)
                if (token == null) {
                    val msg = "$lanFailedReason, 且Google Drive未授权登录"
                    setLastOriginalError(itemId, msg)
                } else {
                    ok = downloadDrivePathWithRetry(context, relPath, target, token, null, itemId, effectiveExpectedSize)
                    Log.i("CloudDebug", "[Orig] Drive download finished ok=$ok targetLen=${if (target.exists()) target.length() else -1}")
                }
            }

            if (ok && target.isFile && target.length() > 0L) {
                result = target
                lastOriginalErrors.remove(itemId)
                Log.i("CloudDebug", "[Orig] success rel=$relPath bytes=${target.length()}")
                notifyOriginalReady(itemId)
                evictOriginalCacheIfNeeded(context)
            } else {
                if (!lastOriginalErrors.containsKey(itemId)) {
                    setLastOriginalError(itemId, "原图未成功拉取 (ok=$ok, targetExists=${target.exists()})")
                }
                Log.w("CloudDebug", "[Orig] failed to retrieve rel=$relPath targetExists=${target.exists()}")
            }
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            val msg = "下载原图IO异常: ${e.javaClass.simpleName}: ${e.message}"
            setLastOriginalError(itemId, msg)
            Log.e("CloudDebug", "[Orig] exception ($relPath): ${e.message}", e)
        } finally {
            future.complete(result)
            inFlightOriginals.remove(itemId, future)
        }
        result
    }

    private fun evictOriginalCacheIfNeeded(context: Context) {
        val files = getOriginalRootDir(context).listFiles()?.filter { it.isFile } ?: return
        var total = files.sumOf { it.length() }
        if (total <= MAX_ORIGINAL_CACHE_BYTES) return
        val limit = (MAX_ORIGINAL_CACHE_BYTES * 0.8).toLong()
        for (f in files.sortedBy { it.lastModified() }) {
            if (total <= limit) break
            val len = f.length()
            if (f.delete()) total -= len
        }
    }

    // =========================================================================
    // 9. 本地 HTTP 206 流式回环代理（mpv 播放在家/出门视频）
    // =========================================================================

    fun getVideoStreamProxyUrl(context: Context, cloudUri: Uri): String? {
        val relPath = cloudUri.getQueryParameter("rel")?.trim().orEmpty()
        if (relPath.isEmpty()) return null
        val port = proxyServer.ensureStarted(context.applicationContext)
        return "http://127.0.0.1:$port/stream?rel=${urlEncode(relPath)}"
    }

    private class LocalMediaProxyServer {
        private val started = AtomicBoolean(false)
        @Volatile private var listenPort: Int = 0
        @Volatile private var appContext: Context? = null
        private val pool: ExecutorService = Executors.newCachedThreadPool()

        fun ensureStarted(context: Context): Int {
            appContext = context.applicationContext
            if (started.compareAndSet(false, true)) {
                val server = ServerSocket(0, 32, InetAddress.getByName("127.0.0.1"))
                listenPort = server.localPort
                Log.i(TAG, "LocalMediaProxyServer listening on 127.0.0.1:$listenPort")
                pool.execute {
                    while (!server.isClosed) {
                        try {
                            val socket = server.accept()
                            pool.execute { handleClient(socket) }
                        } catch (_: Exception) {}
                    }
                }
            }
            return listenPort
        }

        private fun handleClient(socket: Socket) {
            val ctx = appContext ?: return
            socket.use { client ->
                client.soTimeout = 30_000
                val reader = BufferedReader(InputStreamReader(client.getInputStream(), StandardCharsets.ISO_8859_1))
                val parts = (reader.readLine() ?: return).split(" ")
                if (parts.size < 2) return
                val method = parts[0].uppercase()
                var rangeHeader: String? = null
                while (true) {
                    val line = reader.readLine() ?: break
                    if (line.isEmpty()) break
                    val idx = line.indexOf(':')
                    if (idx > 0 && line.substring(0, idx).equals("Range", ignoreCase = true)) {
                        rangeHeader = line.substring(idx + 1).trim()
                    }
                }
                val relPath = Uri.parse("http://127.0.0.1${parts[1]}").getQueryParameter("rel")
                if (relPath.isNullOrBlank()) {
                    writeErr(client, 400, "Missing rel")
                    return
                }
                val upstream = openUpstream(ctx, relPath, method, rangeHeader) ?: run {
                    writeErr(client, 502, "Upstream unreachable")
                    return
                }
                try {
                    val code = upstream.responseCode
                    val msg = upstream.responseMessage ?: if (code == 206) "Partial Content" else "OK"
                    val out = BufferedOutputStream(client.getOutputStream(), 64 * 1024)
                    val sb = StringBuilder()
                        .append("HTTP/1.1 ").append(code).append(' ').append(msg).append("\r\n")
                        .append("Content-Type: ").append(upstream.contentType ?: "application/octet-stream").append("\r\n")
                        .append("Accept-Ranges: ").append(upstream.getHeaderField("Accept-Ranges") ?: "bytes").append("\r\n")
                        .append("Connection: close\r\n")
                    if (upstream.contentLengthLong >= 0L) sb.append("Content-Length: ").append(upstream.contentLengthLong).append("\r\n")
                    upstream.getHeaderField("Content-Range")?.let { sb.append("Content-Range: ").append(it).append("\r\n") }
                    sb.append("\r\n")
                    out.write(sb.toString().toByteArray(StandardCharsets.ISO_8859_1))
                    out.flush()
                    if (method != "HEAD" && code in 200..299) {
                        val buf = ByteArray(64 * 1024)
                        BufferedInputStream(upstream.inputStream, 64 * 1024).use { input ->
                            while (true) {
                                val n = input.read(buf)
                                if (n <= 0) break
                                out.write(buf, 0, n)
                            }
                            out.flush()
                        }
                    }
                } catch (_: SocketException) {
                } catch (e: Exception) {
                    Log.w(TAG, "Proxy stream err: ${e.message}")
                } finally {
                    upstream.disconnect()
                }
            }
        }

        private fun openUpstream(context: Context, relPath: String, method: String, rangeHeader: String?): HttpURLConnection? {
            if (isLanAvailable(context)) {
                try {
                    val lanToken = getLanApiToken(context)
                    val conn = (URL("${getLanBaseUrl(context)}/api/original?path=${urlEncode(relPath)}").openConnection() as HttpURLConnection).apply {
                        requestMethod = if (method == "HEAD") "HEAD" else "GET"
                        connectTimeout = 4000
                        readTimeout = 30_000
                        if (lanToken.isNotEmpty()) setRequestProperty("x-cloudgallery-token", lanToken)
                        if (!rangeHeader.isNullOrBlank()) setRequestProperty("Range", rangeHeader)
                    }
                    if (conn.responseCode in 200..299) return conn
                    conn.disconnect()
                } catch (_: Exception) {
                    invalidateLanProbe()
                }
            }

            var token = getValidAccessTokenBlocking(context) ?: return null
            val rootId = resolveDriveRootFolderId(context, token)
            var fileId = resolveDriveFileIdByPath(context, relPath, token, bypassCache = false)
            if (fileId == null) {
                fileId = fallbackFindDriveFileId(context, relPath, token, rootId) ?: return null
            }
            for (attempt in 0..1) {
                val conn = (URL("$DRIVE_API_BASE/files/$fileId?alt=media").openConnection() as HttpURLConnection).apply {
                    requestMethod = if (method == "HEAD") "HEAD" else "GET"
                    connectTimeout = 8000
                    readTimeout = 30_000
                    setRequestProperty("Authorization", "Bearer $token")
                    if (!rangeHeader.isNullOrBlank()) setRequestProperty("Range", rangeHeader)
                }
                val code = conn.responseCode
                if (code == 401 && attempt == 0) {
                    conn.disconnect()
                    token = getValidAccessTokenBlocking(context, forceRefresh = true) ?: return null
                    continue
                }
                if (code == 404 && attempt == 0) {
                    conn.disconnect()
                    pathToFileIdCache.remove(relPath.trim().replace('\\', '/').trimStart('/'))
                    fileId = resolveDriveFileIdByPath(context, relPath, token, bypassCache = true)
                    if (fileId == null) {
                        fileId = fallbackFindDriveFileId(context, relPath, token, rootId) ?: return null
                    }
                    continue
                }
                return conn
            }
            return null
        }

        private fun writeErr(socket: Socket, code: Int, msg: String) {
            runCatching {
                val body = msg.toByteArray(StandardCharsets.UTF_8)
                val hdr = "HTTP/1.1 $code Error\r\nContent-Type: text/plain\r\nContent-Length: ${body.size}\r\nConnection: close\r\n\r\n"
                socket.getOutputStream().apply {
                    write(hdr.toByteArray(StandardCharsets.ISO_8859_1))
                    write(body)
                    flush()
                }
            }
        }
    }

    // =========================================================================
    // 10. Outbox 投递与流式上传（4K 视频恒定 64KB 内存）
    // =========================================================================

    suspend fun submitOutboxOps(context: Context, opsJsonArray: String): Boolean = withContext(Dispatchers.IO) {
        val bytes = opsJsonArray.toByteArray(StandardCharsets.UTF_8)
        if (isLanAvailable(context)) {
            val lanToken = getLanApiToken(context)
            val ok = try {
                val conn = (URL("${getLanBaseUrl(context)}/api/outbox").openConnection() as HttpURLConnection).apply {
                    requestMethod = "POST"
                    doOutput = true
                    connectTimeout = 4000
                    readTimeout = 10_000
                    setFixedLengthStreamingMode(bytes.size.toLong())
                    setRequestProperty("Content-Type", "application/json; charset=UTF-8")
                    if (lanToken.isNotEmpty()) setRequestProperty("x-cloudgallery-token", lanToken)
                }
                try {
                    conn.outputStream.use { it.write(bytes) }
                    conn.responseCode in 200..299
                } finally {
                    conn.disconnect()
                }
            } catch (_: Exception) {
                false
            }
            if (ok) return@withContext true
        }
        val token = getValidAccessToken(context) ?: return@withContext false
        val pendingDirId = ensureDriveFolderPath(context, "$CLOUD_DIR/outbox/pending", token) ?: return@withContext false
        uploadSmallBytesToDrive(context, pendingDirId, "${System.currentTimeMillis()}_mobile.json", "application/json", bytes, token)
    }

    suspend fun uploadBackupFile(
        context: Context,
        localFile: File,
        remoteRelPath: String,
        mimeType: String = "application/octet-stream",
    ): Boolean = withContext(Dispatchers.IO) {
        if (!localFile.isFile || !localFile.canRead()) return@withContext false
        val effectiveMime = mimeType.ifBlank { "application/octet-stream" }

        if (isLanAvailable(context)) {
            val lanToken = getLanApiToken(context)
            val ok = try {
                val url = "${getLanBaseUrl(context)}/api/upload?path=${urlEncode(remoteRelPath)}&op_type=BACKUP_PHOTO"
                val conn = (URL(url).openConnection() as HttpURLConnection).apply {
                    requestMethod = "POST"
                    doOutput = true
                    connectTimeout = 5000
                    readTimeout = 120_000
                    setFixedLengthStreamingMode(localFile.length())
                    setRequestProperty("Content-Type", effectiveMime)
                    setRequestProperty("x-relative-path", urlEncode(remoteRelPath))
                    setRequestProperty("x-op-type", "BACKUP_PHOTO")
                    if (lanToken.isNotEmpty()) setRequestProperty("x-cloudgallery-token", lanToken)
                }
                try {
                    BufferedOutputStream(conn.outputStream, 64 * 1024).use { out ->
                        FileInputStream(localFile).use { it.copyTo(out, 64 * 1024) }
                        out.flush()
                    }
                    conn.responseCode in 200..299
                } finally {
                    conn.disconnect()
                }
            } catch (_: Exception) {
                invalidateLanProbe()
                false
            }
            if (ok) return@withContext true
        }

        val token = getValidAccessToken(context) ?: return@withContext false
        val norm = remoteRelPath.trim().replace('\\', '/').trimStart('/')
        val slash = norm.lastIndexOf('/')
        val folderPath = if (slash > 0) norm.substring(0, slash) else ""
        val fileName = if (slash >= 0) norm.substring(slash + 1) else norm
        if (fileName.isEmpty()) return@withContext false
        val parentId = ensureDriveFolderPath(context, folderPath, token) ?: return@withContext false
        uploadResumableToDrive(context, parentId, fileName, effectiveMime, localFile, token)
    }

    // =========================================================================
    // 11. 底层 HTTP 与加密辅助
    // =========================================================================

    private fun uploadSmallBytesToDrive(
        context: Context,
        parentId: String,
        fileName: String,
        mimeType: String,
        bytes: ByteArray,
        initialToken: String,
    ): Boolean {
        val boundary = "cg_${System.currentTimeMillis()}"
        val meta = JSONObject().put("name", fileName).put("parents", JSONArray().put(parentId)).toString()
        val prefix = "--$boundary\r\nContent-Type: application/json; charset=UTF-8\r\n\r\n$meta\r\n--$boundary\r\nContent-Type: $mimeType\r\n\r\n".toByteArray(StandardCharsets.UTF_8)
        val suffix = "\r\n--$boundary--\r\n".toByteArray(StandardCharsets.UTF_8)
        var token = initialToken
        for (attempt in 0..1) {
            val conn = (URL("$DRIVE_UPLOAD_BASE/files?uploadType=multipart&fields=id").openConnection() as HttpURLConnection).apply {
                requestMethod = "POST"
                doOutput = true
                connectTimeout = 8000
                readTimeout = 20_000
                setFixedLengthStreamingMode((prefix.size + bytes.size + suffix.size).toLong())
                setRequestProperty("Authorization", "Bearer $token")
                setRequestProperty("Content-Type", "multipart/related; boundary=$boundary")
            }
            try {
                conn.outputStream.use {
                    it.write(prefix)
                    it.write(bytes)
                    it.write(suffix)
                    it.flush()
                }
                val code = conn.responseCode
                if (code == 401 && attempt == 0) {
                    token = getValidAccessTokenBlocking(context, forceRefresh = true) ?: return false
                    continue
                }
                return code in 200..299
            } catch (_: Exception) {
                return false
            } finally {
                conn.disconnect()
            }
        }
        return false
    }

    private fun uploadResumableToDrive(
        context: Context,
        parentId: String,
        fileName: String,
        mimeType: String,
        localFile: File,
        initialToken: String,
    ): Boolean {
        var token = initialToken
        val metaBytes = JSONObject().put("name", fileName).put("parents", JSONArray().put(parentId)).toString().toByteArray(StandardCharsets.UTF_8)
        var uploadUrl: String? = null
        for (attempt in 0..1) {
            val initConn = (URL("$DRIVE_UPLOAD_BASE/files?uploadType=resumable&fields=id").openConnection() as HttpURLConnection).apply {
                requestMethod = "POST"
                doOutput = true
                connectTimeout = 8000
                readTimeout = 20_000
                setRequestProperty("Authorization", "Bearer $token")
                setRequestProperty("Content-Type", "application/json; charset=UTF-8")
                setRequestProperty("X-Upload-Content-Type", mimeType)
                setRequestProperty("X-Upload-Content-Length", localFile.length().toString())
            }
            try {
                initConn.outputStream.use { it.write(metaBytes) }
                val code = initConn.responseCode
                if (code == 401 && attempt == 0) {
                    token = getValidAccessTokenBlocking(context, forceRefresh = true) ?: return false
                    continue
                }
                if (code in 200..299) uploadUrl = initConn.getHeaderField("Location")
                break
            } catch (_: Exception) {
                return false
            } finally {
                initConn.disconnect()
            }
        }
        val sessionUrl = uploadUrl ?: return false
        val putConn = (URL(sessionUrl).openConnection() as HttpURLConnection).apply {
            requestMethod = "PUT"
            doOutput = true
            connectTimeout = 10_000
            readTimeout = 180_000
            setFixedLengthStreamingMode(localFile.length())
            setRequestProperty("Content-Type", mimeType)
        }
        return try {
            BufferedOutputStream(putConn.outputStream, 64 * 1024).use { out ->
                FileInputStream(localFile).use { it.copyTo(out, 64 * 1024) }
                out.flush()
            }
            putConn.responseCode in 200..299
        } catch (_: Exception) {
            false
        } finally {
            putConn.disconnect()
        }
    }

    private fun lanGetJson(context: Context, url: String): JSONObject? {
        val lanToken = getLanApiToken(context)
        val conn = try {
            (URL(url).openConnection() as HttpURLConnection).apply {
                requestMethod = "GET"
                connectTimeout = 3000
                readTimeout = 8000
                if (lanToken.isNotEmpty()) setRequestProperty("x-cloudgallery-token", lanToken)
            }
        } catch (_: Exception) {
            return null
        }
        return try {
            if (conn.responseCode !in 200..299) null
            else JSONObject(conn.inputStream.bufferedReader(StandardCharsets.UTF_8).use { it.readText() })
        } catch (_: Exception) {
            null
        } finally {
            conn.disconnect()
        }
    }

    private fun downloadLanToFile(context: Context, url: String, target: File, rangeHeader: String?): Boolean {
        val lanToken = getLanApiToken(context)
        val conn = try {
            (URL(url).openConnection() as HttpURLConnection).apply {
                requestMethod = "GET"
                connectTimeout = 4000
                readTimeout = 30_000
                if (lanToken.isNotEmpty()) setRequestProperty("x-cloudgallery-token", lanToken)
                if (!rangeHeader.isNullOrBlank()) setRequestProperty("Range", rangeHeader)
            }
        } catch (_: Exception) {
            invalidateLanProbe()
            return false
        }
        return try {
            val code = conn.responseCode
            if (code !in 200..299) {
                Log.w("CloudDebug", "[Orig] LAN HTTP $code for $url")
                false
            } else {
                conn.inputStream.use { writeStreamAtomically(it, target) }
            }
        } catch (e: Exception) {
            Log.w("CloudDebug", "[Orig] LAN download exception: ${e.message}")
            invalidateLanProbe()
            false
        } finally {
            conn.disconnect()
        }
    }

    private fun downloadDriveToFile(
        context: Context,
        fileId: String,
        target: File,
        initialToken: String,
        rangeHeader: String?,
    ): DriveDownloadOutcome {
        var token = initialToken
        val url = "$DRIVE_API_BASE/files/$fileId?alt=media&supportsAllDrives=true"
        for (attempt in 0..1) {
            val conn = try {
                (URL(url).openConnection() as HttpURLConnection).apply {
                    requestMethod = "GET"
                    connectTimeout = 8000
                    readTimeout = 45_000
                    setRequestProperty("Authorization", "Bearer $token")
                    if (!rangeHeader.isNullOrBlank()) setRequestProperty("Range", rangeHeader)
                }
            } catch (e: Exception) {
                Log.w("CloudDebug", "[DriveDownload] connection failed for fileId=$fileId: ${e.message}")
                return DriveDownloadOutcome(false, -1)
            }
            try {
                val code = conn.responseCode
                Log.i("CloudDebug", "[DriveDownload] fileId=$fileId attempt=$attempt httpCode=$code")
                if (code == 401 && attempt == 0) {
                    token = getValidAccessTokenBlocking(context, forceRefresh = true)
                        ?: return DriveDownloadOutcome(false, 401)
                    continue
                }
                if (code !in 200..299) return DriveDownloadOutcome(false, code)
                val ok = conn.inputStream.use { writeStreamAtomically(it, target) }
                Log.i("CloudDebug", "[DriveDownload] fileId=$fileId writeOk=$ok targetLen=${if (target.exists()) target.length() else -1}")
                return DriveDownloadOutcome(ok, code)
            } catch (e: Exception) {
                Log.w("CloudDebug", "[DriveDownload] read stream exception for fileId=$fileId: ${e.message}")
                return DriveDownloadOutcome(false, -1)
            } finally {
                conn.disconnect()
            }
        }
        return DriveDownloadOutcome(false, -1)
    }

    private fun driveDownloadText(context: Context, fileId: String, initialToken: String): String? {
        var token = initialToken
        for (attempt in 0..1) {
            val conn = (URL("$DRIVE_API_BASE/files/$fileId?alt=media").openConnection() as HttpURLConnection).apply {
                requestMethod = "GET"
                connectTimeout = 8000
                readTimeout = 15_000
                setRequestProperty("Authorization", "Bearer $token")
            }
            try {
                val code = conn.responseCode
                if (code == 401 && attempt == 0) {
                    token = getValidAccessTokenBlocking(context, forceRefresh = true) ?: return null
                    continue
                }
                if (code !in 200..299) return null
                return conn.inputStream.bufferedReader(StandardCharsets.UTF_8).use { it.readText() }
            } catch (_: Exception) {
                return null
            } finally {
                conn.disconnect()
            }
        }
        return null
    }

    private fun driveApiGetJson(context: Context, token: String, url: String): JSONObject? {
        var current = token
        for (attempt in 0..1) {
            val conn = openDriveConnection(context, url, current, "GET", null) ?: return null
            try {
                val code = conn.responseCode
                if (code == 401 && attempt == 0) {
                    current = getValidAccessTokenBlocking(context, forceRefresh = true) ?: return null
                    continue
                }
                if (code !in 200..299) {
                    val errBody = try { conn.errorStream?.bufferedReader(StandardCharsets.UTF_8)?.use { it.readText() } } catch (_: Exception) { null }
                    Log.w("CloudDebug", "[DriveApi] HTTP $code for url=$url body=$errBody")
                    return null
                }
                return JSONObject(conn.inputStream.bufferedReader(StandardCharsets.UTF_8).use { it.readText() })
            } catch (e: Exception) {
                Log.w("CloudDebug", "[DriveApi] connection error for url=$url: ${e.message}")
                return null
            } finally {
                conn.disconnect()
            }
        }
        return null
    }

    private fun openDriveConnection(
        context: Context,
        url: String,
        token: String,
        method: String,
        contentType: String?,
    ): HttpURLConnection? = try {
        (URL(url).openConnection() as HttpURLConnection).apply {
            requestMethod = method
            connectTimeout = 8000
            readTimeout = 20_000
            setRequestProperty("Authorization", "Bearer $token")
            if (!contentType.isNullOrBlank()) setRequestProperty("Content-Type", contentType)
        }
    } catch (_: Exception) {
        null
    }

    private fun postFormJson(url: String, params: Map<String, String>): JSONObject? {
        val body = params.entries.joinToString("&") { (k, v) -> "${urlEncode(k)}=${urlEncode(v)}" }.toByteArray(StandardCharsets.UTF_8)
        val conn = (URL(url).openConnection() as HttpURLConnection).apply {
            requestMethod = "POST"
            doOutput = true
            connectTimeout = 8000
            readTimeout = 15_000
            setRequestProperty("Content-Type", "application/x-www-form-urlencoded")
            setFixedLengthStreamingMode(body.size.toLong())
        }
        return try {
            conn.outputStream.use { it.write(body) }
            val stream = if (conn.responseCode in 200..299) conn.inputStream else conn.errorStream
            val text = stream?.bufferedReader(StandardCharsets.UTF_8)?.use { it.readText() }.orEmpty()
            if (conn.responseCode in 200..299) JSONObject(text) else null
        } catch (_: Exception) {
            null
        } finally {
            conn.disconnect()
        }
    }

    private fun writeStreamAtomically(input: InputStream, target: File): Boolean {
        target.parentFile?.mkdirs()
        val tmp = File(target.parentFile, "${target.name}.${Thread.currentThread().id}.tmp")
        return try {
            FileOutputStream(tmp).use {
                input.copyTo(it, 32 * 1024)
                it.flush()
            }
            if (tmp.isFile && tmp.length() > 0L) {
                if (target.exists()) target.delete()
                if (!tmp.renameTo(target)) {
                    tmp.copyTo(target, overwrite = true)
                    tmp.delete()
                }
                true
            } else {
                tmp.delete()
                false
            }
        } catch (_: Exception) {
            tmp.delete()
            false
        }
    }

    private fun generatePkceVerifier(): String =
        Base64.encodeToString(
            ByteArray(48).also { SecureRandom().nextBytes(it) },
            Base64.URL_SAFE or Base64.NO_WRAP or Base64.NO_PADDING,
        )

    private fun generatePkceChallenge(verifier: String): String =
        Base64.encodeToString(
            MessageDigest.getInstance("SHA-256").digest(verifier.toByteArray(StandardCharsets.US_ASCII)),
            Base64.URL_SAFE or Base64.NO_WRAP or Base64.NO_PADDING,
        )

    private fun generateRandomHex(len: Int): String =
        ByteArray(len).also { SecureRandom().nextBytes(it) }.joinToString("") { "%02x".format(it) }

    private fun urlEncode(v: String): String = URLEncoder.encode(v, "UTF-8").replace("+", "%20")
}
