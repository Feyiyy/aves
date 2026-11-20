package deckers.thibault.aves.decoding

import android.content.Context
import android.graphics.Bitmap
import android.graphics.BitmapFactory
import android.graphics.BitmapRegionDecoder
import android.graphics.ColorSpace
import android.graphics.Rect
import android.net.Uri
import android.os.Build
import android.util.Log
import androidx.core.graphics.createBitmap
import com.bumptech.glide.Glide
import deckers.thibault.aves.channel.streams.darttoplatform.ByteSink
import deckers.thibault.aves.cloud.CloudDriveApiClient
import deckers.thibault.aves.glide.AvesAppGlideModule
import deckers.thibault.aves.glide.MultiPageImage
import deckers.thibault.aves.model.ContentAddress
import deckers.thibault.aves.storage.StorageUtils
import deckers.thibault.aves.utils.BitmapRegionDecoderCompat
import deckers.thibault.aves.utils.BitmapUtils
import deckers.thibault.aves.utils.BitmapUtils.describe
import deckers.thibault.aves.utils.LogUtils
import deckers.thibault.aves.utils.MathUtils
import deckers.thibault.aves.utils.MemoryUtils
import deckers.thibault.aves.utils.MimeTypes
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import java.io.ByteArrayInputStream
import java.nio.ByteBuffer
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock
import kotlin.math.max
import kotlin.math.min
import kotlin.math.roundToInt

// As of Android 14 (API 34), `BitmapRegionDecoder` documentation states
// that "only the JPEG, PNG, WebP and HEIF formats are supported"
// but in practice it successfully decodes some others.
class RegionFetcher internal constructor(
    private val context: Context,
) {
    suspend fun fetch(
        contentAddress: ContentAddress,
        decoded: Boolean,
        applyGainmap: Boolean,
        sampleSize: Int,
        regionRect: Rect,
        imageWidth: Int,
        imageHeight: Int,
        requestKey: Pair<Uri, Int?> = Pair(contentAddress.uri, contentAddress.pageId),
        result: ByteSink,
    ) {
        val mimeType = contentAddress.mimeType
        val uri = contentAddress.uri
        val pageId = contentAddress.pageId

        if (pageId != null && MultiPageImage.isSupported(mimeType)) {
            // use export for requested page
            val exportUri = exportUris.getOrPut(requestKey) { createTemporaryExport(contentAddress) }
            fetch(
                ContentAddress(
                    mimeType = EXPORT_MIME_TYPE,
                    uri = exportUri,
                    pageId = null,
                    path = null,
                ),
                decoded = decoded,
                applyGainmap = applyGainmap,
                sampleSize = sampleSize,
                regionRect = regionRect,
                imageWidth = imageWidth,
                imageHeight = imageHeight,
                requestKey = requestKey,
                result = result,
            )
            return
        }

        try {
            val decoder = getOrCreateDecoder(context, uri, requestKey)
            if (decoder == null) {
                Log.w("CloudDebug", "[Region] decoder is null for $contentAddress")
                result.error("fetch-read-null", "failed to open file for $contentAddress regionRect=$regionRect", null)
                return
            }

            val isCloud = uri.scheme == "cloudgallery"
            if (isCloud) {
                Log.i("CloudDebug", "[Region] fetch uri=$uri imageWidth=$imageWidth imageHeight=$imageHeight decoderW=${decoder.width} decoderH=${decoder.height} regionRect=$regionRect sampleSize=$sampleSize")
            }

            // with raw images, the known image size may not match the decoded image size
            // so we scale the requested region accordingly
            var effectiveRect = regionRect
            var effectiveSampleSize = sampleSize

            if (imageWidth != decoder.width || imageHeight != decoder.height) {
                val xf = decoder.width.toDouble() / imageWidth
                val yf = decoder.height.toDouble() / imageHeight
                effectiveRect = Rect(
                    (regionRect.left * xf).roundToInt(),
                    (regionRect.top * yf).roundToInt(),
                    (regionRect.right * xf).roundToInt(),
                    (regionRect.bottom * yf).roundToInt(),
                )
                val factor = MathUtils.highestPowerOf2((1 / max(xf, yf)).roundToInt())
                if (factor > 1) {
                    effectiveSampleSize = max(1, effectiveSampleSize / factor)
                }
            }

            // 安全裁剪：确保 effectiveRect 严格在 decoder 物理边界内，防止 IllegalArgumentException: rectangle is outside the image
            effectiveRect = Rect(
                max(0, effectiveRect.left),
                max(0, effectiveRect.top),
                min(decoder.width, effectiveRect.right),
                min(decoder.height, effectiveRect.bottom)
            )

            if (effectiveRect.width() <= 0 || effectiveRect.height() <= 0) {
                Log.w("CloudDebug", "[Region] effectiveRect is empty after clamp: $effectiveRect for decoder ${decoder.width}x${decoder.height}")
                result.error("fetch-empty-region", "Region empty for $contentAddress regionRect=$regionRect effectiveRect=$effectiveRect", null)
                return
            }

            val options = BitmapFactory.Options().apply {
                inSampleSize = effectiveSampleSize
                // Specifying preferred config and color space avoids the need for conversion afterward,
                // but may prevent decoding (e.g. from RGBA_1010102 to ARGB_8888 on some devices).
                inPreferredConfig = PREFERRED_CONFIG
                if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
                    inPreferredColorSpace = ColorSpace.get(ColorSpace.Named.SRGB)
                }
            }

            val pixelCount = effectiveRect.width() * effectiveRect.height() / effectiveSampleSize
            val targetBitmapSizeBytes = BitmapUtils.getExpectedImageSize(pixelCount.toLong(), options.inPreferredConfig)
            if (!MemoryUtils.canAllocate(targetBitmapSizeBytes)) {
                // decoding a region that large would yield an OOM when creating the bitmap
                result.error("fetch-large-region", "Region too large for $contentAddress regionRect=$regionRect", null)
                return
            }

            var bitmap = decoder.decodeRegion(effectiveRect, options)
            if (bitmap == null) {
                // retry without specifying config or color space,
                // falling back to custom byte conversion afterward
                options.inPreferredConfig = null
                if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O && options.inPreferredColorSpace != null) {
                    options.inPreferredColorSpace = null
                }
                bitmap = decoder.decodeRegion(effectiveRect, options)
            }

            val bytes = BitmapUtils.getBytes(bitmap, recycle = true, decoded = decoded, applyGainmap = applyGainmap, mimeType = mimeType)
            if (bytes == null) {
                result.error("fetch-null", "failed to decode region for $contentAddress regionRect=$regionRect", null)
            } else {
                result.streamBytes(ByteArrayInputStream(bytes))
            }
        } catch (e: Exception) {
            Log.e("CloudDebug", "[Region] fetch exception for $contentAddress regionRect=$regionRect: ${e.message}", e)
            if (EXPORT_MIME_TYPE != mimeType) {
                // retry with export on failure,
                // as some formats are not fully supported by `BitmapRegionDecoder`
                val exportUri = exportUris.getOrPut(requestKey) { createTemporaryExport(contentAddress) }
                fetch(
                    ContentAddress(
                        mimeType = EXPORT_MIME_TYPE,
                        uri = exportUri,
                        pageId = null,
                        path = null,
                    ),
                    decoded = decoded,
                    applyGainmap = applyGainmap,
                    sampleSize = sampleSize,
                    regionRect = regionRect,
                    imageWidth = imageWidth,
                    imageHeight = imageHeight,
                    requestKey = requestKey,
                    result = result,
                )
                return
            }

            result.error("fetch-read-exception", "failed to initialize region decoder for $contentAddress regionRect=$regionRect", e.message)
        }
    }

    private suspend fun createTemporaryExport(contentAddress: ContentAddress): Uri {
        val exportFormat = EXPORT_FORMAT
        Log.d(LOG_TAG, "create export for $contentAddress exportFormat=$exportFormat")

        val target = Glide.with(context)
            .asBitmap()
            .apply(AvesAppGlideModule.uncachedFullImageOptions)
            .load(AvesAppGlideModule.getModel(context, contentAddress))
            .submit()

        try {
            val bitmap = withContext(Dispatchers.IO) { target.get() }
            val tempFile = StorageUtils.createTempFile(context).apply {
                outputStream().use { output ->
                    val encodedExport = bitmap.compress(exportFormat, 100, output)
                    if (!encodedExport) {
                        Log.w(LOG_TAG, "failed export via encoded bytes for $contentAddress exportFormat=$exportFormat, with bitmap=${bitmap.describe()}")

                        val mimeType = contentAddress.mimeType
                        val decodedBytes = BitmapUtils.getBytes(bitmap, recycle = false, decoded = true, applyGainmap = false, mimeType = mimeType)
                        if (decodedBytes != null) {
                            val exportBitmap = createBitmap(bitmap.width, bitmap.height, PREFERRED_CONFIG)
                            exportBitmap.copyPixelsFromBuffer(ByteBuffer.wrap(decodedBytes))
                            val decodedExport = exportBitmap.compress(exportFormat, 100, output)
                            if (!decodedExport) {
                                Log.w(LOG_TAG, "failed to compress exportBitmap=${bitmap.describe()}")
                            }
                        }
                    }
                }
            }
            return Uri.fromFile(tempFile)
        } finally {
            Glide.with(context).clear(target)
        }
    }

    private data class DecoderRef(
        val requestKey: Pair<Uri, Int?>,
        val decoder: BitmapRegionDecoder,
    )

    companion object {
        private val LOG_TAG = LogUtils.createTag<RegionFetcher>()
        private val PREFERRED_CONFIG = Bitmap.Config.ARGB_8888
        private const val DECODER_POOL_SIZE = 3
        private const val EXPORT_MIME_TYPE = MimeTypes.JPEG
        private val EXPORT_FORMAT = Bitmap.CompressFormat.JPEG
        private val decoderPool = ArrayList<DecoderRef>()
        private val exportUris = HashMap<Pair<Uri, Int?>, Uri>()

        private val poolLock = ReentrantLock()

        private fun getOrCreateDecoder(context: Context, uri: Uri, requestKey: Pair<Uri, Int?>): BitmapRegionDecoder? {
            poolLock.withLock {
                var decoderRef = decoderPool.firstOrNull { it.requestKey == requestKey }
                if (decoderRef == null) {
                    val newDecoder = if (uri.scheme == "cloudgallery") {
                        val original = runBlocking { CloudDriveApiClient.fetchOriginal(context, uri) }
                        if (original != null && original.exists() && original.length() > 0L) {
                            Log.i("CloudDebug", "[Region] create region decoder from cloud original file: ${original.absolutePath} (${original.length()} bytes)")
                            try {
                                BitmapRegionDecoderCompat.newInstance(original.absolutePath)
                            } catch (e: Exception) {
                                Log.e("CloudDebug", "[Region] failed to create decoder from path: ${original.absolutePath}", e)
                                null
                            }
                        } else {
                            Log.w("CloudDebug", "[Region] fetchOriginal returned null or missing file for $uri")
                            null
                        }
                    } else {
                        StorageUtils.openInputStream(context, uri)?.use { input ->
                            Log.d(LOG_TAG, "create region decoder for requestKey=$requestKey")
                            BitmapRegionDecoderCompat.newInstance(input)
                        }
                    }
                    if (newDecoder == null) {
                        return null
                    }
                    decoderRef = DecoderRef(requestKey, newDecoder)
                } else {
                    decoderPool.remove(decoderRef)
                }
                decoderPool.add(0, decoderRef)
                trimDecoderPool(DECODER_POOL_SIZE)
                return decoderRef.decoder
            }
        }

        fun clearDecoders() {
            poolLock.withLock {
                trimDecoderPool(0)
            }
        }

        private fun trimDecoderPool(size: Int) {
            while (decoderPool.size > size) {
                val oldDecoderRef = decoderPool.removeAt(decoderPool.size - 1)
                Log.d(LOG_TAG, "recycle region decoder for requestKey=${oldDecoderRef.requestKey}")
                oldDecoderRef.decoder.recycle()
            }
        }
    }
}
