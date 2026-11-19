package deckers.thibault.aves.decoding

import android.content.Context
import android.graphics.Bitmap
import android.os.Build
import android.provider.MediaStore
import android.util.Log
import android.util.Size
import androidx.annotation.RequiresApi
import androidx.core.graphics.scale
import com.bumptech.glide.Glide
import com.bumptech.glide.load.DecodeFormat
import com.bumptech.glide.load.engine.DiskCacheStrategy
import com.bumptech.glide.request.RequestOptions
import com.bumptech.glide.signature.ObjectKey
import deckers.thibault.aves.channel.streams.darttoplatform.ByteSink
import deckers.thibault.aves.glide.AvesAppGlideModule
import deckers.thibault.aves.glide.MultiPageImage
import deckers.thibault.aves.model.ContentAddress
import deckers.thibault.aves.model.VideoThumbnailMethod
import deckers.thibault.aves.storage.MediaStoreUriHelper
import deckers.thibault.aves.utils.BitmapUtils
import deckers.thibault.aves.utils.BitmapUtils.applyExifOrientation
import deckers.thibault.aves.utils.ContextUtils.devicePixelRatio
import deckers.thibault.aves.utils.LogUtils
import deckers.thibault.aves.utils.MimeTypes
import deckers.thibault.aves.utils.MimeTypes.isVideo
import deckers.thibault.aves.utils.MimeTypes.needRotationAfterContentResolverThumbnail
import deckers.thibault.aves.utils.MimeTypes.needRotationAfterGlide
import deckers.thibault.aves.utils.UriUtils.tryParseId
import java.io.ByteArrayInputStream
import kotlin.math.min
import kotlin.math.roundToInt

class ThumbnailFetcher internal constructor(
    private val context: Context,
    private val contentAddress: ContentAddress,
    private val decoded: Boolean,
    private val dateModifiedMillis: Long,
    private val rotationDegrees: Int,
    private val isFlipped: Boolean,
    widthDip: Double?,
    heightDip: Double?,
    private val videoMethods: List<VideoThumbnailMethod>?,
    private val result: ByteSink,
) {
    private val mimeType = contentAddress.mimeType
    private val uri = contentAddress.uri
    private val pageId = contentAddress.pageId

    private val density = context.devicePixelRatio()
    private val defaultSize = (DEFAULT_SIZE_DIP * density).roundToInt()
    private val width: Int = if (widthDip?.takeIf { it > 0 } != null) (widthDip * density).roundToInt() else defaultSize
    private val height: Int = if (heightDip?.takeIf { it > 0 } != null) (heightDip * density).roundToInt() else defaultSize

    private fun isLowQualityFetch() = width == defaultSize || height == defaultSize

    private fun isCustomFetch(): Boolean {
        // use custom Glide models for videos / SVG / TIFF / multi-page images
        if (isVideo(mimeType) || mimeType == MimeTypes.SVG || mimeType == MimeTypes.TIFF) return true
        if (pageId != null && MultiPageImage.isSupported(mimeType)) return true
        return false
    }

    suspend fun fetch() {
        if (uri.scheme.equals("cloudgallery", ignoreCase = true)) {
            fetchCloudGalleryThumbnail()
            return
        }

        var bitmap: Bitmap? = null
        var exception: Exception? = null

        try {
            if (isLowQualityFetch() && !isCustomFetch() && !isFlipped) {
                // Fetch low quality thumbnails when size is not specified.
                // As of Android 11, the Media Store content resolver may return a thumbnail
                // that is automatically rotated according to EXIF orientation, but not flipped,
                // so we skip this step for flipped entries.
                if (uri.scheme != "http" && uri.scheme != "https") {
                    if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.Q) {
                        bitmap = getByResolver()
                    } else if (MediaStoreUriHelper.isMediaStoreContentUri(uri)) {
                        bitmap = getByMediaStore()
                    }
                }
            }
        } catch (e: Exception) {
            exception = e
        }

        // fallback if the native methods failed or for higher quality thumbnails
        if (bitmap == null) {
            try {
                bitmap = getByGlide()
            } catch (e: Exception) {
                exception = e
            }
        }

        if (bitmap != null) {
            if (bitmap.width > width && bitmap.height > height) {
                // rescale when the resulting bitmap is larger than requested
                val scalingFactor: Double = min(bitmap.width.toDouble() / width, bitmap.height.toDouble() / height)
                val dstWidth = (bitmap.width / scalingFactor).roundToInt()
                val dstHeight = (bitmap.height / scalingFactor).roundToInt()
                val reduction = 1f - (dstWidth * dstHeight).toFloat() / (bitmap.width * bitmap.height)
                if (reduction > RESCALE_REDUCTION_THRESHOLD) {
                    Log.d(
                        LOG_TAG, "rescale thumbnail for $contentAddress width=$width height=$height" +
                                ", with bitmap byteCount=${bitmap.byteCount} size=${bitmap.width}x${bitmap.height}, to target=${dstWidth}x${dstHeight}" +
                                ", reduced by ${(reduction * 100).roundToInt()}%)"
                    )
                    bitmap = bitmap.scale(dstWidth, dstHeight)
                }
            }

            if (bitmap.byteCount > BITMAP_SIZE_DANGER_THRESHOLD) {
                result.error(
                    "getThumbnail-large", "thumbnail bitmap dangerously large" +
                            " for $contentAddress width=$width height=$height" +
                            ", with bitmap byteCount=${bitmap.byteCount} size=${bitmap.width}x${bitmap.height} config=${bitmap.config?.name}", null
                )
                return
            }
        }

        // do not recycle bitmaps fetched from `ContentResolver` or Glide as their lifecycle is unknown
        val bytes = BitmapUtils.getBytes(bitmap, recycle = false, decoded = decoded, applyGainmap = false, mimeType = mimeType)
        if (bytes == null) {
            var errorDetails: String? = exception?.message
            if (errorDetails?.isNotEmpty() == true) {
                errorDetails = errorDetails.split(Regex("\n"), 2).first()
            }
            result.error("getThumbnail-null", "failed to get thumbnail for $contentAddress", errorDetails)
        } else {
            result.streamBytes(ByteArrayInputStream(bytes))
        }
    }

    @RequiresApi(api = Build.VERSION_CODES.Q)
    fun getByResolver(): Bitmap? {
        val resolver = context.contentResolver
        var bitmap: Bitmap? = resolver.loadThumbnail(uri, Size(width, height), null)
        if (needRotationAfterContentResolverThumbnail(mimeType)) {
            bitmap = applyExifOrientation(context, bitmap, rotationDegrees, isFlipped)
        }
        return bitmap
    }

    fun getByMediaStore(): Bitmap? {
        val contentId = uri.tryParseId() ?: return null
        val resolver = context.contentResolver
        return if (isVideo(mimeType)) {
            @Suppress("DEPRECATION")
            MediaStore.Video.Thumbnails.getThumbnail(resolver, contentId, MediaStore.Video.Thumbnails.MINI_KIND, null)
        } else {
            @Suppress("DEPRECATION")
            var bitmap = MediaStore.Images.Thumbnails.getThumbnail(resolver, contentId, MediaStore.Images.Thumbnails.MINI_KIND, null)
            // from Android 10 (API 29), returned thumbnail is already rotated according to EXIF orientation
            if (Build.VERSION.SDK_INT < Build.VERSION_CODES.Q && bitmap != null) {
                bitmap = applyExifOrientation(context, bitmap, rotationDegrees, isFlipped)
            }
            bitmap
        }
    }

    fun getByGlide(): Bitmap? {
        // add signature to ignore cache for images which got modified but kept the same URI
        var options = RequestOptions()
            .format(DecodeFormat.PREFER_ARGB_8888)
            .signature(ObjectKey("$dateModifiedMillis-$rotationDegrees-$isFlipped-$width-$pageId"))
            .override(width, height)
        if (isVideo(mimeType)) {
            options = options.diskCacheStrategy(DiskCacheStrategy.RESOURCE)
        }

        val model: Any = if (uri.scheme == "http" || uri.scheme == "https") {
            val thumbParam = uri.getQueryParameter("thumb")
            if (thumbParam != null) {
                "${uri.scheme}://${uri.authority}/api/thumbs/$thumbParam"
            } else {
                uri.toString()
            }
        } else {
            AvesAppGlideModule.getModel(context, contentAddress, videoMethods = videoMethods)
        }

        val target = Glide.with(context)
            .asBitmap()
            .apply(options)
            .load(model)
            .submit(width, height)

        return try {
            var bitmap = target.get()
            if (needRotationAfterGlide(mimeType, pageId)) {
                bitmap = applyExifOrientation(context, bitmap, rotationDegrees, isFlipped)
            }
            bitmap
        } finally {
            Glide.with(context).clear(target)
        }
    }

    private suspend fun fetchCloudGalleryThumbnail() {
        val sha256 = (uri.host ?: uri.authority ?: uri.pathSegments.firstOrNull())?.trim()?.lowercase()
        if (sha256.isNullOrEmpty() || sha256.length !in 16..64) {
            result.error("getThumbnail-cloud", "Invalid cloudgallery URI: $uri", null)
            return
        }

        val thumbParam = uri.getQueryParameter("thumb")?.trim()?.lowercase()
        val thumbKey = if (!thumbParam.isNullOrEmpty() && thumbParam.length in 16..64) thumbParam else sha256

        var localFile: java.io.File? = deckers.thibault.aves.cloud.GoogleDriveSafManager.getLocalThumbIfPresent(context, thumbKey)
        if (localFile == null && thumbKey != sha256) {
            localFile = deckers.thibault.aves.cloud.GoogleDriveSafManager.getLocalThumbIfPresent(context, sha256)
        }

        if (localFile == null) {
            val treeUri = deckers.thibault.aves.cloud.CloudGalleryHolder.getPersistedTreeUri(context)
            localFile = deckers.thibault.aves.cloud.GoogleDriveSafManager.fetchForegroundThumbnail(context, treeUri, thumbKey)
        }

        if (localFile != null && localFile.exists() && localFile.length() > 0L) {
            try {
                if (!decoded) {
                    val fileLen = localFile.length()
                    if (fileLen in 1L..(10L * 1024 * 1024)) {
                        val bytesWithTrailer = ByteArray(fileLen.toInt() + 1)
                        java.io.FileInputStream(localFile).use { it.read(bytesWithTrailer, 0, fileLen.toInt()) }
                        bytesWithTrailer[fileLen.toInt()] = BitmapUtils.FORMAT_BYTE_ENCODED_AS_BYTES[0]
                        deckers.thibault.aves.cloud.AppLogger.log(context, "[ThumbFetch] Success streamBytes: sha=$sha256 key=$thumbKey bytes=${bytesWithTrailer.size}")
                        result.streamBytes(ByteArrayInputStream(bytesWithTrailer))
                        return
                    }
                } else {
                    val bitmap = android.graphics.BitmapFactory.decodeFile(localFile.absolutePath)
                    if (bitmap != null) {
                        val bytes = BitmapUtils.getBytes(bitmap, recycle = false, decoded = true, applyGainmap = false, mimeType = "image/webp")
                        if (bytes != null) {
                            deckers.thibault.aves.cloud.AppLogger.log(context, "[ThumbFetch] Success decoded bitmap: sha=$sha256 key=$thumbKey")
                            result.streamBytes(ByteArrayInputStream(bytes))
                            return
                        }
                    }
                }
            } catch (e: Exception) {
                Log.w(LOG_TAG, "Failed to read local cloud thumb $sha256", e)
            }
        }

        result.error("getThumbnail-cloud-waiting", "Cloud thumbnail pending: $sha256", null)
    }

    companion object {
        private val LOG_TAG = LogUtils.createTag<ThumbnailFetcher>()
        private const val BITMAP_SIZE_DANGER_THRESHOLD = 20 * (1 shl 20) // MiB
        private const val DEFAULT_SIZE_DIP: Double = 64.0
        private const val RESCALE_REDUCTION_THRESHOLD: Float = .15f
    }
}