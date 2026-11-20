package deckers.thibault.aves.channel.streams.darttoplatform

import android.content.Context
import android.graphics.Rect
import android.util.Log
import com.bumptech.glide.Glide
import com.bumptech.glide.request.RequestOptions
import com.bumptech.glide.signature.ObjectKey
import deckers.thibault.aves.channel.streams.BaseStreamHandler
import deckers.thibault.aves.cloud.CloudDriveApiClient
import deckers.thibault.aves.decoding.RegionFetcher
import deckers.thibault.aves.decoding.SvgRegionFetcher
import deckers.thibault.aves.decoding.ThumbnailFetcher
import deckers.thibault.aves.decoding.TiffRegionFetcher
import deckers.thibault.aves.glide.AvesAppGlideModule
import deckers.thibault.aves.model.ContentAddress
import deckers.thibault.aves.model.EntryFields
import deckers.thibault.aves.model.VideoThumbnailMethod
import deckers.thibault.aves.storage.StorageUtils
import deckers.thibault.aves.utils.BitmapUtils
import deckers.thibault.aves.utils.BitmapUtils.applyExifOrientation
import deckers.thibault.aves.utils.LogUtils
import deckers.thibault.aves.utils.MimeTypes
import deckers.thibault.aves.utils.MimeTypes.handleEncodedBytesInFlutter
import deckers.thibault.aves.utils.MimeTypes.isVideo
import deckers.thibault.aves.utils.MimeTypes.needRotationAfterGlide
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import java.io.ByteArrayInputStream
import java.io.InputStream
import java.util.Date

class ImageByteStreamHandler(private val context: Context, private val arguments: Any?) : BaseStreamHandler(), ByteSink {
    private var op: String? = null
    private var decoded: Boolean = false
    private var applyGainmap: Boolean = false
    private val regionFetcher = RegionFetcher(context)

    init {
        if (arguments is Map<*, *>) {
            op = arguments["op"] as String?
            decoded = arguments["decoded"] as Boolean
            applyGainmap = arguments["applyGainmap"] as Boolean? ?: false
        }
    }

    override val logTag = LOG_TAG

    override fun onCall(args: Any?) {
        when (op) {
            "getFullImage" -> ioScope.launch { safeSuspend(::streamFullImage) }
            "getRegion" -> ioScope.launch { safeSuspend(::streamRegion) }
            "getThumbnail" -> ioScope.launch { safeSuspend(::streamThumbnail) }
            else -> endOfStream()
        }
    }

    // Supported image formats:
    // - Flutter (as of v1.20): JPEG, PNG, GIF, Animated GIF, WebP, Animated WebP, BMP, and WBMP
    // - Android: https://developer.android.com/guide/topics/media/media-formats#image-formats
    // - Glide: https://github.com/bumptech/glide/blob/master/library/src/main/java/com/bumptech/glide/load/ImageHeaderParser.java
    private suspend fun streamFullImage() {
        if (arguments !is Map<*, *>) {
            return
        }

        val contentAddress = ContentAddress.fromMap(arguments)
        val sizeBytes = (arguments["sizeBytes"] as Number?)?.toLong()
        val rotationDegrees = arguments["rotationDegrees"] as Int
        val isFlipped = arguments["isFlipped"] as Boolean
        val videoMethods = (arguments["videoMethods"] as List<*>?)?.filterIsInstance<String>()?.mapNotNull { VideoThumbnailMethod.fromKey(it) }

        if (contentAddress == null) {
            error("streamImage-args", "missing arguments", null)
            return
        }

        val mimeType = contentAddress.mimeType
        val isCloud = contentAddress.uri.scheme == "cloudgallery"
        if (isCloud) {
            Log.i("CloudDebug", "[ImageByteStream] streamFullImage uri=${contentAddress.uri} mime=$mimeType decoded=$decoded sizeBytes=$sizeBytes")
        }

        if (!decoded && handleEncodedBytesInFlutter(mimeType)) {
            // the image can be decoded by Flutter codecs,
            // and there is no need for processing on the platform side
            // so we stream it without decoding it
            streamOriginalBytesWithTrailer(contentAddress)
        } else if (isVideo(mimeType)) {
            streamVideoByGlide(
                contentAddress = contentAddress,
                sizeBytes = sizeBytes,
                videoMethods = videoMethods,
                decoded = decoded,
            )
        } else {
            // even if the image could be decoded by Flutter codecs,
            // it needs to be processed on the platform side
            // so we decode, process, optionally reencode, then stream it
            streamImageByGlide(
                contentAddress = contentAddress,
                sizeBytes = sizeBytes,
                rotationDegrees = rotationDegrees,
                isFlipped = isFlipped,
                decoded = decoded,
                applyGainmap = applyGainmap,
            )
        }
    }

    private fun streamOriginalBytesWithTrailer(contentAddress: ContentAddress) {
        val uri = contentAddress.uri
        val isCloud = uri.scheme == "cloudgallery"
        val itemId = (uri.host ?: uri.authority)?.trim()?.lowercase().orEmpty()
        if (isCloud) {
            Log.i("CloudDebug", "[StreamOriginal] start uri=$uri mime=${contentAddress.mimeType}")
        }
        try {
            val stream = StorageUtils.openInputStream(context, uri)
            if (stream != null) {
                if (isCloud) {
                    Log.i("CloudDebug", "[StreamOriginal] stream opened, writing bytes to Flutter sink...")
                }
                val sent = stream.use(::streamBytes)
                if (sent) {
                    if (isCloud) {
                        Log.i("CloudDebug", "[StreamOriginal] successfully sent bytes with trailer for $uri")
                    }
                    success(BitmapUtils.FORMAT_BYTE_ENCODED_AS_BYTES)
                    return
                } else if (isCloud) {
                    Log.e("CloudDebug", "[StreamOriginal] streamBytes returned false for $uri")
                }
            } else if (isCloud) {
                val lastErr = CloudDriveApiClient.getLastOriginalError(itemId) ?: "原图下载失败 (流为null)"
                Log.e("CloudDebug", "[StreamOriginal] StorageUtils.openInputStream returned null for $uri: $lastErr")
                error("getImage-cloud", lastErr, null)
                return
            }
            error("streamImage-image-read-failed", "failed to open stream for $contentAddress", null)
        } catch (e: Exception) {
            val detail = if (isCloud) {
                val lastErr = CloudDriveApiClient.getLastOriginalError(itemId)
                if (!lastErr.isNullOrBlank()) "$lastErr (${e.javaClass.simpleName}: ${e.message})"
                else "${e.javaClass.simpleName}: ${e.message}"
            } else e.stackTraceToString()
            if (isCloud) {
                Log.e("CloudDebug", "[StreamOriginal] exception for $contentAddress: $detail", e)
            }
            error(if (isCloud) "getImage-cloud" else "streamImage-image-read-exception", detail, null)
        }
    }

    private suspend fun streamImageByGlide(
        contentAddress: ContentAddress,
        sizeBytes: Long?,
        rotationDegrees: Int,
        isFlipped: Boolean,
        decoded: Boolean,
        applyGainmap: Boolean
    ) {
        val mimeType = contentAddress.mimeType
        val pageId = contentAddress.pageId
        val uri = contentAddress.uri
        val isCloud = uri.scheme == "cloudgallery"

        if (isCloud) {
            Log.i("CloudDebug", "[GlideFullImage] start uri=$uri mime=$mimeType rot=$rotationDegrees")
        }

        val sigSuffix = if (isCloud) "-orig-v3" else ""
        val target = Glide.with(context)
            .asBitmap()
            .apply(AvesAppGlideModule.uncachedFullImageOptions)
            .apply(RequestOptions().signature(ObjectKey("$uri$sigSuffix")))
            .load(AvesAppGlideModule.getModel(context, contentAddress, sizeBytes))
            .submit()
        try {
            var bitmap = withContext(Dispatchers.IO) { target.get() }
            if (needRotationAfterGlide(mimeType, pageId)) {
                bitmap = applyExifOrientation(context, bitmap, rotationDegrees, isFlipped)
            }
            if (bitmap != null) {
                if (isCloud) {
                    Log.i("CloudDebug", "[GlideFullImage] successfully decoded bitmap=${bitmap.width}x${bitmap.height} for $uri")
                }
                // do not recycle bitmaps fetched from Glide as their lifecycle is unknown
                val bytes = BitmapUtils.getBytes(bitmap, recycle = false, decoded = decoded, applyGainmap = applyGainmap, mimeType = mimeType)
                streamBytes(ByteArrayInputStream(bytes))
            } else {
                if (isCloud) {
                    Log.e("CloudDebug", "[GlideFullImage] decoded bitmap is null for $uri")
                }
                error("streamImage-image-decode-null", "failed to get image for $contentAddress", null)
            }
        } catch (e: Exception) {
            val detail = if (isCloud) {
                val itemId = (uri.host ?: uri.authority)?.trim()?.lowercase().orEmpty()
                val lastErr = CloudDriveApiClient.getLastOriginalError(itemId)
                if (!lastErr.isNullOrBlank()) "$lastErr (${e.javaClass.simpleName}: ${e.message})"
                else "${e.javaClass.simpleName}: ${e.message}"
            } else e.stackTraceToString()
            if (isCloud) {
                Log.e("CloudDebug", "[GlideFullImage] exception for $uri: $detail", e)
            }
            error(if (isCloud) "getImage-cloud" else "streamImage-image-decode-exception", detail, null)
        } finally {
            Glide.with(context).clear(target)
        }
    }

    private suspend fun streamVideoByGlide(
        contentAddress: ContentAddress,
        sizeBytes: Long?,
        videoMethods: List<VideoThumbnailMethod>?,
        decoded: Boolean,
    ) {
        val mimeType = contentAddress.mimeType
        val target = Glide.with(context)
            .asBitmap()
            .apply(AvesAppGlideModule.uncachedFullImageOptions)
            .load(AvesAppGlideModule.getModel(context, contentAddress, sizeBytes, videoMethods = videoMethods))
            .submit()
        try {
            val bitmap = withContext(Dispatchers.IO) { target.get() }
            if (bitmap != null) {
                // do not recycle bitmaps fetched from Glide as their lifecycle is unknown
                val bytes = BitmapUtils.getBytes(bitmap, recycle = false, decoded = decoded, applyGainmap = false, mimeType = mimeType)
                streamBytes(ByteArrayInputStream(bytes))
            } else {
                error("streamImage-video-null", "failed to get image for $contentAddress", null)
            }
        } catch (e: Exception) {
            error("streamImage-video-exception", "failed to get image for $contentAddress", e.stackTraceToString())
        } finally {
            Glide.with(context).clear(target)
        }
    }

    private suspend fun streamRegion() {
        if (arguments !is Map<*, *>) {
            return
        }

        val contentAddress = ContentAddress.fromMap(arguments)
        val sizeBytes = (arguments["sizeBytes"] as Number?)?.toLong()
        val sampleSize = arguments["sampleSize"] as Int?
        val x = arguments["regionX"] as Int?
        val y = arguments["regionY"] as Int?
        val width = arguments["regionWidth"] as Int?
        val height = arguments["regionHeight"] as Int?
        val imageWidth = arguments["imageWidth"] as Int?
        val imageHeight = arguments["imageHeight"] as Int?

        if (contentAddress == null || sampleSize == null || x == null || y == null || width == null || height == null || imageWidth == null || imageHeight == null) {
            error("getRegion-args", "missing arguments", null)
            return
        }

        val isCloud = contentAddress.uri.scheme == "cloudgallery"
        val regionRect = Rect(x, y, x + width, y + height)
        if (isCloud) {
            Log.i("CloudDebug", "[StreamRegion] start uri=${contentAddress.uri} sampleSize=$sampleSize rect=$regionRect imageSize=${imageWidth}x$imageHeight mime=${contentAddress.mimeType}")
        }
        try {
            when (contentAddress.mimeType) {
                MimeTypes.SVG -> SvgRegionFetcher(context).fetch(
                    uri = contentAddress.uri,
                    decoded = decoded,
                    sizeBytes = sizeBytes,
                    scale = sampleSize,
                    regionRect = regionRect,
                    imageWidth = imageWidth,
                    imageHeight = imageHeight,
                    result = this,
                )

                MimeTypes.TIFF -> TiffRegionFetcher(context).fetch(
                    uri = contentAddress.uri,
                    page = contentAddress.pageId ?: 0,
                    decoded = decoded,
                    sampleSize = sampleSize,
                    regionRect = regionRect,
                    result = this,
                )

                else -> regionFetcher.fetch(
                    contentAddress = contentAddress,
                    decoded = decoded,
                    applyGainmap = applyGainmap,
                    sampleSize = sampleSize,
                    regionRect = regionRect,
                    imageWidth = imageWidth,
                    imageHeight = imageHeight,
                    result = this,
                )
            }
        } catch (e: Exception) {
            val detail = if (isCloud) {
                val itemId = (contentAddress.uri.host ?: contentAddress.uri.authority)?.trim()?.lowercase().orEmpty()
                val lastErr = CloudDriveApiClient.getLastOriginalError(itemId)
                if (!lastErr.isNullOrBlank()) "$lastErr (${e.javaClass.simpleName}: ${e.message})"
                else "${e.javaClass.simpleName}: ${e.message}"
            } else e.stackTraceToString()
            if (isCloud) {
                Log.e("CloudDebug", "[StreamRegion] exception: $detail", e)
            }
            error(if (isCloud) "getImage-cloud" else "streamRegion-exception", detail, null)
        }
    }

    private suspend fun streamThumbnail() {
        if (arguments !is Map<*, *>) {
            return
        }

        val contentAddress = ContentAddress.fromMap(arguments)
        val dateModifiedMillis = (arguments[EntryFields.DATE_MODIFIED_MILLIS] as Number?)?.toLong()
        val rotationDegrees = arguments[EntryFields.ROTATION_DEGREES] as Int?
        val isFlipped = arguments[EntryFields.IS_FLIPPED] as Boolean?
        val widthDip = (arguments["widthDip"] as Number?)?.toDouble()
        val heightDip = (arguments["heightDip"] as Number?)?.toDouble()
        val videoMethods = (arguments["videoMethods"] as List<*>?)?.filterIsInstance<String>()?.mapNotNull { VideoThumbnailMethod.fromKey(it) }

        if (contentAddress == null || rotationDegrees == null || isFlipped == null || widthDip == null || heightDip == null) {
            error("getThumbnail-args", "missing arguments", null)
            return
        }

        // convert DIP to physical pixels here, instead of using `devicePixelRatio` in Flutter
        ThumbnailFetcher(
            context = context,
            contentAddress = contentAddress,
            decoded = decoded,
            dateModifiedMillis = dateModifiedMillis ?: (Date().time),
            rotationDegrees = rotationDegrees,
            isFlipped = isFlipped,
            widthDip = widthDip,
            heightDip = heightDip,
            videoMethods = videoMethods,
            result = this,
        ).fetch()
    }

    companion object {
        private val LOG_TAG = LogUtils.createTag<ImageByteStreamHandler>()
        const val CHANNEL = "deckers.thibault/aves/media_byte_stream"
    }
}

interface ByteSink {
    fun streamBytes(inputStream: InputStream): Boolean
    fun error(errorCode: String, errorMessage: String?, errorDetails: Any?)
}
