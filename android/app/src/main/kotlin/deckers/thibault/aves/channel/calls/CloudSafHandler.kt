package deckers.thibault.aves.channel.calls

import android.app.Activity
import android.net.Uri
import android.util.Log
import deckers.thibault.aves.MainActivity
import deckers.thibault.aves.PendingStorageAccessResultHandler
import deckers.thibault.aves.cloud.CloudGalleryHolder
import deckers.thibault.aves.cloud.GoogleDriveSafManager
import deckers.thibault.aves.utils.LogUtils
import io.flutter.plugin.common.BinaryMessenger
import io.flutter.plugin.common.EventChannel
import io.flutter.plugin.common.MethodCall
import io.flutter.plugin.common.MethodChannel
import io.flutter.plugin.common.MethodChannel.MethodCallHandler
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.launch

class CloudSafHandler(private val activity: Activity) : MethodCallHandler {
    private val ioScope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
    private val mainScope = CoroutineScope(SupervisorJob() + Dispatchers.Main)

    fun register(messenger: BinaryMessenger) {
        MethodChannel(messenger, CHANNEL).setMethodCallHandler(this)
        EventChannel(messenger, EVENT_CHANNEL).setStreamHandler(GoogleDriveSafManager)
    }

    override fun onMethodCall(call: MethodCall, result: MethodChannel.Result) {
        when (call.method) {
            "getPersistedTreeUri" -> {
                val uri = CloudGalleryHolder.getPersistedTreeUri(activity)
                result.success(uri?.toString())
            }

            "mountGoogleDrive" -> {
                mountGoogleDrive(result)
            }

            "syncSnapshot" -> {
                val treeUri = CloudGalleryHolder.getPersistedTreeUri(activity)
                if (treeUri == null) {
                    result.error("not_mounted", "Google Drive TreeUri is not mounted", null)
                    return
                }
                syncSnapshot(treeUri, result)
            }

            "startThumbPackUnpack", "startUnpack" -> {
                val treeUriStr = call.argument<String>("treeUri")
                val treeUri = if (treeUriStr != null) Uri.parse(treeUriStr) else CloudGalleryHolder.getPersistedTreeUri(activity)
                if (treeUri == null) {
                    result.error("not_mounted", "Google Drive TreeUri is not mounted", null)
                    return
                }
                val force = call.argument<Boolean>("force") ?: false
                ioScope.launch {
                    val ok = GoogleDriveSafManager.untarThumbsPack(activity, treeUri, force = force)
                    mainScope.launch { result.success(ok) }
                }
            }

            "isUnpacking" -> {
                result.success(GoogleDriveSafManager.isUnpacking())
            }

            "getStatus" -> {
                result.success(GoogleDriveSafManager.getStatusMap())
            }

            "setRateLimitKbps" -> {
                val kbps = call.argument<Number>("rateLimitKbps")?.toLong() ?: 512L
                GoogleDriveSafManager.setRateLimitBytesPerSec(kbps * 1024L)
                result.success(true)
            }

            else -> result.notImplemented()
        }
    }

    private fun mountGoogleDrive(result: MethodChannel.Result) {
        val initialUri = CloudGalleryHolder.getPersistedTreeUri(activity)
        val intent = GoogleDriveSafManager.createOpenTreeIntent(initialUri)

        if (intent.resolveActivity(activity.packageManager) == null) {
            result.error("no_activity", "No activity found to handle ACTION_OPEN_DOCUMENT_TREE", null)
            return
        }

        MainActivity.pendingStorageAccessResultHandlers[MainActivity.DOCUMENT_TREE_ACCESS_REQUEST] =
            PendingStorageAccessResultHandler(
                path = null,
                onGranted = { treeUri ->
                    Log.i(LOG_TAG, "Google Drive tree granted: $treeUri")
                    CloudGalleryHolder.setPersistedTreeUri(activity, treeUri)
                    syncSnapshot(treeUri, result)
                },
                onDenied = {
                    Log.w(LOG_TAG, "Google Drive tree selection cancelled by user")
                    result.error("cancelled", "User cancelled folder selection", null)
                }
            )

        activity.startActivityForResult(intent, MainActivity.DOCUMENT_TREE_ACCESS_REQUEST)
    }

    private fun syncSnapshot(treeUri: Uri, result: MethodChannel.Result) {
        ioScope.launch {
            try {
                Log.i(LOG_TAG, "Starting master snapshot sync from Google Drive SAF: $treeUri")
                val snapshotFile = GoogleDriveSafManager.syncMasterSnapshot(activity, treeUri)
                Log.i(LOG_TAG, "Snapshot successfully downloaded to: ${snapshotFile.absolutePath} (size: ${snapshotFile.length()})")

                // 立即后台异步启动 thumbs_pack.tar 双轨流式解包与热索引
                ioScope.launch {
                    try {
                        Log.i(LOG_TAG, "Starting background stream untar for thumbs_pack.tar...")
                        GoogleDriveSafManager.untarThumbsPack(activity, treeUri)
                    } catch (t: Throwable) {
                        Log.w(LOG_TAG, "Background untar error", t)
                    }
                }

                mainScope.launch {
                    result.success(
                        mapOf(
                            "treeUri" to treeUri.toString(),
                            "snapshotPath" to snapshotFile.absolutePath,
                            "snapshotSize" to snapshotFile.length(),
                        )
                    )
                }
            } catch (t: Throwable) {
                Log.e(LOG_TAG, "Failed to sync snapshot from SAF: $treeUri", t)
                mainScope.launch {
                    result.error("sync_failed", t.message, null)
                }
            }
        }
    }

    companion object {
        private val LOG_TAG = LogUtils.createTag<CloudSafHandler>()
        const val CHANNEL = "deckers.thibault/aves/cloud_saf"
        const val EVENT_CHANNEL = "deckers.thibault/aves/cloud_sync_events"
    }
}
