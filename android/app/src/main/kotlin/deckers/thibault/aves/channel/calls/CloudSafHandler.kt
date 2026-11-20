package deckers.thibault.aves.channel.calls

import android.app.Activity
import android.net.Uri
import android.os.Handler
import android.os.Looper
import android.util.Log
import deckers.thibault.aves.cloud.CloudDriveApiClient
import deckers.thibault.aves.utils.LogUtils
import io.flutter.plugin.common.BinaryMessenger
import io.flutter.plugin.common.EventChannel
import io.flutter.plugin.common.MethodCall
import io.flutter.plugin.common.MethodChannel
import io.flutter.plugin.common.MethodChannel.MethodCallHandler
import kotlinx.coroutines.launch
import java.io.File

class CloudSafHandler(private val activity: Activity) : MethodCallHandler {
    private val mainHandler = Handler(Looper.getMainLooper())

    fun register(messenger: BinaryMessenger) {
        MethodChannel(messenger, CHANNEL).setMethodCallHandler(this)
        EventChannel(messenger, EVENT_CHANNEL).setStreamHandler(CloudDriveApiClient)
    }

    override fun onMethodCall(call: MethodCall, result: MethodChannel.Result) {
        when (call.method) {
            "configureCloud" -> {
                val lanBaseUrl = call.argument<String>("lanBaseUrl")
                val lanApiToken = call.argument<String>("lanApiToken")
                val oauthClientId = call.argument<String>("oauthClientId")
                val oauthClientSecret = call.argument<String>("oauthClientSecret")
                val driveRootFolderName = call.argument<String>("driveRootFolderName")
                val driveRootFolderId = call.argument<String>("driveRootFolderId")
                CloudDriveApiClient.configure(
                    activity,
                    lanBaseUrl = lanBaseUrl,
                    lanApiToken = lanApiToken,
                    oauthClientId = oauthClientId,
                    oauthClientSecret = oauthClientSecret,
                    driveRootFolderName = driveRootFolderName,
                    driveRootFolderId = driveRootFolderId,
                )
                result.success(true)
            }

            "authorizeGoogleDrive" -> {
                val clientId = call.argument<String>("clientId")
                val clientSecret = call.argument<String>("clientSecret")
                CloudDriveApiClient.ioScope.launch {
                    val ok = CloudDriveApiClient.authorizeGoogleDrive(
                        activity,
                        clientId = clientId,
                        clientSecret = clientSecret,
                    )
                    mainHandler.post { result.success(ok) }
                }
            }

            "clearDriveAuth" -> {
                CloudDriveApiClient.clearDriveAuth(activity)
                result.success(true)
            }

            "syncMasterSnapshot" -> {
                val localVer = (call.argument<Number>("localSnapshotVersion") ?: 0L).toLong()
                CloudDriveApiClient.ioScope.launch {
                    try {
                        val syncRes = CloudDriveApiClient.syncMasterSnapshot(activity, localVer)
                        mainHandler.post {
                            if (syncRes != null) {
                                result.success(
                                    mapOf(
                                        "snapshotPath" to syncRes.snapshotFile.absolutePath,
                                        "snapshotVersion" to syncRes.snapshotVersion,
                                        "channel" to syncRes.channel,
                                    )
                                )
                            } else {
                                result.error("sync_failed", "Snapshot sync failed or no access token", null)
                            }
                        }
                    } catch (e: Exception) {
                        Log.e(LOG_TAG, "syncMasterSnapshot failed", e)
                        mainHandler.post { result.error("sync_failed", e.message, null) }
                    }
                }
            }

            "getVideoStreamProxyUrl" -> {
                val uriStr = call.argument<String>("uri")
                if (uriStr.isNullOrEmpty()) {
                    result.success(null)
                    return
                }
                val proxyUrl = CloudDriveApiClient.getVideoStreamProxyUrl(activity, Uri.parse(uriStr))
                result.success(proxyUrl)
            }

            "submitOutboxOps" -> {
                val opsJson = call.argument<String>("opsJson").orEmpty()
                CloudDriveApiClient.ioScope.launch {
                    val ok = CloudDriveApiClient.submitOutboxOps(activity, opsJson)
                    mainHandler.post { result.success(ok) }
                }
            }

            "uploadBackupFile" -> {
                val localPath = call.argument<String>("localPath").orEmpty()
                val remoteRelPath = call.argument<String>("remoteRelPath").orEmpty()
                val mimeType = call.argument<String>("mimeType") ?: "application/octet-stream"
                val file = File(localPath)
                if (!file.isFile) {
                    result.success(false)
                    return
                }
                CloudDriveApiClient.ioScope.launch {
                    val ok = CloudDriveApiClient.uploadBackupFile(activity, file, remoteRelPath, mimeType)
                    mainHandler.post { result.success(ok) }
                }
            }

            "getStatus" -> {
                result.success(CloudDriveApiClient.getStatusMap(activity))
            }

            "getPersistedTreeUri" -> result.success(null)
            "mountGoogleDrive" -> result.error("saf_deprecated", "Google Drive SAF is replaced by REST API v3 in v3.0", null)
            "syncSnapshot" -> result.error("saf_deprecated", "Use syncMasterSnapshot instead", null)
            "startThumbPackUnpack", "startUnpack" -> result.success(true)
            "isUnpacking" -> result.success(false)
            "setRateLimitKbps" -> result.success(true)

            else -> result.notImplemented()
        }
    }

    companion object {
        private val LOG_TAG = LogUtils.createTag<CloudSafHandler>()
        const val CHANNEL = "deckers.thibault/aves/cloud_saf"
        const val EVENT_CHANNEL = "deckers.thibault/aves/cloud_sync_events"
    }
}
