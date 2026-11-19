package deckers.thibault.aves.cloud

import android.content.Context
import android.net.Uri
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob

object CloudGalleryHolder {
    private const val PREFS_NAME = "cloudgallery_saf_prefs"
    private const val KEY_TREE_URI = "google_drive_tree_uri"

    val ioScope = CoroutineScope(SupervisorJob() + Dispatchers.IO)

    fun getPersistedTreeUri(context: Context): Uri? {
        val sp = context.getSharedPreferences(PREFS_NAME, Context.MODE_PRIVATE)
        val uriStr = sp.getString(KEY_TREE_URI, null) ?: return null
        return Uri.parse(uriStr)
    }

    fun setPersistedTreeUri(context: Context, treeUri: Uri?) {
        val sp = context.getSharedPreferences(PREFS_NAME, Context.MODE_PRIVATE)
        if (treeUri == null) {
            sp.edit().remove(KEY_TREE_URI).apply()
        } else {
            sp.edit().putString(KEY_TREE_URI, treeUri.toString()).apply()
        }
    }
}
