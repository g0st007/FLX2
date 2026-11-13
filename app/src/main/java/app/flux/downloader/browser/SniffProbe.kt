package app.flux.downloader.browser

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import java.net.HttpURLConnection
import java.net.URL

/** Lo que se puede saber de un video detectado sin analizarlo con yt-dlp. */
data class SniffMeta(val height: Int?, val kind: String, val isStream: Boolean)

object SniffProbe {

    private val resRegex = Regex("""(\d{3,4})x(\d{3,4})""")
    private const val UA =
        "Mozilla/5.0 (Linux; Android 14; Pixel 7) AppleWebKit/537.36 (KHTML, like Gecko) " +
            "Chrome/126.0.0.0 Mobile Safari/537.36"

    /** Resolución (si la URL la trae, p. ej. .../1280x720/...) y tipo de archivo. Sin red. */
    fun parse(url: String): SniffMeta {
        val path = url.substringBefore('?').lowercase()
        val ext = path.substringAfterLast('.', "")
        val stream = ext == "m3u8" || ext == "mpd"
        val kind = when {
            ext == "m3u8" -> "Transmisión HLS"
            ext == "mpd" -> "Transmisión DASH"
            ext.isNotBlank() && ext.length <= 4 -> ext.uppercase()
            else -> "Video"
        }
        val height = resRegex.find(url)?.groupValues?.get(2)?.toIntOrNull()
        return SniffMeta(height, kind, stream)
    }

    /** Peso en bytes por la cabecera Content-Length (HEAD, o un GET de 1 byte si el servidor no responde a HEAD). */
    suspend fun size(url: String): Long? = withContext(Dispatchers.IO) {
        headSize(url) ?: rangeSize(url)
    }

    private fun open(url: String, method: String): HttpURLConnection =
        (URL(url).openConnection() as HttpURLConnection).apply {
            connectTimeout = 6_000
            readTimeout = 6_000
            instanceFollowRedirects = true
            requestMethod = method
            setRequestProperty("User-Agent", UA)
        }

    private fun headSize(url: String): Long? = runCatching {
        val c = open(url, "HEAD")
        try {
            if (c.responseCode in 200..299) c.contentLengthLong.takeIf { it > 0 } else null
        } finally {
            c.disconnect()
        }
    }.getOrNull()

    private fun rangeSize(url: String): Long? = runCatching {
        val c = open(url, "GET").apply { setRequestProperty("Range", "bytes=0-0") }
        try {
            if (c.responseCode == 206) c.getHeaderField("Content-Range")?.substringAfterLast('/')?.toLongOrNull() else null
        } finally {
            c.disconnect()
        }
    }.getOrNull()
}
