package app.flux.downloader.engine

import android.app.Application
import android.content.Context
import com.yausername.ffmpeg.FFmpeg
import com.yausername.youtubedl_android.YoutubeDL
import com.yausername.youtubedl_android.YoutubeDLRequest
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import app.flux.downloader.settings.AppSettings
import org.json.JSONObject
import java.io.File

/** Una calidad de video disponible. [bytes] es el peso total estimado (video + audio) si se pudo calcular. */
data class Quality(
    val height: Int,
    val bytes: Long?,
    val estimated: Boolean,
)

data class MediaInfo(
    val url: String,
    val title: String,
    val uploader: String?,
    val thumbnail: String?,
    val durationSec: Long,
    val qualities: List<Quality>,
) {
    val heights: List<Int> get() = qualities.map { it.height }
}

data class Choice(
    val audioOnly: Boolean,
    val height: Int?,
    val audioFormat: String = "mp3",
)

/** Envuelve yt-dlp + FFmpeg. Toda la lógica de descarga vive aquí. */
object Engine {

    private val _ready = MutableStateFlow(false)
    val ready: StateFlow<Boolean> = _ready.asStateFlow()

    private val _error = MutableStateFlow<String?>(null)
    val error: StateFlow<String?> = _error.asStateFlow()

    fun init(app: Application) {
        CoroutineScope(Dispatchers.IO).launch {
            try {
                YoutubeDL.getInstance().init(app)
                FFmpeg.getInstance().init(app)
                _ready.value = true
                // Los sitios cambian a diario: actualizamos el extractor en segundo plano,
                // pero sin competir con el primer arranque de la interfaz.
                delay(15_000)
                runCatching { YoutubeDL.getInstance().updateYoutubeDL(app) }
            } catch (e: Exception) {
                _error.value = "No se pudo iniciar el motor: ${e.message}"
            }
        }
    }

    suspend fun awaitReady() {
        withTimeout(60_000) {
            while (!_ready.value) {
                _error.value?.let { error(it) }
                delay(150)
            }
        }
    }

    suspend fun analyze(url: String): MediaInfo {
        awaitReady()
        return withContext(Dispatchers.IO) {
            // Reddit exige sesión para muchos videos: si el post enlaza a RedGifs, se descarga de ahí.
            val target = SiteResolver.resolve(url)
            val request = YoutubeDLRequest(target).apply {
                addOption("--no-playlist")
                addOption("--dump-single-json")
                addOption("--no-warnings")
            }
            val out = YoutubeDL.getInstance().execute(request).out
            val json = JSONObject(out.substring(out.indexOf('{').coerceAtLeast(0)))
            parseInfo(target, json)
        }
    }

    private fun JSONObject.str(key: String): String? =
        if (isNull(key)) null else optString(key).takeIf { it.isNotBlank() && it != "null" }

    private fun parseInfo(url: String, json: JSONObject): MediaInfo {
        val duration = json.optDouble("duration", 0.0).let { if (it.isNaN()) 0.0 else it }
        val formats = json.optJSONArray("formats")

        class F(val height: Int, val bytes: Long?, val estimated: Boolean, val tbr: Double, val avc: Boolean, val hasAudio: Boolean)

        fun sizeOf(f: JSONObject): Pair<Long?, Boolean> {
            val exact = f.optLong("filesize", 0L)
            if (exact > 0) return exact to false
            val approx = f.optLong("filesize_approx", 0L)
            if (approx > 0) return approx to false
            val tbr = f.optDouble("tbr", 0.0)
            if (!tbr.isNaN() && tbr > 0 && duration > 0) return (tbr * 1000 / 8 * duration).toLong() to true
            return null to false
        }

        val video = ArrayList<F>()
        var bestAudio: F? = null
        if (formats != null) {
            for (i in 0 until formats.length()) {
                val f = formats.optJSONObject(i) ?: continue
                val vcodec = f.optString("vcodec", "none")
                val acodec = f.optString("acodec", "none")
                val hasV = vcodec.isNotEmpty() && vcodec != "none" && vcodec != "null"
                val hasA = acodec.isNotEmpty() && acodec != "none" && acodec != "null"
                val (bytes, est) = sizeOf(f)
                val tbr = f.optDouble("tbr", 0.0).let { if (it.isNaN()) 0.0 else it }
                val h = f.optInt("height", 0)
                if (hasV && h > 0) {
                    video += F(h, bytes, est, tbr, vcodec.startsWith("avc") || vcodec.startsWith("h264"), hasA)
                } else if (!hasV && hasA) {
                    val cand = F(0, bytes, est, tbr, false, true)
                    val cur = bestAudio
                    if (cur == null || cand.tbr > cur.tbr) bestAudio = cand
                }
            }
        }

        val audioBest = bestAudio

        // Misma preferencia que usa la descarga (-S res,vcodec:h264): a igual resolución, h264 y mayor bitrate.
        val qualities = video.groupBy { it.height }
            .map { (h, list) ->
                val pick = list.sortedWith(compareByDescending<F> { it.avc }.thenByDescending { it.tbr }).first()
                val audio = if (pick.hasAudio) null else audioBest
                val videoBytes = pick.bytes
                val audioBytes = audio?.bytes
                val total: Long? = when {
                    videoBytes == null -> null
                    pick.hasAudio -> videoBytes
                    audioBytes != null -> videoBytes + audioBytes
                    else -> null
                }
                Quality(h, total, pick.estimated || (audio?.estimated == true))
            }
            .sortedByDescending { it.height }

        return MediaInfo(
            url = url,
            title = json.str("title") ?: "Video",
            uploader = json.str("uploader") ?: json.str("channel") ?: json.str("uploader_id"),
            thumbnail = json.str("thumbnail"),
            durationSec = duration.toLong(),
            qualities = qualities,
        )
    }

    /**
     * Descarga a una carpeta temporal y devuelve el archivo final.
     * [onProgress] recibe (porcentaje 0..100, segundos restantes).
     */
    suspend fun download(
        ctx: Context,
        url: String,
        choice: Choice,
        id: String,
        onProgress: (Float, Long) -> Unit,
    ): File = withContext(Dispatchers.IO) {
        awaitReady()
        val dir = File(ctx.cacheDir, "dl/$id").apply { mkdirs() }

        val req = YoutubeDLRequest(url).apply {
            addOption("--no-playlist")
            addOption("-o", "${dir.absolutePath}/%(title).80B.%(ext)s")
            addOption("--concurrent-fragments", "4")
            addOption("--retries", "10")
            addOption("--embed-metadata")
            AppSettings.speedLimit.value.takeIf { it.isNotBlank() }?.let { addOption("--limit-rate", it) }

            if (choice.audioOnly) {
                addOption("-f", "bestaudio/best")
                addOption("-x")
                addOption("--audio-format", choice.audioFormat)
                addOption("--audio-quality", "0")
            } else {
                val h = choice.height
                val selector = if (h != null) "bv*[height<=$h]+ba/b[height<=$h]/bv*+ba/b" else "bv*+ba/b"
                addOption("-f", selector)
                // Prioriza resolución y, a igualdad, códecs compatibles con cualquier reproductor
                addOption("-S", "res,vcodec:h264,acodec:m4a")
                addOption("--merge-output-format", "mp4")
            }
        }

        YoutubeDL.getInstance().execute(req, id) { progress, eta, _ ->
            onProgress(progress, eta)
        }

        dir.listFiles()
            ?.filter { it.isFile && !it.name.endsWith(".part") && !it.name.endsWith(".ytdl") }
            ?.maxByOrNull { it.length() }
            ?: error("La descarga terminó pero no se generó ningún archivo")
    }

    fun cancel(id: String) {
        runCatching { YoutubeDL.getInstance().destroyProcessById(id) }
    }

    fun cleanup(ctx: Context, id: String) {
        File(ctx.cacheDir, "dl/$id").deleteRecursively()
    }
}
