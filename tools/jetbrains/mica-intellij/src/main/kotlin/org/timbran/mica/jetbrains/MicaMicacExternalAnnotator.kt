package org.timbran.mica.jetbrains

import com.google.gson.JsonParser
import com.intellij.lang.annotation.AnnotationHolder
import com.intellij.lang.annotation.ExternalAnnotator
import com.intellij.lang.annotation.HighlightSeverity
import com.intellij.openapi.util.TextRange
import com.intellij.psi.PsiFile
import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.TimeUnit

class MicaMicacExternalAnnotator : ExternalAnnotator<MicaMicacInput, MicaMicacResult>() {
    override fun collectInformation(file: PsiFile): MicaMicacInput? {
        if (file.fileType != MicaFileType) {
            return null
        }
        val includeDirectory = file.virtualFile?.parent?.path?.let { Path.of(it) }
        return MicaMicacInput(file.text, includeDirectory, configuredEnvironmentFileins())
    }

    override fun doAnnotate(collectedInfo: MicaMicacInput?): MicaMicacResult? {
        val input = collectedInfo ?: return null
        return runMicac(input)
    }

    override fun apply(file: PsiFile, annotationResult: MicaMicacResult?, holder: AnnotationHolder) {
        val result = annotationResult ?: return
        when (result) {
            is MicaMicacResult.Ok -> Unit
            is MicaMicacResult.ToolUnavailable -> {
                holder.newAnnotation(HighlightSeverity.WARNING, result.message)
                    .fileLevel()
                    .create()
            }
            is MicaMicacResult.Errors -> {
                if (result.diagnostics.isEmpty()) {
                    holder.newAnnotation(HighlightSeverity.ERROR, result.message)
                        .fileLevel()
                        .create()
                    return
                }
                for (diagnostic in result.diagnostics) {
                    val range = diagnostic.span?.toTextRange(file.textLength)
                    val builder = holder.newAnnotation(HighlightSeverity.ERROR, diagnostic.message)
                    if (range == null) {
                        builder.fileLevel().create()
                    } else {
                        builder.range(range).create()
                    }
                }
            }
        }
    }
}

data class MicaMicacInput(
    val text: String,
    val includeDirectory: Path?,
    val environmentFileins: List<String>,
)

sealed interface MicaMicacResult {
    data object Ok : MicaMicacResult

    data class Errors(
        val message: String,
        val file: String?,
        val diagnostics: List<MicaMicacDiagnostic>,
    ) : MicaMicacResult

    data class ToolUnavailable(
        val message: String,
    ) : MicaMicacResult
}

data class MicaMicacDiagnostic(
    val title: String,
    val message: String,
    val span: MicaMicacSpan?,
)

data class MicaMicacSpan(
    val start: Int,
    val end: Int,
) {
    fun toTextRange(textLength: Int): TextRange {
        val clampedStart = start.coerceIn(0, textLength)
        val clampedEnd = end.coerceIn(clampedStart, textLength)
        if (clampedStart < clampedEnd) {
            return TextRange(clampedStart, clampedEnd)
        }
        if (clampedStart < textLength) {
            return TextRange(clampedStart, clampedStart + 1)
        }
        if (clampedStart > 0) {
            return TextRange(clampedStart - 1, clampedStart)
        }
        return TextRange(0, 0)
    }
}

fun parseMicacJsonOutput(text: String): MicaMicacResult.Errors? {
    val root = runCatching { JsonParser.parseString(text).asJsonObject }.getOrNull() ?: return null
    if (root["status"]?.asString != "error") {
        return null
    }
    val message = root["message"]?.asString ?: "micac reported an error"
    val file = root["file"]?.asString
    val diagnostics = root["diagnostics"]
        ?.takeIf { it.isJsonArray }
        ?.asJsonArray
        ?.mapNotNull { element ->
            val diagnostic = element.takeIf { it.isJsonObject }?.asJsonObject ?: return@mapNotNull null
            val span = diagnostic["span"]
                ?.takeIf { it.isJsonObject }
                ?.asJsonObject
                ?.let { spanObject ->
                    val start = spanObject["start"]?.asInt ?: return@let null
                    val end = spanObject["end"]?.asInt ?: return@let null
                    MicaMicacSpan(start, end)
                }
            MicaMicacDiagnostic(
                title = diagnostic["title"]?.asString ?: "error",
                message = diagnostic["message"]?.asString ?: message,
                span = span,
            )
        }
        ?: emptyList()
    return MicaMicacResult.Errors(message, file, diagnostics)
}

private fun runMicac(input: MicaMicacInput): MicaMicacResult {
    val source = createTemporarySource(input.includeDirectory)
    return try {
        Files.writeString(source, input.text)
        val command = micacCommand(input.environmentFileins, source)
        val process = ProcessBuilder(command)
            .redirectOutput(ProcessBuilder.Redirect.PIPE)
            .redirectError(ProcessBuilder.Redirect.PIPE)
            .start()
        if (!process.waitFor(5, TimeUnit.SECONDS)) {
            process.destroyForcibly()
            return MicaMicacResult.ToolUnavailable("micac did not finish within 5 seconds")
        }
        val stderr = process.errorStream.bufferedReader().readText()
        val stdout = process.inputStream.bufferedReader().readText()
        if (process.exitValue() == 0) {
            MicaMicacResult.Ok
        } else {
            val parsed = parseMicacJsonOutput(stderr)
            if (parsed != null && parsed.file != null && parsed.file != source.toString()) {
                MicaMicacResult.Errors(
                    "Environment filein ${parsed.file} failed:\n${parsed.message}",
                    parsed.file,
                    emptyList(),
                )
            } else {
                parsed ?: MicaMicacResult.Errors((stderr.ifBlank { stdout }).trim(), null, emptyList())
            }
        }
    } catch (error: Exception) {
        MicaMicacResult.ToolUnavailable(
            "micac is not available; install it with `cargo install --path crates/micac` or set MICA_MICAC",
        )
    } finally {
        Files.deleteIfExists(source)
    }
}

fun micacCommand(environmentFileins: List<String>, currentFile: Path): List<String> {
    val command = mutableListOf(micacExecutable(), "--check", "--format", "json")
    for (filein in environmentFileins) {
        command.add("--filein")
        command.add(filein)
    }
    command.add("--filein")
    command.add(currentFile.toString())
    return command
}

private fun micacExecutable(): String {
    val configuredPath = runCatching { MicaSettings.getInstance().state.micacPath.trim() }
        .getOrDefault("")
    if (configuredPath.isNotEmpty()) {
        return configuredPath
    }
    return System.getProperty("mica.micac.path")
        ?: System.getenv("MICA_MICAC")
        ?: "micac"
}

private fun configuredEnvironmentFileins(): List<String> {
    return runCatching { MicaSettings.getInstance().state.environmentFileins }
        .getOrDefault("")
        .lines()
        .map { it.trim() }
        .filter { it.isNotEmpty() && !it.startsWith("#") }
}

private fun createTemporarySource(includeDirectory: Path?): Path {
    if (includeDirectory != null) {
        val source = runCatching { Files.createTempFile(includeDirectory, ".mica-intellij-", ".mica") }
            .getOrNull()
        if (source != null) {
            return source
        }
    }
    return Files.createTempFile("mica-intellij-", ".mica")
}
