package org.timbran.mica.jetbrains

import com.intellij.testFramework.fixtures.BasePlatformTestCase

class MicaMicacExternalAnnotatorTest : BasePlatformTestCase() {
    fun testParseMicacJsonDiagnostics() {
        val result = parseMicacJsonOutput(
            """
            {
              "status": "error",
              "kind": "source",
              "file": "/tmp/bad.mica",
              "message": "Error: parse error",
              "diagnostics": [
                {
                  "title": "parse error",
                  "message": "expected end after verb body",
                  "span": {
                    "start": 5,
                    "end": 5
                  }
                }
              ]
            }
            """.trimIndent()
        )

        assertNotNull(result)
        assertEquals("Error: parse error", result!!.message)
        assertEquals("/tmp/bad.mica", result.file)
        assertEquals(1, result.diagnostics.size)
        assertEquals("parse error", result.diagnostics[0].title)
        assertEquals("expected end after verb body", result.diagnostics[0].message)
        assertEquals(MicaMicacSpan(5, 5), result.diagnostics[0].span)
    }

    fun testParseMicacJsonPreservesMultipleDiagnostics() {
        val result = parseMicacJsonOutput(
            """
            {
              "status": "error",
              "kind": "source",
              "file": "/tmp/bad.mica",
              "message": "Error: multiple context errors",
              "diagnostics": [
                {
                  "title": "unknown identity",
                  "message": "unknown identity `#missing_one`",
                  "span": {
                    "start": 10,
                    "end": 22
                  }
                },
                {
                  "title": "unknown relation",
                  "message": "unknown relation `MissingRelation`",
                  "span": {
                    "start": 30,
                    "end": 45
                  }
                }
              ]
            }
            """.trimIndent()
        )

        assertNotNull(result)
        assertEquals(2, result!!.diagnostics.size)
        assertEquals("unknown identity `#missing_one`", result.diagnostics[0].message)
        assertEquals(MicaMicacSpan(10, 22), result.diagnostics[0].span)
        assertEquals("unknown relation `MissingRelation`", result.diagnostics[1].message)
        assertEquals(MicaMicacSpan(30, 45), result.diagnostics[1].span)
    }

    fun testZeroWidthSpanExpandsToVisibleRange() {
        assertEquals(5, MicaMicacSpan(5, 5).toTextRange(10).startOffset)
        assertEquals(6, MicaMicacSpan(5, 5).toTextRange(10).endOffset)
        assertEquals(9, MicaMicacSpan(10, 10).toTextRange(10).startOffset)
        assertEquals(10, MicaMicacSpan(10, 10).toTextRange(10).endOffset)
    }

    fun testMicacCommandAddsEnvironmentFileinsBeforeCurrentFile() {
        val command = micacCommand(
            listOf("/project/base.mica", "/project/world.mica"),
            java.nio.file.Path.of("/tmp/current.mica"),
        )

        assertEquals(
            listOf(
                "micac",
                "--check",
                "--format",
                "json",
                "--filein",
                "/project/base.mica",
                "--filein",
                "/project/world.mica",
                "--filein",
                "/tmp/current.mica",
            ),
            command,
        )
    }
}
