package org.timbran.mica.jetbrains

import com.intellij.lexer.FlexAdapter
import com.intellij.psi.tree.IElementType
import com.intellij.psi.TokenType
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test
import org.timbran.mica.jetbrains.MicaElementTypes.*

class MicaLexerTest {
    @Test
    fun testLexerTokens() {
        val lexer = FlexAdapter(_MicaLexer(null))
        val text = "[1, @xs] {:lit -> true}"
        lexer.start(text)
        
        val tokens = mutableListOf<IElementType>()
        while (lexer.tokenType != null) {
            tokens.add(lexer.tokenType!!)
            lexer.advance()
        }
        
        val expected = listOf(
            LBRACKET, INT, COMMA, TokenType.WHITE_SPACE, AT, IDENT, RBRACKET,
            TokenType.WHITE_SPACE, LBRACE, COLON, IDENT, TokenType.WHITE_SPACE, ARROW,
            TokenType.WHITE_SPACE, TRUE_KW, RBRACE
        )
        assertEquals(expected, tokens)
    }

    @Test
    fun testRelationRuleArrow() {
        val lexer = FlexAdapter(_MicaLexer(null))
        val text = "VisibleTo(a, b) :- LocatedIn(a, room)"
        lexer.start(text)
        
        val tokens = mutableListOf<IElementType>()
        while (lexer.tokenType != null) {
            tokens.add(lexer.tokenType!!)
            lexer.advance()
        }
        
        assertTrue(tokens.contains(COLON_DASH))
    }

    @Test
    fun testUnterminatedString() {
        val lexer = FlexAdapter(_MicaLexer(null))
        val text = "\"hello"
        lexer.start(text)
        
        val tokens = mutableListOf<IElementType>()
        while (lexer.tokenType != null) {
            tokens.add(lexer.tokenType!!)
            lexer.advance()
        }
        assertEquals(listOf(STRING), tokens)
    }

    @Test
    fun testFloatTrailingDot() {
        val lexer = FlexAdapter(_MicaLexer(null))
        val text = "1. \n1.. \n3.14"
        lexer.start(text)
        
        val tokens = mutableListOf<IElementType>()
        while (lexer.tokenType != null) {
            tokens.add(lexer.tokenType!!)
            lexer.advance()
        }
        
        val expected = listOf(
            FLOAT, TokenType.WHITE_SPACE, NEWLINE,
            INT, DOT_DOT, TokenType.WHITE_SPACE, NEWLINE,
            FLOAT
        )
        assertEquals(expected, tokens)
    }

    @Test
    fun testFloatTrailingDotAtEOF() {
        val lexer = FlexAdapter(_MicaLexer(null))
        val text = "1."
        lexer.start(text)
        
        val tokens = mutableListOf<IElementType>()
        while (lexer.tokenType != null) {
            tokens.add(lexer.tokenType!!)
            lexer.advance()
        }
        assertEquals(listOf(FLOAT), tokens)
    }
}
