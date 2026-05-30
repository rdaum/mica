package org.timbran.mica.jetbrains

import com.intellij.lexer.FlexAdapter
import com.intellij.lexer.Lexer
import com.intellij.openapi.editor.DefaultLanguageHighlighterColors
import com.intellij.openapi.editor.colors.TextAttributesKey
import com.intellij.openapi.fileTypes.SyntaxHighlighter
import com.intellij.openapi.fileTypes.SyntaxHighlighterBase
import com.intellij.openapi.fileTypes.SyntaxHighlighterFactory
import com.intellij.openapi.project.Project
import com.intellij.openapi.vfs.VirtualFile
import com.intellij.psi.tree.IElementType
import org.timbran.mica.jetbrains.MicaElementTypes.*

class MicaSyntaxHighlighter : SyntaxHighlighterBase() {
    companion object {
        val KEYWORD = TextAttributesKey.createTextAttributesKey("MICA_KEYWORD", DefaultLanguageHighlighterColors.KEYWORD)
        val STRING = TextAttributesKey.createTextAttributesKey("MICA_STRING", DefaultLanguageHighlighterColors.STRING)
        val NUMBER = TextAttributesKey.createTextAttributesKey("MICA_NUMBER", DefaultLanguageHighlighterColors.NUMBER)
        val COMMENT = TextAttributesKey.createTextAttributesKey("MICA_COMMENT", DefaultLanguageHighlighterColors.LINE_COMMENT)
        val IDENTIFIER = TextAttributesKey.createTextAttributesKey("MICA_IDENTIFIER", DefaultLanguageHighlighterColors.IDENTIFIER)
        val OPERATION_SIGN = TextAttributesKey.createTextAttributesKey("MICA_OPERATION_SIGN", DefaultLanguageHighlighterColors.OPERATION_SIGN)
        val PARENTHESES = TextAttributesKey.createTextAttributesKey("MICA_PARENTHESES", DefaultLanguageHighlighterColors.PARENTHESES)
        val BRACKETS = TextAttributesKey.createTextAttributesKey("MICA_BRACKETS", DefaultLanguageHighlighterColors.BRACKETS)
        val BRACES = TextAttributesKey.createTextAttributesKey("MICA_BRACES", DefaultLanguageHighlighterColors.BRACES)
        val COMMA = TextAttributesKey.createTextAttributesKey("MICA_COMMA", DefaultLanguageHighlighterColors.COMMA)
        val SEMICOLON = TextAttributesKey.createTextAttributesKey("MICA_SEMICOLON", DefaultLanguageHighlighterColors.SEMICOLON)
        val DOT = TextAttributesKey.createTextAttributesKey("MICA_DOT", DefaultLanguageHighlighterColors.DOT)
        val ERROR_CODE = TextAttributesKey.createTextAttributesKey("MICA_ERROR_CODE", DefaultLanguageHighlighterColors.METADATA)
        val BAD_CHARACTER = TextAttributesKey.createTextAttributesKey("MICA_BAD_CHARACTER", DefaultLanguageHighlighterColors.INVALID_STRING_ESCAPE)

        private val KEYWORD_KEYS = arrayOf(KEYWORD)
        private val STRING_KEYS = arrayOf(STRING)
        private val NUMBER_KEYS = arrayOf(NUMBER)
        private val COMMENT_KEYS = arrayOf(COMMENT)
        private val IDENTIFIER_KEYS = arrayOf(IDENTIFIER)
        private val OPERATION_SIGN_KEYS = arrayOf(OPERATION_SIGN)
        private val PARENTHESES_KEYS = arrayOf(PARENTHESES)
        private val BRACKETS_KEYS = arrayOf(BRACKETS)
        private val BRACES_KEYS = arrayOf(BRACES)
        private val COMMA_KEYS = arrayOf(COMMA)
        private val SEMICOLON_KEYS = arrayOf(SEMICOLON)
        private val DOT_KEYS = arrayOf(DOT)
        private val ERROR_CODE_KEYS = arrayOf(ERROR_CODE)
        private val BAD_CHARACTER_KEYS = arrayOf(BAD_CHARACTER)
        private val EMPTY_KEYS = emptyArray<TextAttributesKey>()
    }

    override fun getHighlightingLexer(): Lexer {
        return FlexAdapter(_MicaLexer(null))
    }

    override fun getTokenHighlights(tokenType: IElementType): Array<TextAttributesKey> {
        return when {
            MicaTokenSets.KEYWORDS.contains(tokenType) -> KEYWORD_KEYS
            MicaTokenSets.COMMENTS.contains(tokenType) -> COMMENT_KEYS
            MicaTokenSets.STRINGS.contains(tokenType) -> STRING_KEYS
            tokenType == INT || tokenType == FLOAT -> NUMBER_KEYS
            tokenType == org.timbran.mica.jetbrains.MicaElementTypes.ERROR_CODE -> ERROR_CODE_KEYS
            tokenType == IDENT || tokenType == UNDERSCORE -> IDENTIFIER_KEYS
            tokenType == LPAREN || tokenType == RPAREN -> PARENTHESES_KEYS
            tokenType == LBRACKET || tokenType == RBRACKET -> BRACKETS_KEYS
            tokenType == LBRACE || tokenType == RBRACE -> BRACES_KEYS
            tokenType == org.timbran.mica.jetbrains.MicaElementTypes.COMMA -> COMMA_KEYS
            tokenType == SEMI -> SEMICOLON_KEYS
            tokenType == org.timbran.mica.jetbrains.MicaElementTypes.DOT -> DOT_KEYS
            tokenType == com.intellij.psi.TokenType.BAD_CHARACTER -> BAD_CHARACTER_KEYS
            
            // Operators and punctuation
            tokenType == EQ ||
            tokenType == EQ_EQ ||
            tokenType == BANG_EQ ||
            tokenType == LT ||
            tokenType == LT_EQ ||
            tokenType == GT ||
            tokenType == GT_EQ ||
            tokenType == PLUS ||
            tokenType == MINUS ||
            tokenType == STAR ||
            tokenType == SLASH ||
            tokenType == PERCENT ||
            tokenType == AMP_AMP ||
            tokenType == PIPE_PIPE ||
            tokenType == BANG ||
            tokenType == ARROW ||
            tokenType == FAT_ARROW ||
            tokenType == COLON_DASH ||
            tokenType == COLON ||
            tokenType == DOT_DOT ||
            tokenType == HASH ||
            tokenType == AT ||
            tokenType == QUESTION -> OPERATION_SIGN_KEYS

            else -> EMPTY_KEYS
        }
    }
}

class MicaSyntaxHighlighterFactory : SyntaxHighlighterFactory() {
    override fun getSyntaxHighlighter(project: Project?, virtualFile: VirtualFile?): SyntaxHighlighter {
        return MicaSyntaxHighlighter()
    }
}
