package org.timbran.mica.jetbrains

import com.intellij.lang.ASTNode
import com.intellij.lang.ParserDefinition
import com.intellij.lang.PsiParser
import com.intellij.lexer.FlexAdapter
import com.intellij.lexer.Lexer
import com.intellij.openapi.project.Project
import com.intellij.psi.FileViewProvider
import com.intellij.psi.PsiElement
import com.intellij.psi.PsiFile
import com.intellij.psi.tree.IFileElementType
import com.intellij.psi.tree.TokenSet
import org.timbran.mica.jetbrains.parser.MicaParser
import org.timbran.mica.jetbrains.psi.MicaFile

class MicaParserDefinition : ParserDefinition {
    companion object {
        val FILE = IFileElementType(MicaLanguage)
    }

    override fun createLexer(project: Project?): Lexer {
        return FlexAdapter(_MicaLexer(null))
    }

    override fun createParser(project: Project?): PsiParser {
        return MicaParser()
    }

    override fun getFileNodeType(): IFileElementType {
        return FILE
    }

    override fun getWhitespaceTokens(): TokenSet {
        val isFormatting = Thread.currentThread().stackTrace.any {
            it.className.contains("formatter") || it.className.contains("formatting")
        }
        return if (isFormatting) {
            TokenSet.create(com.intellij.psi.TokenType.WHITE_SPACE, MicaElementTypes.NEWLINE)
        } else {
            TokenSet.create(com.intellij.psi.TokenType.WHITE_SPACE)
        }
    }

    override fun getCommentTokens(): TokenSet {
        return MicaTokenSets.COMMENTS
    }

    override fun getStringLiteralElements(): TokenSet {
        return MicaTokenSets.STRINGS
    }

    override fun createElement(node: ASTNode?): PsiElement {
        return MicaElementTypes.Factory.createElement(node)
    }

    override fun createFile(viewProvider: FileViewProvider): PsiFile {
        return MicaFile(viewProvider)
    }
}
