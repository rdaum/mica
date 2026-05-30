package org.timbran.mica.jetbrains

import com.intellij.formatting.*
import com.intellij.lang.ASTNode
import com.intellij.psi.formatter.common.AbstractBlock
import com.intellij.psi.codeStyle.CodeStyleSettings
import org.timbran.mica.jetbrains.psi.*
import java.util.ArrayList

class MicaFormattingModelBuilder : FormattingModelBuilder {
    override fun createModel(formattingContext: FormattingContext): FormattingModel {
        val settings = formattingContext.codeStyleSettings
        return FormattingModelProvider.createFormattingModelForPsiFile(
            formattingContext.containingFile,
            MicaFormattingBlock(formattingContext.node, null, null, settings),
            settings
        )
    }
}

class MicaFormattingBlock(
    node: ASTNode,
    wrap: Wrap?,
    alignment: Alignment?,
    private val settings: CodeStyleSettings
) : AbstractBlock(node, wrap, alignment) {

    private val spacingBuilder = SpacingBuilder(settings, MicaLanguage)

    override fun buildChildren(): List<Block> {
        val blocks = ArrayList<Block>()
        var child = myNode.firstChildNode
        while (child != null) {
            if (child.elementType !== com.intellij.psi.TokenType.WHITE_SPACE) {
                blocks.add(MicaFormattingBlock(child, null, null, settings))
            }
            child = child.treeNext
        }
        return blocks
    }

    private fun endsWithComma(node: ASTNode): Boolean {
        var lastLeaf = node
        while (lastLeaf.firstChildNode != null) {
            lastLeaf = lastLeaf.lastChildNode
        }
        return lastLeaf.elementType === MicaElementTypes.COMMA
    }

    private fun endsWithNewline(block: Block): Boolean {
        val node = (block as? ASTBlock)?.node ?: return false
        var lastLeaf = node
        while (lastLeaf.firstChildNode != null) {
            lastLeaf = lastLeaf.lastChildNode
        }
        return lastLeaf.elementType === MicaElementTypes.NEWLINE
    }

    private fun nodeGetsIndent(node: ASTNode): Boolean {
        val parent = node.treeParent ?: return false
        val parentPsi = parent.psi
        val psi = node.psi
        val type = node.elementType

        // 1. Boundary Keyword Dedents (end, else, elseif, catch, finally)
        if (type === MicaElementTypes.END_KW ||
            type === MicaElementTypes.ELSE_KW ||
            type === MicaElementTypes.ELSEIF_KW ||
            type === MicaElementTypes.CATCH_KW ||
            type === MicaElementTypes.FINALLY_KW
        ) {
            return false
        }

        // 2. Container Block Indent Prevention (the MicaBlock node itself does not get any indent)
        if (psi is MicaBlock) {
            return false
        }

        // 3. Block Contents: any child statement inside a MicaBlock gets normal indent
        if (parentPsi is MicaBlock) {
            // Exclude separator tokens (newlines and semicolons) from block indentation
            if (type === MicaElementTypes.NEWLINE || type === MicaElementTypes.SEMI) {
                return false
            }
            return true
        }

        // 4. Relation Rule Bodies
        if (parentPsi is MicaRelationRule && psi is MicaRuleBody) {
            return true
        }

        // 5. Comma-Separated Continuations
        if (parentPsi is MicaExpr || parentPsi is MicaMethodHeader || parentPsi is MicaMethodClause) {
            // Check if we are inside a relation rule body to avoid double-indenting continuations
            var isInsideRuleBody = false
            var ancestor: ASTNode? = parent
            while (ancestor != null) {
                if (ancestor.psi is MicaRuleBody) {
                    isInsideRuleBody = true
                    break
                }
                ancestor = ancestor.treeParent
            }

            if (!isInsideRuleBody) {
                var prev = node.treePrev
                while (prev != null) {
                    if (prev.elementType === MicaElementTypes.COMMA) {
                        return true
                    }
                    prev = prev.treePrev
                }
            }
        }

        // 6. Trailing Comma Continuation from Previous Statement (across separate parsed statements)
        var prevStmt = node.treePrev
        while (prevStmt != null &&
            (prevStmt.elementType === MicaElementTypes.NEWLINE ||
             prevStmt.elementType === MicaElementTypes.SEMI ||
             prevStmt.elementType === com.intellij.psi.TokenType.WHITE_SPACE)
        ) {
            prevStmt = prevStmt.treePrev
        }
        if (prevStmt != null && endsWithComma(prevStmt)) {
            return true
        }

        // 7. Method Clauses
        if (parentPsi is MicaMethodItem && psi is MicaMethodClause) {
            return true
        }

        return false
    }

    private fun calculateIndentSpaces(node: ASTNode): Int {
        val indentOptions = settings.getCommonSettings(MicaLanguage).indentOptions
        val indentSize = indentOptions?.INDENT_SIZE ?: 2

        var current: ASTNode? = node
        var spaces = 0
        while (current != null) {
            if (nodeGetsIndent(current)) {
                spaces += indentSize
            }
            current = current.treeParent
        }
        return spaces
    }

    override fun getIndent(): Indent? {
        return if (nodeGetsIndent(myNode)) {
            Indent.getNormalIndent()
        } else {
            Indent.getNoneIndent()
        }
    }

    override fun getSpacing(child1: Block?, child2: Block): Spacing? {
        val node2 = (child2 as? ASTBlock)?.node
        if (node2 != null) {
            val type2 = node2.elementType
            if (type2 === MicaElementTypes.NEWLINE || type2 === MicaElementTypes.SEMI) {
                return Spacing.createSpacing(0, 0, 0, false, 0)
            }
        }

        val node1 = (child1 as? ASTBlock)?.node
        if (node1 != null) {
            val type1 = node1.elementType
            if (type1 === MicaElementTypes.NEWLINE || endsWithNewline(child1)) {
                if (node2 != null) {
                    val spaces = calculateIndentSpaces(node2)
                    return Spacing.createSpacing(spaces, spaces, 0, false, 0)
                }
            }
        }

        return Spacing.getReadOnlySpacing()
    }

    override fun isLeaf(): Boolean {
        return myNode.firstChildNode == null
    }

    override fun getChildAttributes(newChildIndex: Int): ChildAttributes {
        val psi = myNode.psi
        // If cursor is inside a block, method, verb, or structured statement, the next line should be indented
        if (psi is MicaBlock ||
            psi is MicaVerbItem ||
            psi is MicaMethodItem ||
            psi is MicaIfStmt ||
            psi is MicaWhileStmt ||
            psi is MicaForStmt ||
            psi is MicaBeginStmt ||
            psi is MicaTryStmt ||
            psi is MicaFnStmt
        ) {
            return ChildAttributes(Indent.getNormalIndent(), null)
        }
        return ChildAttributes(Indent.getNoneIndent(), null)
    }
}
