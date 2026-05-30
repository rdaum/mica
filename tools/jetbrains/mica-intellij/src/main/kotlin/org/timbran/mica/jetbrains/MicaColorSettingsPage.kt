package org.timbran.mica.jetbrains

import com.intellij.openapi.editor.colors.TextAttributesKey
import com.intellij.openapi.fileTypes.SyntaxHighlighter
import com.intellij.openapi.options.colors.AttributesDescriptor
import com.intellij.openapi.options.colors.ColorDescriptor
import com.intellij.openapi.options.colors.ColorSettingsPage
import javax.swing.Icon

class MicaColorSettingsPage : ColorSettingsPage {
    companion object {
        private val DESCRIPTORS = arrayOf(
            AttributesDescriptor("Keyword", MicaSyntaxHighlighter.KEYWORD),
            AttributesDescriptor("String", MicaSyntaxHighlighter.STRING),
            AttributesDescriptor("Number", MicaSyntaxHighlighter.NUMBER),
            AttributesDescriptor("Comment", MicaSyntaxHighlighter.COMMENT),
            AttributesDescriptor("Identifier", MicaSyntaxHighlighter.IDENTIFIER),
            AttributesDescriptor("Symbol", MicaSyntaxHighlighter.SYMBOL_LITERAL),
            AttributesDescriptor("Identity", MicaSyntaxHighlighter.IDENTITY_LITERAL),
            AttributesDescriptor("Operator", MicaSyntaxHighlighter.OPERATION_SIGN),
            AttributesDescriptor("Parentheses", MicaSyntaxHighlighter.PARENTHESES),
            AttributesDescriptor("Brackets", MicaSyntaxHighlighter.BRACKETS),
            AttributesDescriptor("Braces", MicaSyntaxHighlighter.BRACES),
            AttributesDescriptor("Comma", MicaSyntaxHighlighter.COMMA),
            AttributesDescriptor("Semicolon", MicaSyntaxHighlighter.SEMICOLON),
            AttributesDescriptor("Dot", MicaSyntaxHighlighter.DOT),
            AttributesDescriptor("Error code", MicaSyntaxHighlighter.ERROR_CODE),
            AttributesDescriptor("Bad character", MicaSyntaxHighlighter.BAD_CHARACTER)
        )
    }

    override fun getAttributeDescriptors(): Array<AttributesDescriptor> = DESCRIPTORS

    override fun getColorDescriptors(): Array<ColorDescriptor> = ColorDescriptor.EMPTY_ARRAY

    override fun getDisplayName(): String = "Mica"

    override fun getIcon(): Icon = MicaIcons.FILE

    override fun getHighlighter(): SyntaxHighlighter = MicaSyntaxHighlighter()

    override fun getDemoText(): String = """
        // This is a comment in Mica
        let x = 42;
        let y = 3.14159;
        let name = "Mica Language";
        
        #lamp.state = :on;
        
        verb :polish(actor, item)
            if not actor.can_reach(item) then
                raise E_NOT_REACHABLE;
            end
            item.cleanliness = item.cleanliness + 10;
        end
        
        VisibleTo(a, b) :- LocatedIn(a, room) && LocatedIn(b, room);
    """.trimIndent()

    override fun getAdditionalHighlightingTagToDescriptorMap(): Map<String, TextAttributesKey>? = null
}
