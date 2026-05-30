package org.timbran.mica.jetbrains

import com.intellij.psi.tree.IElementType
import org.jetbrains.annotations.NonNls

class MicaTokenType(debugName: @NonNls String) : IElementType(debugName, MicaLanguage) {
    override fun toString(): String = "MicaTokenType." + super.toString()
}
