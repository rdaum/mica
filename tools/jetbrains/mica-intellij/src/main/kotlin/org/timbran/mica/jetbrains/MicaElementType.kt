package org.timbran.mica.jetbrains

import com.intellij.psi.tree.IElementType
import org.jetbrains.annotations.NonNls

class MicaElementType(debugName: @NonNls String) : IElementType(debugName, MicaLanguage)
