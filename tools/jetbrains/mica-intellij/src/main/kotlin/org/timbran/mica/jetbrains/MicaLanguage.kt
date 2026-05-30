package org.timbran.mica.jetbrains

import com.intellij.lang.Language

object MicaLanguage : Language("Mica") {
    private fun readResolve(): Any = MicaLanguage
}
