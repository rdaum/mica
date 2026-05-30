package org.timbran.mica.jetbrains

import com.intellij.openapi.fileTypes.LanguageFileType
import javax.swing.Icon

object MicaFileType : LanguageFileType(MicaLanguage) {
    override fun getName(): String = "Mica"

    override fun getDescription(): String = "Mica language file"

    override fun getDefaultExtension(): String = "mica"

    override fun getIcon(): Icon = MicaIcons.FILE
}
