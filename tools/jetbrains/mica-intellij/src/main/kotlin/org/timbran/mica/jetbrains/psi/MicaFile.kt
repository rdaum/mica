package org.timbran.mica.jetbrains.psi

import com.intellij.extapi.psi.PsiFileBase
import com.intellij.openapi.fileTypes.FileType
import com.intellij.psi.FileViewProvider
import org.timbran.mica.jetbrains.MicaFileType
import org.timbran.mica.jetbrains.MicaLanguage

class MicaFile(viewProvider: FileViewProvider) : PsiFileBase(viewProvider, MicaLanguage) {
    override fun getFileType(): FileType {
        return MicaFileType
    }

    override fun toString(): String {
        return "Mica File"
    }
}
