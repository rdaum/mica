package org.timbran.mica.jetbrains

import com.intellij.testFramework.fixtures.BasePlatformTestCase

class MicaFileTypeTest : BasePlatformTestCase() {
    fun testMicaFileType() {
        val psiFile = myFixture.configureByText("temp.mica", "")
        val underlyingVirtualFile = psiFile.virtualFile
        
        assertNotNull("Underlying VirtualFile should not be null", underlyingVirtualFile)
        assertEquals("Mica files should map to MicaFileType", MicaFileType, underlyingVirtualFile?.fileType)
    }
}



