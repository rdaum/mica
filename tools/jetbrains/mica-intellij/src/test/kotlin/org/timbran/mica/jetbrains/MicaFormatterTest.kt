package org.timbran.mica.jetbrains

import com.intellij.openapi.command.WriteCommandAction
import com.intellij.psi.codeStyle.CodeStyleManager
import com.intellij.testFramework.fixtures.BasePlatformTestCase
import org.timbran.mica.jetbrains.psi.MicaFile

class MicaFormatterTest : BasePlatformTestCase() {
    private fun doTest(unformatted: String, expected: String) {
        val psiFile = myFixture.configureByText("test.mica", unformatted.trimIndent())
        assertTrue(psiFile is MicaFile)

        WriteCommandAction.runWriteCommandAction(project) {
            CodeStyleManager.getInstance(project).reformat(psiFile)
        }
        
        assertEquals(expected.trimIndent(), psiFile.text.trim())
    }

    fun testMicaDefaultsUseTwoSpaceIndent() {
        val settings = com.intellij.psi.codeStyle.CodeStyleSettingsManager.getSettings(project)
        val indentOptions = settings.getCommonSettings(MicaLanguage).indentOptions
        assertNotNull(indentOptions)
        assertEquals(2, indentOptions?.INDENT_SIZE)
        assertEquals(2, indentOptions?.TAB_SIZE)
        assertEquals(2, indentOptions?.CONTINUATION_INDENT_SIZE)
        assertFalse("Mica should use spaces by default", indentOptions?.USE_TAB_CHARACTER ?: true)
    }

    fun testVerbReformat() {
        val unformatted = """
        verb get(actor @ #player, item @ #thing)
        if Portable(item)
        return true
        else
        return false
        end
        end
        """
        
        val expected = """
        verb get(actor @ #player, item @ #thing)
          if Portable(item)
            return true
          else
            return false
          end
        end
        """
        
        doTest(unformatted, expected)
    }

    fun testMethodReformat() {
        val unformatted = """
        method #move_into :move
        roles actor @ #player, item @ #portable
        do
        require CanMove(actor, item)
        end
        """
        
        val expected = """
        method #move_into :move
          roles actor @ #player, item @ #portable
        do
          require CanMove(actor, item)
        end
        """
        
        doTest(unformatted, expected)
    }

    fun testRelationRuleReformat() {
        val unformatted = """
        VisibleTo(actor, obj) :-
        LocatedIn(actor, room),
        LocatedIn(obj, room)
        """
        
        val expected = """
        VisibleTo(actor, obj) :-
          LocatedIn(actor, room),
          LocatedIn(obj, room)
        """
        
        doTest(unformatted, expected)
    }

    fun testCommaContinuationReformat() {
        val unformatted = """
        roles actor @ #player,
        item @ #portable
        """
        
        val expected = """
        roles actor @ #player,
          item @ #portable
        """
        
        doTest(unformatted, expected)
    }

    fun testNestedTryReformat() {
        val unformatted = """
        try
        begin
        return 1
        end
        catch E_Error
        return 0
        end
        """
        
        val expected = """
        try
          begin
            return 1
          end
        catch E_Error
          return 0
        end
        """
        
        doTest(unformatted, expected)
    }

    fun testUnclosedVerbReformatRecovery() {
        val unformatted = """
        verb trim(text)
        let x = 1
        """
        
        val expected = """
        verb trim(text)
          let x = 1
        """
        
        doTest(unformatted, expected)
    }

    fun testFormatterPreservesNonIndentSpacing() {
        val unformatted = """
        verb source/keep_spacing(a, b)
        let result  =  source/call( a,  b )
        return result
        end
        """
        
        val expected = """
        verb source/keep_spacing(a, b)
          let result  =  source/call( a,  b )
          return result
        end
        """
        
        doTest(unformatted, expected)
    }
}
