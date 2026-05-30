package org.timbran.mica.jetbrains

import com.intellij.psi.util.PsiTreeUtil
import com.intellij.testFramework.fixtures.BasePlatformTestCase
import org.timbran.mica.jetbrains.psi.MicaMethodItem
import org.timbran.mica.jetbrains.psi.MicaRelationRule
import org.timbran.mica.jetbrains.psi.MicaVerbItem

class MicaParserTest : BasePlatformTestCase() {
    fun testVerbEnvelope() {
        val text = """
            verb get(actor @ #player, item @ #thing)
              if Portable(item)
                return true
              else
                return false
              end
            end
        """.trimIndent()
        val psiFile = myFixture.configureByText("test.mica", text)
        
        val verbItem = PsiTreeUtil.findChildOfType(psiFile, MicaVerbItem::class.java)
        assertNotNull("Should parse verb item", verbItem)
    }

    fun testMethodEnvelope() {
        val text = """
            method #move_into :move
              roles actor @ #player, item @ #portable
            do
              require CanMove(actor, item)
              assert LocatedIn(item, destination)
            end
        """.trimIndent()
        val psiFile = myFixture.configureByText("test.mica", text)
        
        val methodItem = PsiTreeUtil.findChildOfType(psiFile, MicaMethodItem::class.java)
        assertNotNull("Should parse method item", methodItem)
    }

    fun testRelationRule() {
        val text = """
            VisibleTo(actor, obj) :- LocatedIn(actor, room), LocatedIn(obj, room)
        """.trimIndent()
        val psiFile = myFixture.configureByText("test.mica", text)
        
        val relationRule = PsiTreeUtil.findChildOfType(psiFile, MicaRelationRule::class.java)
        assertNotNull("Should parse relation rule", relationRule)
    }
}
