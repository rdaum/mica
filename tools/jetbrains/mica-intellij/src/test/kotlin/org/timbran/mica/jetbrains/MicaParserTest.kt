package org.timbran.mica.jetbrains

import com.intellij.psi.util.PsiTreeUtil
import com.intellij.psi.PsiErrorElement
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
        assertNull("Should not produce parser errors", PsiTreeUtil.findChildOfType(psiFile, PsiErrorElement::class.java))
    }

    fun testSlashQualifiedRelationRule() {
        val text = """
            command/TrustedForGrammar(actor, provider) :-
              Delegates(actor, #player, ?distance),
              command/TrustedForGrammar(#player, provider)
        """.trimIndent()
        val psiFile = myFixture.configureByText("test.mica", text)

        val relationRule = PsiTreeUtil.findChildOfType(psiFile, MicaRelationRule::class.java)
        assertNotNull("Should parse slash-qualified relation rule", relationRule)
        assertNull("Should not produce parser errors", PsiTreeUtil.findChildOfType(psiFile, PsiErrorElement::class.java))
    }

    fun testMultilineRelationRuleWithLowercaseVariableNames() {
        val text = """
            CanRead(actor, relation) :-
              HasRole(actor, role),
              RoleCanRead(role, surface, relation),
              RelationInSurface(surface, relation)
        """.trimIndent()
        val psiFile = myFixture.configureByText("test.mica", text)

        val relationRule = PsiTreeUtil.findChildOfType(psiFile, MicaRelationRule::class.java)
        assertNotNull("Should parse multiline relation rule", relationRule)
        assertNull("Should not produce parser errors", PsiTreeUtil.findChildOfType(psiFile, PsiErrorElement::class.java))
    }

    fun testCapabilitiesFileRelationRules() {
        val text = java.nio.file.Files.readString(
            java.nio.file.Path.of("../../../apps/shared/capabilities.mica").toAbsolutePath().normalize()
        )
        val psiFile = myFixture.configureByText("capabilities.mica", text)

        assertTrue(
            "Should parse capabilities relation rules",
            PsiTreeUtil.findChildrenOfType(psiFile, MicaRelationRule::class.java).size >= 4
        )
        assertNull("Should not produce parser errors", PsiTreeUtil.findChildOfType(psiFile, PsiErrorElement::class.java))
    }

    fun testConsecutiveMultilineRelationRules() {
        val text = """
            CanRead(actor, relation) :-
              HasRole(actor, role),
              RoleCanRead(role, surface),
              RelationInSurface(surface, relation)

            CanWrite(actor, relation) :-
              HasRole(actor, role),
              RoleCanWrite(role, surface),
              RelationInSurface(surface, relation)

            CanInvoke(actor, selector) :-
              HasRole(actor, role),
              RoleCanInvoke(role, surface),
              SelectorInSurface(surface, selector)
        """.trimIndent()
        val psiFile = myFixture.configureByText("capabilities-slice.mica", text)

        assertEquals(3, PsiTreeUtil.findChildrenOfType(psiFile, MicaRelationRule::class.java).size)
        assertNull("Should not produce parser errors", PsiTreeUtil.findChildOfType(psiFile, PsiErrorElement::class.java))
    }

}
