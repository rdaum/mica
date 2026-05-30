package org.timbran.mica.jetbrains

import com.intellij.ide.structureView.*
import com.intellij.ide.util.treeView.smartTree.TreeElement
import com.intellij.lang.PsiStructureViewFactory
import com.intellij.navigation.ItemPresentation
import com.intellij.navigation.NavigationItem
import com.intellij.openapi.editor.Editor
import com.intellij.psi.PsiElement
import com.intellij.psi.PsiFile
import com.intellij.psi.util.PsiTreeUtil
import org.timbran.mica.jetbrains.psi.MicaFile
import org.timbran.mica.jetbrains.psi.MicaMethodItem
import org.timbran.mica.jetbrains.psi.MicaRelationRule
import org.timbran.mica.jetbrains.psi.MicaVerbItem
import javax.swing.Icon
import com.intellij.icons.AllIcons

class MicaStructureViewFactory : PsiStructureViewFactory {
    override fun getStructureViewBuilder(psiFile: PsiFile): StructureViewBuilder? {
        if (psiFile !is MicaFile) return null
        return object : TreeBasedStructureViewBuilder() {
            override fun createStructureViewModel(editor: Editor?): StructureViewModel {
                return MicaStructureViewModel(psiFile, editor)
            }
        }
    }
}

class MicaStructureViewModel(psiFile: MicaFile, editor: Editor?) :
    StructureViewModelBase(psiFile, editor, MicaStructureViewElement(psiFile)),
    StructureViewModel.ElementInfoProvider {

    override fun getSuitableClasses(): Array<Class<*>> {
        return arrayOf(
            MicaFile::class.java,
            MicaVerbItem::class.java,
            MicaMethodItem::class.java,
            MicaRelationRule::class.java
        )
    }

    override fun isAlwaysShowsPlus(element: StructureViewTreeElement?): Boolean = false

    override fun isAlwaysLeaf(element: StructureViewTreeElement?): Boolean {
        val value = element?.value
        return value is MicaVerbItem || value is MicaMethodItem || value is MicaRelationRule
    }
}

class MicaStructureViewElement(private val element: PsiElement) : StructureViewTreeElement, NavigationItem {
    override fun getValue(): Any = element

    override fun navigate(requestFocus: Boolean) {
        if (element is com.intellij.pom.Navigatable) {
            element.navigate(requestFocus)
        }
    }

    override fun canNavigate(): Boolean =
        element is com.intellij.pom.Navigatable && element.canNavigate()

    override fun canNavigateToSource(): Boolean =
        element is com.intellij.pom.Navigatable && element.canNavigateToSource()

    override fun getName(): String? {
        return when (element) {
            is MicaFile -> element.name
            is MicaVerbItem -> element.methodHeader?.text?.trim() ?: "verb"
            is MicaMethodItem -> element.methodHeader?.text?.trim() ?: "method"
            is MicaRelationRule -> getRelationHeadName(element)
            else -> null
        }
    }

    override fun getPresentation(): ItemPresentation {
        return object : ItemPresentation {
            override fun getPresentableText(): String? {
                return when (element) {
                    is MicaFile -> element.name
                    is MicaVerbItem -> element.methodHeader?.text?.trim() ?: "verb"
                    is MicaMethodItem -> element.methodHeader?.text?.trim() ?: "method"
                    is MicaRelationRule -> getRelationHeadName(element)
                    else -> element.text
                }
            }

            override fun getLocationString(): String? = null

            override fun getIcon(unused: Boolean): Icon? {
                return when (element) {
                    is MicaFile -> MicaIcons.FILE
                    is MicaVerbItem -> AllIcons.Nodes.Method
                    is MicaMethodItem -> AllIcons.Nodes.Method
                    is MicaRelationRule -> AllIcons.Nodes.Property
                    else -> null
                }
            }
        }
    }

    override fun getChildren(): Array<TreeElement> {
        if (element is MicaFile) {
            val childrenElements = PsiTreeUtil.findChildrenOfAnyType(
                element,
                MicaVerbItem::class.java,
                MicaMethodItem::class.java,
                MicaRelationRule::class.java
            )
            return childrenElements
                .sortedBy { it.textOffset }
                .map { MicaStructureViewElement(it) }
                .toTypedArray()
        }
        return emptyArray()
    }

    private fun getRelationHeadName(rule: MicaRelationRule): String {
        val fullText = rule.text.substringBefore(":-").trim()
        val openParenIdx = fullText.indexOf('(')
        val targetText = if (openParenIdx != -1) {
            fullText.substring(0, openParenIdx).trim()
        } else {
            fullText
        }
        return targetText.ifEmpty { fullText }
    }
}
