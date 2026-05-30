package org.timbran.mica.jetbrains

import com.intellij.testFramework.fixtures.BasePlatformTestCase
import org.timbran.mica.jetbrains.psi.MicaFile

class MicaStructureViewTest : BasePlatformTestCase() {
    fun testStructureView() {
        val text = """
            verb get(actor @ #player, item @ #thing)
              if Portable(item)
                return true
              end
            end

            method #move_into :move
              roles actor @ #player, item @ #portable
            do
              require CanMove(actor, item)
            end

            VisibleTo(actor, obj) :- LocatedIn(actor, room), LocatedIn(obj, room)
        """.trimIndent()

        val psiFile = myFixture.configureByText("test.mica", text)
        assertTrue(psiFile is MicaFile)

        val factory = MicaStructureViewFactory()
        val builder = factory.getStructureViewBuilder(psiFile)
        assertNotNull("StructureViewBuilder should be constructed", builder)

        val treeBasedBuilder = builder as com.intellij.ide.structureView.TreeBasedStructureViewBuilder
        val model = treeBasedBuilder.createStructureViewModel(null)
        assertNotNull("StructureViewModel should be constructed", model)

        // Verify the root element
        val root = model.root as MicaStructureViewElement
        assertEquals("test.mica", root.presentation.presentableText)

        // Verify the children
        val children = root.children
        assertEquals(3, children.size)

        // 1. Verb Item
        val verbElement = children[0] as MicaStructureViewElement
        assertEquals("get(actor @ #player, item @ #thing)", verbElement.presentation.presentableText)
        assertNotNull("Presentation must be valid", verbElement.presentation)
        assertTrue("Verb element must support navigation", verbElement.canNavigate())
        assertTrue("Verb element must support navigate to source", verbElement.canNavigateToSource())

        // 2. Method Item
        val methodElement = children[1] as MicaStructureViewElement
        assertEquals("#move_into :move", methodElement.presentation.presentableText)
        assertNotNull("Presentation must be valid", methodElement.presentation)
        assertTrue("Method element must support navigation", methodElement.canNavigate())

        // 3. Relation Rule
        val ruleElement = children[2] as MicaStructureViewElement
        assertEquals("VisibleTo", ruleElement.presentation.presentableText)
        assertNotNull("Presentation must be valid", ruleElement.presentation)
        assertTrue("Rule element must support navigation", ruleElement.canNavigate())

        model.dispose()
    }

    fun testStructureViewKeepsSlashQualifiedVerbHeader() {
        val text = """
            verb source/agent_tool_result_summary(request, args)
              let result_row = one source/AgentToolResult(request, ?result, ?status, ?error)
              return result_row
            end
        """.trimIndent()

        val psiFile = myFixture.configureByText("slash-qualified.mica", text)
        assertTrue(psiFile is MicaFile)

        val factory = MicaStructureViewFactory()
        val builder = factory.getStructureViewBuilder(psiFile)
        assertNotNull("StructureViewBuilder should be constructed", builder)

        val treeBasedBuilder = builder as com.intellij.ide.structureView.TreeBasedStructureViewBuilder
        val model = treeBasedBuilder.createStructureViewModel(null)

        val root = model.root as MicaStructureViewElement
        val children = root.children
        assertEquals(1, children.size)

        val verbElement = children[0] as MicaStructureViewElement
        assertEquals(
            "source/agent_tool_result_summary(request, args)",
            verbElement.presentation.presentableText
        )

        model.dispose()
    }

    fun testStructureViewFindsAllSlashQualifiedVerbsInLongFile() {
        val text = """
            verb source/agent_file_context_line_limit()
              let limit = one source/RuntimeConfig(#source/config_agent_file_context_line_limit, ?limit)
              limit != nothing && limit > 0 && return limit
              return 20000
            end

            verb source/current_file_context_text(endpoint)
              let repository = source/current_repository()
              let revision = source/current_revision()
              let path = one source/SelectedPath(endpoint, ?path)
              if path == nothing
                path = source/current_file()
              end
              return path
            end

            verb source/agent_history_text(endpoint)
              let lines = []
              for found in source/AgentTurnEndpoint(?turn, endpoint)
                let turn = found[:turn]
                let role = one source/AgentTurnRole(turn, ?role)
                lines = [@lines, role]
              end
              return string_join(lines, "\n\n")
            end

            verb source/agent_export_excerpt(text @ #string, limit)
              let excerpt_limit = limit
              if limit == nothing || limit <= 0
                excerpt_limit = 600
              end
              string_len(text) <= excerpt_limit && return text
              return string_concat(string_slice(text, 0, excerpt_limit), "\n...[truncated in summary; full text below]...")
            end

            verb source/agent_tool_result_summary(request, args)
              let result_row = one source/AgentToolResult(request, ?result, ?status, ?error)
              return source/agent_export_excerpt(result_row, 900)
            end
        """.trimIndent()

        val psiFile = myFixture.configureByText("agent-loop.mica", text)
        assertTrue(psiFile is MicaFile)

        val factory = MicaStructureViewFactory()
        val builder = factory.getStructureViewBuilder(psiFile)
        assertNotNull("StructureViewBuilder should be constructed", builder)

        val treeBasedBuilder = builder as com.intellij.ide.structureView.TreeBasedStructureViewBuilder
        val model = treeBasedBuilder.createStructureViewModel(null)

        val root = model.root as MicaStructureViewElement
        val names = root.children.map { it.presentation.presentableText }
        assertEquals(
            listOf(
                "source/agent_file_context_line_limit()",
                "source/current_file_context_text(endpoint)",
                "source/agent_history_text(endpoint)",
                "source/agent_export_excerpt(text @ #string, limit)",
                "source/agent_tool_result_summary(request, args)"
            ),
            names
        )

        model.dispose()
    }

    fun testStructureViewFindsUiComposeExcerptVerbs() {
        val text = """
            // DOM composition for the source viewer.

            verb source/empty_node(message)
              return dom_element("p", {:class -> "source-empty"}, [dom_text(message)])
            end

            verb source/meta_chip_node(label, value)
              return dom_element("span", {:class -> "source-chip"}, [dom_text(label), dom_text(": "), dom_text(value)])
            end

            verb source/provider_label(provider)
              string_starts_with(provider, "mica-source-index/static-analysis") && return "source index static-analysis"
              string_starts_with(provider, "mica-source-index/tree-sitter-rust") && return "source index tree-sitter-rust"
              return provider
            end

            verb source/region_tab_button_node(region, value, label, active)
              let class = "source-region-tab"
              if value == active
                class = "source-region-tab active"
              end
              return dom_element("button", {:type -> "submit", :class -> class}, [dom_text(label)])
            end
        """.trimIndent()

        val psiFile = myFixture.configureByText("ui-compose.mica", text)
        assertTrue(psiFile is MicaFile)

        val factory = MicaStructureViewFactory()
        val builder = factory.getStructureViewBuilder(psiFile)
        assertNotNull("StructureViewBuilder should be constructed", builder)

        val treeBasedBuilder = builder as com.intellij.ide.structureView.TreeBasedStructureViewBuilder
        val model = treeBasedBuilder.createStructureViewModel(null)

        val root = model.root as MicaStructureViewElement
        val names = root.children.map { it.presentation.presentableText }
        assertEquals(
            listOf(
                "source/empty_node(message)",
                "source/meta_chip_node(label, value)",
                "source/provider_label(provider)",
                "source/region_tab_button_node(region, value, label, active)"
            ),
            names
        )

        model.dispose()
    }
}
