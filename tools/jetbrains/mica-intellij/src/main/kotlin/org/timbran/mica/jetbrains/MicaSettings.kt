package org.timbran.mica.jetbrains

import com.intellij.openapi.components.PersistentStateComponent
import com.intellij.openapi.components.State
import com.intellij.openapi.components.Storage
import com.intellij.openapi.components.service
import com.intellij.openapi.fileChooser.FileChooserDescriptorFactory
import com.intellij.openapi.options.BoundConfigurable
import com.intellij.openapi.ui.DialogPanel
import com.intellij.ui.dsl.builder.AlignX
import com.intellij.ui.dsl.builder.bindText
import com.intellij.ui.dsl.builder.panel
import com.intellij.ui.dsl.builder.rows

@State(name = "MicaSettings", storages = [Storage("mica.xml")])
class MicaSettings : PersistentStateComponent<MicaSettings.State> {
    data class State(
        var micacPath: String = "",
        var environmentFileins: String = "",
    )

    private var state = State()

    override fun getState(): State = state

    override fun loadState(state: State) {
        this.state = state
    }

    companion object {
        fun getInstance(): MicaSettings = service()
    }
}

class MicaConfigurable : BoundConfigurable("Mica") {
    override fun createPanel(): DialogPanel {
        val settings = MicaSettings.getInstance()
        return panel {
            row("micac path:") {
                textFieldWithBrowseButton(
                    "Select micac",
                    null,
                    FileChooserDescriptorFactory.createSingleFileOrExecutableAppDescriptor(),
                )
                    .align(AlignX.FILL)
                    .bindText(
                        { settings.state.micacPath },
                        { value -> settings.state.micacPath = value.trim() },
                    )
            }.rowComment("Used for live compiler diagnostics. Leave empty to use MICA_MICAC or micac on PATH.")

            row("Environment fileins:") {
                textArea()
                    .rows(6)
                    .align(AlignX.FILL)
                    .bindText(
                        { settings.state.environmentFileins },
                        { value -> settings.state.environmentFileins = value.trim() },
                    )
            }.rowComment("One file path per line. These fileins are checked before the current editor buffer.")
        }
    }
}
