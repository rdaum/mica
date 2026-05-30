package org.timbran.mica.jetbrains

import com.intellij.psi.tree.TokenSet
import org.timbran.mica.jetbrains.MicaElementTypes.*

object MicaTokenSets {
    val KEYWORDS = TokenSet.create(
        LET_KW, CONST_KW, IF_KW, ELSEIF_KW, ELSE_KW, END_KW, BEGIN_KW,
        FOR_KW, IN_KW, WHILE_KW, RETURN_KW, RAISE_KW, RECOVER_KW,
        ONE_KW, SPAWN_KW, AFTER_KW, NOT_KW, BREAK_KW, CONTINUE_KW,
        TRY_KW, CATCH_KW, AS_KW, FINALLY_KW, FN_KW, METHOD_KW,
        VERB_KW, DO_KW, ASSERT_KW, RETRACT_KW, REQUIRE_KW,
        TRUE_KW, FALSE_KW, NOTHING_KW
    )
    
    val COMMENTS = TokenSet.create(LINE_COMMENT)
    val STRINGS = TokenSet.create(STRING)
}
