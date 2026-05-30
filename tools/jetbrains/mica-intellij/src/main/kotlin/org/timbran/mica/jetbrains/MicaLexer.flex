package org.timbran.mica.jetbrains;

import com.intellij.lexer.FlexLexer;
import com.intellij.psi.tree.IElementType;
import static org.timbran.mica.jetbrains.MicaElementTypes.*;

%%

%class _MicaLexer
%implements FlexLexer
%unicode
%function advance
%type IElementType
%eof{  return;
%eof}

%{
  private boolean isNextCharDot() {
    return zzMarkedPos < zzEndRead && zzBuffer.charAt(zzMarkedPos) == '.';
  }
%}

WHITE_SPACE=[ \t]+
NEWLINE=(\r?\n)
LINE_COMMENT="//"[^\r\n]*
INT=[0-9]+
FLOAT=[0-9]+"."[0-9]*
IDENT=[a-zA-Z_][a-zA-Z0-9_]*
ERROR_CODE="E_"[a-zA-Z0-9_]+
STRING=\"([^\"\\]|\\.)*\"?

%%
<YYINITIAL> {
  {WHITE_SPACE}      { return com.intellij.psi.TokenType.WHITE_SPACE; }
  {NEWLINE}          { return NEWLINE; }
  {LINE_COMMENT}     { return LINE_COMMENT; }

  // Keywords
  "let"              { return LET_KW; }
  "const"            { return CONST_KW; }
  "if"               { return IF_KW; }
  "elseif"           { return ELSEIF_KW; }
  "else"             { return ELSE_KW; }
  "end"              { return END_KW; }
  "begin"            { return BEGIN_KW; }
  "for"              { return FOR_KW; }
  "in"               { return IN_KW; }
  "while"            { return WHILE_KW; }
  "return"           { return RETURN_KW; }
  "raise"            { return RAISE_KW; }
  "recover"          { return RECOVER_KW; }
  "one"              { return ONE_KW; }
  "spawn"            { return SPAWN_KW; }
  "after"            { return AFTER_KW; }
  "not"              { return NOT_KW; }
  "break"            { return BREAK_KW; }
  "continue"         { return CONTINUE_KW; }
  "try"              { return TRY_KW; }
  "catch"            { return CATCH_KW; }
  "as"               { return AS_KW; }
  "finally"          { return FINALLY_KW; }
  "fn"               { return FN_KW; }
  "method"           { return METHOD_KW; }
  "verb"             { return VERB_KW; }
  "do"               { return DO_KW; }
  "assert"           { return ASSERT_KW; }
  "retract"          { return RETRACT_KW; }
  "require"          { return REQUIRE_KW; }
  "true"             { return TRUE_KW; }
  "false"            { return FALSE_KW; }
  "nothing"          { return NOTHING_KW; }

  // Symbols
  "("                { return LPAREN; }
  ")"                { return RPAREN; }
  "["                { return LBRACKET; }
  "]"                { return RBRACKET; }
  "{"                { return LBRACE; }
  "}"                { return RBRACE; }
  ","                { return COMMA; }
  ";"                { return SEMI; }
  ".."               { return DOT_DOT; }
  "."                { return DOT; }
  ":-"               { return COLON_DASH; }
  ":"                { return COLON; }
  "=="               { return EQ_EQ; }
  "=>"               { return FAT_ARROW; }
  "="                { return EQ; }
  "!="               { return BANG_EQ; }
  "!"                { return BANG; }
  "<="               { return LT_EQ; }
  "<"                { return LT; }
  ">="               { return GT_EQ; }
  ">"                { return GT; }
  "&&"               { return AMP_AMP; }
  "||"               { return PIPE_PIPE; }
  "->"               { return ARROW; }
  "+"                { return PLUS; }
  "-"                { return MINUS; }
  "*"                { return STAR; }
  "/"                { return SLASH; }
  "%"                { return PERCENT; }
  "#"                { return HASH; }
  "@"                { return AT; }
  "?"                { return QUESTION; }
  "_"                { return UNDERSCORE; }

  {ERROR_CODE}       { return ERROR_CODE; }
  {IDENT}            { return IDENT; }
  {INT}              { return INT; }
  {FLOAT}            {
    if (isNextCharDot()) {
      yypushback(1);
      return INT;
    }
    return FLOAT;
  }
  {STRING}           { return STRING; }

  [^]                { return com.intellij.psi.TokenType.BAD_CHARACTER; }
}
