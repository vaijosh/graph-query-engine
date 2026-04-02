grammar Gremlin;


query
    : G traversal EOF
    ;

traversal
    : step+
    ;

step
    : DOT IDENT LPAREN arguments? RPAREN
    ;

arguments
    : argument (COMMA argument)*
    ;

argument
    : STRING
    | NUMBER
    | qualifiedName
    | nestedCall
    ;

nestedCall
    : IDENT LPAREN arguments? RPAREN (DOT IDENT LPAREN arguments? RPAREN)*
    ;

qualifiedName
    : IDENT (DOT IDENT)*
    ;

G       : 'g';
DOT     : '.';
COMMA   : ',';
LPAREN  : '(';
RPAREN  : ')';

IDENT   : [a-zA-Z_] [a-zA-Z_0-9]*;
NUMBER  : '-'? [0-9]+ ('.' [0-9]+)?;
STRING  : '\'' ( ~['\\\r\n] | '\\' . )* '\''
        | '"' ( ~["\\\r\n] | '\\' . )* '"'
        ;

WS      : [ \t\r\n]+ -> skip;

