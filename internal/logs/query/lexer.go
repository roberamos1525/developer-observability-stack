// Package query implements the log query language: a lexer, a recursive
// descent parser producing an AST, an entry matcher, and an index planner
// that turns the AST into candidate posting lists.
//
// Grammar (informal):
//
//	query    = [ or ] { "|" stage }
//	or       = and { "OR" and }
//	and      = unary { ["AND"] unary }          (juxtaposition means AND)
//	unary    = ("NOT" | "-") unary | primary
//	primary  = "(" or ")" | field op value | word | "quoted phrase"
//	op       = ":" | "=" | "!=" | ">" | ">=" | "<" | "<=" | "=~" | "!~"
//	stage    = "limit" N | "sort" ("asc"|"desc") | "since" DURATION
//	         | "between" TIME TIME | "count" [ "by" field { "," field } ]
package query

import (
	"fmt"
	"strings"
)

type tokenKind int

const (
	tEOF tokenKind = iota
	tWord
	tString
	tLParen
	tRParen
	tPipe
	tComma
	tOp
	tMinus
	tAnd
	tOr
	tNot
)

func (k tokenKind) String() string {
	return [...]string{"end of query", "word", "string", "'('", "')'", "'|'", "','", "operator", "'-'", "AND", "OR", "NOT"}[k]
}

type token struct {
	kind tokenKind
	text string
	pos  int
}

// SyntaxError describes a parse failure with its byte offset.
type SyntaxError struct {
	Pos int
	Msg string
}

func (e *SyntaxError) Error() string {
	return fmt.Sprintf("syntax error at position %d: %s", e.Pos, e.Msg)
}

const specialChars = "()|,:\"'=!<>~"

func isSpace(c byte) bool { return c == ' ' || c == '\t' || c == '\n' || c == '\r' }

func lex(input string) ([]token, error) {
	var toks []token
	i := 0
	for i < len(input) {
		c := input[i]
		if isSpace(c) {
			i++
			continue
		}
		start := i
		switch {
		case c == '(':
			toks = append(toks, token{tLParen, "(", start})
			i++
		case c == ')':
			toks = append(toks, token{tRParen, ")", start})
			i++
		case c == '|' && i+1 < len(input) && input[i+1] == '|':
			toks = append(toks, token{tOr, "||", start})
			i += 2
		case c == '|':
			toks = append(toks, token{tPipe, "|", start})
			i++
		case c == ',':
			toks = append(toks, token{tComma, ",", start})
			i++
		case c == '"' || c == '\'':
			s, n, err := lexString(input[i:], c)
			if err != nil {
				return nil, &SyntaxError{start, err.Error()}
			}
			toks = append(toks, token{tString, s, start})
			i += n
		case c == ':' || c == '=' || c == '!' || c == '<' || c == '>':
			op := string(c)
			if i+1 < len(input) {
				two := input[i : i+2]
				switch two {
				case "!=", ">=", "<=", "=~", "!~":
					op = two
				}
			}
			if op == "!" {
				return nil, &SyntaxError{start, "unexpected '!' (use != or !~, or NOT)"}
			}
			toks = append(toks, token{tOp, op, start})
			i += len(op)
		case c == '~':
			return nil, &SyntaxError{start, "unexpected '~'"}
		case c == '-' && i+1 < len(input) && !isSpace(input[i+1]) && !prevIsOp(toks):
			toks = append(toks, token{tMinus, "-", start})
			i++
		default:
			for i < len(input) && !isSpace(input[i]) && !strings.ContainsRune(specialChars, rune(input[i])) {
				i++
			}
			w := input[start:i]
			switch w {
			case "AND", "&&":
				toks = append(toks, token{tAnd, w, start})
			case "OR", "||":
				toks = append(toks, token{tOr, w, start})
			case "NOT":
				toks = append(toks, token{tNot, w, start})
			default:
				toks = append(toks, token{tWord, w, start})
			}
		}
	}
	return append(toks, token{tEOF, "", len(input)}), nil
}

func prevIsOp(toks []token) bool {
	return len(toks) > 0 && toks[len(toks)-1].kind == tOp
}

func lexString(s string, quote byte) (string, int, error) {
	var b strings.Builder
	for i := 1; i < len(s); i++ {
		c := s[i]
		if c == '\\' && i+1 < len(s) {
			i++
			switch s[i] {
			case 'n':
				b.WriteByte('\n')
			case 't':
				b.WriteByte('\t')
			case '\\', '"', '\'':
				b.WriteByte(s[i])
			default:
				b.WriteByte('\\')
				b.WriteByte(s[i])
			}
			continue
		}
		if c == quote {
			return b.String(), i + 1, nil
		}
		b.WriteByte(c)
	}
	return "", 0, fmt.Errorf("unterminated string")
}
