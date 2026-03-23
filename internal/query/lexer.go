package query

import (
	"fmt"
	"strings"
	"unicode"
)

// Lexer tokenizes input strings
type Lexer struct {
	input string
	pos   int
	ch    byte
}

// NewLexer creates a new lexer
func NewLexer(input string) *Lexer {
	l := &Lexer{input: input}
	l.readChar()
	return l
}

func (l *Lexer) readChar() {
	if l.pos >= len(l.input) {
		l.ch = 0
	} else {
		l.ch = l.input[l.pos]
	}
	l.pos++
}

func (l *Lexer) peekChar() byte {
	if l.pos >= len(l.input) {
		return 0
	}
	return l.input[l.pos]
}

func (l *Lexer) skipWhitespace() {
	for l.ch == ' ' || l.ch == '\t' || l.ch == '\n' || l.ch == '\r' {
		l.readChar()
	}
}

// NextToken returns the next token from the input
func (l *Lexer) NextToken() (Token, string) {
	l.skipWhitespace()

	var tok Token
	var lit string

	switch l.ch {
	case 0:
		return EOF, ""
	case '=':
		tok = EQ
		lit = "="
	case '!':
		if l.peekChar() == '=' {
			l.readChar()
			tok = NEQ
			lit = "!="
		} else {
			tok = ILLEGAL
			lit = string(l.ch)
		}
	case '<':
		if l.peekChar() == '=' {
			l.readChar()
			tok = LTE
			lit = "<="
		} else if l.peekChar() == '>' {
			l.readChar()
			tok = NEQ
			lit = "<>"
		} else {
			tok = LT
			lit = "<"
		}
	case '>':
		if l.peekChar() == '=' {
			l.readChar()
			tok = GTE
			lit = ">="
		} else {
			tok = GT
			lit = ">"
		}
	case '+':
		tok = PLUS
		lit = "+"
	case '-':
		// Could be minus or negative number
		if isDigit(l.peekChar()) {
			l.readChar()
			tok, lit = l.readNumber()
			lit = "-" + lit
			return tok, lit
		}
		tok = MINUS
		lit = "-"
	case '*':
		tok = MUL
		lit = "*"
	case '/':
		tok = DIV
		lit = "/"
	case '(':
		tok = LPAREN
		lit = "("
	case ')':
		tok = RPAREN
		lit = ")"
	case ',':
		tok = COMMA
		lit = ","
	case ';':
		tok = SEMICOLON
		lit = ";"
	case '\'':
		tok = STRING
		lit = l.readString('\'')
	case '"':
		tok = STRING
		lit = l.readString('"')
	default:
		if isLetter(l.ch) || l.ch == '_' {
			lit = l.readIdentifier()
			tok = LookupKeyword(lit)
			return tok, lit
		} else if isDigit(l.ch) {
			return l.readNumber()
		} else {
			tok = ILLEGAL
			lit = string(l.ch)
		}
	}

	l.readChar()
	return tok, lit
}

func (l *Lexer) readIdentifier() string {
	start := l.pos - 1
	for isLetter(l.ch) || isDigit(l.ch) || l.ch == '_' {
		l.readChar()
	}
	return l.input[start : l.pos-1]
}

func (l *Lexer) readNumber() (Token, string) {
	start := l.pos - 1
	hasDot := false
	hasDuration := false

	for {
		if isDigit(l.ch) {
			l.readChar()
		} else if l.ch == '.' && !hasDot {
			hasDot = true
			l.readChar()
		} else if isDurationSuffix(l.ch) && !hasDuration {
			// Duration suffixes: u, ms, s, m, h, d, w
			hasDuration = true
			if l.ch == 'm' && l.peekChar() == 's' {
				l.readChar()
				l.readChar()
			} else {
				l.readChar()
			}
		} else {
			break
		}
	}

	lit := l.input[start : l.pos-1]

	if hasDuration {
		return DURATION, lit
	}
	return NUMBER, lit
}

func (l *Lexer) readString(quote byte) string {
	l.readChar() // skip opening quote
	start := l.pos - 1
	for l.ch != quote && l.ch != 0 {
		if l.ch == '\\' {
			l.readChar() // skip escape
		}
		l.readChar()
	}
	return l.input[start : l.pos-1]
}

func isLetter(ch byte) bool {
	return unicode.IsLetter(rune(ch))
}

func isDigit(ch byte) bool {
	return ch >= '0' && ch <= '9'
}

func isDurationSuffix(ch byte) bool {
	return ch == 'u' || ch == 's' || ch == 'm' || ch == 'h' || ch == 'd' || ch == 'w'
}

// Tokenize returns all tokens from input
func Tokenize(input string) []struct {
	Token Token
	Lit   string
} {
	l := NewLexer(input)
	var tokens []struct {
		Token Token
		Lit   string
	}

	for {
		tok, lit := l.NextToken()
		tokens = append(tokens, struct {
			Token Token
			Lit   string
		}{tok, lit})
		if tok == EOF {
			break
		}
	}

	return tokens
}

// Helper for parsing duration strings
func ParseDuration(s string) (int64, error) {
	s = strings.TrimSpace(s)
	if len(s) == 0 {
		return 0, nil
	}

	// Find where the number ends
	i := 0
	for i < len(s) && (isDigit(s[i]) || s[i] == '.') {
		i++
	}

	if i == 0 {
		return 0, nil
	}

	numStr := s[:i]
	suffix := strings.ToLower(s[i:])

	var num float64
	_, err := fmt.Sscanf(numStr, "%f", &num)
	if err != nil {
		return 0, err
	}

	var multiplier int64
	switch suffix {
	case "u", "us":
		multiplier = 1000 // nanoseconds
	case "ms":
		multiplier = 1000000
	case "s":
		multiplier = 1000000000
	case "m":
		multiplier = 60 * 1000000000
	case "h":
		multiplier = 3600 * 1000000000
	case "d":
		multiplier = 86400 * 1000000000
	case "w":
		multiplier = 7 * 86400 * 1000000000
	default:
		// Assume seconds if no suffix
		multiplier = 1000000000
	}

	return int64(num * float64(multiplier)), nil
}
