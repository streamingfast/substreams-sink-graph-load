package schema

import (
	"unicode"

	"github.com/ettle/strcase"
)

func NormalizeField(in string) string {
	return caser.ToSnake(in)
}

var caser = strcase.NewCaser(
	false,
	map[string]bool{},
	func(prev, curr, next rune) strcase.SplitAction {
		preserveNumberFormatting := true
		splitCase := true
		splitAcronym := true
		delimiters := []rune{'*', '.', ','}
		splitBeforeNumber := true
		splitAfterNumber := true

		// The most common case will be that it's just a letter
		// There are safe cases to process
		if isLower(curr) && !isNumber(prev) {
			return strcase.Noop
		}
		if isUpper(prev) && isUpper(curr) && isUpper(next) {
			return strcase.Noop
		}

		if preserveNumberFormatting {
			if (curr == '.' || curr == ',') &&
				isNumber(prev) && isNumber(next) {
				return strcase.Noop
			}
		}

		if unicode.IsSpace(curr) {
			return strcase.SkipSplit
		}
		for _, d := range delimiters {
			if curr == d {
				return strcase.SkipSplit
			}
		}

		if splitBeforeNumber {
			if isNumber(curr) && !isNumber(prev) {
				if preserveNumberFormatting && (prev == '.' || prev == ',') {
					return strcase.Noop
				}
				if isUpper(prev) {
					return strcase.Noop
				}
				return strcase.Split
			}
		}

		if splitAfterNumber {
			if isNumber(prev) && !isNumber(curr) && !isUpper(curr) {
				return strcase.Split
			}
		}

		if splitCase {
			squeezed := isNumber(prev) && isNumber(next)
			if !isUpper(prev) && isUpper(curr) && !squeezed {
				return strcase.Split
			}
		}

		if splitAcronym {
			if isUpper(prev) && isUpper(curr) && isLower(next) {
				return strcase.Split
			}
		}

		return strcase.Noop
	},
)

// Unicode functions, optimized for the common case of ascii
// No performance lost by wrapping since these functions get inlined by the compiler

func isUpper(r rune) bool {
	return unicode.IsUpper(r)
}

func isLower(r rune) bool {
	return unicode.IsLower(r)
}

func isNumber(r rune) bool {
	if r >= '0' && r <= '9' {
		return true
	}
	return unicode.IsNumber(r)
}

func isSpace(r rune) bool {
	if r == ' ' || r == '\t' || r == '\n' || r == '\r' {
		return true
	} else if r < 128 {
		return false
	}
	return unicode.IsSpace(r)
}

func toUpper(r rune) rune {
	if r >= 'a' && r <= 'z' {
		return r - 32
	} else if r < 128 {
		return r
	}
	return unicode.ToUpper(r)
}

func toLower(r rune) rune {
	if r >= 'A' && r <= 'Z' {
		return r + 32
	} else if r < 128 {
		return r
	}
	return unicode.ToLower(r)
}
