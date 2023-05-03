package stablehash

import (
	"math/big"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBigDecimal_IntAndExp(t *testing.T) {
	tests := []struct {
		value          string
		expectedBigInt string
		expectedScale  int64
	}{
		{"0.1", "1", 1},
		// {"0.1", "1000000000000000000000000000000000", 34},

		// {"-0.1", "-1", 1},
		// {"198.98765544", "19898765544", 8},
		// {"0.00000093937698", "93937698", 14},
		// {"98765587998098786876.0", "98765587998098786876", 0},
		// {"98765000000", "98765", -6},
		// {"-98765000000", "-98765", -6},
		// {"98765000000.1", "987650000001", 1},
		// {"-98765000000.2", "-987650000002", 1},

		// // Positive rounding outside max scale (34)
		// {"0.1234567890123456789012345678901234", "1234567890123456789012345678901234", 34},
		// {"0.12345678901234567890123456789012344", "1234567890123456789012345678901234", 34},
		// {"0.12345678901234567890123456789012345", "1234567890123456789012345678901235", 34},
		// {"0.12345678901234567890123456789012346", "1234567890123456789012345678901235", 34},

		// // Negative rounding outside max scale (34)
		// {"-0.1234567890123456789012345678901234", "-1234567890123456789012345678901234", 34},
		// {"-0.12345678901234567890123456789012344", "-12345678901234567890123456789012344", 35},
		// {"-0.12345678901234567890123456789012345", "-12345678901234567890123456789012345", 35},
		// {"-0.12345678901234567890123456789012346", "-12345678901234567890123456789012346", 35},

		// // Normalize negative numbers have a bug where scale is actually MAX + 1
		// {"-0.123456789012345678901234567890123424", "-12345678901234567890123456789012342", 35},
		// {"-0.123456789012345678901234567890123425", "-12345678901234567890123456789012342", 35},
		// {"-0.123456789012345678901234567890123426", "-12345678901234567890123456789012342", 35},

		// // Showcasing rounding effects when max digits is split before/after dot
		// {"12.123456789012345678901234567890124", "1212345678901234567890123456789012", 32},
		// {"12.123456789012345678901234567890125", "1212345678901234567890123456789013", 32},
		// {"12.123456789012345678901234567890126", "1212345678901234567890123456789013", 32},

		// {"-12.1234567890123456789012345678901234", "-12123456789012345678901234567890123", 33},
		// {"-12.1234567890123456789012345678901235", "-12123456789012345678901234567890123", 33},
		// {"-12.1234567890123456789012345678901236", "-12123456789012345678901234567890123", 33},

		// {"1234567890123.123456789012345678901834567890124", "1234567890123123456789012345678902", 21},
		// {"-1234567890123.123456789012345678901894567890124", "-12345678901231234567890123456789018", 22},

		// // Showcasing rounding effects when max digits is all before dot
		// {"1234567890123456789012345678901234", "1234567890123456789012345678901234", 0},
		// {"12345678901234567890123456789012344", "1234567890123456789012345678901234", -1},
		// {"12345678901234567890123456789012345", "1234567890123456789012345678901235", -1},
		// {"12345678901234567890123456789012346", "1234567890123456789012345678901235", -1},

		// {"-12345678901234567890123456789012345", "-12345678901234567890123456789012345", 0},
		// {"-123456789012345678901234567890123454", "-12345678901234567890123456789012345", -1},
		// {"-123456789012345678901234567890123455", "-12345678901234567890123456789012345", -1},
		// {"-123456789012345678901234567890123456", "-12345678901234567890123456789012345", -1},

		// {"10000000000000000000000000000000000000000", "1", -40},
		// {"100000000000000000000000000000000000000001", "1", -41},

		// {"19999999999999999999999999999999994", "1999999999999999999999999999999999", -1},
		// {"19999999999999999999999999999999995", "2", -34},
		// {"19999999999999999999999999999999985", "1999999999999999999999999999999999", -1},

		// {"1999999999999999999999999999999999", "1999999999999999999999999999999999", 0},
		// {"199999999999999999999999999999999", "199999999999999999999999999999999", 0},
		// {"19999999999999999999999999999999999", "2", -34},
		// {"199999999999999999999999999999999999999999", "2", -41},

		// {"1444444444444444444444444444444444", "1444444444444444444444444444444444", 0},
		// {"14444444444444444444444444444444444", "1444444444444444444444444444444444", -1},
		// {"144444444444444444444444444444444444", "1444444444444444444444444444444444", -2},

		// {"1555555555555555555555555555555555", "1555555555555555555555555555555555", 0},
		// {"15555555555555555555555555555555555", "1555555555555555555555555555555556", -1},
		// {"155555555555555555555555555555555555", "1555555555555555555555555555555556", -2},
	}
	for _, tt := range tests {
		t.Run(tt.value, func(t *testing.T) {
			// After many tests in `graph-node`, the rounding after precision goes over is to
			// Toward Positive Infinity (rounding up if number is positive, truncating if number is negative)
			//
			// See https://en.wikipedia.org/wiki/IEEE_754#Directed_roundings (toward +∞)

			expectedBigInt, ok := (&big.Int{}).SetString(tt.expectedBigInt, 10)
			require.True(t, ok)

			actual, err := NewBigDecimalFromString(tt.value)
			require.NoError(t, err)

			msg := []any{
				"For %s [BigInt (expected %s, actual %s), Scale (expected %d, actual %d)]",
				tt.value,
				tt.expectedBigInt, actual.Int,
				tt.expectedScale, actual.Scale,
			}

			assert.True(t, expectedBigInt.Cmp(actual.Int) == 0, msg...)
			assert.Equal(t, tt.expectedScale, actual.Scale, msg...)
		})
	}

}
