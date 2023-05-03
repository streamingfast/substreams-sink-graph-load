package stablehash

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFastHasher_SingleValue(t *testing.T) {
	tests := []struct {
		name     string
		value    any
		expected string
	}{
		{"i8 negative", I8(-4), "234333316714235907961649213803594184029"},
		{"i8 positive", I8(8), "263946226580928315975306067326554590217"},
		{"i16 negative", I16(-256), "2575436948546927940500443723565624388"},
		{"i16 positive", I16(256), "201589876719799452230445857493583317400"},

		{"u8 small", U8(8), "263946226580928315975306067326554590217"},
		{"u8 high", U8(255), "182395296116387546137591220945749437249"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hashable := ToHashable(tt.value)
			require.NotNil(t, hashable)
			assert.Equal(t, tt.expected, FastHash(hashable).String())
		})
	}
}

func Test_reverseBytesInPlace(t *testing.T) {
	tests := []struct {
		name string
		in   []byte
		want []byte
	}{
		{"empty", []byte{}, []byte{}},
		{"single", []byte{0x1}, []byte{0x1}},
		{"pair", []byte{0x1, 0x2}, []byte{0x2, 0x1}},
		{"uneven", []byte{0x1, 0x2, 0x3}, []byte{0x3, 0x2, 0x1}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, reverseBytesInPlace(tt.in))
		})
	}
}
