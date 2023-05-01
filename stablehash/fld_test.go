package stablehash

import (
	"math"
	"testing"

	"github.com/shabbyrobe/go-num"
	"github.com/stretchr/testify/assert"
)

func TestFldMix_Mix(t *testing.T) {
	u128 := func(x uint64) num.U128 {
		return num.U128From64(x)
	}

	// Rust version
	// 	let mut a = FldMix::new();
	// 	a.mix(100, u64::MAX);
	// 	a.mix(10, 10);
	// 	a.mix(999, 100);

	a := NewFldMix()
	a.Mix(u128(100), math.MaxUint64)
	a.Mix(u128(10), 10)
	a.Mix(u128(999), 100)

	// 	let mut b = FldMix::new();
	// 	b.mix(10, 10);
	// 	b.mix(999, 100);
	// 	b.mix(100, u64::MAX);
	b := NewFldMix()
	b.Mix(u128(10), 10)
	b.Mix(u128(999), 100)
	b.Mix(u128(100), math.MaxUint64)

	// 	assert_eq!(a, b);
	assert.Equal(t, a, b)

	// 	let mut c = FldMix::new();
	// 	c.mix(999, 100);
	// 	c.mix(10, 10);
	c := NewFldMix()
	c.Mix(u128(999), 100)
	c.Mix(u128(10), 10)

	// 	let mut d = FldMix::new();
	// 	d.mix(100, u64::MAX);
	d := NewFldMix()
	d.Mix(u128(100), math.MaxUint64)

	// 	c.combine(d);
	// 	assert_eq!(b, c);
	c.combine(d)
	assert.Equal(t, b, c)
}
