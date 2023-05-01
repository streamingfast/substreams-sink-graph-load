package stablehash

import (
	"fmt"
	"math"
	"math/big"

	"github.com/shabbyrobe/go-num"
	"golang.org/x/exp/constraints"
)

// MAX_U192 is the max value of type U192 equivalent to 6277101735386680763835789423207666416102355444464034512895
var MAX_U192 = U192{math.MaxUint64, math.MaxUint64, math.MaxUint64}

type U192 [3]uint64

func NewU192() (out U192) {
	return out
}

func NewU192FromString(x string) (out U192, err error) {
	number := big.Int{}
	_, success := number.SetString(x, 0)
	if !success {
		return out, fmt.Errorf("invalid input %q", x)
	}

	return NewU192FromBigInt(&number)
}

func MustNewU192FromString(x string) (out U192) {
	out, err := NewU192FromString(x)
	if err != nil {
		panic(fmt.Errorf("invalid string for U192: %w", err))
	}

	return out
}

var _ big.Word = big.Word(18446744073709551615)

func NewU192FromBigInt(x *big.Int) (out U192, err error) {
	if x == nil {
		return out, fmt.Errorf("cannot be nil")
	}

	if x.Sign() <= -1 {
		return out, fmt.Errorf("only unsigned integer accepted, got %q", x)
	}

	if bitCount := x.BitLen(); bitCount > 192 {
		return out, fmt.Errorf("has %d bits but U192 accepts a maximum of 192 bits", bitCount)
	}

	words := x.Bits()
	switch len(words) {
	case 0:
		break

	case 1:
		out[0] = uint64(words[0])

	case 2:
		out[0] = uint64(words[0])
		out[1] = uint64(words[1])

	case 3:
		out[0] = uint64(words[0])
		out[1] = uint64(words[1])
		out[2] = uint64(words[2])

	default:
		// This can happen only on 32 bits platform
		panic(fmt.Errorf("32 bits platform not supported"))
	}

	return
}

func MustNewU192FromBigInt(x *big.Int) (out U192) {
	out, err := NewU192FromBigInt(x)
	if err != nil {
		panic(fmt.Errorf("invalid *big.Int for U192: %w", err))
	}

	return out
}

func (left U192) Mul(right U192) U192 {
	// Rust version
	// // The generated implementation of this method was 360 lines long!
	// let me = &self.0;
	// let you = &other.0;
	// me == left, you == right

	// let mult = |m: usize, y: usize| {
	// 	let v = me[m] as u128 * you[y] as u128;
	// 	(v as u64, (v >> 64) as u64)
	// };
	multAt := func(l int, r int) (uint64, uint64) {
		v := num.U128From64(left[l]).Mul64(right[r])
		hi, low := v.Raw()
		return low, hi
	}

	// let (r0, r1) = mult(0, 0);
	// let (low, hi0) = mult(1, 0);
	// let (r1, overflow0) = low.overflowing_add(r1);
	// let (low, hi1) = mult(0, 1);
	// let (r1, overflow1) = low.overflowing_add(r1);
	r0, r1 := multAt(0, 0)
	low, hi0 := multAt(1, 0)
	r1, overflow0 := overflowingAdd(low, r1)
	low, hi1 := multAt(0, 1)
	r1, overflow1 := overflowingAdd(low, r1)

	// let r2 = (hi0 + overflow0 as u64)
	// 	.wrapping_add(hi1 + overflow1 as u64)
	// 	.wrapping_add(me[2].wrapping_mul(you[0]))
	// 	.wrapping_add(me[1].wrapping_mul(you[1]))
	// 	.wrapping_add(me[0].wrapping_mul(you[2]));
	r2 := (hi0 + bool_to_uint64(overflow0)) +
		(hi1 + bool_to_uint64(overflow1)) +
		(left[2] * right[0]) +
		(left[1] * right[1]) +
		(left[0] * right[2])

	// U192([r0, r1, r2])
	return U192{r0, r1, r2}
}

func (left U192) Add(right U192) U192 {
	// Rust version (in comment)
	// let me = &self.0;
	// let you = &other.0;
	// me == left, you == right

	// let (r0, overflow0) = me[0].overflowing_add(you[0]);
	// let (res, overflow1a) = me[1].overflowing_add(you[1]);
	// let (r1, overflow1b) = res.overflowing_add(overflow0 as u64);
	r0, overflow0 := overflowingAdd(left[0], right[0])
	res, overflow1a := overflowingAdd(left[1], right[1])
	r1, overflow1b := overflowingAdd(res, bool_to_uint64(overflow0))

	// let r2 = me[2]
	// 	.wrapping_add(you[2])
	// 	.wrapping_add(overflow1a as u64 + overflow1b as u64);
	r2 := left[2] + right[2] + (bool_to_uint64(overflow1a) + bool_to_uint64(overflow1b))

	// U192([r0, r1, r2])
	return U192{r0, r1, r2}
}

func (left U192) Sub(right U192) U192 {
	// Rust version
	// let me = &self.0;
	// let you = &other.0;
	// me == left, you == right

	// let (r0, overflow0) = me[0].overflowing_sub(you[0]);
	// let (res, overflow1a) = me[1].overflowing_sub(you[1]);
	// let (r1, overflow1b) = res.overflowing_sub(overflow0 as u64);
	r0, overflow0 := overflowingSub(left[0], right[0])
	res, overflow1a := overflowingSub(left[1], right[1])
	r1, overflow1b := overflowingSub(res, bool_to_uint64(overflow0))

	// let r2 = me[2]
	// 	.wrapping_sub(you[2])
	// 	.wrapping_sub(overflow1a as u64 + overflow1b as u64);
	r2 := left[2] - right[2] - (bool_to_uint64(overflow1a) + bool_to_uint64(overflow1b))

	// U192([r0, r1, r2])
	return U192{r0, r1, r2}
}

func (left U192) toRust() string {
	return fmt.Sprintf("U192([%d, %d, %d])", left[0], left[1], left[2])
}

func (left U192) String() string {
	return left.asBigInt().String()
}

func (left U192) asBigInt() *big.Int {
	return (&big.Int{}).SetBits([]big.Word{big.Word(left[0]), big.Word(left[1]), big.Word(left[2])})
}

func overflowingAdd[T constraints.Unsigned](a T, b T) (c T, overflow bool) {
	c = a + b
	return c, c < a
}

func overflowingSub[T constraints.Unsigned](a T, b T) (c T, overflow bool) {
	c = a - b
	return c, b > a
}

func bool_to_uint64(x bool) uint64 {
	if x {
		return 1
	}

	return 0
}
