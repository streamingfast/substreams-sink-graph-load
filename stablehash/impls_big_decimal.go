package stablehash

import (
	"fmt"
	"math/big"
	"strconv"
	"strings"
)

// Max signigicant digits accepted by `graph-node`
//
// See https://github.com/graphprotocol/graph-node/blob/9d013f75f2a565e3d126737593e3a30d1b2f212e/graph/src/data/store/scalar.rs#L46
const MAX_SIGNIFICANT_DIGITS = uint64(34)

var bigZero = big.NewInt(0)
var bigOne = big.NewInt(1)
var bigTwo = big.NewInt(2)
var bigFive = big.NewInt(5)
var bigTen = big.NewInt(10)

// BigDecimal replicates `graph-node` way of representing, parsing and printing
// big decimal values.
//
// The goal of this type is not for **arithmetic** at all. It's sole purpose is
// to parse a number just like `graph-node` would do it for deterministic stable
// hashing purposes.
type BigDecimal struct {
	Int   *big.Int
	Scale int64
}

func NewBigDecimalFromString(s string) (BigDecimal, error) {
	base_part, exponent_value := s, int64(0)
	if loc := strings.IndexAny(s, "eE"); loc != -1 {
		// let (base, exp) = (&s[..loc], &s[loc + 1..]);
		//
		// // special consideration for rust 1.0.0 which would not parse a leading '+'
		//let exp = match exp.chars().next() {
		// 	Some('+') => &exp[1..],
		// 	_ => exp,
		// };
		// slice up to `loc` and 1 after to skip the 'e' char
		base, expRaw := s[:loc], strings.TrimPrefix(s[loc+1:], "+")

		exp, err := strconv.ParseInt(expRaw, 0, 64)
		if err != nil {
			return BigDecimal{}, fmt.Errorf("invalid exponent value %q: %w", expRaw, err)
		}

		base_part = base
		exponent_value = exp
	}

	// Comment from Rust `bigdecimal` crate codebase: `TEMPORARY: Test for emptiness - remove once BigInt supports similar error`
	if base_part == "" {
		return BigDecimal{}, fmt.Errorf("failed to parse empty string")
	}

	digits, decimal_offset := base_part, int64(0)
	if loc := strings.IndexAny(s, "."); loc != -1 {
		// let (lead, trail) = (&base_part[..loc], &base_part[loc + 1..]);
		lead, trail := base_part[:loc], base_part[loc+1:]

		// let mut digits = String::from(lead);
		// digits.push_str(trail);
		// copy leading characters + trailing characters after '.' into the digits string
		digits = lead + trail
		decimal_offset = int64(len(trail))
	}

	// let scale = decimal_offset - exponent_value;
	// let big_int = try!(BigInt::from_str_radix(&digits, radix));
	scale := decimal_offset - exponent_value
	big_int, ok := (&big.Int{}).SetString(digits, 10)
	if !ok {
		return BigDecimal{}, fmt.Errorf("invalid digits part %q", digits)
	}

	out := BigDecimal{Int: big_int, Scale: scale}
	// out.normalizeInPlace()

	return out, nil
}

func (b *BigDecimal) isZero() bool {
	// The `Sign` calls on big.Int returns 0 if number is equal 0 (-1 or 1 otherwise)
	return b.Scale == 0 && b.Int.Sign() == 0
}

func (b *BigDecimal) normalizeInPlace() {
	if b.isZero() {
		return
	}

	// Round to the maximum significant digits.
	b.withPrecisionInPlace(MAX_SIGNIFICANT_DIGITS)

	// let (bigint, exp) = big_decimal.as_bigint_and_exponent();
	bigint, exp := b.Int, b.Scale

	// let (sign, mut digits) = bigint.to_radix_be(10);
	sign, digits := bigint.Sign(), bigint.Abs(bigint).String()

	// let trailing_count = digits.iter().rev().take_while(|i| **i == 0).count();
	// digits.truncate(digits.len() - trailing_count);
	digits, trailingCount := trailingZeroTruncated(digits)
	// let int_val = num_bigint::BigInt::from_radix_be(sign, &digits, 10).unwrap();
	b.Int, _ = (&big.Int{}).SetString(digits, 10)
	if sign == -1 {
		b.Int = b.Int.Neg(b.Int)
	}

	// let scale = exp - trailing_count as i64;
	b.Scale = exp - trailingCount
	// BigDecimal(bigdecimal::BigDecimal::new(int_val, scale))
}

func trailingZeroTruncated(in string) (string, int64) {
	out := strings.TrimSuffix(in, "0")
	return out, int64(len(in) - len(out))
}

func (b *BigDecimal) withPrecisionInPlace(prec uint64) {
	digits := b.digits()

	if digits > prec {
		diff := digits - prec
		p := ten_to_the(diff)

		var q *big.Int
		// let (mut q, r) = self.int_val.div_rem(&p);
		q, r := (&big.Int{}).DivMod(b.Int, p, &big.Int{})

		// check for "leading zero" in remainder term; otherwise round
		tenTimesR := (&big.Int{}).Mul(bigTen, r)
		if p.Cmp(tenTimesR) == -1 {
			q = q.Add(q, get_rounding_term(r))
		}

		b.Int = q
		b.Scale = b.Scale - int64(diff)
		return
	}

	if digits < prec {
		diff := prec - digits
		p := ten_to_the(diff)

		b.Int = (p).Mul(b.Int, p)
		b.Scale = b.Scale + int64(diff)
		return
	}

	return
}

// Digits gives number of digits in the non-scaled integer representation
func (b *BigDecimal) digits() uint64 {
	bInt := b.Int
	if bInt.Sign() == 0 {
		return 1
	}

	// guess number of digits based on number of bits in UInt
	// let mut digits = (int.bits() as f64 / 3.3219280949) as u64;
	bits := uint(bInt.BitLen()) - bInt.TrailingZeroBits()
	digits := uint64(float64(bits) / 3.3219280949)

	// let mut num = ten_to_the(digits);
	num := ten_to_the(digits)

	// while int >= &num {
	// 	num *= 10u8;
	// 	digits += 1;
	// }
	for bInt.Cmp(num) >= 0 {
		num = num.Mul(num, bigTen)
		digits += 1
	}

	return digits
}

func ten_to_the(pow uint64) *big.Int {
	return (&big.Int{}).Exp(bigTen, big.NewInt(int64(pow)), nil)

	// Rust version, not ported because we should be good with Golang version
	// if pow < 20 {
	//     BigInt::from(10u64.pow(pow as u32))
	// } else {
	//     let (half, rem) = pow.div_rem(&16);

	//     let mut x = ten_to_the(half);

	//     for _ in 0..4 {
	//         x = &x * &x;
	//     }

	//     if rem == 0 {
	//         x
	//     } else {
	//         x * ten_to_the(rem)
	//     }
	// }
}

func get_rounding_term(num *big.Int) *big.Int {
	if num.Sign() == 0 {
		return bigZero
	}

	// let digits = (num.bits() as f64 / 3.3219280949) as u64;
	bits := uint(num.BitLen()) - num.TrailingZeroBits()
	digits := uint64(float64(bits) / 3.3219280949)

	// let mut n = ten_to_the(digits);
	n := ten_to_the(digits)

	// loop-method
	for {
		if num.Cmp(n) == -1 {
			return bigOne
		}

		n = n.Mul(n, bigFive)
		if num.Cmp(n) == -1 {
			return bigZero
		}

		n = n.Mul(n, bigTwo)
	}

	// string-method
	// let s = format!("{}", num);
	// let high_digit = u8::from_str(&s[0..1]).unwrap();
	// if high_digit < 5 { 0 } else { 1 }
}
