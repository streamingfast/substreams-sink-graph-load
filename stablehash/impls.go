package stablehash

import (
	"fmt"
	"math"
	"math/big"
)

// HashReflect perform stable hashing of value using reflecting to determine the type.
// A `true` value is returned if hash was performed correctly, `false` if type is unhandled.
func HashReflect(in any, addr FieldAddress, hasher Hasher) (ok bool) {
	converted := ToHashable(in)
	if converted == nil {
		return false
	}

	converted.StableHash(addr, hasher)
	return true
}

func ToHashable(in any) Hashable {
	switch v := in.(type) {
	case bool:
		return Bool(v)
	case int8:
		return I8(v)
	case int16:
		return I16(v)
	case int32:
		return I32(v)
	case int64:
		return I64(v)
	case uint8:
		return U8(v)
	case uint16:
		return U16(v)
	case uint32:
		return U32(v)
	case uint64:
		return U64(v)
	case string:
		return String(v)
	case []byte:
		return Bytes(v)
	case *big.Int:
		return (*BigInt)(v)
	case Hashable:
		return v
	default:
		return nil
	}
}

// MustHashReflect is like [HashReflect] but infaillible.
func MustHashReflect(in any, addr FieldAddress, hasher Hasher) {
	ok := HashReflect(in, addr, hasher)
	if !ok {
		panic(fmt.Errorf("don't know how to hash value of type %T", in))
	}
}

type String string

func (b String) StableHash(addr FieldAddress, hasher Hasher) {
	Bytes(b).StableHash(addr, hasher)
}

type Bytes []byte

func (b Bytes) StableHash(addr FieldAddress, hasher Hasher) {
	if len(b) != 0 {
		hasher.Write(addr, b)
	}
}

func Some[T Hashable](in T) Optional[T] {
	return Optional[T]{t: &in}
}

func None[T Hashable]() Optional[T] {
	return Optional[T]{t: nil}
}

type Optional[T Hashable] struct {
	t *T
}

func (u *Optional[T]) IsSome() bool {
	return u != nil && u.t != nil
}

func (u *Optional[T]) IsNone() bool {
	return u == nil || u.t == nil
}

func (u *Optional[T]) StableHash(addr FieldAddress, hasher Hasher) {
	if u.IsSome() {
		(*u.t).StableHash(addr.Child(0), hasher)
		hasher.Write(addr, nil)
	}
}

type Bool bool

var (
	boolBytesFalse = []byte{0x0}
	boolBytesTrue  = []byte{0x1}
)

func (i Bool) StableHash(addr FieldAddress, hasher Hasher) {
	bytes := boolBytesFalse
	if i {
		bytes = boolBytesTrue
	}

	hasher.Write(addr, bytes)
}

type I8 int8

func (i I8) StableHash(addr FieldAddress, hasher Hasher) {
	stableHashInt(i < 0, []byte{byte(math.Abs(float64(i)))}, addr, hasher)
}

type I16 int16

func (i I16) StableHash(addr FieldAddress, hasher Hasher) {
	stableHashInt(i < 0, le.AppendUint16(make([]byte, 0, 2), uint16(math.Abs(float64(i)))), addr, hasher)
}

type I32 int32

func (i I32) StableHash(addr FieldAddress, hasher Hasher) {
	stableHashInt(i < 0, le.AppendUint32(make([]byte, 0, 4), uint32(math.Abs(float64(i)))), addr, hasher)
}

type I64 int64

func (i I64) StableHash(addr FieldAddress, hasher Hasher) {
	stableHashInt(i < 0, le.AppendUint64(make([]byte, 0, 8), uint64(math.Abs(float64(i)))), addr, hasher)
}

type U8 uint8

func (u U8) StableHash(addr FieldAddress, hasher Hasher) {
	stableHashInt(false, []byte{byte(u)}, addr, hasher)
}

type U16 uint16

func (u U16) StableHash(addr FieldAddress, hasher Hasher) {
	stableHashInt(false, le.AppendUint16(make([]byte, 0, 2), uint16(u)), addr, hasher)
}

type U32 uint32

func (u U32) StableHash(addr FieldAddress, hasher Hasher) {
	stableHashInt(false, le.AppendUint32(make([]byte, 0, 4), uint32(u)), addr, hasher)
}

type U64 uint64

func (u U64) StableHash(addr FieldAddress, hasher Hasher) {
	stableHashInt(false, le.AppendUint64(make([]byte, 0, 8), uint64(u)), addr, hasher)
}

type BigInt big.Int

func (i *BigInt) StableHash(addr FieldAddress, hasher Hasher) {
	value := (*big.Int)(i)

	negative := value.Sign() == -1
	bytes := reverseBytesInPlace(value.Bytes())

	stableHashInt(negative, bytes, addr, hasher)
}

func reverseBytesInPlace(bytes []byte) []byte {
	byteCount := len(bytes)
	for i := 0; i < byteCount/2; i++ {
		bytes[i], bytes[byteCount-i-1] = bytes[byteCount-i-1], bytes[i]
	}

	return bytes
}

func stableHashInt(negative bool, leBytes []byte, addr FieldAddress, hasher Hasher) {
	// Rust version
	// // Having the negative sign be a child makes it possible to change the schema
	// // from u32 to i64 in a backward compatible way.
	// // This is also allowing for negative 0, like float, which is not used by
	// // any standard impl but may be used by some types.
	// if self.is_negative {
	//     state.write(field_address.child(0), &[]);
	// }
	// let canon = trim_zeros(self.little_endian);
	// if !canon.is_empty() {
	//     state.write(field_address, canon);
	// }
	if negative {
		hasher.Write(addr.Child(0), nil)
	}

	canonical := trim_zeros(leBytes)

	if len(canonical) > 0 {
		hasher.Write(addr, canonical)
	}
}

func trim_zeros(bytes []byte) (out []byte) {
	if len(bytes) == 0 {
		return bytes
	}

	// Rust version
	// let mut end = bytes.len();
	// while end != 0 && bytes[end - 1] == 0 {
	// 	end -= 1;
	// }
	// &bytes[0..end]
	end := len(bytes)
	for end != 0 && bytes[end-1] == 0 {
		end -= 1
	}

	return bytes[0:end]
}

type Map[K comparable, V Hashable] map[K]V

func (m Map[K, V]) StableHash(addr FieldAddress, hasher Hasher) {
	// Rust version
	// for member in items { # Note member here is a tuple (k, v)
	for k, v := range m {
		// // Must create an independent hasher to "break" relationship between
		// // independent field addresses.
		// // See also a817fb02-7c77-41d6-98e4-dee123884287
		// let mut new_hasher = H::new();
		// let (a, b) = field_address.unordered();
		newHasher := hasher.New()
		a, b := addr.Unordered()

		// member.stable_hash(a, &mut new_hasher);
		MustHashReflect(k, a.Child(0), newHasher)
		v.StableHash(a.Child(1), newHasher)

		// state.write(b, new_hasher.to_bytes().as_ref());
		hasher.Write(b, newHasher.ToBytes())
	}
}

// MapUnsafe is like Map[K, V StableHashable] but it drops the requierement on StableHashable
// for the value type.
//
// Useful when you already have a map where you know the `V` is already [StableHashable] or
// it's a primitive types that will be handled by [MustHashReflect].
//
// Panics if either K or K is not stable hashable.
type MapUnsafe[K comparable, V any] map[K]V

func (m MapUnsafe[K, V]) StableHash(addr FieldAddress, hasher Hasher) {
	for k, v := range m {
		newHasher := hasher.New()
		a, b := addr.Unordered()

		MustHashReflect(k, a.Child(0), newHasher)
		MustHashReflect(v, a.Child(1), newHasher)

		hasher.Write(b, newHasher.ToBytes())
	}
}

func (b BigDecimal) StableHash(addr FieldAddress, hasher Hasher) {
	// let (int, exp) = self.as_bigint_and_exponent();
	// StableHash::stable_hash(&exp, field_address.child(1), state);
	I64(b.Scale).StableHash(addr.Child(1), hasher)

	// Normally it would be a red flag to pass field_address in after having used a child slot.
	// But, we know the implementation of StableHash for BigInt will not use child(1) and that
	// it will not in the future due to having no forward schema evolutions for ints and the
	// stability guarantee.
	//
	// For reference, ints use child(0) for the sign and write the little endian bytes to the parent slot.
	// BigInt(int).stable_hash(field_address, state);
	(*BigInt)(b.Int).StableHash(addr, hasher)
}
