package stablehash

import "fmt"

// HashReflect perform stable hashing of value using reflecting to determine the type.
// A `true` value is returned if hash was performed correctly, `false` if type is unhandled.
func HashReflect(in any, addr FieldAddress, hasher StableHasher) (ok bool) {
	converted := toStableHashable(in)
	if converted == nil {
		return false
	}

	converted.StableHash(addr, hasher)
	return true
}

func toStableHashable(in any) StableHashable {
	switch v := in.(type) {
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
	case StableHashable:
		return v
	default:
		return nil
	}
}

// MustHashReflect is like [HashReflect] but infaillible.
func MustHashReflect(in any, addr FieldAddress, hasher StableHasher) {
	ok := HashReflect(in, addr, hasher)
	if !ok {
		panic(fmt.Errorf("don't know how to hash value of type %T", in))
	}
}

type String string

func (b String) StableHash(addr FieldAddress, hasher StableHasher) {
	Bytes(b).StableHash(addr, hasher)
}

type Bytes []byte

func (b Bytes) StableHash(addr FieldAddress, hasher StableHasher) {
	if len(b) != 0 {
		hasher.Write(addr, b)
	}
}

func Some[T StableHashable](in T) Optional[T] {
	return Optional[T]{t: &in}
}

func None[T StableHashable]() Optional[T] {
	return Optional[T]{t: nil}
}

type Optional[T StableHashable] struct {
	t *T
}

func (u *Optional[T]) IsSome() bool {
	return u != nil && u.t != nil
}

func (u *Optional[T]) IsNone() bool {
	return u == nil || u.t == nil
}

func (u *Optional[T]) StableHash(addr FieldAddress, hasher StableHasher) {
	if u.IsSome() {
		(*u.t).StableHash(addr.Child(0), hasher)
		hasher.Write(addr, nil)
	}
}

type U8 uint8

func (u U8) StableHash(addr FieldAddress, hasher StableHasher) {
	stableHashInt(false, []byte{byte(u)}, addr, hasher)
}

type U16 uint16

func (u U16) StableHash(addr FieldAddress, hasher StableHasher) {
	stableHashInt(false, le.AppendUint16(make([]byte, 0, 2), uint16(u)), addr, hasher)
}

type U32 uint32

func (u U32) StableHash(addr FieldAddress, hasher StableHasher) {
	stableHashInt(false, le.AppendUint32(make([]byte, 0, 4), uint32(u)), addr, hasher)
}

type U64 uint64

func (u U64) StableHash(addr FieldAddress, hasher StableHasher) {
	stableHashInt(false, le.AppendUint64(make([]byte, 0, 8), uint64(u)), addr, hasher)
}

func stableHashInt(negative bool, leBytes []byte, addr FieldAddress, hasher StableHasher) {
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
	end := len(bytes) - 1
	for end != 0 && bytes[end-1] == 0 {
		end -= 1
	}

	return bytes[0:end]
}

type Map[K comparable, V StableHashable] map[K]V

func (m Map[K, V]) StableHash(addr FieldAddress, hasher StableHasher) {
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

func (m MapUnsafe[K, V]) StableHash(addr FieldAddress, hasher StableHasher) {
	for k, v := range m {
		newHasher := hasher.New()
		a, b := addr.Unordered()

		MustHashReflect(k, a.Child(0), newHasher)
		MustHashReflect(v, a.Child(1), newHasher)

		hasher.Write(b, newHasher.ToBytes())
	}
}
