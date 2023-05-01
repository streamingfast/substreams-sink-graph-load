package stablehash

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

type U32 uint32

func (u U32) StableHash(addr FieldAddress, hasher StableHasher) {
	stableHashInt(false, le.AppendUint32(make([]byte, 0, 4), uint32(u)), addr, hasher)
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
