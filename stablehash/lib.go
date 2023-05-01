package stablehash

import "github.com/shabbyrobe/go-num"

func FastStableHash(value StableHashable) num.U128 {
	hasher := NewFastStableHasher()
	value.StableHash(Address{}.Root(), hasher)

	return hasher.Finish()
}

type StableHashable interface {
	StableHash(FieldAddress, StableHasher)
}

type StableHasher interface {
	Write(fieldAddress FieldAddress, bytes []byte)
}
