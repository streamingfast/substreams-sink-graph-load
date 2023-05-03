package stablehash

import "github.com/shabbyrobe/go-num"

func FastHash(value Hashable) num.U128 {
	hasher := NewFastHasher()
	value.StableHash(Address{}.Root(), hasher)

	return hasher.Finish()
}

type Hashable interface {
	StableHash(FieldAddress, Hasher)
}

type Hasher interface {
	New() Hasher
	Write(fieldAddress FieldAddress, bytes []byte)
	Mixin(other Hasher)
	ToBytes() []byte
}
