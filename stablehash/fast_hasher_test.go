package stablehash

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFastHasher_DoubleChild(t *testing.T) {
	assert.Equal(t, "261232071512772414229682083989926651266", FastHash(&DoubleChild{}).String())
}

func TestFastHasher_AddOptionalField(t *testing.T) {
	one := &One[U32]{One: U32(5)}
	two := &TwoOptional{One: U32(5), Two: None[U32]()}

	assert.Equal(t, "102568403942768160221811810082933398928", FastHash(one).String())
	assert.Equal(t, "102568403942768160221811810082933398928", FastHash(two).String())
}

func TestFastHasher_TupleAddOptionalField(t *testing.T) {
	one := &One[U32]{One: U32(5)}
	two := &TwoOptional{One: U32(5), Two: None[U32]()}
	tuple := &Tuple2[*One[U32], *TwoOptional]{One: one, Two: two}

	assert.Equal(t, "210303380251691017811466509002544125279", FastHash(tuple).String())
}

func TestFastHasher_AddDefaultField(t *testing.T) {
	one := &One[String]{One: String("one")}
	two := &Two[String]{One: String("one"), Two: String("")}

	assert.Equal(t, "237994494046445339248193596542695086083", FastHash(one).String())
	assert.Equal(t, "237994494046445339248193596542695086083", FastHash(two).String())
}

func TestFastHasher_TupleAddDefaultField(t *testing.T) {
	one := &One[String]{One: String("one")}
	two := &Two[String]{One: String("one"), Two: String("")}
	tuple := &Tuple2[*One[String], *Two[String]]{One: one, Two: two}

	assert.Equal(t, "337538645577122176555714212704832450090", FastHash(tuple).String())
}

func TestFastHasher_MapEqual(t *testing.T) {
	first := MapUnsafe[uint32, string]{
		1: "one",
		2: "two",
		3: "three",
	}

	second := MapUnsafe[uint32, string]{
		3: "three",
		1: "one",
		2: "two",
	}

	assert.Equal(t, "60093794751952876589018848897648863192", FastHash(first).String())
	assert.Equal(t, "60093794751952876589018848897648863192", FastHash(second).String())
}

func TestFastHasher_MapNotEqualCount(t *testing.T) {
	first := MapUnsafe[uint32, string]{
		1: "one",
		2: "two",
		3: "three",
		0: "",
	}

	second := MapUnsafe[uint32, string]{
		3: "three",
		1: "one",
		2: "two",
	}

	assert.NotEqual(t, FastHash(first).String(), FastHash(second).String())
}

func TestFastHasher_MapNotEqualKey(t *testing.T) {
	first := MapUnsafe[uint32, string]{
		1: "one",
		2: "two",
		3: "three",
	}

	second := MapUnsafe[uint32, string]{
		9: "one",
		2: "two",
		3: "three",
	}

	assert.NotEqual(t, FastHash(first).String(), FastHash(second).String())
}

func TestFastHasher_MapNotEqualValue(t *testing.T) {
	first := MapUnsafe[uint32, string]{
		1: "X",
		2: "two",
		3: "three",
	}

	second := MapUnsafe[uint32, string]{
		1: "one",
		2: "two",
		3: "three",
	}

	assert.NotEqual(t, FastHash(first).String(), FastHash(second).String())
}

func TestFastHasher_MapNotEqualSwap(t *testing.T) {
	first := MapUnsafe[uint32, string]{
		1: "one",
		2: "two",
	}

	second := MapUnsafe[uint32, string]{
		1: "two",
		2: "one",
	}

	assert.NotEqual(t, FastHash(first).String(), FastHash(second).String())
}

type DoubleChild struct {
}

func (c *DoubleChild) StableHash(addr FieldAddress, hasher Hasher) {
	hasher.Write(addr.Child(1), nil)
	hasher.Write(addr.Child(1), nil)
}

type One[T Hashable] struct {
	One T
}

func (o *One[T]) StableHash(addr FieldAddress, hasher Hasher) {
	o.One.StableHash(addr.Child(0), hasher)
}

type Two[T Hashable] struct {
	One T
	Two T
}

func (o *Two[T]) StableHash(addr FieldAddress, hasher Hasher) {
	o.One.StableHash(addr.Child(0), hasher)
	o.Two.StableHash(addr.Child(1), hasher)
}

type TwoOptional struct {
	One U32
	Two Optional[U32]
}

func (o *TwoOptional) StableHash(addr FieldAddress, hasher Hasher) {
	o.One.StableHash(addr.Child(0), hasher)
	o.Two.StableHash(addr.Child(1), hasher)
}

type Tuple2[T1 Hashable, T2 Hashable] struct {
	One T1
	Two T2
}

func (o *Tuple2[T1, T2]) StableHash(addr FieldAddress, hasher Hasher) {
	o.One.StableHash(addr.Child(0), hasher)
	o.Two.StableHash(addr.Child(1), hasher)
}
