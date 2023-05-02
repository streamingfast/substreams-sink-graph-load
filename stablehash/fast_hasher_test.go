package stablehash

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFastStableHasher_DoubleChild(t *testing.T) {
	assert.Equal(t, "261232071512772414229682083989926651266", FastStableHash(&DoubleChild{}).String())
}

func TestFastStableHasher_AddOptionalField(t *testing.T) {
	one := &One[U32]{One: U32(5)}
	two := &TwoOptional{One: U32(5), Two: None[U32]()}

	assert.Equal(t, "102568403942768160221811810082933398928", FastStableHash(one).String())
	assert.Equal(t, "102568403942768160221811810082933398928", FastStableHash(two).String())
}

func TestFastStableHasher_TupleAddOptionalField(t *testing.T) {
	one := &One[U32]{One: U32(5)}
	two := &TwoOptional{One: U32(5), Two: None[U32]()}
	tuple := &Tuple2[*One[U32], *TwoOptional]{One: one, Two: two}

	assert.Equal(t, "210303380251691017811466509002544125279", FastStableHash(tuple).String())
}

func TestFastStableHasher_AddDefaultField(t *testing.T) {
	one := &One[String]{One: String("one")}
	two := &Two[String]{One: String("one"), Two: String("")}

	assert.Equal(t, "237994494046445339248193596542695086083", FastStableHash(one).String())
	assert.Equal(t, "237994494046445339248193596542695086083", FastStableHash(two).String())
}

func TestFastStableHasher_TupleAddDefaultField(t *testing.T) {
	one := &One[String]{One: String("one")}
	two := &Two[String]{One: String("one"), Two: String("")}
	tuple := &Tuple2[*One[String], *Two[String]]{One: one, Two: two}

	assert.Equal(t, "337538645577122176555714212704832450090", FastStableHash(tuple).String())
}

func TestFastStableHasher_MapEqual(t *testing.T) {
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

	assert.Equal(t, "60093794751952876589018848897648863192", FastStableHash(first).String())
	assert.Equal(t, "60093794751952876589018848897648863192", FastStableHash(second).String())
}

func TestFastStableHasher_MapNotEqualCount(t *testing.T) {
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

	assert.NotEqual(t, FastStableHash(first).String(), FastStableHash(second).String())
}

func TestFastStableHasher_MapNotEqualKey(t *testing.T) {
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

	assert.NotEqual(t, FastStableHash(first).String(), FastStableHash(second).String())
}

func TestFastStableHasher_MapNotEqualValue(t *testing.T) {
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

	assert.NotEqual(t, FastStableHash(first).String(), FastStableHash(second).String())
}

func TestFastStableHasher_MapNotEqualSwap(t *testing.T) {
	first := MapUnsafe[uint32, string]{
		1: "one",
		2: "two",
	}

	second := MapUnsafe[uint32, string]{
		1: "two",
		2: "one",
	}

	assert.NotEqual(t, FastStableHash(first).String(), FastStableHash(second).String())
}

type DoubleChild struct {
}

func (c *DoubleChild) StableHash(addr FieldAddress, hasher StableHasher) {
	hasher.Write(addr.Child(1), nil)
	hasher.Write(addr.Child(1), nil)
}

type One[T StableHashable] struct {
	One T
}

func (o *One[T]) StableHash(addr FieldAddress, hasher StableHasher) {
	o.One.StableHash(addr.Child(0), hasher)
}

type Two[T StableHashable] struct {
	One T
	Two T
}

func (o *Two[T]) StableHash(addr FieldAddress, hasher StableHasher) {
	o.One.StableHash(addr.Child(0), hasher)
	o.Two.StableHash(addr.Child(1), hasher)
}

type TwoOptional struct {
	One U32
	Two Optional[U32]
}

func (o *TwoOptional) StableHash(addr FieldAddress, hasher StableHasher) {
	o.One.StableHash(addr.Child(0), hasher)
	o.Two.StableHash(addr.Child(1), hasher)
}

type Tuple2[T1 StableHashable, T2 StableHashable] struct {
	One T1
	Two T2
}

func (o *Tuple2[T1, T2]) StableHash(addr FieldAddress, hasher StableHasher) {
	o.One.StableHash(addr.Child(0), hasher)
	o.Two.StableHash(addr.Child(1), hasher)
}
