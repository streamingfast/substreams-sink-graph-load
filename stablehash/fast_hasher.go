package stablehash

import (
	"fmt"

	"github.com/shabbyrobe/go-num"
	"github.com/zeebo/xxh3"
)

var _ StableHasher = (*FastStableHasher)(nil)

type FastStableHasher struct {
	mixer FldMix
	count uint64
}

func NewFastStableHasher() *FastStableHasher {
	return &FastStableHasher{
		mixer: NewFldMix(),
		count: 0,
	}
}

func NewFastStableHasherFromBytes(bytes []byte) (*FastStableHasher, error) {
	if len(bytes) != 32 {
		return nil, fmt.Errorf("accepting exactly 32 bytes, got %d", len(bytes))
	}

	// Rust version
	// Self {
	//   mixer: FldMix::from_bytes(bytes[0..24].try_into().unwrap()),
	//   count: u64::from_le_bytes(bytes[24..32].try_into().unwrap()),
	// }

	mixer, err := NewFldMixFromBytes(bytes[0:24])
	if err != nil {
		return nil, fmt.Errorf("invalid mixed bytes: %w", err)
	}

	return &FastStableHasher{
		mixer: mixer,
		count: le.Uint64(bytes[24:32]),
	}, nil
}

// New implements StableHasher
func (*FastStableHasher) New() StableHasher {
	return NewFastStableHasher()
}

func (h *FastStableHasher) Mixin(other *FastStableHasher) {
	// Rust version
	// self.mixer.mixin(&other.mixer);
	// self.count = self.count.wrapping_add(other.count);

	h.mixer.Mixin(&other.mixer)
	h.count += other.count
}

func (h *FastStableHasher) ToBytes() (out []byte) {
	// Rust version
	// let mixer = self.mixer.to_bytes();
	// let count = self.count.to_le_bytes();
	//
	// let mut bytes = [0; 32];
	// bytes[0..24].copy_from_slice(&mixer);
	// bytes[24..32].copy_from_slice(&count);
	// bytes
	out = make([]byte, 32)
	copy(out[0:24], h.mixer.ToBytes())
	le.PutUint64(out[24:32], h.count)

	return
}

func (h *FastStableHasher) Write(fieldAddress FieldAddress, bytes []byte) {
	address, ok := fieldAddress.(Address)
	if !ok {
		panic(fmt.Errorf("this hasher only accepts FieldAddress of type Address, got %T", fieldAddress))
	}

	// Rust version
	// let hash = xxhash_rust::xxh3::xxh3_128_with_seed(bytes, field_address as u64);
	// self.mixer.mix(hash, (field_address >> 64) as u64);
	// self.count += 1;
	low, high := address.LowHigh()
	hash := hash128Seed(bytes, low)
	h.mixer.Mix(hash, high)
	h.count += 1
}

func (h *FastStableHasher) Finish() num.U128 {
	// Rust version
	// 	xxhash_rust::xxh3::xxh3_128_with_seed(&self.mixer.to_bytes(), self.count)
	return hash128Seed(h.mixer.ToBytes(), h.count)
}

func hash128Seed(bytes []byte, seed uint64) num.U128 {
	hash := xxh3.Hash128Seed(bytes, seed)
	return num.U128FromRaw(hash.Hi, hash.Lo)
}
