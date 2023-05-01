package stablehash

import (
	"encoding/binary"
	"fmt"
	"math"

	"github.com/shabbyrobe/go-num"
)

type FldMix U192

var (
	FLDMIX_P = U192{2305843009213693959, 2305843009213693950, 0}
	FLDMIX_Q = U192{18446744073709551609, 0, 0}
	FLDMIX_R = U192{8, 0, 0}
	FLDMIX_I = U192{
		16140901064495857665,
		18446744073709551615,
		18446744073709551615,
	}
)

var le = binary.LittleEndian

func NewFldMix() FldMix {
	return FldMix(FLDMIX_I)
}

func NewFldMixFromBytes(bytes []byte) (FldMix, error) {
	if len(bytes) != 24 {
		return FldMix{}, fmt.Errorf("accepting exactly 24 bytes, got %d", len(bytes))
	}

	// Rust version
	// let v0 = u64::from_le_bytes(bytes[0..8].try_into().unwrap());
	// let v1 = u64::from_le_bytes(bytes[8..16].try_into().unwrap());
	// let v2 = u64::from_le_bytes(bytes[16..24].try_into().unwrap());
	v0 := le.Uint64(bytes[0:8])
	v1 := le.Uint64(bytes[8:16])
	v2 := le.Uint64(bytes[16:24])

	// Self(U192([v0, v1, v2]))
	return FldMix(U192{v0, v1, v2}), nil
}

func (m *FldMix) Mix(value num.U128, seed uint64) {
	// Rust version
	// // See also 0d123631-c654-4246-8d26-092c21d43037
	// let v0 = seed & (u64::MAX >> 1);
	// let v1 = value as u64;
	// let v2 = (value >> 64) as u64;
	// let value = U192([v0, v1, v2]);
	// self.0 = Self::u(self.0, value);
	hi, low := value.Raw()

	v0 := seed & (math.MaxUint64 >> 1)
	v1 := low
	v2 := hi

	self := (*U192)(m)
	*self = fldmix_u(U192(*m), U192{v0, v1, v2})
}

func (m *FldMix) Mixin(value *FldMix) {
	// Rust version
	// self.0 = Self::u(self.0, value.0);

	self := (*U192)(m)
	*self = fldmix_u(U192(*m), U192(*value))
}

func (m *FldMix) ToBytes() (out []byte) {
	// Rust version
	// 	let mut bytes = [0; 24];
	// 	bytes[0..8].copy_from_slice(&self.0 .0[0].to_le_bytes());
	// 	bytes[8..16].copy_from_slice(&self.0 .0[1].to_le_bytes());
	// 	bytes[16..24].copy_from_slice(&self.0 .0[2].to_le_bytes());
	// 	bytes
	data := (*U192)(m)
	out = make([]byte, 24)
	le.PutUint64(out[0:8], data[0])
	le.PutUint64(out[8:16], data[1])
	le.PutUint64(out[16:24], data[2])

	return
}

func (m *FldMix) combine(value FldMix) {
	// Rust version
	// let x = self.0;
	// let y = other.0;
	// self.0 = Self::u(x, y);

	self := (*U192)(m)
	*self = fldmix_u(U192(*m), U192(value))
}

func fldmix_u(x, y U192) U192 {
	// Rust Version
	// Self::P + (Self::Q * (x + y)) + ((Self::R * x) * y)
	qMulXPlusY := FLDMIX_Q.Mul(x.Add(y))
	rMulXMulY := FLDMIX_R.Mul(x.Mul(y))

	return FLDMIX_P.Add(qMulXPlusY.Add(rMulXMulY))
}
