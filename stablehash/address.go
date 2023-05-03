package stablehash

import "github.com/shabbyrobe/go-num"

type FieldAddress interface {
	// Root is allowed on empty receiver
	Root() FieldAddress
	Child(number uint64) FieldAddress
	Unordered() (FieldAddress, FieldAddress)

	String() string
}

var _ FieldAddress = Address{}

type Address num.U128

// Rust version of implementation
// impl FieldAddress for u128 {
//     fn root() -> Self {
//         17
//     }
//     #[inline]
//     fn child(&self, number: u64) -> Self {
//         profile_method!(child);
//
//         self.wrapping_mul(486_187_739).wrapping_add(number as u128)
//     }
//     #[inline]
//     fn unordered(&self) -> (Self, Self) {
//         (Self::root(), *self)
//     }
// }

func AddressRoot() FieldAddress {
	return Address(num.U128From64(17))
}

// Child implements FieldAddress
func (a Address) Child(number uint64) FieldAddress {
	value := (num.U128)(a)

	return Address(value.Mul64(486_187_739).Add64(number))
}

// Root implements FieldAddress
func (Address) Root() FieldAddress {
	return AddressRoot()
}

// Unordered implements FieldAddress
func (a Address) Unordered() (FieldAddress, FieldAddress) {
	return a.Root(), a
}

func (a Address) AsUint64() uint64 {
	return (num.U128)(a).AsUint64()
}

func (a Address) LowHigh() (low, high uint64) {
	high, low = (num.U128)(a).Raw()
	return
}

func (a Address) String() string {
	return (num.U128)(a).String()
}
