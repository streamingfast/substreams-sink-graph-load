package stablehash

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func u192(x string) U192 { return MustNewU192FromString(x) }

func TestU192(t *testing.T) {
	assert.Equal(t, "248", u192("248").String())
	assert.Equal(t, "6277101735386680763835789423207666416102355444464034512895", u192("6277101735386680763835789423207666416102355444464034512895").String())
	assert.Equal(t, "6277101735386680763835789423207666416102355444464034512895", MAX_U192.String())

	assert.PanicsWithError(t, "invalid string for U192: has 193 bits but U192 accepts a maximum of 192 bits", func() {
		u192("6277101735386680763835789423207666416102355444464034512896")
	})

	x := u192("2092367245128893587945263141069222138700785148154678170965")
	assert.Equal(t, "U192([6148914691236517205, 6148914691236517205, 6148914691236517205])", x.toRust())
}

func TestU192_Add(t *testing.T) {
	tests := []struct {
		name  string
		left  U192
		right U192
		want  U192
	}{
		{"no_overflow", u192("248"), u192("248"), u192("496")},
		{"overflow", u192("248"), MAX_U192, u192("247")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.left.Add(tt.right))
		})
	}

	// Rust version
	// #[test]
	// fn add_u192() {
	//     let a = U192([248, 0, 0]);
	//     let b = U192([248, 0, 0]);
	//     let c = U192([
	//         18446744073709551615,
	//         18446744073709551615,
	//         18446744073709551615,
	//     ]);

	//     assert_eq!(U192([496, 0, 0]), a + b);
	//     assert_eq!(U192([247, 0, 0]), a + c);
	// }
}

func TestU192_Sub(t *testing.T) {
	tests := []struct {
		name  string
		left  U192
		right U192
		want  U192
	}{
		{"no_overflow", u192("249"), u192("248"), u192("1")},
		{"overflow", u192("248"), u192("249"), MAX_U192},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.left.Sub(tt.right))
		})
	}

	// Rust version
	// #[test]
	// fn sub_u192() {
	//     let a = U192([248, 0, 0]);
	//     let b = U192([249, 0, 0]);

	//     assert_eq!(
	//         U192([
	//             18446744073709551615,
	//             18446744073709551615,
	//             18446744073709551615
	//         ]),
	//         a - b
	//     );
	//     assert_eq!(U192([1, 0, 0]), b - a);
	// }
}

func TestU192_Mul(t *testing.T) {
	tests := []struct {
		name  string
		left  U192
		right U192
		want  U192
	}{
		{"no_overflow", u192("248"), u192("249"), u192("61752")},
		{"overflow", u192("2092367245128893587945263141069222138700785148154678170965"), u192("4"), U192{6148914691236517204, 6148914691236517205, 6148914691236517205}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.left.Mul(tt.right))
		})
	}

	// Rust version
	// #[test]
	// fn mul_u192() {
	//     let a = U192([248, 0, 0]);
	//     let b = U192([249, 0, 0]);

	//     // 2092367245128893587945263141069222138700785148154678170965
	//     let c = U192([
	//         6148914691236517205,
	//         6148914691236517205,
	//         6148914691236517205,
	//     ]);

	//     assert_eq!(U192([61752, 0, 0]), a * b);
	//     assert_eq!(
	//         U192([
	//             6148914691236517204,
	//             6148914691236517205,
	//             6148914691236517205
	//         ]),
	//         c * U192([4, 0, 0])
	//     );
	// }
}

func TestOverflowingAdd(t *testing.T) {
	var a uint8 = 4
	var b uint8 = 254

	c1, overflowC1 := overflowingAdd(a, b)
	assert.Equal(t, uint8(2), c1)
	assert.Equal(t, true, overflowC1)

	// Rust version
	// #[test]
	// fn overflowing_add_u8() {
	//     let a: u8 = 4;
	//     let b: u8 = 254;

	//     let (c1, overflow_c1) = a.overflowing_add(b);

	//     assert_eq!(2, c1);
	//     assert_eq!(true, overflow_c1);
	// }
}

func TestOverflowingSub(t *testing.T) {
	var a uint8 = 4
	var b uint8 = 254

	c1, overflowC1 := overflowingSub(a, b)
	assert.Equal(t, uint8(6), c1)
	assert.Equal(t, true, overflowC1)

	c2, overflowC2 := overflowingSub(b, a)
	assert.Equal(t, uint8(250), c2)
	assert.Equal(t, false, overflowC2)

	// Rust version
	// #[test]
	// fn overflowing_sub_u8() {
	//     let a: u8 = 4;
	//     let b: u8 = 254;

	//     let (c1, overflow_c1) = a.overflowing_sub(b);
	//     let (c2, overflow_c2) = b.overflowing_sub(a);

	//     assert_eq!(6, c1);
	//     assert_eq!(true, overflow_c1);
	//     assert_eq!(250, c2);
	//     assert_eq!(false, overflow_c2);
	// }
}

func TestGolangWrapping_uint8(t *testing.T) {
	var a uint8 = 4
	var b uint8 = 254

	assert.Equal(t, uint8(248), a*b)

	// Rust version
	// mod test {
	// 	#[test]
	// 	fn wrapping_mul_u8() {
	// 		let a: u8 = 4;
	// 		let b: u8 = 254;
	//
	// 		assert_eq!(248, a.wrapping_mul(b))
	// 	}
	// }
}
