package poi

import (
	"encoding/base64"
	"fmt"
	"math/big"
	"strings"

	"github.com/streamingfast/substreams-graph-load/stablehash"
	pbentity "github.com/streamingfast/substreams-sink-entity-changes/pb/sf/substreams/sink/entity/v1"
)

type EntityValue pbentity.Value

func (v *EntityValue) StableHash(addr stablehash.FieldAddress, hasher stablehash.Hasher) {
	hashable, variant := v.toStableHashable()
	if hashable == nil {
		panic(fmt.Errorf("Value of type %T not implemented yet", (*pbentity.Value)(v).GetTyped()))
	}

	hashable.StableHash(addr.Child(0), hasher)
	hasher.Write(addr, []byte{variant})
}

func (v *EntityValue) toStableHashable() (stablehash.Hashable, byte) {
	switch v := (*pbentity.Value)(v).GetTyped().(type) {
	case *pbentity.Value_String_:
		value := v.String_

		// Strip null characters since they are not accepted by Postgres.
		if strings.Contains(value, "\u0000") {
			value = strings.ReplaceAll(value, "\u0000", "")
		}

		return stablehash.String(value), 0x1

	case *pbentity.Value_Int32:
		return stablehash.I32(v.Int32), 0x2

	case *pbentity.Value_Bigdecimal:
		bigDecimal, err := stablehash.NewBigDecimalFromString(v.Bigdecimal)
		if err != nil {
			panic(fmt.Errorf("received Value_Bigdecimal value %q, should have been parsable: %w", v.Bigdecimal, err))
		}

		return bigDecimal, 0x3

	case *pbentity.Value_Bool:
		return stablehash.Bool(v.Bool), 0x4

	case *pbentity.Value_Array:
		converted := make(stablehash.List[*EntityValue], len(v.Array.Value))
		for i, value := range v.Array.Value {
			converted[i] = (*EntityValue)(value)
		}

		return stablehash.List[*EntityValue](converted), 0x5

	case *pbentity.Value_Bytes:
		data, err := base64.StdEncoding.DecodeString(v.Bytes)
		if err != nil {
			panic(fmt.Errorf("received invalid Value_Bytes value %q, should have been base64 decodable (standard padded): %w", v.Bytes, err))
		}

		return stablehash.Bytes(data), 0x6

	case *pbentity.Value_Bigint:
		value, ok := (&big.Int{}).SetString(v.Bigint, 10)
		if !ok {
			panic(fmt.Errorf("received invalid Value_BigInt %q", v.Bigint))
		}

		return (*stablehash.BigInt)(value), 0x7

	default:
		return nil, 0x0
	}
}
