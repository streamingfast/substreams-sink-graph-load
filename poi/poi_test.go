package poi

import (
	"fmt"
	"math/big"
	"testing"

	pbentity "github.com/streamingfast/substreams-graph-load/pb/entity/v1"
	"github.com/stretchr/testify/assert"
)

func TestProofOfIndexing(t *testing.T) {
	poi := NewProofOfIndexing(1, VersionFast)

	poi.SetEntity(&pbentity.EntityChange{
		Entity:    "BlockMeta",
		Id:        "day:first:20150730",
		Operation: pbentity.EntityChange_CREATE,
		Fields: []*pbentity.Field{
			field("at", "2015-07-30 00:00:00"),
			field("number", big.NewInt(1)),
			field("hash", Base64("iOltRTe+pNnAXRJUmQezJWHTvzH0Wq5zTNwRnxNAbLY=")),
			field("parent_hash", Base64("1OVnQPh2rvjAELhqQNX1Z0WhGNCQajTmmuyMDbHLj6M=")),
			field("timestamp", "2015-07-30T15:26:28Z"),
		},
	})

	assert.Equal(t, "993dd21dad9750a531331324bb07e2bcd9501521e1b1c7110800000000000000", poi.DebugCurrent())
}

func TestProofOfIndexing_Pool(t *testing.T) {
	poi := NewProofOfIndexing(1, VersionFast)

	sqrt := new(big.Int)
	sqrt, _ = sqrt.SetString("8927094545831003674704908909", 10)
	poi.SetEntity(&pbentity.EntityChange{
		Entity:    "Pool",
		Id:        "0x1d42064fc4beb5f8aaf85f4617ae8b3b5b8bd801",
		Operation: pbentity.EntityChange_CREATE,
		Fields: []*pbentity.Field{
			field("token0", "0x1f9840a85d5af5bf1d1762f925bdaddc4201f984"),
			field("feeGrowthGlobal0X128", big.NewInt(0)),
			field("volumeToken0", big.NewFloat(0)),
			field("feesUSD", big.NewFloat(0)),
			field("token0Price", big.NewFloat(78.76601952474081448516162931189449750144335446771553281655550790474587287531348715556056299391062978)),
			field("liquidityProviderCount", big.NewInt(1)),
			field("totalValueLockedToken1", big.NewFloat(0.01264381746197226)),
			field("txCount", big.NewInt(1)),
			field("createdAtTimestamp", big.NewInt(1620157956)),
			field("collectedFeesToken0", big.NewFloat(0)),
			field("totalValueLockedUSDUntracked", big.NewFloat(0.000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000)),
			field("volumeUSD", big.NewFloat(0)),
			field("token1Price", big.NewFloat(0.01269583008045613912537880652252357634826606549593750361690139278516628255672527307728256336238519793)),
			field("totalValueLockedETHUntracked", big.NewFloat(0.012643817461972260)),
			field("sqrtPrice", sqrt),
			field("feeGrowthGlobal1X128", big.NewInt(0)),
			field("collectedFeesToken1", big.NewFloat(0)),
			field("untrackedVolumeUSD", big.NewFloat(0)),
			field("totalValueLockedToken0", big.NewFloat(0.999999999999999924)),
			field("token1", "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"),
			field("collectedFeesUSD", big.NewFloat(0)),
			field("totalValueLockedETH", big.NewFloat(0.02528763492394452)),
			field("feeTier", big.NewInt(3000)),
			field("observationIndex", big.NewInt(0)),
			field("createdAtBlockNumber", big.NewInt(12369739)),
			field("totalValueLockedUSD", big.NewFloat(0.00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000)),
			field("volumeToken1", big.NewFloat(0)),
			field("tick", big.NewInt(0)),
			field("liquidity", big.NewInt(383995753785830744)),
		},
	})

	assert.Equal(t, "993dd21dad9750a531331324bb07e2bcd9501521e1b1c7110800000000000000", poi.DebugCurrent())
}

type Base64 string

func field(name string, value any) *pbentity.Field {
	f := &pbentity.Field{Name: name}
	switch v := value.(type) {
	case string:
		f.NewValue = &pbentity.Value{Typed: &pbentity.Value_String_{String_: v}}

	case Base64:
		f.NewValue = &pbentity.Value{Typed: &pbentity.Value_Bytes{Bytes: string(v)}}

	case *big.Int:
		f.NewValue = &pbentity.Value{Typed: &pbentity.Value_Bigint{Bigint: v.String()}}

	case *big.Float:
		f.NewValue = &pbentity.Value{Typed: &pbentity.Value_Bigdecimal{Bigdecimal: v.String()}}

	default:
		panic(fmt.Errorf("value of type %T no handled right now", v))
	}

	return f
}
