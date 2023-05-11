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

	default:
		panic(fmt.Errorf("value of type %T no handled right now", v))
	}

	return f
}
