package poi

import (
	"encoding/hex"
	"fmt"

	pbentity "github.com/streamingfast/substreams-sink-graphcsv/pb/entity/v1"
	"github.com/streamingfast/substreams-sink-graphcsv/stablehash"
)

type ProofOfIndexingEvent interface {
	stablehash.Hashable
}

var _ ProofOfIndexingEvent = ProofOfIndexingSetEntity{}

func NewProofOfIndexingSetEntity(entity *pbentity.EntityChange) ProofOfIndexingSetEntity {
	event := ProofOfIndexingSetEntity{
		EntityType: entity.Entity,
		EntityID:   entity.Id,
		Data:       make(stablehash.Map[string, *pbentity.Value], len(entity.Fields)),
	}

	for _, field := range entity.Fields {
		event.Data[field.Name] = field.NewValue
	}

	return event
}

type ProofOfIndexingSetEntity struct {
	EntityType string
	EntityID   string
	Data       stablehash.Map[string, *pbentity.Value]
}

// StableHash implements ProofOfIndexingEvent
func (e ProofOfIndexingSetEntity) StableHash(addr stablehash.FieldAddress, hasher stablehash.Hasher) {
	fmt.Printf("Entity %s@%s poi before %s\n", e.EntityType, e.EntityID, hex.EncodeToString(hasher.ToBytes()))

	stablehash.String(e.EntityType).StableHash(addr.Child(0), hasher)
	fmt.Printf("Entity poi after type %s (addr %s)\n", hex.EncodeToString(hasher.ToBytes()), addr.Child(0))

	stablehash.String(e.EntityID).StableHash(addr.Child(1), hasher)

	fmt.Printf("Entity poi after id %s (addr %s)\n", hex.EncodeToString(hasher.ToBytes()), addr.Child(1))
	e.Data.StableHash(addr.Child(2), hasher)

	fmt.Printf("Entity poi after fields %s (addr %s)\n", hex.EncodeToString(hasher.ToBytes()), addr.Child(2))
	fmt.Println()

	// This is the ProofOfIndexEvent variant in `graph-node`, SetEntity is 2
	hasher.Write(addr, []byte{0x2})
}

var _ ProofOfIndexingEvent = ProofOfIndexingRemoveEntity{}

func NewProofOfIndexingRemoveEntity(entity *pbentity.EntityChange) ProofOfIndexingRemoveEntity {
	event := ProofOfIndexingRemoveEntity{
		EntityType: entity.Entity,
		EntityID:   entity.Id,
	}

	return event
}

type ProofOfIndexingRemoveEntity struct {
	EntityType string
	EntityID   string
}

// StableHash implements ProofOfIndexingEvent
func (e ProofOfIndexingRemoveEntity) StableHash(addr stablehash.FieldAddress, hasher stablehash.Hasher) {
	stablehash.String(e.EntityType).StableHash(addr.Child(0), hasher)
	stablehash.String(e.EntityID).StableHash(addr.Child(1), hasher)

	// This is the ProofOfIndexEvent variant in `graph-node`, RemoveEntity is 1
	hasher.Write(addr, []byte{0x1})
}
