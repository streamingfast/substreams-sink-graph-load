package poi

import (
	"encoding/hex"
	"fmt"

	pbentity "github.com/streamingfast/substreams-sink-graphcsv/pb/entity/v1"
	"github.com/streamingfast/substreams-sink-graphcsv/stablehash"
	"go.uber.org/zap"
)

type ProofOfIndexing struct {
	blockNumber uint64
	stream      *BlockEventStream
}

func NewProofOfIndexing(blockNumber uint64, version Version) *ProofOfIndexing {
	if version == VersionLegacy {
		panic("legacy proof of indexing not supported")
	}

	return &ProofOfIndexing{
		blockNumber: blockNumber,
		stream: &BlockEventStream{
			vecLength:    0,
			handlerStart: 0,
			blockNumber:  blockNumber,
			hasher:       stablehash.NewFastHasher(),
		},
	}
}

func (p *ProofOfIndexing) Write(event ProofOfIndexingEvent) {
	p.stream.Write(event)
}

func (p *ProofOfIndexing) SetEntity(entity *pbentity.EntityChange) {
	// We could improve the hashing speed by avoid the transformation to ProofOfIndexingSetEntity entierly
	p.stream.Write(NewProofOfIndexingSetEntity(entity))
}

func (p *ProofOfIndexing) RemoveEntity(entity *pbentity.EntityChange) {
	// We could improve the hashing speed by avoid the transformation to ProofOfIndexingRemoveEntity entierly
	p.stream.Write(NewProofOfIndexingRemoveEntity(entity))
}

// Pause returns the current `poi` bytes up to now.
func (p *ProofOfIndexing) Pause(prev []byte) ([]byte, error) {
	p.stream.fastHasherWrite(stablehash.U64(p.stream.vecLength), []uint64{
		1, 0, p.stream.blockNumber, 0,
	})

	if len(prev) > 0 {
		if tracer.Enabled() {
			zlog.Debug("pausing PoI has previous value", zap.Uint64("block_num", p.blockNumber), zap.String("previous", hex.EncodeToString(prev)))
		}

		prevHasher, err := stablehash.NewFastHasherFromBytes(prev)
		if err != nil {
			return nil, fmt.Errorf("invalid previous value %q: %w", hex.EncodeToString(prev), err)
		}

		p.stream.hasher.Mixin(prevHasher)
	}

	out := p.stream.hasher.ToBytes()

	if tracer.Enabled() {
		zlog.Debug("paused PoI", zap.Uint64("block_num", p.blockNumber), zap.String("current", hex.EncodeToString(out)))
	}

	return out, nil
}

// DebugCurrent returns the currently bytes value of the POI, it's useful for debugging
// purposes and nothing else.
func (p *ProofOfIndexing) DebugCurrent() string {
	return hex.EncodeToString(p.stream.hasher.ToBytes())
}

type BlockEventStream struct {
	vecLength    uint64
	handlerStart uint64
	blockNumber  uint64
	hasher       stablehash.Hasher
}

func (e *BlockEventStream) Write(event ProofOfIndexingEvent) {
	children := []uint64{
		1,             // kvp -> v
		0,             // PoICausalityRegion.blocks: Vec<Block>
		e.blockNumber, // Vec<Block> -> [i]
		0,             // Block.events -> Vec<ProofOfIndexingEvent>
		e.vecLength,
	}

	e.fastHasherWrite(event, children)
	e.vecLength += 1
}

func (e *BlockEventStream) fastHasherWrite(hashable stablehash.Hashable, children []uint64) {
	addr := stablehash.AddressRoot()
	for _, child := range children {
		addr = addr.Child(child)
	}

	hashable.StableHash(addr, e.hasher)
}

func (e *BlockEventStream) StartHandler() {
	e.handlerStart = e.vecLength
}

//go:generate go-enum -f=$GOFILE --marshal --names --nocase

// ENUM(
//
//	Legacy
//	Fast
//
// )
type Version uint
