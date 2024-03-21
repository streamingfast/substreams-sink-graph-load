package graphload

import pbentity "github.com/streamingfast/substreams-sink-entity-changes/pb/sf/substreams/sink/entity/v1"

type EntityChangeAtBlockNum struct {
	EntityChange *pbentity.EntityChange `json:"entity_change,omitempty"`
	BlockNum     uint64                 `json:"block_num,omitempty"`
}
