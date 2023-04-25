package csvprocessor

import (
	"fmt"

	pbentity "github.com/streamingfast/substreams-sink-graphcsv/pb/entity/v1"
	"github.com/streamingfast/substreams-sink-graphcsv/schema"
)

const FieldTypeBigint = "Bigint"
const FieldTypeString = "String_"
const FieldTypeBigdecimal = "Bigdecimal"
const FieldTypeBytes = "Bytes"
const FieldTypeInt = "Int"
const FieldTypeFloat = "Float"
const FieldTypeBoolean = "Boolean"

type Entity struct {
	StartBlock uint64
	Fields     map[string]interface{}
}

func blockRange(start, stop uint64) string {
	if stop == 0 {
		return fmt.Sprintf("[%d,)", start)
	}

	return fmt.Sprintf("[%d,%d]", start, stop)
}

func (e *Entity) Update(newEnt *Entity) {
	e.StartBlock = newEnt.StartBlock
	for k, v := range newEnt.Fields {
		e.Fields[k] = v
	}
}

func newEntity(in *EntityChangeAtBlockNum, desc *schema.EntityDesc) (*Entity, error) {
	if in.EntityChange.Operation == pbentity.EntityChange_DELETE {
		return nil, nil
	}

	e := &Entity{
		StartBlock: in.BlockNum,
	}
	e.Fields = make(map[string]interface{})
	for _, f := range in.EntityChange.Fields {
		fieldDesc, ok := desc.Fields[f.Name]
		if !ok {
			return nil, fmt.Errorf("invalid field %q not part of entity", f.Name)
		}

		var expectedTypedField string

		switch fieldDesc.Type {
		case schema.FieldTypeID, schema.FieldTypeString:
			expectedTypedField = FieldTypeString
		case schema.FieldTypeBigInt:
			expectedTypedField = FieldTypeBigint
		case schema.FieldTypeBigDecimal:
			expectedTypedField = FieldTypeBigdecimal
		case schema.FieldTypeBytes:
			expectedTypedField = FieldTypeBytes
		case schema.FieldTypeInt:
			expectedTypedField = FieldTypeInt
		case schema.FieldTypeFloat:
			expectedTypedField = FieldTypeFloat
		case schema.FieldTypeBoolean:
			expectedTypedField = FieldTypeBoolean
		default:
			return nil, fmt.Errorf("invalid field type: %q", fieldDesc.Type)
		}

		v, ok := f.NewValue.Typed[expectedTypedField]
		if !ok {
			return nil, fmt.Errorf("invalid field %q: wrong type %q", f.Name, fieldDesc.Type)
		}
		e.Fields[f.Name] = v
	}
	return e, nil
}

type EntityChangeAtBlockNum struct {
	EntityChange struct {
		Entity    string                          `json:"entity"`
		ID        string                          `json:"id"`
		Operation pbentity.EntityChange_Operation `json:"operation"`
		Fields    []struct {
			Name     string `json:"name"`
			NewValue struct {
				Typed map[string]interface{} `json:"Typed"`
			} `json:"new_value"`
		}
	} `json:"entity_change"`
	BlockNum uint64 `json:"block_num"`
}
