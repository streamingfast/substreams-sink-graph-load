package schema

import (
	"fmt"
	"io/ioutil"
	"sort"
	"strings"

	"github.com/vektah/gqlparser/ast"
	"github.com/vektah/gqlparser/parser"
)

type EntityDesc struct {
	Name          string
	Fields        map[string]*Field
	orderedFields []*Field
	Immutable     bool
	// vid           uint64
}

// maybe not needed, psql sets it
//func (d *EntityDesc) NextVID() uint64 {
//	vid := d.vid
//	d.vid++
//	return vid
//}

type Field struct {
	Name string
	Type FieldType
	//GoType string

	Nullable bool
	Array    bool
}

func (e *EntityDesc) OrderedFields() []*Field {
	if e.orderedFields != nil {
		return e.orderedFields
	}

	for _, f := range e.Fields {
		e.orderedFields = append(e.orderedFields, f)
	}
	sort.Slice(e.orderedFields, func(i, j int) bool { return e.orderedFields[i].Name < e.orderedFields[j].Name })
	return e.OrderedFields()
}

// field types
type FieldType string

const FieldTypeID FieldType = "ID"
const FieldTypeString FieldType = "String"
const FieldTypeInt FieldType = "Int"
const FieldTypeFloat FieldType = "Float"
const FieldTypeBoolean FieldType = "Boolean"
const FieldTypeBigInt FieldType = "BigInt"
const FieldTypeBigDecimal FieldType = "BigDecimal"
const FieldTypeBytes FieldType = "Bytes"

//		"ID":         "string",
//		"String":     "string",
//		"Int":        "int64",
//		"Float":      "float64",
//		"Boolean":    "entity.Bool",
//		"BigInt":     "entity.Int",
//		"BigDecimal": "entity.Float",
//		"Bytes":      "entity.Bytes",

func GetEntityNamesFromSchema(filename string) (entities []string, err error) {
	graphqlSchemaContent, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("reading file: %w", err)
	}

	graphqlSchemaDoc, gqlErr := parser.ParseSchema(&ast.Source{
		Input: string(graphqlSchemaContent),
	})
	if gqlErr != nil {
		return nil, fmt.Errorf("parsing gql: %w", gqlErr)
	}

	for _, def := range graphqlSchemaDoc.Definitions {
		for _, dir := range def.Directives {
			if dir.Name == "entity" {
				entities = append(entities, strings.ToLower(def.Name))
			}
		}
	}
	if len(entities) == 0 {
		return nil, fmt.Errorf("no entities found from graphql schema file")
	}
	return
}

func GetEntitiesFromSchema(filename string) (entities []*EntityDesc, err error) {
	graphqlSchemaContent, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("reading file: %w", err)
	}

	graphqlSchemaDoc, gqlErr := parser.ParseSchema(&ast.Source{
		Input: string(graphqlSchemaContent),
	})
	if gqlErr != nil {
		return nil, fmt.Errorf("parsing gql: %w", gqlErr)
	}

	for _, def := range graphqlSchemaDoc.Definitions {
		ent, err := parseEntity(def)
		if err != nil {
			return nil, err
		}
		if ent != nil {
			entities = append(entities, ent)
		}
	}

	if len(entities) == 0 {
		return nil, fmt.Errorf("no entities found from graphql schema file")
	}

	entities = append(entities, &EntityDesc{
		Name: PoiEntityName,
		Fields: map[string]*Field{
			"id": {
				Name:     "id",
				Type:     "ID",
				Nullable: false,
				Array:    false,
			},
			"digest": {
				Name:     "digest",
				Type:     "Bytes",
				Nullable: false,
				Array:    false,
			},
		},
	})

	return
}

func parseEntity(def *ast.Definition) (*EntityDesc, error) {
	if def.Kind != "OBJECT" {
		return nil, nil
	}
	var isEntity bool
	var immutable bool
	for _, dir := range def.Directives {
		if dir.Name == "entity" {
			isEntity = true

			for _, arg := range dir.Arguments {
				switch arg.Name {
				case "immutable":
					immutable = true
				default:
					return nil, fmt.Errorf("invalid argument %q for directive @%q on field %s", arg.Name, dir.Name, def.Name)
				}
			}

			break
		}
	}
	if !isEntity {
		return nil, nil
	}

	fields := make(map[string]*Field)
	for _, field := range def.Fields {
		fieldDef, err := ParseFieldDefinition(field)
		if err != nil {
			return nil, fmt.Errorf("entity %q: %w", def.Name, err)
		}
		if fieldDef == nil {
			continue
		}
		fields[fieldDef.Name] = fieldDef
	}

	out := &EntityDesc{
		Fields:    fields,
		Name:      strings.ToLower(def.Name),
		Immutable: immutable,
	}

	return out, nil
}

func ParseFieldDefinition(field *ast.FieldDefinition) (*Field, error) {
	f := &Field{
		Name:  field.Name,
		Type:  toFieldType(field.Type.Name()),
		Array: bool(field.Type.Elem != nil),
	}
	if field.Type.Elem != nil {
		f.Nullable = !field.Type.Elem.NonNull
	} else {
		f.Nullable = !field.Type.NonNull
	}

	for _, directive := range field.Directives {
		if directive.Name == "derivedFrom" {
			return nil, nil
		}
	}

	return f, nil
}

func toFieldType(in string) FieldType {
	switch in {
	case string(FieldTypeID):
		return FieldTypeID
	case string(FieldTypeString):
		return FieldTypeString
	case string(FieldTypeInt):
		return FieldTypeInt
	case string(FieldTypeFloat):
		return FieldTypeFloat
	case string(FieldTypeBoolean):
		return FieldTypeBoolean
	case string(FieldTypeBigInt):
		return FieldTypeBigInt
	case string(FieldTypeBigDecimal):
		return FieldTypeBigDecimal
	case string(FieldTypeBytes):
		return FieldTypeBytes
	default:
		return FieldTypeID // when referencing another object
	}
}
