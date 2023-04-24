package schema

import (
	"fmt"
	"io/ioutil"

	"github.com/vektah/gqlparser/ast"
	"github.com/vektah/gqlparser/parser"
)

type Entity struct {
	Name      string
	Fields    []Field
	Immutable bool
}

type Field struct {
	Name   string
	Type   string
	GoType string

	Array   bool
	Derived bool
	Hidden  bool
}

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
				entities = append(entities, def.Name)
			}
		}
	}
	if len(entities) == 0 {
		return nil, fmt.Errorf("no entities found from graphql schema file")
	}
	return
}
