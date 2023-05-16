package main

import (
	"fmt"
	"strings"

	_ "github.com/lib/pq"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	. "github.com/streamingfast/cli"
	"github.com/streamingfast/substreams-graph-load/schema"
)

var listEntitiesCmd = Command(listEntitiesE,
	"list-entities <graphql-schema>",
	"list the entities in this subgraph, including poi2$, so you know which entities to process with the 'tocsv' command",
	ExactArgs(1),
	Flags(func(flags *pflag.FlagSet) {
		// do not print info logs and such
		enableMetrics = false
		enablePprof = false
	}),
)

func listEntitiesE(cmd *cobra.Command, args []string) error {
	graphqlSchemaFilename := args[0]
	entities, err := schema.GetEntityNamesFromSchema(graphqlSchemaFilename)
	if err != nil {
		return err
	}
	entities = append(entities, "poi2$")
	fmt.Println(strings.Join(entities, " "))
	return nil
}
