package main

import (
	"fmt"
	"github.com/jmoiron/sqlx"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	. "github.com/streamingfast/cli"
	"github.com/streamingfast/substreams-graph-load/postgres"
	"github.com/streamingfast/substreams-graph-load/schema"
	"go.uber.org/zap"
	"os"
	"strings"
)

var extractIndexesCmd = Command(
	extractIndexesRun,
	"extract <deployment-hash|sgdx_schema> <psql-dsn> <graphql-schema>",
	"Extract all indexes of a database given a schema name or a deployment hash",
	ExactArgs(3),
	Flags(func(flags *pflag.FlagSet) {
		flags.Bool("save", false, "save contents to a ddl file")
	}),
)

type indexData struct {
	indexName string
	indexDef  string
}

func (i *indexData) PrintOutput() {
	fmt.Printf("=> indexName %s indexDef %s\n", i.indexName, i.indexDef)
}

type DatabaseIndexData struct {
	data map[string][]*indexData
}

func (d *DatabaseIndexData) PrintOutput() {
	for k, indexes := range d.data {
		fmt.Println("====> table ", k)
		for _, i := range indexes {
			i.PrintOutput()
		}
	}
}

func (d *DatabaseIndexData) ToString() string {
	var sb strings.Builder

	for _, value := range d.data {
		for _, index := range value {
			sb.WriteString(index.indexDef)
			sb.WriteString("\n")
		}
	}

	return sb.String()
}

func extractIndexesRun(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()

	schemaOrHash := args[0]
	psqlDSN := args[1]
	graphqlSchema := args[2]
	zlog.Debug("psql DSN", zap.String("dsn", psqlDSN))

	postgresDSN, err := postgres.ParseDSN(psqlDSN)
	if err != nil {
		return fmt.Errorf("invalid postgres DSN %q: %w", psqlDSN, err)
	}

	var pgSchema string
	var sqlxDB *sqlx.DB
	switch {
	case strings.HasPrefix(schemaOrHash, "sgd"):
		pgSchema = schemaOrHash
		zlog.Debug("postgresql schema", zap.String("sgd", pgSchema))
	case strings.HasPrefix(schemaOrHash, "Qm"):
		sqlxDB, err = postgres.CreatePostgresDB(ctx, postgresDSN)
		if err != nil {
			return fmt.Errorf("creating postgres db: %w", err)
		}
		pgSchema, err = schema.GetSubgraphSchema(ctx, sqlxDB, schemaOrHash)
		if err != nil {
			return fmt.Errorf("unable to retrieve specs: %q", err)
		}

		zlog.Debug("postgresql schema from deployment hash", zap.String("deployment_hash", schemaOrHash), zap.String("sgd", pgSchema))
	default:
		return fmt.Errorf("invalid value for first parameter: %q, should be either a postgresql schema (ex: sgd1) or a deployment Qm hash", schemaOrHash)
	}

	zlog.Debug("graphql schema", zap.String("path", graphqlSchema))
	graphqlEntities, err := schema.GetEntitiesFromSchema(graphqlSchema)
	if err != nil {
		return fmt.Errorf("reading schema from %q: %w", graphqlSchema, err)
	}
	graphqlSchemaTables := make(map[string]bool)

	for _, elem := range graphqlEntities {
		graphqlSchemaTables[elem.Name] = true
	}

	query := fmt.Sprintf("SELECT tablename, indexname, indexdef FROM pg_indexes WHERE schemaname = '%s' ORDER BY tablename, indexname;", pgSchema)
	rows, err := sqlxDB.Query(query)
	if err != nil {
		return fmt.Errorf("fetching database index data")
	}

	databaseIndexData := &DatabaseIndexData{
		data: make(map[string][]*indexData),
	}
	var tableName string
	var indexName string
	var indexDef string

	defer rows.Close()

	for rows.Next() {
		err = rows.Scan(&tableName, &indexName, &indexDef)
		if err != nil {
			return fmt.Errorf("scanning row: %w", err)
		}
		zlog.Debug("scanned row", zap.String("table_name", tableName), zap.String("index_name", indexName), zap.String("index_def", indexDef))

		// only extract indexes for the tables that are part of the schema
		if ok := graphqlSchemaTables[tableName]; ok {
			databaseIndexData.data[tableName] = append(databaseIndexData.data[tableName], &indexData{indexName: indexName, indexDef: indexDef + ";"})
		}
	}

	err = rows.Err()
	if err != nil {
		return fmt.Errorf("scanning rows: %w", err)
	}

	databaseIndexData.PrintOutput()

	if mustGetBool(cmd, "save") {
		d1 := []byte(databaseIndexData.ToString())
		err := os.WriteFile("./create_indexes.ddl", d1, 0644)
		if err != nil {
			return fmt.Errorf("writing file create_indexes.ddl: %w", err)
		}
	}

	return nil
}
