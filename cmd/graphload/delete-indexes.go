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
	"strings"
)

var deleteIndexesCmd = Command(
	deleteIndexesRun,
	"delete <deployment-hash|sgdx_schema> <psql-dsn> <graphql-schema>",
	"Delete all indexes of a database given a schema name or a deployment hash", ExactArgs(3),
	Flags(func(flags *pflag.FlagSet) {}),
)

func deleteIndexesRun(cmd *cobra.Command, args []string) error {
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
		sqlxDB.SetMaxOpenConns(postgres.MAX_CONNECTIONS)
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
		zlog.Debug("adding graphql entity", zap.String("entity", elem.Name))
		graphqlSchemaTables[elem.Name] = true
	}

	query := fmt.Sprintf("SELECT tablename, indexname FROM pg_indexes WHERE schemaname = '%s' ORDER BY tablename, indexname;", pgSchema)
	rows, err := sqlxDB.Query(query)
	if err != nil {
		return fmt.Errorf("fetching database index data")
	}

	var tableName string
	var indexName string
	indexNames := make(map[string][]string)

	defer rows.Close()

	for rows.Next() {
		err = rows.Scan(&tableName, &indexName)
		if err != nil {
			return fmt.Errorf("scanning row: %w", err)
		}
		zlog.Debug("scanned row", zap.String("table_name", tableName), zap.String("index_name", indexName))

		// only delete indexes for the tables that are part of the schema
		if ok := graphqlSchemaTables[tableName]; ok {
			if strings.Contains(indexName, "pkey") || strings.Contains(indexName, "block_range_excl") {
				continue
			}

			indexNames[tableName] = append(indexNames[tableName], indexName)
		}
	}

	if len(indexNames) == 0 {
		zlog.Info("no indexes to delete")
		return nil
	}

	err = rows.Err()
	if err != nil {
		return fmt.Errorf("scanning rows: %w", err)
	}

	for table, idxNames := range indexNames {
		for _, idx := range idxNames {
			// indexes that are part of constraints will not be dropped
			err := dropIndex(sqlxDB, pgSchema, idx)
			if err != nil {
				zlog.Error("failed to drop index", zap.String("table", table), zap.String("idx", idx))
			} else {
				zlog.Info("dropped index", zap.String("table", table), zap.String("idx", idx))
			}
		}

	}

	return nil
}

func dropIndex(sqlxDB *sqlx.DB, pgSchema string, idx string) error {
	rows, err := sqlxDB.Query(fmt.Sprintf("DROP INDEX IF EXISTS %s.%s;", pgSchema, idx))
	if rows != nil {
		defer rows.Close()
	}

	return err
}
