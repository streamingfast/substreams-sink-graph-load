package main

import (
	"fmt"
	"github.com/abourget/llerrgroup"
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
		sqlxDB.SetMaxOpenConns(10)
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

	lenStmt := 0
	for _, idxN := range indexNames {
		lenStmt += len(idxN)
	}

	logCh := make(chan string, lenStmt)
	errCh := make(chan error, lenStmt)

	zlog.Debug("launching wait group to delete indexes")
	llg := llerrgroup.New(10)

	go func() {
		defer close(logCh)
		for log := range logCh {
			zlog.Info(log)
		}
	}()

	go func() {
		defer close(errCh)
		for err := range errCh {
			zlog.Error("creating index", zap.Error(err))
		}
	}()

	for t, idxs := range indexNames {
		zlog.Debug("launching worker to delete indexes", zap.String("table", t))
		if llg.Stop() {
			fmt.Println("ok")
			continue
		}
		table := t
		idxNames := idxs

		llg.Go(
			func() error {
				for _, idx := range idxNames {
					// indexes that are part of constraints will not be dropped
					_, err = sqlxDB.Query(fmt.Sprintf("DROP INDEX IF EXISTS %s.%s;", pgSchema, idx))
					if err != nil {
						errCh <- err
					} else {
						logCh <- fmt.Sprintf("dropped index for table %s indexDef %s", table, idx)
					}
				}

				return nil
			},
		)
	}

	if err := llg.Wait(); err != nil {
		return err
	}

	return nil
}
