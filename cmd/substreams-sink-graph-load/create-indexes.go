package main

import (
	"bufio"
	"fmt"
	"github.com/abourget/llerrgroup"
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

var createIndexesCmd = Command(
	createIndexesRun,
	"create-index <deployment-hash|sgdx_schema> <create-indexes-filepath> <psql-dsn> <graphql-schema>",
	"Create all indexes of a database given a schema name or a deployment hash", ExactArgs(4),
	Flags(func(flags *pflag.FlagSet) {}),
)

func createIndexesRun(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()

	schemaOrHash := args[0]
	createIndexesFilepath := args[1]
	if !strings.HasSuffix(createIndexesFilepath, ".ddl") {
		return fmt.Errorf("create indexes file is not a ddl file")
	}

	psqlDSN := args[2]
	graphqlSchema := args[3]
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
		graphqlSchemaTables[elem.Name] = true
	}

	var tableName string
	indexDefs := make(map[string][]string)

	// todo: read createIndexesFilepath and extract create indexes in the indexDefs
	file, err := os.Open(createIndexesFilepath)
	if err != nil {
		return fmt.Errorf("opening file %s: %w", createIndexesFilepath, err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	// optionally, resize scanner's capacity for lines over 64K, see next example
	for scanner.Scan() {
		indexDef := scanner.Text()
		splitStr := pgSchema + "."
		indexLineSplit := strings.Split(indexDef, splitStr)[1]
		tableName = strings.Split(indexLineSplit, " ")[0]

		if ok := graphqlSchemaTables[tableName]; ok {
			indexDefs[tableName] = append(indexDefs[tableName], indexDef)
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("reading lines from scanner: %w", err)
	}

	lenStmt := 0
	for _, idxDefs := range indexDefs {
		lenStmt += len(idxDefs)
	}

	logCh := make(chan string, lenStmt)
	errCh := make(chan error, lenStmt)

	numOfClients := new(int32)
	*numOfClients = 0

	llg := llerrgroup.New(postgres.MAX_CONNECTIONS)

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

	for t, idxs := range indexDefs {
		zlog.Info("launching go routine to create indexes", zap.String("table", t))
		if llg.Stop() {
			continue
		}
		table := t
		idxDefs := idxs

		llg.Go(func() error {
			for _, indexDef := range idxDefs {
				err = createIndex(sqlxDB, indexDef)
				if err != nil {
					errCh <- err
				} else {
					logCh <- fmt.Sprintf("created index for table %s indexDef %s", table, indexDef)
				}
			}
			return nil
		})
	}

	if err := llg.Wait(); err != nil {
		return err
	}

	return nil
}

func createIndex(sqlxDB *sqlx.DB, indexDef string) error {
	rows, err := sqlxDB.Query(indexDef)
	if rows != nil {
		defer rows.Close()
	}

	return err
}
