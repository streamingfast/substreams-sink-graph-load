package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/jmoiron/sqlx"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	. "github.com/streamingfast/cli"
	"github.com/streamingfast/dstore"

	"github.com/streamingfast/substreams-sink-graphcsv/postgres"
	"github.com/streamingfast/substreams-sink-graphcsv/schema"
	"go.uber.org/zap"
)

var injectCSVCmd = Command(injectCSVE,
	"inject-csv (deployment-hash|sgdx-schema) <input-path> <entity> <graphql-schema> <psql-dsn> <start-block> <stop-block>",
	"Injects generated CSV entities for <subgraph-name>'s deployment version <version> into the database pointed by <psql-dsn> argument. Can be run in parallel for multiple entities up to the same stop-block",
	ExactArgs(7),
	Flags(func(flags *pflag.FlagSet) {}),
)

func injectCSVE(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()

	schemaOrHash := args[0]

	inputPath := args[1]
	entity := args[2]

	graphqlSchema := args[3]
	psqlDSN := args[4]
	startBlock, err := strconv.ParseUint(args[5], 10, 64)
	if err != nil {
		return fmt.Errorf("invalid start block %q: %w", args[5], err)
	}
	stopBlock, err := strconv.ParseUint(args[6], 10, 64)
	if err != nil {
		return fmt.Errorf("invalid stop block %q: %w", args[6], err)
	}

	postgresDSN, err := postgres.ParseDSN(psqlDSN)
	if err != nil {
		return fmt.Errorf("invalid postgres DSN %q: %w", psqlDSN, err)
	}

	var pgSchema string
	switch {
	case strings.HasPrefix(schemaOrHash, "sgd"):
		pgSchema = schemaOrHash
	case strings.HasPrefix(schemaOrHash, "Qm"):
		sqlxDB, err := createPostgresDB(ctx, postgresDSN)
		if err != nil {
			return fmt.Errorf("creating postgres db: %w", err)
		}
		pgSchema, err = getSubgraphSchema(ctx, sqlxDB, schemaOrHash)
		if err != nil {
			return fmt.Errorf("unable to retrieve specs: %q", err)
		}
	default:
		return fmt.Errorf("invalid value for first parameter: %q, should be either a postgresql schema (ex: sgd1) or a deployment Qm hash", schemaOrHash)
	}

	zlog.Info("connecting to input store")
	inputStore, err := dstore.NewStore(inputPath, "", "", false)
	if err != nil {
		return fmt.Errorf("unable to create input store: %w", err)
	}

	pool, err := pgxpool.Connect(ctx, fmt.Sprintf("%s pool_min_conns=%d pool_max_conns=%d", postgresDSN.DSN(), 2, 3))
	if err != nil {
		return fmt.Errorf("connecting to postgres: %w", err)
	}

	graphqlEntities, err := schema.GetEntitiesFromSchema(graphqlSchema)
	if err != nil {
		return fmt.Errorf("reading schema from %q: %w", graphqlSchema, err)
	}

	nonNullableFields := []string{"id"}
	for _, ent := range graphqlEntities {
		if ent.Name != entity {
			continue
		}

		for _, f := range ent.Fields {
			if f.Name == "id" {
				continue
			}
			if f.Name == "block_range" || f.Name == "block$" {
				nonNullableFields = append(nonNullableFields, f.Name)
				continue
			}
			if !f.Nullable {
				nonNullableFields = append(nonNullableFields, f.Name)
			}
		}
	}

	t0 := time.Now()

	tableName := entity
	filler := NewTableFiller(pool, pgSchema, tableName, startBlock, stopBlock, nonNullableFields, inputStore)
	theTableName := tableName
	if err := filler.Run(ctx); err != nil {
		return fmt.Errorf("table filler %q: %w", theTableName, err)
	}

	zlog.Info("table done", zap.Duration("total", time.Since(t0)))
	return nil
}

type TableFiller struct {
	pqSchema          string
	tblName           string
	nonNullableFields []string

	in            dstore.Store
	startBlockNum uint64
	stopBlockNum  uint64
	pool          *pgxpool.Pool
}

func NewTableFiller(pool *pgxpool.Pool, pqSchema, tblName string, startBlockNum, stopBlockNum uint64, nonNullableFields []string, inStore dstore.Store) *TableFiller {
	return &TableFiller{
		tblName:           tblName,
		pqSchema:          pqSchema,
		pool:              pool,
		nonNullableFields: nonNullableFields,
		startBlockNum:     startBlockNum,
		stopBlockNum:      stopBlockNum,
		in:                inStore,
	}
}

type sortedFilenames []string

func (p sortedFilenames) Len() int           { return len(p) }
func (p sortedFilenames) Less(i, j int) bool { return p[i] > p[j] }
func (p sortedFilenames) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

//func (t *TblShard) pruneFiles(ctx context.Context, filenames []string) (out sortedFilenames, err error) {
//	conn, err := t.pool.Acquire(ctx)
//	if err != nil {
//		return nil, fmt.Errorf("unable to acquire conn: %w", err)
//	}
//	defer conn.Release()
//	filesToPrune := map[string]bool{}
//	for _, filename := range filenames {
//		filesToPrune[filename] = true
//	}
//
//	result, err := conn.Query(ctx, fmt.Sprintf("SELECT * FROM %s.progress$ WHERE (table_name=$1)", t.pqSchema), t.tblName)
//	if err != nil {
//		return nil, err
//	}
//
//	for result.Next() {
//		values, err := result.Values()
//		if err != nil {
//			return nil, fmt.Errorf("unable to retrieve result row: %w", err)
//		}
//		injectedFilename := values[0].(string)
//		if _, found := filesToPrune[injectedFilename]; found {
//			delete(filesToPrune, injectedFilename)
//		}
//	}
//	for filename := range filesToPrune {
//		out = append(out, filename)
//	}
//	sort.Sort(out)
//	return out, nil
//}

//func (t *TableFiller) markProgress(ctx context.Context, filename string, timestamp time.Time) error {
//	conn, err := t.pool.Acquire(ctx)
//	if err != nil {
//		return fmt.Errorf("unable to acquire conn: %w", err)
//	}
//	defer conn.Release()
//
//	query := fmt.Sprintf("INSERT INTO %s.progress$ (filename, table_name, injected_at) VALUES ($1, $2, $3)", t.pqSchema)
//	_, err = conn.Exec(ctx, query, filename, t.tblName, timestamp)
//	if err != nil {
//		return err
//	}
//	return nil
//}

func s(str string) *string {
	return &str
}

func extractFieldsFromFirstLine(ctx context.Context, filename string, store dstore.Store) ([]string, error) {
	fl, err := store.OpenObject(ctx, filename)
	if err != nil {
		return nil, fmt.Errorf("opening csv: %w", err)
	}
	defer fl.Close()

	r := csv.NewReader(fl)
	out, err := r.Read()
	if err != nil {
		return nil, err
	}

	if out[0] != "id" {
		return nil, fmt.Errorf("invalid CSV: first column should be 'id'")
	}
	if out[1] != "block_range" && out[1] != "block$" {
		return nil, fmt.Errorf("invalid CSV: second column should be 'block_range' or 'block$'")
	}

	return out, nil
}

func (t *TableFiller) Run(ctx context.Context) error {
	zlog.Info("table filler", zap.String("table", t.tblName))

	loadFiles, err := injectFilesToLoad(t.in, t.tblName, t.stopBlockNum, t.startBlockNum)
	if err != nil {
		return fmt.Errorf("listing files: %w", err)
	}

	if len(loadFiles) == 0 {
		return fmt.Errorf("no file to process")
	}
	dbFields, err := extractFieldsFromFirstLine(ctx, loadFiles[0], t.in)
	if err != nil {
		return fmt.Errorf("extracting fields from first csv line: %w", err)
	}

	//prunedFilenames, err := t.pruneFiles(ctx, loadFiles)
	//if err != nil {
	//	return fmt.Errorf("unable to prune filename list: %w", err)
	//}

	zlog.Info("files to load",
		zap.String("table", t.tblName),
		zap.Int("file_count", len(loadFiles)),
		//		zap.Int("pruned_file_count", len(prunedFilenames)),
	)
	prunedFilenames := loadFiles

	for _, filename := range prunedFilenames {
		zlog.Info("opening file", zap.String("file", filename))

		if err := t.injectFile(ctx, filename, dbFields, t.nonNullableFields); err != nil {
			return fmt.Errorf("failed to inject file %q: %w", filename, err)
		}

		// FIXME: set up a way to mark progress
		//
		//if err := t.markProgress(ctx, filename, time.Now()); err != nil {
		//	return fmt.Errorf("failed to mark progress file %q: %w", filename, err)
		//}
	}

	return nil
}

func (t *TableFiller) injectFile(ctx context.Context, filename string, dbFields, nonNullableFields []string) error {
	fl, err := t.in.OpenObject(ctx, filename)
	if err != nil {
		return fmt.Errorf("opening csv: %w", err)
	}
	defer fl.Close()

	query := fmt.Sprintf(`COPY %s.%s ("%s") FROM STDIN WITH (FORMAT CSV, HEADER, FORCE_NOT_NULL ("%s"))`, t.pqSchema, t.tblName, strings.Join(dbFields, `","`), strings.Join(nonNullableFields, `","`))
	zlog.Info("loading file into sql", zap.String("filename", filename), zap.String("table_name", t.tblName), zap.Strings("db_fields", dbFields), zap.Strings("non_nullable_fields", nonNullableFields))

	t0 := time.Now()

	conn, err := t.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("pool acquire: %w", err)
	}
	defer conn.Release()

	tag, err := conn.Conn().PgConn().CopyFrom(ctx, fl, query)
	if err != nil {
		return fmt.Errorf("failed COPY FROM for %q: %w", t.tblName, err)
	}
	count := tag.RowsAffected()
	elapsed := time.Since(t0)
	zlog.Info("loaded file into sql",
		zap.String("filename", filename),
		zap.String("table_name", t.tblName),
		zap.Int64("rows_affected", count),
		zap.Duration("elapsed", elapsed),
	)

	return nil
}

func injectFilesToLoad(inputStore dstore.Store, tableName string, stopBlockNum, desiredStartBlockNum uint64) (out []string, err error) {
	err = inputStore.Walk(context.Background(), tableName+"/", func(filename string) (err error) {
		startBlockNum, _, err := getBlockRange(filename)
		if err != nil {
			return fmt.Errorf("fail reading block range in %q: %w", filename, err)
		}

		if stopBlockNum != 0 && startBlockNum >= stopBlockNum {
			return dstore.StopIteration
		}

		if startBlockNum < desiredStartBlockNum {
			return nil
		}

		if strings.Contains(filename, ".csv") {
			out = append(out, filename)
		}

		return nil
	})
	return
}

func createPostgresDB(ctx context.Context, connectionInfo *postgres.DSN) (*sqlx.DB, error) {
	dsn := connectionInfo.DSN()
	zlog.Info("connecting to postgres", zap.String("data_source", dsn))
	dbConnecCtx, dbCancel := context.WithTimeout(ctx, 5*time.Second)
	defer dbCancel()

	db, err := sqlx.ConnectContext(dbConnecCtx, "postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to postgres: %w", err)
	}

	db.SetMaxOpenConns(250)

	zlog.Info("database connections created")
	return db, nil
}

func getSubgraphSchema(ctx context.Context, db *sqlx.DB, deploymentID string) (string, error) {
	query := `
	SELECT
	select name AS schema 
	FROM public.deployment_schemas 
	WHERE subgraph = $1`
	row := &subgraphSchemaResponse{}
	if err := db.GetContext(ctx, row, query, deploymentID); err != nil {
		return "", fmt.Errorf("fetch schema for %q: %w", deploymentID, err)
	}
	return row.Schema, nil
}

type subgraphSchemaResponse struct {
	Schema string `db:"schema"`
}

var blockRangeRegex = regexp.MustCompile(`(\d{10})-(\d{10})`)

func getBlockRange(filename string) (uint64, uint64, error) {
	match := blockRangeRegex.FindStringSubmatch(filename)
	if match == nil {
		return 0, 0, fmt.Errorf("no block range in filename: %s", filename)
	}

	startBlock, _ := strconv.ParseUint(match[1], 10, 64)
	stopBlock, _ := strconv.ParseUint(match[2], 10, 64)
	return startBlock, stopBlock, nil
}
