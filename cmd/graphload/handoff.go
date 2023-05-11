package main

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v4/pgxpool"
	_ "github.com/lib/pq"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	. "github.com/streamingfast/cli"

	"github.com/streamingfast/substreams-graph-load/postgres"
	"go.uber.org/zap"
)

var handoffCmd = Command(handoffE,
	"handoff <deployment-hash> <block_hash> <block_num> <psql-dsn>",
	"informs the graph-node instance that the deployment <deployment-hash> has been processed up to a certain block, so you can reassign indexing",
	ExactArgs(4),
	Flags(func(flags *pflag.FlagSet) {}),
)

func handoffE(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()

	deploymentHash := args[0]
	if !strings.HasPrefix(deploymentHash, "Qm") {
		return fmt.Errorf("invalid deployment-hash %q: should start with 'Qm'", deploymentHash)
	}
	blockHash := strings.TrimPrefix(args[1], strings.ToLower("0x"))
	blockNum, err := strconv.ParseUint(args[2], 10, 64)
	if err != nil {
		return fmt.Errorf("invalid block_num %q: %w", args[2], err)
	}

	psqlDSN := args[3]
	postgresDSN, err := postgres.ParseDSN(psqlDSN)
	if err != nil {
		return fmt.Errorf("invalid postgres DSN %q: %w", psqlDSN, err)
	}

	pool, err := pgxpool.Connect(ctx, fmt.Sprintf("%s pool_min_conns=%d pool_max_conns=%d", postgresDSN.DSN(), 2, 3))
	if err != nil {
		return fmt.Errorf("connecting to postgres: %w", err)
	}

	query := fmt.Sprintf(`UPDATE subgraphs.subgraph_deployment set latest_ethereum_block_hash='%s',latest_ethereum_block_number=%d,entity_count=%d where deployment='%s'`,
		blockHash,
		blockNum,
		1000000,
		deploymentHash)

	zlog.Info("Setting block latest ethereum block", zap.String("block_hash", blockHash), zap.Uint64("block_num", blockNum), zap.String("deployment", deploymentHash))

	conn, err := pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("pool acquire: %w", err)
	}
	defer conn.Release()

	ret := conn.Conn().PgConn().Exec(ctx, query)
	res, err := ret.ReadAll()
	if err != nil {
		return err
	}

	if len(res) != 1 {
		return fmt.Errorf("invalid return from postgres: expecting single result, got %d", len(res))
	}

	if res[0].Err != nil {
		return res[0].Err
	}

	zlog.Info("complete", zap.Int64("rows_affected", res[0].CommandTag.RowsAffected()))
	return nil
}
