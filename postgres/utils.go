package postgres

import (
	"context"
	"fmt"
	"github.com/jmoiron/sqlx"
	"go.uber.org/zap"
	"time"
)

const MAX_CONNECTIONS = 10

func CreatePostgresDB(ctx context.Context, connectionInfo *DSN) (*sqlx.DB, error) {
	dsn := connectionInfo.DSN()
	zlog.Info("connecting to postgres", zap.String("data_source", dsn))
	dbConnectCtx, dbCancel := context.WithTimeout(ctx, 5*time.Second)
	defer dbCancel()

	db, err := sqlx.ConnectContext(dbConnectCtx, "postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to postgres: %w", err)
	}

	db.SetMaxOpenConns(250)

	zlog.Info("database connections created")
	return db, nil
}
