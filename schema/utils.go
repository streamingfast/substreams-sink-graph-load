package schema

import (
	"context"
	"fmt"
	"github.com/jmoiron/sqlx"
)

func GetSubgraphSchema(ctx context.Context, db *sqlx.DB, deploymentID string) (string, error) {
	query := `
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
