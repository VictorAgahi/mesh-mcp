package stock

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Announce tells the listeners that a SKU changed.
func Announce(ctx context.Context, db *pgxpool.Pool, sku string) error {
	_, err := db.Exec(ctx, "SELECT pg_notify('stock_changed', $1)", sku)
	return err
}
