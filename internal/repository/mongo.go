package repository

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/Discordia-Grupo-16/discordia-chat/internal/config"
)

// Connect abre una conexión a MongoDB con pool y timeouts acotados por cfg, y
// la verifica con un ping antes de devolverla. Si el ping falla, cierra el
// cliente para no dejar goroutines de monitoreo colgadas.
func Connect(ctx context.Context, cfg config.Config) (*mongo.Client, error) {
	opts := options.Client().
		ApplyURI(cfg.MongoURI).
		SetMinPoolSize(cfg.MongoMinPoolSize).
		SetMaxPoolSize(cfg.MongoMaxPoolSize).
		SetConnectTimeout(cfg.MongoConnectTimeout).
		SetServerSelectionTimeout(cfg.MongoServerSelectionTimeout).
		SetTimeout(cfg.MongoOperationTimeout)

	client, err := mongo.Connect(opts)
	if err != nil {
		return nil, fmt.Errorf("mongo connect: %w", err)
	}

	if err := client.Ping(ctx, nil); err != nil {
		_ = client.Disconnect(context.WithoutCancel(ctx))
		return nil, fmt.Errorf("mongo ping: %w", err)
	}

	return client, nil
}

// IndexSpecs mapea cada colección a los índices que debe tener al arranque.
type IndexSpecs map[string][]mongo.IndexModel

// EnsureIndexes crea los índices declarados en specs. `createIndexes` es
// idempotente en Mongo: si un índice ya existe con las mismas keys y
// opciones, no hace nada; solo falla si el mismo nombre existe con una
// definición distinta, lo cual señala un cambio de esquema mal migrado.
func EnsureIndexes(ctx context.Context, db *mongo.Database, specs IndexSpecs) error {
	for collection, models := range specs {
		if len(models) == 0 {
			continue
		}
		if _, err := db.Collection(collection).Indexes().CreateMany(ctx, models); err != nil {
			return fmt.Errorf("ensure indexes for %s: %w", collection, err)
		}
	}
	return nil
}
