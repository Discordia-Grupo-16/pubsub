package repository_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/Discordia-Grupo-16/discordia-chat/internal/config"
	"github.com/Discordia-Grupo-16/discordia-chat/internal/repository"
)

// connectForTest levanta un cliente contra el Mongo declarado en MONGO_URI
// (default localhost:27017, igual que config.Load) con timeouts cortos.
// Si no hay un Mongo real escuchando, salta el test: este repo no tiene
// containers propios todavía (llegan con SCRUM-129/SCRUM-138), así que la
// verificación contra un Mongo real queda para quien corra `make test` con
// uno levantado en local o para CI.
func connectForTest(t *testing.T) (*mongo.Client, config.Config) {
	t.Helper()

	cfg := config.Load()
	cfg.MongoConnectTimeout = 2 * time.Second
	cfg.MongoServerSelectionTimeout = 2 * time.Second

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	client, err := repository.Connect(ctx, cfg)
	if err != nil {
		t.Skipf("no hay un MongoDB alcanzable en %q, se salta el test de integración: %v", cfg.MongoURI, err)
	}

	t.Cleanup(func() {
		_ = client.Disconnect(context.Background())
	})

	return client, cfg
}

func TestConnect_PingsSuccessfully(t *testing.T) {
	client, _ := connectForTest(t)
	assert.NotNil(t, client)
}

func TestConnect_FailsFastOnUnreachableHost(t *testing.T) {
	cfg := config.Load()
	cfg.MongoURI = "mongodb://127.0.0.1:1"
	cfg.MongoConnectTimeout = 200 * time.Millisecond
	cfg.MongoServerSelectionTimeout = 200 * time.Millisecond

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	_, err := repository.Connect(ctx, cfg)

	assert.Error(t, err)
}

func TestEnsureIndexes_IsIdempotent(t *testing.T) {
	client, cfg := connectForTest(t)
	db := client.Database(cfg.MongoDB)
	collection := "messages_test_" + time.Now().Format("20060102150405.000000")

	t.Cleanup(func() {
		_ = db.Collection(collection).Drop(context.Background())
	})

	specs := repository.IndexSpecs{collection: repository.MessagesIndexes()}

	err := repository.EnsureIndexes(context.Background(), db, specs)
	require.NoError(t, err)

	// Crear los mismos índices una segunda vez no debe fallar: es la base
	// de que `EnsureIndexes` se pueda correr en cada arranque del servicio.
	err = repository.EnsureIndexes(context.Background(), db, specs)
	require.NoError(t, err)

	specsCreated, err := db.Collection(collection).Indexes().ListSpecifications(context.Background())
	require.NoError(t, err)

	// _id_ más los dos índices declarados en MessagesIndexes.
	assert.Len(t, specsCreated, 3)
}

func TestEnsureIndexes_SkipsEmptySpecs(t *testing.T) {
	err := repository.EnsureIndexes(context.Background(), nil, repository.IndexSpecs{
		"messages": nil,
	})

	assert.NoError(t, err)
}
