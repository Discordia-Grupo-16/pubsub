package main

import (
	"context"
	"log/slog"
	"os"

	"github.com/Discordia-Grupo-16/discordia-chat/internal/config"
	"github.com/Discordia-Grupo-16/discordia-chat/internal/repository"
)

func main() {
	cfg := config.Load()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	logger.Info("starting discordia-chat", "env", cfg.Env, "port", cfg.Port)

	ctx := context.Background()

	client, err := repository.Connect(ctx, cfg)
	if err != nil {
		logger.Error("mongo connect failed", "error", err)
		os.Exit(1)
	}
	defer func() {
		if err := client.Disconnect(context.Background()); err != nil {
			logger.Error("mongo disconnect failed", "error", err)
		}
	}()

	db := client.Database(cfg.MongoDB)
	if err := repository.EnsureIndexes(ctx, db, repository.IndexSpecs{
		repository.MessagesCollection: repository.MessagesIndexes(),
	}); err != nil {
		logger.Error("mongo ensure indexes failed", "error", err)
		os.Exit(1)
	}

	logger.Info("mongo ready", "database", cfg.MongoDB)
}
