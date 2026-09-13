package repository

import (
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// MessagesCollection es el nombre de la colección de mensajes.
const MessagesCollection = "messages"

// MessagesIndexes son los índices mínimos de persistencia de mensajes
// (discordia-docs/PLAN-SPRINT-1.md §3.4): {channelId, createdAt} para leer
// el historial de un canal en orden, y el único {channelId, clientMessageId}
// que implementa la idempotencia del reenvío que la consigna pide cubrir.
func MessagesIndexes() []mongo.IndexModel {
	return []mongo.IndexModel{
		{
			Keys: bson.D{{Key: "channelId", Value: 1}, {Key: "createdAt", Value: 1}},
		},
		{
			Keys:    bson.D{{Key: "channelId", Value: 1}, {Key: "clientMessageId", Value: 1}},
			Options: options.Index().SetUnique(true),
		},
	}
}
