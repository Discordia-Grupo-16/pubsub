package pubsub

import "context"

// Bus es lo que un servicio necesita del cliente de Pub/Sub. Depender de
// esta interfaz en vez de del *Client concreto es lo que permite sustituirlo
// por el bus en memoria de pubsubtest en los tests del servicio.
type Bus interface {
	// Publish manda un evento al bus.
	Publish(ctx context.Context, event Envelope) error

	// Subscribe registra un consumer que vive hasta que se cancela ctx.
	Subscribe(ctx context.Context, spec Subscription, handler Handler) error
}

var _ Bus = (*Client)(nil)
