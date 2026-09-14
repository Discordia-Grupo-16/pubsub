package pubsub

import (
	"errors"
	"fmt"
)

// ErrPermanent marca un fallo del que no tiene sentido reintentar. Los RNF
// piden distinguir fallos transitorios (reintentar) de permanentes
// (compensar o notificar): un timeout de red se reintenta, un payload que no
// respeta el contrato va a fallar exactamente igual las tres veces
// siguientes y solo bloquea la cola.
var ErrPermanent = errors.New("permanent failure")

// Permanent envuelve un error para que el consumer lo mande directo a la
// dead-letter queue, sin reintentos. Devuelve nil si err es nil, para poder
// escribir `return pubsub.Permanent(validate(x))` sin ramificar.
//
//	if !isMember(userID) {
//	    return pubsub.Permanent(fmt.Errorf("user %s is not a member", userID))
//	}
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%w: %w", ErrPermanent, err)
}

// IsPermanent responde si el error está marcado como permanente. Todo lo que
// no lo esté se trata como transitorio y entra al ciclo de reintentos.
func IsPermanent(err error) bool {
	return errors.Is(err, ErrPermanent)
}
