package pubsub_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Discordia-Grupo-16/pubsub"
)

var errNotAMember = errors.New("user is not a member")

func TestPermanent_MarksTheErrorAndKeepsTheCause(t *testing.T) {
	err := pubsub.Permanent(errNotAMember)

	require.Error(t, err)
	assert.True(t, pubsub.IsPermanent(err))
	assert.ErrorIs(t, err, errNotAMember, "la causa original sigue accesible para quien loguee el fallo")
	assert.Contains(t, err.Error(), "user is not a member")
}

func TestPermanent_NilStaysNil(t *testing.T) {
	assert.NoError(t, pubsub.Permanent(nil), "permite escribir return Permanent(validate(x)) sin ramificar")
}

func TestIsPermanent_TransientByDefault(t *testing.T) {
	assert.False(t, pubsub.IsPermanent(errors.New("connection reset by peer")),
		"lo que no está marcado se reintenta: un fallo de red no es definitivo")
	assert.False(t, pubsub.IsPermanent(nil))
}

func TestIsPermanent_SurvivesWrapping(t *testing.T) {
	err := fmt.Errorf("handling chat.message.sent: %w", pubsub.Permanent(errNotAMember))

	assert.True(t, pubsub.IsPermanent(err), "el handler puede envolver el error con contexto propio")
}

func TestIsPermanent_InvalidEnvelopeIsNotAutomaticallyPermanent(t *testing.T) {
	// ErrInvalidEnvelope y ErrPermanent son independientes: es el consumer
	// el que decide que un sobre ilegible no se reintenta.
	assert.False(t, pubsub.IsPermanent(pubsub.ErrInvalidEnvelope))
}
