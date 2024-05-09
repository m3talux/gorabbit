package gorabbit_test

import (
	"testing"

	"github.com/KardinalAI/gorabbit"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestJSONMarshaller(t *testing.T) {
	m := gorabbit.NewJSONMarshaller()
	assert.NotNil(t, m)

	assert.Equal(t, "application/json", m.ContentType())

	data, err := m.Marshal("test")
	require.NoError(t, err)
	assert.Equal(t, []byte(`"test"`), data)
}

func TestTextMarshaller(t *testing.T) {
	m := gorabbit.NewTextMarshaller()
	assert.NotNil(t, m)

	assert.Equal(t, "text/plain", m.ContentType())

	data, err := m.Marshal("test")
	require.NoError(t, err)
	assert.Equal(t, []byte(`test`), data)
}
