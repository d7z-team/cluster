package cluster

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLoadTLSConfig(t *testing.T) {
	config, err := loadTLSConfig(tlsFiles{})
	require.NoError(t, err)
	require.Nil(t, config)
}

func TestLoadTLSConfigRejectsIncompleteKeyPair(t *testing.T) {
	_, err := loadTLSConfig(tlsFiles{CertFile: "client.crt"})
	require.Error(t, err)
}
