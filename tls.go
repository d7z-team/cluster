package cluster

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
)

type tlsFiles struct {
	CAFile     string
	CertFile   string
	KeyFile    string
	ServerName string
}

func loadTLSConfig(files tlsFiles) (*tls.Config, error) {
	if files.CAFile == "" && files.CertFile == "" && files.KeyFile == "" && files.ServerName == "" {
		return nil, nil
	}
	if (files.CertFile == "") != (files.KeyFile == "") {
		return nil, errors.New("incomplete tls configuration, need both cert-file and key-file")
	}

	tlsConfig := &tls.Config{
		MinVersion: tls.VersionTLS12,
		ServerName: files.ServerName,
	}

	if files.CAFile != "" {
		caCert, err := os.ReadFile(files.CAFile)
		if err != nil {
			return nil, fmt.Errorf("failed to read ca file: %w", err)
		}

		caCertPool := x509.NewCertPool()
		if !caCertPool.AppendCertsFromPEM(caCert) {
			return nil, errors.New("failed to add ca certificate")
		}
		tlsConfig.RootCAs = caCertPool
	}

	if files.CertFile != "" {
		cert, err := tls.LoadX509KeyPair(files.CertFile, files.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("failed to load client certificate: %w", err)
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}

	return tlsConfig, nil
}
