// Command test-server is a local stand-in for the Databricks APIs that baton-databricks calls
// from Validate and the incremental sync event feed (system.access.audit). It serves TLS on
// one address and routes by Host header: "localhost" is the account API and OAuth host, and
// "<deployment_name>.localhost" is that workspace. See README.md for usage, endpoints, seed
// data and which behaviors come from the Databricks docs.
package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"flag"
	"fmt"
	"log"
	"math/big"
	"net"
	"net/http"
	"os"
	"time"
)

type config struct {
	addr                string
	caOut               string
	warehouseStartDelay time.Duration
	chunkRows           int
}

func main() {
	var cfg config
	flag.StringVar(&cfg.addr, "addr", "localhost:8765", "address to listen on")
	flag.StringVar(&cfg.caOut, "ca-out", "./test-server-ca.pem", "file to write the generated CA certificate to (PEM)")
	flag.DurationVar(&cfg.warehouseStartDelay, "warehouse-start-delay", 0,
		"how long a stopped warehouse takes to reach RUNNING after a statement starts it")
	flag.IntVar(&cfg.chunkRows, "chunk-rows", 0, "rows per result chunk; 0 returns every row in a single chunk")
	flag.Parse()

	if err := run(cfg); err != nil {
		fmt.Fprintf(os.Stderr, "test-server: %v\n", err)
		os.Exit(1)
	}
}

func run(cfg config) error {
	if cfg.chunkRows < 0 {
		return errors.New("--chunk-rows must be >= 0")
	}

	srv := newServer(cfg, time.Now())

	hosts := []string{"localhost", "*.localhost"}
	for _, ws := range srv.st.workspaces {
		hosts = append(hosts, ws.DeploymentName+".localhost")
	}
	caPEM, cert, err := generateCertificates(hosts)
	if err != nil {
		return err
	}
	if err := os.WriteFile(cfg.caOut, caPEM, 0o600); err != nil {
		return fmt.Errorf("writing CA certificate: %w", err)
	}
	httpServer := &http.Server{
		Addr:              cfg.addr,
		Handler:           logRequests(srv),
		ReadHeaderTimeout: 10 * time.Second,
		TLSConfig: &tls.Config{
			MinVersion:   tls.VersionTLS12,
			Certificates: []tls.Certificate{cert},
		},
	}

	log.Printf("test-server: listening on https://%s (CA written to %s)", cfg.addr, cfg.caOut)
	log.Printf("test-server: account API at https://%s, workspaces at https://<deployment_name>.%s", cfg.addr, cfg.addr)
	return httpServer.ListenAndServeTLS("", "")
}

// generateCertificates creates a throwaway CA and a server certificate for hosts (DECISION: the
// connector always builds https:// URLs, so the server needs TLS; clients trust the CA through
// SSL_CERT_FILE). Go accepts the *.localhost wildcard, but OpenSSL (and so curl) doesn't match a
// wildcard that close to the top level, so each seeded workspace host is listed too.
func generateCertificates(hosts []string) ([]byte, tls.Certificate, error) {
	now := time.Now()

	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, tls.Certificate{}, fmt.Errorf("generating CA key: %w", err)
	}
	caTemplate := &x509.Certificate{
		SerialNumber:          randomSerial(),
		Subject:               pkix.Name{CommonName: "baton-databricks test-server CA"},
		NotBefore:             now.Add(-time.Hour),
		NotAfter:              now.Add(7 * 24 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		return nil, tls.Certificate{}, fmt.Errorf("creating CA certificate: %w", err)
	}
	caCert, err := x509.ParseCertificate(caDER)
	if err != nil {
		return nil, tls.Certificate{}, fmt.Errorf("parsing CA certificate: %w", err)
	}

	leafKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, tls.Certificate{}, fmt.Errorf("generating server key: %w", err)
	}
	leafTemplate := &x509.Certificate{
		SerialNumber: randomSerial(),
		Subject:      pkix.Name{CommonName: "localhost"},
		NotBefore:    now.Add(-time.Hour),
		NotAfter:     now.Add(7 * 24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     hosts,
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")},
	}
	leafDER, err := x509.CreateCertificate(rand.Reader, leafTemplate, caCert, &leafKey.PublicKey, caKey)
	if err != nil {
		return nil, tls.Certificate{}, fmt.Errorf("creating server certificate: %w", err)
	}

	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})
	return caPEM, tls.Certificate{Certificate: [][]byte{leafDER, caDER}, PrivateKey: leafKey}, nil
}

func randomSerial() *big.Int {
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 62))
	if err != nil {
		return big.NewInt(time.Now().UnixNano())
	}
	return serial
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(status int) {
	r.status = status
	r.ResponseWriter.WriteHeader(status)
}

func logRequests(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		started := time.Now()
		next.ServeHTTP(rec, r)
		//nolint:gosec // G706: local test-server log; request values are %q-quoted, so they cannot forge log lines.
		log.Printf("%q %q -> %d (%s)", r.Method, r.Host+r.URL.Path, rec.status, time.Since(started).Round(time.Millisecond))
	})
}
