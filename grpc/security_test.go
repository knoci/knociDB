package knocigrpc

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type fakeStream struct {
	ctx context.Context
}

func (s *fakeStream) SetHeader(metadata.MD) error  { return nil }
func (s *fakeStream) SendHeader(metadata.MD) error { return nil }
func (s *fakeStream) SetTrailer(metadata.MD)       {}
func (s *fakeStream) Context() context.Context     { return s.ctx }
func (s *fakeStream) SendMsg(any) error            { return nil }
func (s *fakeStream) RecvMsg(any) error            { return nil }

func TestCheckToken(t *testing.T) {
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(
		"authorization", "Bearer secret",
		"x-api-key", "secret",
	))
	if !checkToken(ctx, "secret") {
		t.Fatal("valid token rejected")
	}
	if checkToken(context.Background(), "secret") {
		t.Fatal("missing metadata accepted")
	}
	if checkToken(ctx, "wrong") {
		t.Fatal("wrong token accepted")
	}

	ctx = metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "  Bearer secret  "))
	if !checkToken(ctx, "secret") {
		t.Fatal("whitespace token rejected")
	}
	ctx = metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-api-key", " secret "))
	if !checkToken(ctx, "secret") {
		t.Fatal("whitespace api key rejected")
	}
}

func TestAuthUnaryInterceptor(t *testing.T) {
	called := false
	handler := func(ctx context.Context, req any) (any, error) {
		called = true
		return "ok", nil
	}
	interceptor := authUnaryInterceptor("secret")

	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer secret"))
	if _, err := interceptor(ctx, nil, nil, handler); err != nil || !called {
		t.Fatalf("valid token failed: %v called=%v", err, called)
	}

	called = false
	if _, err := interceptor(context.Background(), nil, nil, handler); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("missing token should be Unauthenticated, got %v", err)
	}
	if called {
		t.Fatal("handler must not run without token")
	}
}

func TestAuthStreamInterceptor(t *testing.T) {
	called := false
	handler := func(srv any, stream grpc.ServerStream) error {
		called = true
		return nil
	}
	interceptor := authStreamInterceptor("secret")
	stream := &fakeStream{ctx: metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-api-key", "secret"))}
	if err := interceptor(nil, stream, nil, handler); err != nil || !called {
		t.Fatalf("valid stream token failed: %v called=%v", err, called)
	}

	called = false
	empty := &fakeStream{ctx: context.Background()}
	if err := interceptor(nil, empty, nil, handler); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("missing stream token should be Unauthenticated, got %v", err)
	}
	if called {
		t.Fatal("handler must not run without token")
	}
}

func TestTokenCredentials(t *testing.T) {
	creds := tokenCredentials{token: "secret"}
	md, err := creds.GetRequestMetadata(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if md["authorization"] != "Bearer secret" || md["x-api-key"] != "secret" {
		t.Fatalf("unexpected metadata: %#v", md)
	}
	if !creds.RequireTransportSecurity() {
		t.Fatal("token credentials must require TLS")
	}
}

func TestSecureRejectsInvalidConfiguration(t *testing.T) {
	cert, key := testTLSCertificate(t)
	badCA := filepath.Join(t.TempDir(), "bad-ca.pem")
	if err := os.WriteFile(badCA, []byte("invalid PEM"), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, opt := range []ServerSecurityOptions{
		{},
		{TLSCertFile: cert},
		{TLSCertFile: key, TLSKeyFile: key},
		{TLSCertFile: cert, TLSKeyFile: key, EnableAuth: true},
		{TLSCertFile: cert, TLSKeyFile: key, RequireClientCert: true},
		{TLSCertFile: cert, TLSKeyFile: key, TLSClientCAFile: cert},
		{TLSCertFile: cert, TLSKeyFile: key, RequireClientCert: true, TLSClientCAFile: badCA},
	} {
		if srv, _, err := StartSecure("127.0.0.1:0", nil, opt); err == nil {
			srv.Stop()
			t.Errorf("unexpectedly accepted invalid server configuration: %+v", opt)
		}
	}
	for _, opt := range []ClientSecurityOptions{
		{TLSCACertFile: badCA},
		{ClientCertFile: cert},
		{ClientCertFile: cert, ClientKeyFile: cert},
		{EnableAuth: true},
	} {
		if cli, err := DialSecure("127.0.0.1:0", opt); err == nil {
			_ = cli.Close()
			t.Errorf("unexpectedly accepted invalid client configuration: %+v", opt)
		}
	}
}

func testTLSCertificate(t *testing.T) (string, string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		NotBefore:             now.Add(-time.Minute),
		NotAfter:              now.Add(time.Hour),
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
		BasicConstraintsValid: true,
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
	}
	cert, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	der, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	certPath, keyPath := filepath.Join(dir, "cert.pem"), filepath.Join(dir, "key.pem")
	if err := os.WriteFile(certPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert}), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
	return certPath, keyPath
}
