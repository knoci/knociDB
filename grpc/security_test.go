package knocigrpc

import (
	"context"

	"github.com/knoci/knocidb"
	"testing"

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
	if creds.RequireTransportSecurity() {
		t.Fatal("token credentials should not require TLS by themselves")
	}
}

func TestStartSecureWithoutTLS(t *testing.T) {
	dbOpts := knocidb.DefaultOptions
	dbOpts.DirPath = t.TempDir()
	db, err := knocidb.Open(dbOpts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	srv, lis, err := StartSecure("127.0.0.1:0", db, ServerSecurityOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if srv == nil || lis == nil {
		t.Fatal("nil server or listener")
	}
	srv.GracefulStop()

	srv, lis, err = StartSecure("127.0.0.1:0", db, ServerSecurityOptions{EnableAuth: true, AuthToken: "token"})
	if err != nil {
		t.Fatal(err)
	}
	if srv == nil || lis == nil {
		t.Fatal("nil auth server or listener")
	}
	srv.GracefulStop()
}
