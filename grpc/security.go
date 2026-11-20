package knocigrpc

import (
	"context"
	"crypto/subtle"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"os"
	"strings"

	"github.com/knoci/knocidb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// ServerSecurityOptions 指定服务端的 TLS 与鉴权配置
type ServerSecurityOptions struct {
	TLSCertFile       string
	TLSKeyFile        string
	TLSClientCAFile   string
	RequireClientCert bool
	EnableAuth        bool
	AuthToken         string
}

// ClientSecurityOptions 指定客户端的 TLS 与鉴权配置
type ClientSecurityOptions struct {
	TLSCACertFile  string
	TLSServerName  string
	ClientCertFile string
	ClientKeyFile  string
	EnableAuth     bool
	AuthToken      string
}

// StartSecure 启动 TLS 服务，可额外启用双向证书和 token 鉴权。
func StartSecure(addr string, db *knocidb.DB, sec ServerSecurityOptions, extra ...grpc.ServerOption) (*grpc.Server, net.Listener, error) {
	if err := validateTokenOptions(sec.EnableAuth, sec.AuthToken); err != nil {
		return nil, nil, err
	}
	if sec.TLSCertFile == "" || sec.TLSKeyFile == "" {
		return nil, nil, fmt.Errorf("secure gRPC server requires a TLS certificate and key")
	}
	if sec.RequireClientCert != (sec.TLSClientCAFile != "") {
		return nil, nil, fmt.Errorf("client CA must be configured together with required client certificates")
	}
	cert, err := tls.LoadX509KeyPair(sec.TLSCertFile, sec.TLSKeyFile)
	if err != nil {
		return nil, nil, fmt.Errorf("load server TLS certificate and key: %w", err)
	}
	cfg := &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}
	if sec.RequireClientCert {
		pool, err := loadCAPool(sec.TLSClientCAFile)
		if err != nil {
			return nil, nil, fmt.Errorf("load client CA: %w", err)
		}
		cfg.ClientAuth = tls.RequireAndVerifyClientCert
		cfg.ClientCAs = pool
	}
	var opts []grpc.ServerOption
	if sec.EnableAuth {
		opts = append(opts, grpc.ChainUnaryInterceptor(authUnaryInterceptor(sec.AuthToken)),
			grpc.ChainStreamInterceptor(authStreamInterceptor(sec.AuthToken)))
	}
	opts = append(opts, extra...)
	opts = append(opts, grpc.Creds(credentials.NewTLS(cfg)))
	return Start(addr, db, opts...)
}

// DialSecure 验证服务端的 TLS 证书。未配置 CA 文件时使用系统信任根。
func DialSecure(addr string, sec ClientSecurityOptions, extra ...grpc.DialOption) (*Client, error) {
	if err := validateTokenOptions(sec.EnableAuth, sec.AuthToken); err != nil {
		return nil, err
	}
	if (sec.ClientCertFile == "") != (sec.ClientKeyFile == "") {
		return nil, fmt.Errorf("client TLS certificate and key must be configured together")
	}
	cfg := &tls.Config{ServerName: sec.TLSServerName, MinVersion: tls.VersionTLS12}
	if sec.TLSCACertFile != "" {
		pool, err := loadCAPool(sec.TLSCACertFile)
		if err != nil {
			return nil, fmt.Errorf("load server CA: %w", err)
		}
		cfg.RootCAs = pool
	}
	if sec.ClientCertFile != "" {
		cert, err := tls.LoadX509KeyPair(sec.ClientCertFile, sec.ClientKeyFile)
		if err != nil {
			return nil, fmt.Errorf("load client TLS certificate and key: %w", err)
		}
		cfg.Certificates = []tls.Certificate{cert}
	}
	opts := append([]grpc.DialOption(nil), extra...)
	opts = append(opts, grpc.WithTransportCredentials(credentials.NewTLS(cfg)))
	if sec.EnableAuth {
		opts = append(opts, grpc.WithPerRPCCredentials(tokenCredentials{token: sec.AuthToken}))
	}
	return Dial(addr, opts...)
}

func validateTokenOptions(enabled bool, token string) error {
	if enabled != (token != "") || token != strings.TrimSpace(token) {
		return fmt.Errorf("token auth requires a non-empty token without surrounding whitespace when enabled")
	}
	return nil
}

func loadCAPool(path string) (*x509.CertPool, error) {
	pem, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("no PEM certificates in %q", path)
	}
	return pool, nil
}

// 简单的 token 鉴权（支持 Authorization: Bearer <token> 或 x-api-key: <token>）
func authUnaryInterceptor(token string) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		if ok := checkToken(ctx, token); !ok {
			return nil, status.Error(codes.Unauthenticated, "invalid token")
		}
		return handler(ctx, req)
	}
}

func authStreamInterceptor(token string) grpc.StreamServerInterceptor {
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if ok := checkToken(ss.Context(), token); !ok {
			return status.Error(codes.Unauthenticated, "invalid token")
		}
		return handler(srv, ss)
	}
}

type tokenCredentials struct{ token string }

func (t tokenCredentials) GetRequestMetadata(ctx context.Context, uri ...string) (map[string]string, error) {
	return map[string]string{"authorization": "Bearer " + t.token, "x-api-key": t.token}, nil
}
func (t tokenCredentials) RequireTransportSecurity() bool { return true }

func checkToken(ctx context.Context, token string) bool {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return false
	}
	if vals := md.Get("authorization"); len(vals) > 0 {
		for _, v := range vals {
			if subtle.ConstantTimeCompare([]byte(strings.TrimSpace(v)), []byte("Bearer "+token)) == 1 {
				return true
			}
		}
	}
	if vals := md.Get("x-api-key"); len(vals) > 0 {
		for _, v := range vals {
			if subtle.ConstantTimeCompare([]byte(strings.TrimSpace(v)), []byte(token)) == 1 {
				return true
			}
		}
	}
	return false
}
