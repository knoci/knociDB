package knocigrpc

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"io/ioutil"
	"net"
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

// StartSecure 启动启用了 TLS/鉴权的 gRPC 服务
func StartSecure(addr string, db *knocidb.DB, sec ServerSecurityOptions, extra ...grpc.ServerOption) (*grpc.Server, net.Listener, error) {
	opts := make([]grpc.ServerOption, 0, 4)
	if sec.TLSCertFile != "" && sec.TLSKeyFile != "" {
		var creds credentials.TransportCredentials
		if sec.RequireClientCert && sec.TLSClientCAFile != "" {
			// mTLS
			cert, _ := tls.LoadX509KeyPair(sec.TLSCertFile, sec.TLSKeyFile)
			caBytes, _ := ioutil.ReadFile(sec.TLSClientCAFile)
			pool := x509.NewCertPool()
			pool.AppendCertsFromPEM(caBytes)
			cfg := &tls.Config{Certificates: []tls.Certificate{cert}, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: pool}
			creds = credentials.NewTLS(cfg)
		} else {
			creds, _ = credentials.NewServerTLSFromFile(sec.TLSCertFile, sec.TLSKeyFile)
		}
		opts = append(opts, grpc.Creds(creds))
	}
	if sec.EnableAuth && sec.AuthToken != "" {
		opts = append(opts, grpc.ChainUnaryInterceptor(authUnaryInterceptor(sec.AuthToken)))
		opts = append(opts, grpc.ChainStreamInterceptor(authStreamInterceptor(sec.AuthToken)))
	}
	opts = append(opts, extra...)
	return Start(addr, db, opts...)
}

// DialSecure 建立启用了 TLS/鉴权的客户端连接
func DialSecure(addr string, sec ClientSecurityOptions, extra ...grpc.DialOption) (*Client, error) {
	opts := make([]grpc.DialOption, 0, 3)
	if sec.TLSCACertFile != "" {
		creds, _ := credentials.NewClientTLSFromFile(sec.TLSCACertFile, sec.TLSServerName)
		opts = append(opts, grpc.WithTransportCredentials(creds))
	} else {
		opts = append(opts, grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{InsecureSkipVerify: true})))
	}
	if sec.EnableAuth && sec.AuthToken != "" {
		opts = append(opts, grpc.WithPerRPCCredentials(tokenCredentials{token: sec.AuthToken}))
	}
	opts = append(opts, extra...)
	return Dial(addr, opts...)
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
func (t tokenCredentials) RequireTransportSecurity() bool { return false }

func checkToken(ctx context.Context, token string) bool {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return false
	}
	if vals := md.Get("authorization"); len(vals) > 0 {
		for _, v := range vals {
			if strings.TrimSpace(v) == "Bearer "+token {
				return true
			}
		}
	}
	if vals := md.Get("x-api-key"); len(vals) > 0 {
		for _, v := range vals {
			if strings.TrimSpace(v) == token {
				return true
			}
		}
	}
	return false
}
