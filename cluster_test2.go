package knocidb

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/lni/dragonboat/v4/statemachine"
	"github.com/stretchr/testify/require"
)

func TestM3_BinaryRaftCodecAndBatchIdempotency(t *testing.T) {
	opts := DefaultOptions
	opts.DirPath = t.TempDir()
	opts.TTLScanInterval = 0
	db, err := Open(opts)
	require.NoError(t, err)
	defer db.Close()

	sm := NewKVStateMachine(1, 1, db)
	ods := sm.asOnDisk()

	// Encode compact binary Put and Batch commands
	putBin := encodeRaftCommand(Command{
		Op:      OpPut,
		Key:     []byte("k1"),
		Value:   []byte("v1"),
		BatchID: 1001,
	})
	batchBin := encodeRaftBatchCommand(BatchCommand{
		Op:      OpBatch,
		BatchID: 2002,
		Commands: []Command{
			{Op: OpPut, Key: []byte("k2"), Value: []byte("v2")},
			{Op: OpPut, Key: []byte("k3"), Value: []byte("v3")},
		},
	})
	entries := []statemachine.Entry{
		{Index: 1, Cmd: putBin},
		{Index: 2, Cmd: batchBin},
		// Retried proposal with same BatchID=2002 on a newer Raft index must be idempotently skipped!
		{Index: 3, Cmd: encodeRaftBatchCommand(BatchCommand{
			Op:      OpBatch,
			BatchID: 2002,
			Commands: []Command{
				{Op: OpPut, Key: []byte("k2"), Value: []byte("should-not-overwrite")},
			},
		})},
	}
	res, err := ods.Update(entries)
	require.NoError(t, err)
	require.Equal(t, uint64(1), res[0].Result.Value)
	require.Equal(t, uint64(2), res[1].Result.Value)
	require.Equal(t, uint64(0), res[2].Result.Value)
	require.NoError(t, ods.Sync())

	// Verify k2 was NOT overwritten by duplicate BatchID=2002
	val, err := sm.Lookup(raftLookupKey("k2"))
	require.NoError(t, err)
	require.Equal(t, []byte("v2"), val)

	// Reopen IOnDiskStateMachine and verify persisted appliedIndex == 3
	sm2 := NewKVStateMachine(1, 1, db)
	idx, err := sm2.asOnDisk().Open(nil)
	require.NoError(t, err)
	require.Equal(t, uint64(3), idx)
}

func TestM3_OnlineStreamingRaftSnapshotWithoutClosingDB(t *testing.T) {
	srcOpts := DefaultOptions
	srcOpts.DirPath = t.TempDir()
	srcOpts.TTLScanInterval = 0
	srcDB, err := Open(srcOpts)
	require.NoError(t, err)
	defer srcDB.Close()

	srcSM := NewKVStateMachine(1, 1, srcDB)
	exp := time.Now().Add(10 * time.Second).UnixMilli()
	_, err = srcSM.Update(statemachine.Entry{
		Index: 10,
		Cmd: encodeRaftBatchCommand(BatchCommand{
			Op: OpBatch,
			Commands: []Command{
				{Op: OpPut, Key: []byte("a"), Value: []byte("alpha")},
				{Op: OpPut, Key: []byte("ttl-k"), Value: []byte("temp"), ExpireAt: exp},
			},
		}),
	})
	require.NoError(t, err)

	// PrepareSnapshot + SaveSnapshot must NOT close srcDB!
	ods := srcSM.asOnDisk()
	ctx, err := ods.PrepareSnapshot()
	require.NoError(t, err)

	// Write a newer key while snapshot is being streamed — must succeed because DB is NOT closed!
	require.NoError(t, srcDB.Put([]byte("concurrent-after-prep"), []byte("live")))

	var snapBuf bytes.Buffer
	require.NoError(t, ods.SaveSnapshot(ctx, &snapBuf, nil))
	require.True(t, bytes.HasPrefix(snapBuf.Bytes(), []byte(raftSnapshotV2Magic)))

	// Recover into a fresh destination DB that already has a stale key
	dstOpts := DefaultOptions
	dstOpts.DirPath = t.TempDir()
	dstOpts.TTLScanInterval = 0
	dstDB, err := Open(dstOpts)
	require.NoError(t, err)
	defer dstDB.Close()
	require.NoError(t, dstDB.Put([]byte("stale"), []byte("must-be-cleared")))

	dstSM := NewKVStateMachine(1, 2, dstDB)
	require.NoError(t, dstSM.asOnDisk().RecoverFromSnapshot(&snapBuf, nil))
	require.Equal(t, uint64(10), dstSM.AppliedIndex())

	gotA, err := dstDB.Get([]byte("a"))
	require.NoError(t, err)
	require.Equal(t, []byte("alpha"), gotA)

	gotTTL, err := dstDB.Get([]byte("ttl-k"))
	require.NoError(t, err)
	require.Equal(t, []byte("temp"), gotTTL)
	left, hasTTL, err := dstDB.GetTTL([]byte("ttl-k"))
	require.NoError(t, err)
	require.True(t, hasTTL)
	require.Greater(t, left, time.Duration(0))

	_, err = dstDB.Get([]byte("stale"))
	require.ErrorIs(t, err, ErrKeyNotFound)
	_, err = dstDB.Get([]byte("concurrent-after-prep"))
	require.ErrorIs(t, err, ErrKeyNotFound)
}

func generateTestMTLSFiles(t *testing.T, dir string) (caFile, certFile, keyFile string) {
	t.Helper()
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	caTmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "knocidb-test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	require.NoError(t, err)

	nodeKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	nodeTmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "localhost"},
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
	}
	nodeDER, err := x509.CreateCertificate(rand.Reader, nodeTmpl, caTmpl, &nodeKey.PublicKey, caKey)
	require.NoError(t, err)

	caFile = filepath.Join(dir, "ca.pem")
	certFile = filepath.Join(dir, "node.pem")
	keyFile = filepath.Join(dir, "node.key")

	require.NoError(t, os.WriteFile(caFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER}), 0o600))
	require.NoError(t, os.WriteFile(certFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: nodeDER}), 0o600))
	keyBytes, err := x509.MarshalECPrivateKey(nodeKey)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyBytes}), 0o600))
	return caFile, certFile, keyFile
}

func TestM3_ClusterOnDiskTTLReadConsistencyAndMTLS(t *testing.T) {
	base := t.TempDir()
	caFile, certFile, keyFile := generateTestMTLSFiles(t, base)

	raftAddr := freeTestTCPAddr(t)
	rpcAddr := freeTestTCPAddr(t)

	cfg := DefaultClusterOptions(1, 501, raftAddr, filepath.Join(base, "raft"))
	cfg.RPCAddress = rpcAddr
	cfg.PeerRPC = map[uint64]string{1: rpcAddr}
	cfg.MutualTLS = true
	cfg.CAFile = caFile
	cfg.CertFile = certFile
	cfg.KeyFile = keyFile

	dbOpts := DefaultOptions
	dbOpts.DirPath = filepath.Join(base, "db")
	dbOpts.SnapshotPath = filepath.Join(base, "snap")

	rdb, err := OpenCluster(cfg, dbOpts)
	require.NoError(t, err)
	defer rdb.Close()

	require.Eventually(t, func() bool { return rdb.IsLeader() }, 5*time.Second, 100*time.Millisecond)

	// Deterministic Raft TTL
	ctx := context.Background()
	require.NoError(t, rdb.PutWithTTLContext(ctx, []byte("ephemeral"), []byte("soon"), 150*time.Millisecond))
	require.NoError(t, rdb.PutContext(ctx, []byte("persistent"), []byte("forever")))

	// Verify all three ReadConsistency levels
	for _, level := range []ReadConsistency{ReadLinearizable, ReadFollowerIndex, ReadStaleLocal} {
		v, err := rdb.GetContext(ctx, []byte("persistent"), level)
		require.NoError(t, err)
		require.Equal(t, []byte("forever"), v)
	}

	// Verify linearizable ScanContext
	it, err := rdb.ScanContext(ctx, nil, nil, ReadLinearizable)
	require.NoError(t, err)
	foundPersistent := false
	for ; it.Valid(); it.Next() {
		if string(it.Key()) == "persistent" {
			foundPersistent = true
		}
	}
	require.NoError(t, it.Close())
	require.True(t, foundPersistent)

	// Verify mTLS RPC loopback call succeeds with valid client cert
	got, err := rdb.forwardGet(rpcAddr, []byte("persistent"))
	require.NoError(t, err)
	require.Equal(t, []byte("forever"), got)

	// Dynamic PeerRPC registry
	rdb.RegisterPeerRPC(2, "127.0.0.1:19999")
	addr, ok := rdb.PeerRPCAddress(2)
	require.True(t, ok)
	require.Equal(t, "127.0.0.1:19999", addr)

	// Wait for deterministic TTL expiry
	time.Sleep(220 * time.Millisecond)
	_, err = rdb.GetContext(ctx, []byte("ephemeral"), ReadLinearizable)
	require.ErrorIs(t, err, ErrKeyNotFound)
}

func freeTestTCPAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := l.Addr().String()
	_ = l.Close()
	return addr
}
