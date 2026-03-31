// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package driver

import (
	"context"
	"fmt"
	"math"
	"reflect"
	"sync"
	"time"
	"unsafe"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/store/copr"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/tikv/client-go/v2/config"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/util"
	"github.com/tikv/client-go/v2/util/async"
	pd "github.com/tikv/pd/client"
	pdhttp "github.com/tikv/pd/client/http"
	"github.com/tikv/pd/client/opt"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

const (
	// ddlGRPCInitialWindowSize is the gRPC initial window size for DDL backfill
	// operations (4 MiB). This is much smaller than the default 128 MiB to reduce
	// memory consumption during global sort ADD INDEX. With large regions (1 GiB),
	// the default 128 MiB windows across multiple connections per store can consume
	// over 1.5 GiB just for flow-control buffers (4 conns * 3 stores * 128 MiB).
	// Using 4 MiB windows reduces this to ~48 MiB while still allowing sufficient
	// throughput for DDL coprocessor scans.
	ddlGRPCInitialWindowSize int32 = 4 * 1024 * 1024

	// ddlGRPCInitialConnWindowSize is the gRPC initial connection-level window
	// size for DDL backfill operations (4 MiB), matching the stream-level size.
	ddlGRPCInitialConnWindowSize int32 = 4 * 1024 * 1024

	// ddlGRPCConnectionCount is the connection count per TiKV store for the
	// dedicated DDL store. Using a single connection further caps the per-store
	// in-flight gRPC memory during read-index global sort.
	ddlGRPCConnectionCount uint = 1
)

var ddlClientConfigMu sync.Mutex

// OpenDDLStore creates a dedicated kv.Storage for DDL backfill (read-index)
// operations with smaller gRPC window sizes to reduce memory consumption.
//
// The returned store has its own RPC client and gRPC connection pools, so its
// tuned window sizes do not affect the main OLTP store. It shares the same PD
// cluster and TLS configuration but creates independent PD client and safepoint
// KV instances so that closing the DDL store does not affect the main store.
//
// The caller is responsible for closing the returned store when it is no longer
// needed. The DDL store must be closed before the main store.
func OpenDDLStore(mainStore kv.Storage) (kv.Storage, error) {
	ts, ok := mainStore.(*tikvStore)
	if !ok {
		// Not a real TiKV store (e.g., unistore for tests). Return the main
		// store directly; DDL will work the same way but without separate gRPC
		// window tuning.
		logutil.BgLogger().Info("main store is not a TiKV store, DDL store will reuse it")
		return mainStore, nil
	}

	etcdAddrs := ts.etcdAddrs
	if len(etcdAddrs) == 0 {
		return mainStore, nil
	}

	tidbCfg := config.GetGlobalConfig()
	security := tidbCfg.Security
	tikvConfig := tidbCfg.TiKVClient
	pdConfig := tidbCfg.PDClient

	keyspaceName := ts.keyspace
	var apiCtx = pd.NewAPIContextV1()
	if len(keyspaceName) > 0 {
		apiCtx = pd.NewAPIContextV2(keyspaceName)
	}

	pdCli, err := pd.NewClientWithAPIContext(
		context.Background(), apiCtx, "tidb-ddl-store", etcdAddrs,
		pd.SecurityOption{
			CAPath:   security.ClusterSSLCA,
			CertPath: security.ClusterSSLCert,
			KeyPath:  security.ClusterSSLKey,
		},
		opt.WithGRPCDialOptions(
			grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(math.MaxInt32)),
			grpc.WithKeepaliveParams(keepalive.ClientParameters{
				Time:    time.Duration(tikvConfig.GrpcKeepAliveTime) * time.Second,
				Timeout: time.Duration(tikvConfig.GrpcKeepAliveTimeout) * time.Second,
			}),
		),
		opt.WithCustomTimeoutOption(time.Duration(pdConfig.PDServerTimeout)*time.Second),
		opt.WithForwardingOption(tidbCfg.EnableForwarding),
	)
	if err != nil {
		return nil, errors.Trace(err)
	}
	pdCli = util.InterceptedPDClient{Client: pdCli}

	tlsConfig, err := security.ToTLSConfig()
	if err != nil {
		pdCli.Close()
		return nil, errors.Trace(err)
	}

	spkv, err := tikv.NewEtcdSafePointKV(etcdAddrs, tlsConfig)
	if err != nil {
		pdCli.Close()
		return nil, errors.Trace(err)
	}

	var pdClient *tikv.CodecPDClient
	if keyspaceName == "" {
		pdClient = tikv.NewCodecPDClient(tikv.ModeTxn, pdCli)
	} else {
		pdClient, err = tikv.NewCodecPDClientWithKeyspace(tikv.ModeTxn, pdCli, keyspaceName)
		if err != nil {
			spkv.Close()
			pdCli.Close()
			return nil, errors.Trace(err)
		}
	}

	codec := pdClient.GetCodec()

	rpcClient := tikv.NewRPCClient(
		tikv.WithSecurity(security),
		tikv.WithCodec(codec),
	)

	// Override gRPC window sizes on the DDL RPC client to use smaller buffers.
	// This reduces memory consumption during global sort ADD INDEX.
	//
	// NOTE: client-go does not currently export WithGRPCDialOptions from the
	// tikv package (it is in internal/client). We use reflect+unsafe to set
	// the field directly. This should be replaced with a proper API call once
	// client-go exports tikv.WithGRPCDialOptions.
	// Tracked in: https://github.com/pingcap/tidb/issues/64911
	setRPCClientGRPCDialOptions(rpcClient, []grpc.DialOption{
		grpc.WithInitialWindowSize(ddlGRPCInitialWindowSize),
		grpc.WithInitialConnWindowSize(ddlGRPCInitialConnWindowSize),
	})

	clusterID := pdCli.GetClusterID(context.TODO())
	uuid := fmt.Sprintf("tikv-%v/%s/ddl", clusterID, keyspaceName)

	ddlClient := &ddlRPCClient{
		Client: rpcClient,
		cfg: ddlRPCClientConfig{
			grpcConnectionCount: ddlGRPCConnectionCount,
			maxBatchSize:        0,
		},
	}

	s, err := tikv.NewKVStore(uuid, pdClient, spkv, &injectTraceClient{Client: ddlClient},
		tikv.WithPDHTTPClient("tikv-ddl-store", etcdAddrs,
			pdhttp.WithTLSConfig(tlsConfig),
			pdhttp.WithMetrics(metrics.PDAPIRequestCounter, metrics.PDAPIExecutionHistogram)))
	if err != nil {
		_ = rpcClient.Close()
		_ = spkv.Close()
		pdCli.Close()
		return nil, errors.Trace(err)
	}

	coprCacheConfig := &config.GetGlobalConfig().TiKVClient.CoprCache
	coprStore, err := copr.NewStore(s, coprCacheConfig)
	if err != nil {
		s.Close()
		return nil, errors.Trace(err)
	}

	store := &tikvStore{
		KVStore:   s,
		etcdAddrs: etcdAddrs,
		tlsConfig: tlsConfig,
		memCache:  kv.NewCacheDB(),
		enableGC:  false, // DDL store does not run GC.
		coprStore: coprStore,
		codec:     codec,
		clusterID: clusterID,
		keyspace:  keyspaceName,
	}

	logutil.BgLogger().Info("opened dedicated DDL store with reduced gRPC window sizes",
		zap.Uint("grpcConnectionCount", ddlGRPCConnectionCount),
		zap.Uint("maxBatchSize", ddlClient.cfg.maxBatchSize),
		zap.Int32("initialWindowSize", ddlGRPCInitialWindowSize),
		zap.Int32("initialConnWindowSize", ddlGRPCInitialConnWindowSize),
		zap.String("uuid", uuid))

	// Do not cache in mc.cache — this is a purpose-specific store.
	return store, nil
}

// setRPCClientGRPCDialOptions sets custom gRPC dial options on an RPCClient
// using reflect+unsafe. This is necessary because client-go's
// internal/client.WithGRPCDialOptions is not exported from the tikv package.
//
// Both field lookups use FieldByName (not offset assumptions) for resilience
// against struct reordering in client-go upgrades. If the internal layout
// changes in a way that breaks this, the function logs a warning and the DDL
// store falls back to default window sizes (still functional, just larger).
//
// TODO: remove this once client-go exports tikv.WithGRPCDialOptions.
func setRPCClientGRPCDialOptions(rpcClient any, opts []grpc.DialOption) {
	rv := reflect.ValueOf(rpcClient).Elem()
	optionField := rv.FieldByName("option")
	if !optionField.IsValid() || optionField.IsNil() {
		logutil.BgLogger().Warn("failed to set DDL gRPC dial options: 'option' field not found on RPCClient")
		return
	}
	// Dereference the *option pointer to access the option struct fields.
	optionStruct := reflect.NewAt(optionField.Type().Elem(), unsafe.Pointer(optionField.Pointer())).Elem()
	grpcField := optionStruct.FieldByName("gRPCDialOptions")
	if !grpcField.IsValid() {
		logutil.BgLogger().Warn("failed to set DDL gRPC dial options: 'gRPCDialOptions' field not found on option struct")
		return
	}
	// Write to the unexported field via unsafe pointer.
	*(*[]grpc.DialOption)(unsafe.Pointer(grpcField.UnsafeAddr())) = opts
}

type ddlRPCClientConfig struct {
	grpcConnectionCount uint
	maxBatchSize        uint
}

// ddlRPCClient wraps the dedicated DDL RPC client and temporarily overrides
// client-go's global TiKV config for the duration of request dispatch. This is
// a hack for validation until client-go exports per-client connection-count
// configuration.
type ddlRPCClient struct {
	tikv.Client
	cfg ddlRPCClientConfig
}

func (c *ddlRPCClient) SendRequest(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (*tikvrpc.Response, error) {
	restore := c.overrideConfig()
	defer restore()
	return c.Client.SendRequest(ctx, addr, req, timeout)
}

func (c *ddlRPCClient) SendRequestAsync(ctx context.Context, addr string, req *tikvrpc.Request, cb async.Callback[*tikvrpc.Response]) {
	restore := c.overrideConfig()
	defer restore()
	c.Client.SendRequestAsync(ctx, addr, req, cb)
}

func (c *ddlRPCClient) overrideConfig() func() {
	ddlClientConfigMu.Lock()
	restore := config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVClient.GrpcConnectionCount = c.cfg.grpcConnectionCount
		conf.TiKVClient.MaxBatchSize = c.cfg.maxBatchSize
	})
	return func() {
		restore()
		ddlClientConfigMu.Unlock()
	}
}
