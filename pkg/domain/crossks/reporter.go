// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package crossks

import (
	"context"
	"strconv"

	ddlutil "github.com/pingcap/tidb/pkg/ddl/util"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/domain/serverinfo"
	"github.com/pingcap/tidb/pkg/keyspace"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/tikv/client-go/v2/oracle"
	pd "github.com/tikv/pd/client"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"go.uber.org/zap"
)

type minStartTSReporter struct {
	coordinator *schemaCoordinator
	etcdCli     *clientv3.Client
	serverID    string
}

// ReportMinStartTS reports self server min start timestamp to ETCD.
// The main logic is copied from InfoSyncer.ReportMinStartTS
func (r *minStartTSReporter) ReportMinStartTS(store kv.Storage, session *concurrency.Session) {
	if r.etcdCli == nil {
		return
	}
	// Calculate the lower limit of the start timestamp to avoid extremely old transaction delaying GC.
	currentVer, err := store.CurrentVersion(kv.GlobalTxnScope)
	if err != nil {
		logutil.BgLogger().Warn("update cross keyspace minStartTS failed", zap.Error(err))
		return
	}
	now := oracle.GetTimeFromTS(currentVer.Ver)
	// GCMaxWaitTime is in seconds, GCMaxWaitTime * 1000 converts it to milliseconds.
	lowerLimit := oracle.GoTimeToLowerLimitStartTS(now, vardef.GCMaxWaitTime.Load()*1000)
	minStartTS := r.coordinator.minStartTS(lowerLimit, oracle.GoTimeToTS(now))

	path := infosync.ServerMinStartTSPath + "/" + r.serverID
	codec := store.GetCodec()
	if pd.IsKeyspaceUsingKeyspaceLevelGC(codec.GetKeyspaceMeta()) {
		path = keyspace.MakeKeyspaceEtcdNamespace(codec) + path
	}
	// r.etcdCli is namespaced for user SQL metadata; bypass that with a raw KV
	// connection so unified GC can report at the un-namespaced root path.
	client := clientv3.NewKV(r.etcdCli)
	err = ddlutil.PutKVToEtcd(context.Background(), client, serverinfo.KeyOpDefaultRetryCnt, path,
		strconv.FormatUint(minStartTS, 10),
		clientv3.WithLease(session.Lease()))
	if err != nil {
		logutil.BgLogger().Warn("update cross keyspace minStartTS failed", zap.Error(err))
	}
	logutil.BgLogger().Debug("cross keyspace ReportMinStartTS", zap.Uint64("final minStartTS", minStartTS))
}
