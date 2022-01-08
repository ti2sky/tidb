// Copyright 2021 PingCAP, Inc.
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

package server

import (
	"fmt"
	"github.com/pingcap/tidb/util/replayutil"
	"net/http"
	"strconv"

	"github.com/gorilla/mux"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/util/logutil"
)

// SQLRecorderHandler is the handler for dumping plan replayer file.
type SQLRecorderHandler struct {
	address    string
	statusPort uint
}

func (s *Server) newSQLRecorderHandler() *SQLRecorderHandler {
	cfg := config.GetGlobalConfig()
	prh := &SQLRecorderHandler{
		address:    cfg.AdvertiseAddress,
		statusPort: cfg.Status.StatusPort,
	}
	return prh
}

func (h SQLRecorderHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	var err error
	cfg := config.GetGlobalConfig()
	params := mux.Vars(req)
	var unit bool
	if time, ok := params[pRecordTime]; ok {
		if time == "ms" {
			unit = true
		}
	}
	// set replay meta TS first.
	cfg.ReplayMetaTS, err = strconv.ParseInt(params[pStartTS], 10, 64)
	if err != nil {
		fmt.Println(err.Error())
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	cfg.EnableRecordSQL.Store(true)
	cfg.RecordSQLUnit.Store(unit)
	fmt.Println("start recording...")
	logutil.InitRecord(params[pFileName])

	w.WriteHeader(http.StatusOK)
	return
}

// SQLRecorderCloser is the handler for dumping plan replayer file.
type SQLRecorderCloser struct {
	address    string
	statusPort uint
}

func (s *Server) newSQLRecorderCloser() *SQLRecorderCloser {
	cfg := config.GetGlobalConfig()
	prh := &SQLRecorderCloser{
		address:    cfg.AdvertiseAddress,
		statusPort: cfg.Status.StatusPort,
	}
	return prh
}

func (h SQLRecorderCloser) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	cfg := config.GetGlobalConfig()
	cfg.EnableRecordSQL.Store(false)
	logutil.StopRecord()
	fmt.Println("Stop recording...")
	w.WriteHeader(http.StatusOK)
	return
}

// SQLReplayHandler Replay handler
type SQLReplayHandler struct {
	Store kv.Storage
}

func (s *Server) newSQLReplayHandler(store kv.Storage) *SQLReplayHandler {
	prh := &SQLReplayHandler{store}
	return prh
}

func (h SQLReplayHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	params := mux.Vars(req)
	var speed float64
	var err error
	if s, ok := params[pReplaySpeed]; ok {
		speed, err = strconv.ParseFloat(s, 2)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte("Error argument for the speed!"))
		}
	}
	replayutil.StartReplay(params[pFileName], h.Store, speed)
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("success"))
	return
}

// SQLReplayCloser Replay handler
type SQLReplayCloser struct {
	Store kv.Storage
}

func (s *Server) newSQLReplayCloser(store kv.Storage) *SQLReplayCloser {
	prh := &SQLReplayCloser{store}
	return prh
}

func (h SQLReplayCloser) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	go replayutil.StopReplay()
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("success"))
	return
}
