// Copyright 2015 The etcd Authors
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

package raftTransport

import (
	"github.com/ColdToo/Cold2DB/domain"
	types "github.com/ColdToo/Cold2DB/raftTransport/types"
	"github.com/ColdToo/Cold2DB/raftproto"
	"go.uber.org/zap"
)

type remote struct {
	lg       *zap.Logger
	localID  types.ID
	id       types.ID
	status   *peerStatus
	pipeline *pipeline
}

func startRemote(tr *Transport, urls types.URLs, remoteId types.ID) *remote {
	picker := newURLPicker(urls)
	status := newPeerStatus(tr.Logger, tr.LocalID, remoteId)
	pipeline := &pipeline{
		peerID: remoteId,
		tr:     tr,
		picker: picker,
		status: status,
		raft:   tr.Raft,
		errorc: tr.ErrorC,
	}
	pipeline.start()

	return &remote{
		lg:       tr.Logger,
		localID:  tr.LocalID,
		id:       remoteId,
		status:   status,
		pipeline: pipeline,
	}
}

func (g *remote) send(m raftproto.Message) {
	select {
	case g.pipeline.msgc <- m:
	default:
		if g.status.isActive() {
			g.lg.Warn(
				"dropped internal Raft message since sending buffer is full (overloaded network)",
				zap.String("message-type", m.Type.String()),
				zap.String("local-member-id", g.localID.String()),
				zap.String("from", types.ID(m.From).String()),
				zap.String("remote-peer-id", g.id.String()),
				zap.Bool("remote-peer-active", g.status.isActive()),
			)

		} else {
			domain.Log.Warn("dropped Raft message since sending buffer is full (overloaded network)").
				Str("message-type", m.Type.String()).
				Str("local-member-id", g.localID.String()).Str()

			g.lg.Warn(
				"dropped Raft message since sending buffer is full (overloaded network)",
				zap.String("message-type", m.Type.String()),
				zap.String("local-member-id", g.localID.String()),
				zap.String("from", types.ID(m.From).String()),
				zap.String("remote-peer-id", g.id.String()),
				zap.Bool("remote-peer-active", g.status.isActive()),
			)
		}
	}
}

func (g *remote) stop() {
	g.pipeline.stop()
}

func (g *remote) Pause() {
	g.stop()
}

func (g *remote) Resume() {
	g.pipeline.start()
}
