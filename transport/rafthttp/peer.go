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

package rafthttp

import (
	"sync"
	"time"

	"github.com/youzan/ZanRedisDB/pkg/types"
	"github.com/youzan/ZanRedisDB/raft"
	"github.com/youzan/ZanRedisDB/raft/raftpb"
	"github.com/youzan/ZanRedisDB/snap"
	"github.com/youzan/ZanRedisDB/stats"
)

const (
	// ConnReadTimeout and ConnWriteTimeout are the i/o timeout set on each connection rafthttp pkg creates.
	// A 5 seconds timeout is good enough for recycling bad connections. Or we have to wait for
	// tcp keepalive failing to detect a bad connection, which is at minutes level.
	// For long term streaming connections, rafthttp pkg sends application level linkHeartbeatMessage
	// to keep the connection alive.
	// For short term pipeline connections, the connection MUST be killed to avoid it being
	// put back to http pkg connection pool.
	ConnReadTimeout  = 5 * time.Second
	ConnWriteTimeout = 5 * time.Second

	recvBufSize = 4096 * 8
	// maxPendingProposals holds the proposals during one leader election process.
	// Generally one leader election takes at most 1 sec. It should have
	// 0-2 election conflicts, and each one takes 0.5 sec.
	// We assume the number of concurrent proposers is smaller than 4096.
	// One client blocks on its proposal for at least 1 sec, so 4096 is enough
	// to hold all proposals.

	// since we are using the transport in multi-raft, we need more buffer to hold
	// all proposals in all raft group
	maxPendingProposals = 4096 * 16

	streamAppV3 = "streamMsgAppV3"
	streamAppV2 = "streamMsgAppV2"
	streamMsg   = "streamMsg"
	pipelineMsg = "pipeline"
	sendSnap    = "sendMsgSnap"
)

type Peer interface {
	// send sends the message to the remote peer. The function is non-blocking
	// and has no promise that the message will be received by the remote.
	// When it fails to send message out, it will report the status to underlying
	// raft.
	send(m raftpb.Message)

	// sendSnap sends the merged snapshot message to the remote peer. Its behavior
	// is similar to send.
	sendSnap(m snap.Message)

	// update updates the urls of remote peer.
	update(urls types.URLs)

	// attachOutgoingConn attaches the outgoing connection to the peer for
	// stream usage. After the call, the ownership of the outgoing
	// connection hands over to the peer. The peer will close the connection
	// when it is no longer used.
	attachOutgoingConn(conn *outgoingConn)
	// activeSince returns the time that the connection with the
	// peer becomes active.
	activeSince() time.Time
	// stop performs any necessary finalization and terminates the peer
	// elegantly.
	stop()
}

// peer is the representative of a remote raft node. Local raft node sends
// messages to the remote through peer.
// Each peer has two underlying mechanisms to send out a message: stream and
// pipeline.
// A stream is a receiver initialized long-polling connection, which
// is always open to transfer messages. Besides general stream, peer also has
// a optimized stream for sending msgApp since msgApp accounts for large part
// of all messages. Only raft leader uses the optimized stream to send msgApp
// to the remote follower node.
// A pipeline is a series of http clients that send http requests to the remote.
// It is only used when the stream has not been established.
type peer struct {
	// id of the remote raft peer node
	id types.ID
	r  Raft

	status *peerStatus

	picker *urlPicker

	msgAppV3Writer *streamWriter
	msgAppV2Writer *streamWriter
	writer         *streamWriter
	pipeline       *pipeline
	snapSender     *snapshotSender // snapshot sender to send v3 snapshot messages
	msgAppV2Reader *streamReader
	msgAppV3Reader *streamReader
	msgAppReader   *streamReader

	mu     sync.Mutex
	paused bool

	stopc chan struct{}
}

func startPeer(transport *Transport, urls types.URLs, peerID types.ID, ps *stats.PeerStats) *peer {
	plog.Infof("starting peer %s...", peerID)
	defer plog.Infof("started peer %s", peerID)

	status := newPeerStatus(peerID)
	picker := newURLPicker(urls)
	errorc := transport.ErrorC
	r := transport.Raft
	pipeline := &pipeline{
		peerID:    peerID,
		tr:        transport,
		picker:    picker,
		status:    status,
		peerStats: ps,
		raft:      r,
		errorc:    errorc,
	}
	pipeline.start()

	p := &peer{
		id:             peerID,
		r:              r,
		status:         status,
		picker:         picker,
		msgAppV3Writer: startStreamWriter(peerID, status, ps, r),
		msgAppV2Writer: startStreamWriter(peerID, status, ps, r),
		writer:         startStreamWriter(peerID, status, ps, r),
		pipeline:       pipeline,
		snapSender:     newSnapshotSender(transport, picker, peerID, status),
		stopc:          make(chan struct{}),
	}

	p.msgAppV2Reader = startStreamReader(peerID, streamTypeMsgAppV2, transport, picker, status, r)
	p.msgAppV3Reader = startStreamReader(peerID, streamTypeMsgAppV3, transport, picker, status, r)
	p.msgAppReader = startStreamReader(peerID, streamTypeMessage, transport, picker, status, r)

	return p
}

func (p *peer) send(m raftpb.Message) {
	p.mu.Lock()
	paused := p.paused
	p.mu.Unlock()

	if paused {
		return
	}

	writec, name := p.pick(m)
	select {
	case writec <- m:
	default:
		p.r.ReportUnreachable(m.To, m.ToGroup)
		if isMsgSnap(m) {
			p.r.ReportSnapshot(m.To, m.ToGroup, raft.SnapshotFailure)
		}
		if p.status.isActive() {
			plog.MergeWarningf("dropped internal raft message to %s since %s's sending buffer is full (bad/overloaded network)", p.id, name)
		}
		plog.Debugf("dropped %s to %s since %s's sending buffer is full", m.Type, p.id, name)
	}
}

func (p *peer) sendSnap(m snap.Message) {
	go p.snapSender.send(m)
}

func (p *peer) update(urls types.URLs) {
	p.picker.update(urls)
}

func (p *peer) attachOutgoingConn(conn *outgoingConn) {
	var ok bool
	switch conn.t {
	case streamTypeMsgAppV3:
		ok = p.msgAppV3Writer.attach(conn)
	case streamTypeMsgAppV2:
		ok = p.msgAppV2Writer.attach(conn)
	case streamTypeMessage:
		ok = p.writer.attach(conn)
	default:
		plog.Panicf("unhandled stream type %s", conn.t)
	}
	if !ok {
		conn.Close()
	}
}

func (p *peer) activeSince() time.Time { return p.status.activeSince() }

// Pause pauses the peer. The peer will simply drops all incoming
// messages without returning an error.
func (p *peer) Pause() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.paused = true
	p.msgAppReader.pause()
	p.msgAppV2Reader.pause()
	p.msgAppV3Reader.pause()
}

// Resume resumes a paused peer.
func (p *peer) Resume() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.paused = false
	p.msgAppReader.resume()
	p.msgAppV2Reader.resume()
	p.msgAppV3Reader.resume()
}

func (p *peer) stop() {
	plog.Infof("stopping peer %s...", p.id)
	defer plog.Infof("stopped peer %s", p.id)

	close(p.stopc)
	p.msgAppV2Writer.stop()
	p.msgAppV3Writer.stop()
	p.writer.stop()
	p.pipeline.stop()
	p.snapSender.stop()
	p.msgAppV2Reader.stop()
	p.msgAppV3Reader.stop()
	p.msgAppReader.stop()
}

// pick picks a chan for sending the given message. The picked chan and the picked chan
// string name are returned.
func (p *peer) pick(m raftpb.Message) (writec chan<- raftpb.Message, picked string) {
	var ok bool
	// Considering MsgSnap may have a big size, e.g., 1G, and will block
	// stream for a long time, only use one of the N pipelines to send MsgSnap.
	if isMsgSnap(m) {
		return p.pipeline.msgc, pipelineMsg
	} else if writec, ok = p.msgAppV3Writer.writec(); ok && isMsgApp(m) {
		return writec, streamAppV3
	} else if writec, ok = p.msgAppV2Writer.writec(); ok && isMsgApp(m) {
		return writec, streamAppV2
	} else if writec, ok = p.writer.writec(); ok {
		return writec, streamMsg
	}
	return p.pipeline.msgc, pipelineMsg
}

func isMsgApp(m raftpb.Message) bool { return m.Type == raftpb.MsgApp }

func isMsgSnap(m raftpb.Message) bool { return m.Type == raftpb.MsgSnap }
