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
	"bytes"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/youzan/ZanRedisDB/pkg/types"
	"github.com/youzan/ZanRedisDB/raft/raftpb"
	"github.com/youzan/ZanRedisDB/stats"
)

func TestMsgAppV2(t *testing.T) {
	grp1 := raftpb.Group{NodeId: 1, GroupId: 1, RaftReplicaId: 1}
	grp2 := raftpb.Group{NodeId: 2, GroupId: 1, RaftReplicaId: 2}
	tests := []raftpb.Message{
		linkHeartbeatMessage,
		{
			Type:      raftpb.MsgApp,
			From:      1,
			FromGroup: grp1,
			To:        2,
			ToGroup:   grp2,
			Term:      1,
			LogTerm:   1,
			Index:     0,
			Entries: []raftpb.Entry{
				{Term: 1, Index: 1, Data: []byte("some data")},
				{Term: 1, Index: 2, Data: []byte("some data")},
				{Term: 1, Index: 3, Data: []byte("some data")},
			},
		},
		// consecutive MsgApp
		{
			Type:      raftpb.MsgApp,
			From:      1,
			FromGroup: grp1,
			To:        2,
			ToGroup:   grp2,
			Term:      1,
			LogTerm:   1,
			Index:     3,
			Entries: []raftpb.Entry{
				{Term: 1, Index: 4, Data: []byte("some data")},
			},
		},
		linkHeartbeatMessage,
		// consecutive MsgApp after linkHeartbeatMessage
		{
			Type:      raftpb.MsgApp,
			From:      1,
			FromGroup: grp1,
			To:        2,
			ToGroup:   grp2,
			Term:      1,
			LogTerm:   1,
			Index:     4,
			Entries: []raftpb.Entry{
				{Term: 1, Index: 5, Data: []byte("some data")},
			},
		},
		// MsgApp with higher term
		{
			Type:      raftpb.MsgApp,
			From:      1,
			FromGroup: grp1,
			To:        2,
			ToGroup:   grp2,
			Term:      3,
			LogTerm:   1,
			Index:     5,
			Entries: []raftpb.Entry{
				{Term: 3, Index: 6, Data: []byte("some data")},
			},
		},
		linkHeartbeatMessage,
		// consecutive MsgApp
		{
			Type:      raftpb.MsgApp,
			From:      1,
			FromGroup: grp1,
			To:        2,
			ToGroup:   grp2,
			Term:      3,
			LogTerm:   2,
			Index:     6,
			Entries: []raftpb.Entry{
				{Term: 3, Index: 7, Data: []byte("some data")},
			},
		},
		// consecutive empty MsgApp
		{
			Type:      raftpb.MsgApp,
			From:      1,
			FromGroup: grp1,
			To:        2,
			ToGroup:   grp2,
			Term:      3,
			LogTerm:   2,
			Index:     7,
			Entries:   nil,
		},
		linkHeartbeatMessage,
	}
	b := &bytes.Buffer{}
	enc := newMsgAppV2Encoder(b, &stats.PeerStats{})
	dec := newMsgAppV2Decoder(b, types.ID(2), types.ID(1))
	m := &raftpb.BatchMessages{}
	m.Msgs = make([]raftpb.Message, 0, 1)

	for i, tt := range tests {
		if err := enc.encode(&tt); err != nil {
			t.Errorf("#%d: unexpected encode message error: %v", i, err)
			continue
		}
		m.Msgs = m.Msgs[:0]
		var err error
		m, err = dec.decode(m)
		if err != nil {
			t.Errorf("#%d: unexpected decode message error: %v", i, err)
			continue
		}
		if !reflect.DeepEqual(m.Msgs[0], tt) {
			t.Errorf("#%d: message = %+v, want %+v", i, m, tt)
		}
	}
}

func BenchmarkAppV2EncodeDecode(b *testing.B) {
	grp1 := raftpb.Group{NodeId: 1, GroupId: 1, RaftReplicaId: 1}
	grp2 := raftpb.Group{NodeId: 2, GroupId: 1, RaftReplicaId: 2}
	tests := []raftpb.Message{
		// consecutive MsgApp
		{
			Type:      raftpb.MsgApp,
			From:      1,
			FromGroup: grp1,
			To:        2,
			ToGroup:   grp2,
			Term:      1,
			LogTerm:   1,
			Index:     3,
			Entries: []raftpb.Entry{
				{Term: 1, Index: 4, Data: []byte("some data")},
			},
		},
		{
			Type:      raftpb.MsgApp,
			From:      1,
			FromGroup: grp1,
			To:        2,
			ToGroup:   grp2,
			Term:      1,
			LogTerm:   1,
			Index:     4,
			Entries: []raftpb.Entry{
				{Term: 1, Index: 5, Data: []byte("some data")},
			},
		},
		{
			Type:      raftpb.MsgApp,
			From:      1,
			FromGroup: grp1,
			To:        2,
			ToGroup:   grp2,
			Term:      3,
			LogTerm:   1,
			Index:     5,
			Entries: []raftpb.Entry{
				{Term: 3, Index: 6, Data: []byte("some data")},
			},
		},
		{
			Type:      raftpb.MsgApp,
			From:      1,
			FromGroup: grp1,
			To:        2,
			ToGroup:   grp2,
			Term:      3,
			LogTerm:   2,
			Index:     6,
			Entries: []raftpb.Entry{
				{Term: 3, Index: 7, Data: []byte("some data")},
			},
		},
	}
	buf := &bytes.Buffer{}
	enc := newMsgAppV2Encoder(buf, &stats.PeerStats{})
	dec := newMsgAppV2Decoder(buf, types.ID(2), types.ID(1))

	b.Run("bench-encode-decode", func(b *testing.B) {
		start := time.Now()
		for l := 0; l < b.N; l++ {
			bm := &raftpb.BatchMessages{}
			bm.Msgs = make([]raftpb.Message, 0, 1)
			for j := 0; j < 10; j++ {
				for i, tt := range tests {
					if err := enc.encode(&tt); err != nil {
						panic(fmt.Sprintf("#%d: unexpected encode message error: %v", i, err))
					}
				}
			}
			for j := 0; j < 10; j++ {
				for i, _ := range tests {
					bm.Msgs = bm.Msgs[:0]
					_, err := dec.decode(bm)
					if err != nil {
						panic(fmt.Sprintf("#%d: unexpected decode message error: %v", i, err))
					}
				}
			}
		}
		b.Logf("cost time: %v, run times: %v", time.Since(start), b.N)
	})
}

func BenchmarkAppV2BatchEncodeDecode(b *testing.B) {
	grp1 := raftpb.Group{NodeId: 1, GroupId: 1, RaftReplicaId: 1}
	grp2 := raftpb.Group{NodeId: 2, GroupId: 1, RaftReplicaId: 2}
	tests := []raftpb.Message{
		// consecutive MsgApp
		{
			Type:      raftpb.MsgApp,
			From:      1,
			FromGroup: grp1,
			To:        2,
			ToGroup:   grp2,
			Term:      1,
			LogTerm:   1,
			Index:     3,
			Entries: []raftpb.Entry{
				{Term: 1, Index: 4, Data: []byte("some data")},
			},
		},
		{
			Type:      raftpb.MsgApp,
			From:      1,
			FromGroup: grp1,
			To:        2,
			ToGroup:   grp2,
			Term:      1,
			LogTerm:   1,
			Index:     4,
			Entries: []raftpb.Entry{
				{Term: 1, Index: 5, Data: []byte("some data")},
			},
		},
		{
			Type:      raftpb.MsgApp,
			From:      1,
			FromGroup: grp1,
			To:        2,
			ToGroup:   grp2,
			Term:      3,
			LogTerm:   1,
			Index:     5,
			Entries: []raftpb.Entry{
				{Term: 3, Index: 6, Data: []byte("some data")},
			},
		},
		{
			Type:      raftpb.MsgApp,
			From:      1,
			FromGroup: grp1,
			To:        2,
			ToGroup:   grp2,
			Term:      3,
			LogTerm:   2,
			Index:     6,
			Entries: []raftpb.Entry{
				{Term: 3, Index: 7, Data: []byte("some data")},
			},
		},
	}
	buf := &bytes.Buffer{}
	enc := newMsgAppV2BatchEncoder(buf, &stats.PeerStats{})
	dec := newMsgAppV2BatchDecoder(buf, types.ID(2), types.ID(1))

	b.Run("bench-batch-encode-decode", func(b *testing.B) {
		var batched raftpb.BatchMessages
		batched.Msgs = make([]raftpb.Message, 0, 10*len(tests))
		start := time.Now()
		for l := 0; l < b.N; l++ {
			bm := &raftpb.BatchMessages{}
			bm.Msgs = make([]raftpb.Message, 0, 1)
			for j := 0; j < 10; j++ {
				for _, tt := range tests {
					batched.Msgs = append(batched.Msgs, tt)
				}
			}
			if err := enc.encodeBatch(&batched); err != nil {
				panic(fmt.Sprintf("unexpected encode message error: %v", err))
			}
			var err error
			bm.Msgs = bm.Msgs[:0]
			bm, err = dec.decode(bm)
			if err != nil {
				panic(fmt.Sprintf("unexpected decode message error: %v", err))
			}
			if len(bm.Msgs) != len(batched.Msgs) {
				panic(fmt.Sprintf("unexpected decode message length: %v %v", len(bm.Msgs), len(batched.Msgs)))
			}
			batched.Msgs = batched.Msgs[:0]
		}
		b.Logf("cost time: %v, run times: %v", time.Since(start), b.N)
	})
}
