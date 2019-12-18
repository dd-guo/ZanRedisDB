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
	"encoding/binary"
	"fmt"
	"io"
	"time"

	"github.com/youzan/ZanRedisDB/pkg/pbutil"
	"github.com/youzan/ZanRedisDB/pkg/types"
	"github.com/youzan/ZanRedisDB/raft/raftpb"
	"github.com/youzan/ZanRedisDB/stats"
)

const (
	msgAppV2BatchBufSize = 16 * 1024 * 1024
)

type msgAppV2BatchEncoder struct {
	w  io.Writer
	ps *stats.PeerStats

	buf       []byte
	uint64buf []byte
	uint8buf  []byte
}

func newMsgAppV2BatchEncoder(w io.Writer, ps *stats.PeerStats) *msgAppV2BatchEncoder {
	return &msgAppV2BatchEncoder{
		w:         w,
		ps:        ps,
		buf:       make([]byte, msgAppV2BatchBufSize),
		uint64buf: make([]byte, 8),
		uint8buf:  make([]byte, 1),
	}
}

func (enc *msgAppV2BatchEncoder) encode(m *raftpb.Message) error {
	var bm raftpb.BatchMessages
	bm.Msgs = append(bm.Msgs, *m)
	return enc.encodeBatch(&bm)
}

func (enc *msgAppV2BatchEncoder) encodeBatch(bm *raftpb.BatchMessages) error {
	start := time.Now()
	if err := binary.Write(enc.w, binary.BigEndian, msgTypeApp); err != nil {
		return err
	}
	// write size of message
	ns := bm.Size()
	if err := binary.Write(enc.w, binary.BigEndian, uint64(ns)); err != nil {
		return err
	}
	// write message
	if ns < msgAppV2BatchBufSize {
		if _, err := bm.MarshalTo(enc.buf[:ns]); err != nil {
			return err
		}
		if _, err := enc.w.Write(enc.buf[:ns]); err != nil {
			return err
		}
	} else {
		if _, err := enc.w.Write(pbutil.MustMarshal(bm)); err != nil {
			return err
		}
	}

	enc.ps.Succ(time.Since(start))
	return nil
}

type msgAppV2BatchDecoder struct {
	r             io.Reader
	local, remote types.ID

	buf       []byte
	uint64buf []byte
	uint8buf  []byte
}

func newMsgAppV2BatchDecoder(r io.Reader, local, remote types.ID) *msgAppV2BatchDecoder {
	return &msgAppV2BatchDecoder{
		r:         r,
		local:     local,
		remote:    remote,
		buf:       make([]byte, msgAppV2BatchBufSize),
		uint64buf: make([]byte, 8),
		uint8buf:  make([]byte, 1),
	}
}

func (dec *msgAppV2BatchDecoder) decode(m *raftpb.BatchMessages) (*raftpb.BatchMessages, error) {
	var (
		typ uint8
	)
	if _, err := io.ReadFull(dec.r, dec.uint8buf); err != nil {
		return m, err
	}
	typ = uint8(dec.uint8buf[0])
	switch typ {
	case msgTypeLinkHeartbeat:
		m.Msgs = append(m.Msgs, linkHeartbeatMessage)
		return m, nil
	case msgTypeApp:
		var size uint64
		if err := binary.Read(dec.r, binary.BigEndian, &size); err != nil {
			return m, err
		}
		var buf []byte
		if size < msgAppV2BatchBufSize {
			buf = dec.buf[:size]
		} else {
			buf = make([]byte, int(size))
		}
		if _, err := io.ReadFull(dec.r, buf); err != nil {
			return m, err
		}
		err := pbutil.MaybeUnmarshal(m, buf)
		if err != nil {
			return m, err
		}
	default:
		return m, fmt.Errorf("failed to parse type %d in msgappv2 stream", typ)
	}
	return m, nil
}
