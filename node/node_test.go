package node

import (
	"testing"
	"time"
)

func BenchmarkBatchRequestMarshal(b *testing.B) {
	br := &BatchInternalRaftRequest{}
	br.ReqId = 1
	irr := InternalRaftRequest{
		Data: make([]byte, 100),
	}
	irr.Header.Timestamp = time.Now().UnixNano()
	br.Reqs = append(br.Reqs, irr)

	b.SetParallelism(2)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			br.Marshal()
		}
	})
}

func BenchmarkRequestMarshal(b *testing.B) {
	irr := InternalRaftRequest{
		Data: make([]byte, 100),
	}
	irr.Header.Timestamp = time.Now().UnixNano()

	b.SetParallelism(2)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			irr.Marshal()
		}
	})
}
