package node

import (
	"sync"
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

func BenchmarkBatchRequestMarshal2(b *testing.B) {
	br := &BatchInternalRaftRequest{}
	br.ReqId = 1
	irr := InternalRaftRequest{
		Data: make([]byte, 32),
	}
	irr.Header.Timestamp = time.Now().UnixNano()
	br.Reqs = append(br.Reqs, irr)

	b.ResetTimer()
	start := time.Now()
	var wg sync.WaitGroup
	for g := 0; g < 100; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < b.N; i++ {
				br.Marshal()
			}
		}()
	}
	wg.Wait()
	cost := time.Since(start)
	b.Logf("bench cost: %v for %v", cost, b.N)
}

func BenchmarkPoolGet(b *testing.B) {
	br := &BatchInternalRaftRequest{}
	br.ReqId = 1
	irr := InternalRaftRequest{
		Data: make([]byte, 32),
	}
	irr.Header.Timestamp = time.Now().UnixNano()
	br.Reqs = append(br.Reqs, irr)

	b.ResetTimer()
	start := time.Now()
	var wg sync.WaitGroup
	var pool sync.Pool
	pool.New = func() interface{} {
		return make([]byte, br.Size())
	}
	for g := 0; g < 100; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < b.N; i++ {
				bs := pool.Get().([]byte)
				pool.Put(bs)
			}
		}()
	}
	wg.Wait()
	cost := time.Since(start)
	b.Logf("bench cost: %v for %v", cost, b.N)
}

func BenchmarkMakeSlice(b *testing.B) {
	br := &BatchInternalRaftRequest{}
	br.ReqId = 1
	irr := InternalRaftRequest{
		Data: make([]byte, 32),
	}
	irr.Header.Timestamp = time.Now().UnixNano()
	br.Reqs = append(br.Reqs, irr)

	b.ResetTimer()
	start := time.Now()
	var wg sync.WaitGroup
	for g := 0; g < 100; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < b.N; i++ {
				bs := make([]byte, br.Size())
				_ = bs
			}
		}()
	}
	wg.Wait()
	cost := time.Since(start)
	b.Logf("bench cost: %v for %v", cost, b.N)
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
