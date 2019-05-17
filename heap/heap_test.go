//  Copyright (c) 2019 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the
//  License. You may obtain a copy of the License at
//  http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing,
//  software distributed under the License is distributed on an "AS
//  IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
//  express or implied. See the License for the specific language
//  governing permissions and limitations under the License.

package heap

import (
	"bytes"
	cheap "container/heap"
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	"github.com/couchbase/rhmap/store"
)

func TestSize10x1x1(t *testing.T) {
	test(t, 10, 1 * 16, 1 * 16)
}

func TestSize100x10x10(t *testing.T) {
	test(t, 10, 10 * 16, 10 * 16)
}

func TestSize10000x1000x1000(t *testing.T) {
	test(t, 10000, 1000 * 16, 1000 * 16)
}

func test(t *testing.T, amount,
	heapChunkSizeBytes, dataChunkSizeBytes int) {
	dir, _ := ioutil.TempDir("", "testHeap")

	h := &Heap{
		LessFunc: func(a, b []byte) bool {
			return bytes.Compare(a, b) < 0
		},
		Heap: &store.Chunks{
			PathPrefix:     dir,
			FileSuffix:     ".heap",
			ChunkSizeBytes: heapChunkSizeBytes,
		},
		Data: &store.Chunks{
			PathPrefix:     dir,
			FileSuffix:     ".data",
			ChunkSizeBytes: dataChunkSizeBytes,
		},
	}

	defer func() {
		h.Heap.Close()
		h.Data.Close()

		os.RemoveAll(dir)
	}()

	min := "99999999"

	pushNums := func(start, end, delta int) {
		n := h.Len()

		for i := start; i != end; i = i + delta {
			istr := fmt.Sprintf("%08d", i)
			iv := []byte(istr)

			if min > istr {
				min = istr
			}

			cheap.Push(h, iv)
			n++

			if h.Err != nil {
				t.Fatalf("expected no h.Err, got: %v", h.Err)
			}

			top, err := h.Get(0)
			if err != nil {
				t.Fatalf("Get(0) err: %v", err)
			}

			if len(top) != 8 {
				t.Fatalf("len(top) %d != 8, top: %s", len(top), top)
			}

			if string(top) != min {
				t.Fatalf("top %q != min %q", top, min)
			}

			if h.Len() != n {
				t.Fatalf("h.Len() %d != %d", h.Len(), i + 1)
			}
		}
	}

	popAll := func() {
		for h.Len() > 0 {
			top := cheap.Pop(h).([]byte)

			if h.Err != nil {
				t.Fatalf("h.Err on pop: %v", h.Err)
			}

			if string(top) < min {
				t.Fatalf("popped was < min")
			}

			min = string(top)
		}
	}

	pushNums(amount, 0, -1)

	pushNums(0, amount, 1)

	popAll()

	pushNums(0, amount, 2)

	pushNums(amount, 0, -2)

	popAll()
}
