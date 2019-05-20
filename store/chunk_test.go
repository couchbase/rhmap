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

package store

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"
)

func BenchmarkChunkTruncate(b *testing.B) {
	dir, _ := ioutil.TempDir("", "testChunk")
	defer os.RemoveAll(dir)

	chunkSizeBytes := 8 * 1024

	chunks := &Chunks{
		PathPrefix:     fmt.Sprintf("%s_test", dir),
		FileSuffix:     ".testChunk",
		ChunkSizeBytes: chunkSizeBytes,
	}

	buf := make([]byte, 1024)

	appendStuff := func() {
		if chunks.LastChunkLen != 0 {
			b.Fatalf("LastChunkLen not 0")
		}

		for i := 0; i < 40; i++ {
			offset, size, err := chunks.BytesAppend(buf)
			if err != nil {
				b.Fatal(err)
			}
			if offset != uint64(i*len(buf)) ||
				size != uint64(len(buf)) {
				b.Fatalf("wrong offset/size")
			}
		}

		err := chunks.BytesTruncate(0)
		if err != nil {
			b.Fatal(err)
		}

		if chunks.LastChunkLen != 0 {
			b.Fatalf("LastChunkLen not 0 after truncate")
		}
	}

	appendStuff()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		appendStuff()
	}
}
