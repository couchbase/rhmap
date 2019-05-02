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

package rhmap

import (
	"bytes"
	"fmt"
	"reflect"
	"testing"
)

func TestSize1(t *testing.T) {
	r := NewRHMap(1)
	test(t, r, true)
	r.Reset()
	test(t, r, true)
}

func TestSize2(t *testing.T) {
	r := NewRHMap(2)
	test(t, r, true)
	r.Reset()
	test(t, r, true)
}

func TestSize10(t *testing.T) {
	r := NewRHMap(10)
	test(t, r, true)
	r.Reset()
	test(t, r, true)
}

func TestSize18NonGrowing(t *testing.T) {
	r := NewRHMap(18)
	r.MaxDistance = 100000

	test(t, r, false)
	if r.Count != 18 {
		t.Fatalf("wrong size")
	}
	if len(r.Items) != 18 {
		t.Fatalf("it unexpectedly grew")
	}

	r.Reset()
	if r.Count != 0 {
		t.Fatalf("expected empty after Reset()")
	}

	test(t, r, false)
	if r.Count != 18 {
		t.Fatalf("wrong size")
	}
	if len(r.Items) != 18 {
		t.Fatalf("it unexpectedly grew")
	}
}

func test(t *testing.T, r *RHMap, checkCopyToEnabled bool) {
	ops := 0

	g := map[string][]byte{}

	var checkCopyTo func()

	get := func(k string) {
		ops++

		rv, rok := r.Get([]byte(k))

		gv, gok := g[k]

		if rok != gok {
			t.Fatalf("ops: %d, get different ok", ops)
		}
		if (rv != nil) != (gv != nil) {
			t.Fatalf("ops: %d, get different nil's", ops)
		}
		if len(rv) != len(gv) {
			t.Fatalf("ops: %d, get different len()'s", ops)
		}
		if !bytes.Equal(rv, gv) {
			t.Fatalf("ops: %d, get different bytes.Equal()'s, %+v vs %+v",
				ops, rv, gv)
		}
		if r.Count != len(g) {
			t.Fatalf("ops: %d, get different counts", ops)
		}
	}

	set := func(k, v string) {
		ops++

		_, gexists := g[k]
		gwasNew := !gexists

		rwasNew, err := r.Set([]byte(k), []byte(v))

		g[k] = []byte(v)

		if err != nil {
			t.Fatalf("ops: %d, set err", ops)
		}
		if r.Count != len(g) {
			t.Fatalf("ops: %d, set different counts", ops)
		}
		if rwasNew != gwasNew {
			t.Fatalf("ops: %d, set different wasNew", ops)
		}

		checkCopyTo()
	}

	del := func(k string) {
		ops++

		rprevious, rexisted := r.Del([]byte(k))

		gprevious, gexisted := g[k]
		delete(g, k)

		if r.Count != len(g) {
			t.Fatalf("ops: %d, del different counts", ops)
		}
		if !bytes.Equal(rprevious, gprevious) {
			t.Fatalf("ops: %d, del different previous", ops)
		}
		if rexisted != gexisted {
			t.Fatalf("ops: %d, del different existed", ops)
		}

		checkCopyTo()
	}

	checkCopyTo = func() {
		if !checkCopyToEnabled {
			return
		}

		r2 := NewRHMap(1)
		r2.MaxDistance = 1
		r.CopyTo(r2)

		g2 := map[string][]byte{}
		r2.Visit(func(k Key, v Val) bool {
			g2[string(k)] = v
			return true
		})

		if !reflect.DeepEqual(g, g2) {
			t.Fatalf("ops: %d, set g vs g2", ops)
		}
	}

	// ------------------------------------------

	get("not a key")
	get("nothing there")

	set("a", "A")
	get("a")
	get("b")

	set("a", "AA")
	get("a")
	get("b")

	set("b", "B")
	get("a")
	get("b")
	get("c")

	get("not a key")
	get("nothing there")

	del("a")
	get("a")
	get("b")
	get("c")

	del("a")
	get("a")
	get("b")
	get("c")

	del("b")
	get("a")
	get("b")
	get("c")

	set("a", "A")
	set("b", "B")
	set("c", "C")
	set("d", "D")
	set("e", "E")
	set("f", "F")
	set("a1", "A1")
	set("b1", "B1")
	set("c1", "C1")
	set("d1", "D1")
	set("e1", "E1")
	set("f1", "F1")
	set("a11", "A11")
	set("b11", "B11")
	set("c11", "C11")
	set("d11", "D11")
	set("e11", "E11")
	set("f11", "F11") // 18 entries.

	get("a")
	get("b")
	get("c")
	get("d")
	get("e")
	get("f")

	get("not a key")
	get("nothing there")
}
