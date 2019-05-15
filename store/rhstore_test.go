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
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"reflect"
	"testing"
)

func TestSize1(t *testing.T) {
	r := NewRHStore(1)
	test(t, r, true, nil)
	r.Reset()
	test(t, r, true, nil)

	// -------------------------------------------------

	v, found := r.Get(nil)
	if found == true || v != nil {
		t.Errorf("nil get")
	}

	wasNew, err := r.Set(nil, []byte("zero len key disallowed"))
	if wasNew == true || err != ErrKeyZeroLen {
		t.Errorf("nil set")
	}

	v, found, err = r.Del(nil)
	if err == nil {
		t.Errorf("del expected err")
	}
	if found == true || v != nil {
		t.Errorf("nil del")
	}
}

func TestSize2(t *testing.T) {
	r := NewRHStore(2)
	test(t, r, true, nil)
	r.Reset()
	test(t, r, true, nil)
}

func TestSize10(t *testing.T) {
	r := NewRHStore(10)
	test(t, r, true, nil)
	r.Reset()
	test(t, r, true, nil)
}

func TestSize18NonGrowing(t *testing.T) {
	r := NewRHStore(18)
	r.MaxDistance = 100000

	testSize18NonGrowing(t, r)
}

func testSize18NonGrowing(t *testing.T, r *RHStore) {
	test(t, r, false, nil)
	if r.Count != 18 {
		t.Fatalf("wrong size")
	}
	if len(r.Slots) != 18*3 {
		t.Fatalf("it unexpectedly grew")
	}

	r.Reset()
	if r.Count != 0 {
		t.Fatalf("expected empty after Reset()")
	}
	if len(r.Slots) != 18*3 {
		t.Fatalf("it unexpectedly grew")
	}
	for i := 0; i < len(r.Slots); i++ {
		if r.Slots[i] != 0 {
			t.Fatalf("reset failed")
		}
	}
	if len(r.Bytes) != 0 {
		t.Fatalf("reset failed")
	}

	test(t, r, false, func(
		g map[string][]byte,
		get func(k string),
		set func(k, v string),
		del func(k string)) {
		// At this point, r.Slots looks roughly like...
		//
		// [{Key:[102 49 49] Val:[70 49 49] Distance:1}
		//  {Key:[97 49 49] Val:[65 49 49] Distance:1}
		//  {Key:[100] Val:[68] Distance:1}
		//  {Key:[101 49 49] Val:[69 49 49] Distance:1}
		//  {Key:[101] Val:[69] Distance:2}
		//  {Key:[100 49 49] Val:[68 49 49] Distance:2}
		//  {Key:[102 49] Val:[70 49] Distance:2}
		//  {Key:[99 49 49] Val:[67 49 49] Distance:3}
		//  {Key:[98 49 49] Val:[66 49 49] Distance:3}
		//  {Key:[100 49] Val:[68 49] Distance:3}
		//  {Key:[98 49] Val:[66 49] Distance:4}
		//  {Key:[99 49] Val:[67 49] Distance:0}
		//  {Key:[101 49] Val:[69 49] Distance:1}
		//  {Key:[98] Val:[66] Distance:0}
		//  {Key:[97 49] Val:[65 49] Distance:1}
		//  {Key:[99] Val:[67] Distance:1}
		//  {Key:[97] Val:[65] Distance:0}
		//  {Key:[102] Val:[70] Distance:0}]

		if r.Count != 18 {
			t.Fatalf("wrong size after main test: %d", r.Count)
		}

		ikey, _ := r.ItemKey(r.Item(0))
		if string(ikey) != "f11" {
			ikey, _ := r.ItemKey(r.Item(0))
			ival, _ := r.ItemVal(r.Item(0))
			t.Errorf("expected 0th key to be f11, got: %v, %v, %d",
				ikey, ival, r.Size)

			for i := 0; i < r.Size; i++ {
				item := r.Item(i)
				ikey, _ := r.ItemKey(item)
				ival, _ := r.ItemVal(item)

				fmt.Printf("  %v => %v, %d\n",
					ikey, ival, item.Distance())
			}
		}

		ikey, _ = r.ItemKey(r.Item(1))
		if string(ikey) != "a11" {
			t.Errorf("expected 1th key to be a11, got: %v", ikey)
		}

		// Deleting f11 causes a bunch of left-shift's.
		del("f11")

		if r.Count != 17 {
			t.Fatalf("wrong size after f11 delete: %d", r.Count)
		}

		// Try another key.
		set("california", "hi")
		get("california")
		del("california")
		get("california")

		if r.Count != 17 {
			t.Fatalf("wrong size after california set and del: %d", r.Count)
		}
		if len(r.Slots) != 18*3 {
			t.Fatalf("it unexpectedly grew after california")
		}

		// Fully loaded after set x.
		set("x", "xxx")
		if r.Count != 18 {
			t.Fatalf("wrong size, 18 != %d", r.Count)
		}
		if len(r.Slots) != 18*3 {
			t.Fatalf("it unexpectedly grew after x")
		}

		del("not-there")

		// Forced to grow since we are fully loaded.
		set("y", "yyy")
		if r.Count != 19 {
			t.Fatalf("wrong size, 19 != %d", r.Count)
		}
		if len(r.Slots) != 36*3 {
			t.Fatalf("it didn't grow as expected after y")
		}
	})
}

type andThen func(
	g map[string][]byte,
	get func(k string),
	set func(k, v string),
	del func(k string))

func test(t *testing.T, r *RHStore,
	checkCopyToEnabled bool, andThen andThen) {
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
			r.Visit(func(k Key, v Val) bool {
				fmt.Printf("  k: %s, v: %s\n", k, v)
				return true
			})

			t.Fatalf("ops: %d, set different counts, k: %s, v: %s,"+
				" r.Count: %d, len(g): %d, g: %+v",
				ops, k, v, r.Count, len(g), g)
		}
		if rwasNew != gwasNew {
			t.Fatalf("ops: %d, set different wasNew", ops)
		}

		checkCopyTo()
	}

	del := func(k string) {
		ops++

		rprevious, rexisted, err := r.Del([]byte(k))
		if err != nil {
			t.Fatalf("ops: %d, del err: %v", ops, err)
		}

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

		r2 := NewRHStore(1)
		r2.MaxDistance = 1
		r.CopyTo(r2)

		g2 := map[string][]byte{}
		r2.Visit(func(k Key, v Val) bool {
			g2[string(k)] = v
			return true
		})

		if !reflect.DeepEqual(g, g2) {
			t.Fatalf("ops: %d,\n g: %v vs\n g2: %v", ops, g, g2)
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
	set("a1", "")
	set("b1", "")
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

	if andThen != nil {
		andThen(g, get, set, del)
	}
}

func TestRHStoreFileDefaultOptions(t *testing.T) {
	options := DefaultRHStoreFileOptions
	testRHStoreFile(t, options)
}

func TestRHStoreFileSize1(t *testing.T) {
	options := DefaultRHStoreFileOptions
	options.StartSize = 1
	testRHStoreFile(t, options)
}

func TestRHStoreFileSize2(t *testing.T) {
	options := DefaultRHStoreFileOptions
	options.StartSize = 2
	testRHStoreFile(t, options)
}

func TestRHStoreFileSize10(t *testing.T) {
	options := DefaultRHStoreFileOptions
	options.StartSize = 10
	testRHStoreFile(t, options)
}

func testRHStoreFile(t *testing.T, options RHStoreFileOptions) {
	dir, _ := ioutil.TempDir("", "testRHStoreFile")
	defer os.RemoveAll(dir)

	sf, err := CreateRHStoreFile(dir, options)
	if err != nil {
		t.Fatal(err)
	}

	defer sf.Close()

	r := &sf.RHStore

	test(t, r, true, nil)
	r.Reset()
	test(t, r, true, nil)
}

func TestRHStoreFileSize18NonGrowing(t *testing.T) {
	options := DefaultRHStoreFileOptions
	options.StartSize = 18
	options.MaxDistance = 100000

	dir, _ := ioutil.TempDir("", "testRHStoreFile")
	defer os.RemoveAll(dir)

	sf, err := CreateRHStoreFile(dir, options)
	if err != nil {
		t.Fatal(err)
	}

	defer sf.Close()

	r := &sf.RHStore

	testSize18NonGrowing(t, r)
}
