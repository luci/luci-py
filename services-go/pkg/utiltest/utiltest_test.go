// Copyright 2013 The Swarming Authors. All rights reserved.
// Use of this source code is governed by the Apache v2.0 license that can be
// found in the LICENSE file.

package utiltest

import (
	"testing"
)

func TestAssertEqual(t *testing.T) {
	j := true
	var i interface{} = &j
	AssertEqual(t, &j, i)
	if t.Failed() {
		t.Fatal("Expected success")
	}
}

func TestAssertEqualFail(t *testing.T) {
	t2 := &testing.T{}
	AssertEqual(t2, true, false)
	if !t2.Failed() {
		t.Fatal("Expected failure")
	}
}

func TestAssertEqualIndex(t *testing.T) {
	j := true
	var i interface{} = &j
	AssertEqualIndex(t, 24, &j, i)
	if t.Failed() {
		t.Fatal("Expected success")
	}
}

func TestAssertEqualIndexFail(t *testing.T) {
	t2 := &testing.T{}
	AssertEqualIndex(t2, 24, true, false)
	if !t2.Failed() {
		t.Fatal("Expected failure")
	}
}

func TestAssertEqualf(t *testing.T) {
	j := true
	var i interface{} = &j
	AssertEqualf(t, &j, i, "foo %s %d", "bar", 2)
	if t.Failed() {
		t.Fatal("Expected success")
	}
}

func TestAssertEqualfFail(t *testing.T) {
	t2 := &testing.T{}
	AssertEqualf(t2, true, false, "foo %s %d", "bar", 2)
	if !t2.Failed() {
		t.Fatal("Expected failure")
	}
}
