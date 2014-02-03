// Copyright 2014 The Swarming Authors. All rights reserved.
// Use of this source code is governed by the Apache v2.0 license that can be
// found in the LICENSE file.

package utiltest

import (
	"fmt"
	"testing"
)

// WARNING: Any code change to this file will trigger a test failure of
// TestDecorateMax or TestDecorate. Make sure to update the expectation
// accordingly. Sorry for the inconvenience.

const file = "utiltest/location_dependent_test.go"

func a() string {
	return b()
}

func b() string {
	return c()
}

func c() string {
	return d()
}

func d() string {
	return Decorate("Foo")
}

func TestDecorateMax(t *testing.T) {
	// Sadly this test is dependent on the position of the code above.
	base := 19
	expected := fmt.Sprintf("%s:%d: %s:%d: %s:%d: Foo", file, base, file, base+4, file, base+8)
	AssertEqual(t, expected, a())
}

func TestDecorate(t *testing.T) {
	// Sadly this test is dependent on the position of this code.
	a := Decorate("Foo")
	expected := fmt.Sprintf("%s:43: Foo", file)
	AssertEqual(t, expected, a)
}
