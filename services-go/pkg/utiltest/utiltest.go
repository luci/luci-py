// Copyright 2013 The Swarming Authors. All rights reserved.
// Use of this source code is governed by the Apache v2.0 license that can be
// found in the LICENSE file.

// Package utiltest contains testing utilities.
package utiltest

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"
)

const sep = string(os.PathSeparator)

var blacklistedItems []string = []string{
	filepath.Join("runtime", "proc.c"),
	filepath.Join("testing", "testing.go"),
	filepath.Join("utiltest", "utiltest.go"),
}

// truncatePath only keep the base filename and its immediate containing directory.
func truncatePath(file string) string {
	if index := strings.LastIndex(file, sep); index >= 0 {
		if index2 := strings.LastIndex(file[:index], sep); index2 >= 0 {
			// Keep the first directory to help figure out which file it is.
			return file[index2+1:]
		}
		return file[index+1:]
	}
	return file
}

// Decorate adds a prefix 'file:line: ' to a string, containing the 3 callers
// in the stack.
//
// It is inspired by testing's decorate().
func Decorate(s string) string {
	type item struct {
		file string
		line int
	}
	items := make([]item, 4)
	for i := len(items); i > 0; i-- {
		_, file, line, ok := runtime.Caller(i) // decorate + log + public function.
		if ok {
			items[i-1].file = truncatePath(file)
			items[i-1].line = line
		} else {
			items[i-1].file = ""
		}
	}
	blacklisted := false
	for i := range items {
		for _, b := range blacklistedItems {
			if items[i].file == b {
				items[i].file = ""
				blacklisted = true
				break
			}
		}
	}
	if !blacklisted {
		items[0].file = ""
	}
	for _, i := range items {
		if i.file != "" {
			s = fmt.Sprintf("%s:%d: %s", i.file, i.line, s)
		}
	}
	return s
}

// AssertEqual verifies that two objects are equals and fails the test case
// otherwise.
//
// TODO(maruel): testing.TB is 1.2+
func AssertEqual(t *testing.T, expected, actual interface{}) {
	AssertEqualf(t, expected, actual, "assertEqual() failure.\nExpected: %#v\nActual:   %#v", expected, actual)
}

// AssertEqualIndex verifies that two objects are equals and fails the test case
// otherwise.
//
// It is meant to be used in loops where a list of intrant->expected is
// processed so the assert failure message contains the index of the failing
// expectation.
func AssertEqualIndex(t *testing.T, index int, expected, actual interface{}) {
	AssertEqualf(t, expected, actual, "assertEqual() failure.\nIndex: %d\nExpected: %#v\nActual:   %#v", index, expected, actual)
}

// AssertEqualf verifies that two objects are equals and fails the test case
// otherwise.
//
// This functions enables specifying an arbitrary string on failure.
func AssertEqualf(t *testing.T, expected, actual interface{}, format string, items ...interface{}) {
	if !reflect.DeepEqual(actual, expected) {
		t.Fatalf(Decorate(format), items...)
	}
}
