// Copyright 2013 The Swarming Authors. All rights reserved.
// Use of this source code is governed by the Apache v2.0 license that can be
// found in the LICENSE file.

// +build !appengine

package aedmz

import (
	"bytes"
	"code.google.com/p/swarming/services-go/pkg/ofh"
	"code.google.com/p/swarming/services-go/third_party/code.google.com/p/leveldb-go/leveldb/memdb"
	"net/http"
	"time"
)

type appContextImplMock struct {
	now time.Time
}

func (a appContextImplMock) Now() time.Time {
	return a.now
}

// NewAppMock returns an *appContext to be used in unit tests.
//
// It has AppID "Yo" and version "v1".
//
// It is impossible to import aedmztest here because it would cause an import
// cycle.
func NewAppMock() *appContext {
	return NewAppInternal("Yo", "v1", &bytes.Buffer{}, ofh.MakeStubProvider(http.DefaultClient), memdb.New(nil), appContextImplMock{}).(*appContext)
}

func CloseRequest(r RequestContext) {
	// Closing is not necessary when running standalone.
}
