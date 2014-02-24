// Copyright 2013 The Swarming Authors. All rights reserved.
// Use of this source code is governed by the Apache v2.0 license that can be
// found in the LICENSE file.

// +build !appengine

package aedmztest

import (
	"bytes"
	"code.google.com/p/swarming/services-go/pkg/aedmz"
	"code.google.com/p/swarming/services-go/pkg/ofh"
	"code.google.com/p/swarming/services-go/third_party/code.google.com/p/leveldb-go/leveldb/memdb"
	"net/http"
)

// NewAppMock returns an AppContext to be used in unit tests.
//
// It has AppID "Yo" and version "v1".
func NewAppMock() aedmz.AppContext {
	return aedmz.NewApp("Yo", "v1", &bytes.Buffer{}, ofh.MakeStubProvider(http.DefaultClient), memdb.New(nil))
}

// CloseRequest closes a testing aedmz.RequestContext.
//
// It is only necessary to call it when creating a RequestContext with
// AppContext.NewContext() directly without using a route where InjectContext()
// was not called on. InjectContext() cleans up the context on the user's
// behalf.
func CloseRequest(r aedmz.RequestContext) {
	// Closing is not necessary when running standalone.
}
