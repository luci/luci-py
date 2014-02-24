// Copyright 2013 The Swarming Authors. All rights reserved.
// Use of this source code is governed by the Apache v2.0 license that can be
// found in the LICENSE file.

// +build appengine

package main

import (
	"code.google.com/p/swarming/services-go/isolateserver/server"
	"code.google.com/p/swarming/services-go/pkg/aedmz"
	"net/http"
)

func init() {
	server.SetupHandlers(http.DefaultServeMux, aedmz.NewApp("", ""))
}
