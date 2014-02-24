// Copyright 2013 The Swarming Authors. All rights reserved.
// Use of this source code is governed by the Apache v2.0 license that can be
// found in the LICENSE file.

// +build !appengine

package main

import (
	"code.google.com/p/swarming/services-go/isolateserver/server"
	"code.google.com/p/swarming/services-go/pkg/aedmz"
	"code.google.com/p/swarming/services-go/third_party/code.google.com/p/leveldb-go/leveldb/memdb"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
)

func runServer() int {
	log.SetFlags(log.Ldate | log.Lmicroseconds)

	// Notes:
	// - On AppEngine, the instance's name and version is used instead.
	// - To log to both a file and os.Stderr, use io.TeeWriter.
	// TODO(maruel): the application name should be retrieved from app.yaml and
	// used as the default.
	app := aedmz.NewApp("isolateserver-dev", "v0.1", os.Stderr, nil, memdb.New(nil))

	addr := ":8080"
	l, err := net.Listen("tcp", addr)
	if err != nil {
		fmt.Printf("Failed to listed on %s: %s", addr, err)
		return 1
	}
	mux := http.NewServeMux()
	server.SetupHandlers(mux, app)
	srv := &http.Server{Addr: addr, Handler: mux}
	srv.Serve(l)
	return 0
}

func main() {
	os.Exit(runServer())
}
