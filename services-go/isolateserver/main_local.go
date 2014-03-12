// Copyright 2013 The Swarming Authors. All rights reserved.
// Use of this source code is governed by the Apache v2.0 license that can be
// found in the LICENSE file.

// +build !appengine

package main

import (
	"code.google.com/p/swarming/services-go/isolateserver/server"
	"code.google.com/p/swarming/services-go/pkg/aedmz"
	"code.google.com/p/swarming/services-go/third_party/code.google.com/p/leveldb-go/leveldb/memdb"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
)

var settingsFile = "settings.json"

type Settings struct {
	// Port to listen to.
	Http  string // :8080
	Https string // :10443

	// SSL Certificate pair.
	PublicKey  string // cert.pem
	PrivateKey string // key.pem
}

func readJsonFile(filePath string, object interface{}) error {
	f, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("Failed to open %s: %s", filePath, err)
	}
	defer f.Close()
	if err = json.NewDecoder(f).Decode(object); err != nil {
		return fmt.Errorf("Failed to decode %s: %s", filePath, err)
	}
	return nil
}

// writeJsonFile writes object as json encoded into filePath with 2 spaces indentation.
func writeJsonFile(filePath string, object interface{}) error {
	d, err := json.MarshalIndent(object, "", "  ")
	if err != nil {
		return fmt.Errorf("Failed to encode %s: %s", filePath, err)
	}

	f, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return fmt.Errorf("Failed to open %s: %s", filePath, err)
	}
	defer f.Close()
	if _, err := f.Write(d); err != nil {
		return fmt.Errorf("Failed to write %s: %s", filePath, err)
	}
	return nil
}

func startHTTP(addr string, mux http.Handler, wg *sync.WaitGroup) (net.Listener, error) {
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("Failed to listed on %s: %s", addr, err)
	}
	srv := &http.Server{Addr: addr, Handler: mux}
	wg.Add(1)
	go func() {
		defer wg.Done()
		srv.Serve(l)
	}()
	return l, nil
}

func startHTTPS(addr string, mux http.Handler, wg *sync.WaitGroup, cert, priv string) (net.Listener, error) {
	if cert == "" || priv == "" {
		return nil, fmt.Errorf("Both public and private keys must be specified. If you don't want https support, change the port.")
	}
	c, err := tls.LoadX509KeyPair(cert, priv)
	if err != nil {
		return nil, fmt.Errorf("Failed to load certificates %s/%s: %s", cert, priv, err)
	}
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("Failed to listed on %s: %s", addr, err)
	}
	srv := &http.Server{Addr: addr, Handler: mux}
	config := &tls.Config{NextProtos: []string{"http/1.1"}, Certificates: []tls.Certificate{c}}
	l2 := tls.NewListener(l, config)
	wg.Add(1)
	go func() {
		defer wg.Done()
		srv.Serve(l2)
	}()
	return l, nil
}

// runServer opens the configuration files, read them, starts the server. Then
// it waits for a Ctrl-C and quit. All the opened files are from the current
// working directory.
func runServer() int {
	log.SetFlags(log.Ldate | log.Lmicroseconds)

	settings := &Settings{
		Http: ":8080",
	}
	if err := readJsonFile(settingsFile, settings); err != nil {
		err = writeJsonFile(settingsFile, settings)
		if err != nil {
			log.Printf("Failed to initialize settings. %s", err)
			return 1
		}
		fmt.Printf("A configuration file was generated for you with the default settings: %s\n", settingsFile)
		fmt.Printf("Please update it as desired and rerun this command.\n")
		return 2
	}

	if settings.Http == "" && settings.Https == "" {
		log.Printf("At least one of http or https must be set.")
		return 1
	}

	// Handle Ctrl-C.
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt)

	// Notes:
	// - On AppEngine, the instance's name and version is used instead.
	// - To log to both a file and os.Stderr, use io.TeeWriter.
	// TODO(maruel): the application name should be retrieved from app.yaml and
	// used as the default.
	app := aedmz.NewApp("isolateserver-dev", "v0.1", os.Stderr, nil, memdb.New(nil))

	// TODO(maruel): Load index.yaml to configure the secondary indexes on the db.
	// TODO(maruel): Load app.yaml to add routes to support static/
	var err error
	var wg sync.WaitGroup
	sockets := make([]net.Listener, 0, 2)
	if settings.Http != "" {
		mux := http.NewServeMux()
		server.SetupHandlers(mux, app)
		var listener net.Listener
		listener, err = startHTTP(settings.Http, mux, &wg)
		if err != nil {
			log.Printf("%s", err)
			quit <- os.Interrupt
		} else {
			sockets = append(sockets, listener)
		}
		log.Printf("Listening HTTP on %s", settings.Http)
	}

	if err == nil && settings.Https != "" {
		mux := http.NewServeMux()
		server.SetupHandlers(mux, app)
		listener, err := startHTTPS(settings.Https, mux, &wg, settings.PublicKey, settings.PrivateKey)
		if err != nil {
			log.Printf("%s", err)
			quit <- os.Interrupt
		} else {
			sockets = append(sockets, listener)
		}
		log.Printf("Listening HTTPS on %s", settings.Https)
	}

	<-quit

	for _, listener := range sockets {
		listener.Close()
	}

	// TODO(maruel): Only wait 1 minute.
	log.Printf("Waiting for on-going requests...")
	app.WaitForOngoingRequests()
	return 0
}

func main() {
	os.Exit(runServer())
}
