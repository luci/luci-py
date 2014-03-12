// Copyright 2013 The Swarming Authors. All rights reserved.
// Use of this source code is governed by the Apache v2.0 license that can be
// found in the LICENSE file.

package server

import (
	"bytes"
	"code.google.com/p/swarming/services-go/pkg/aedmztest"
	ut "code.google.com/p/swarming/services-go/pkg/utiltest"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"
)

func newServer() *httptest.Server {
	m := http.NewServeMux()
	SetupHandlers(m, aedmztest.NewAppMock(nil))
	return httptest.NewServer(m)
}

// TODO(maruel): testing.TB is 1.2+
func get(t *testing.T, ts *httptest.Server, resource string, status int) string {
	r, err := http.Get(ts.URL + resource)
	return commonRequest(t, r, err, status)
}

func post(t *testing.T, ts *httptest.Server, resource, contentType string, body io.Reader, status int) string {
	r, err := http.Post(ts.URL+resource, contentType, body)
	return commonRequest(t, r, err, status)
}

func commonRequest(t *testing.T, r *http.Response, err error, status int) string {
	if err != nil {
		t.Fatal(err)
	}
	ut.AssertEqual(t, status, r.StatusCode)
	body, err := ioutil.ReadAll(r.Body)
	r.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	return string(body)
}

func TestWarmup(t *testing.T) {
	ts := newServer()
	defer ts.Close()

	body := get(t, ts, "/_ah/warmup", http.StatusOK)
	ut.AssertEqual(t, "Warmed up", body)
}

// It must fail.
func TestWarmupPOST(t *testing.T) {
	ts := newServer()
	defer ts.Close()

	// TODO(maruel): Should be http.StatusMethodNotAllowed.
	body := post(t, ts, "/_ah/warmup", "application/stream", &bytes.Buffer{}, http.StatusNotFound)
	ut.AssertEqual(t, "404 page not found\n", body)
}

func TestRoot(t *testing.T) {
	ts := newServer()
	defer ts.Close()

	body := get(t, ts, "/", http.StatusOK)
	ut.AssertEqual(t, "Root", body)
}
