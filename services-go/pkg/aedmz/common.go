// Copyright 2013 The Swarming Authors. All rights reserved.
// Use of this source code is governed by the Apache v2.0 license that can be
// found in the LICENSE file.

// AppEngine aedmz layer.
//
// This file contains code and interfaces that is common between a local server
// and an AppEngine server.

package aedmz

import (
	gorillaContext "code.google.com/p/swarming/services-go/third_party/github.com/gorilla/context"
	"errors"
	"net/http"
)

// ErrNotFound is returned when a object requested in DB or Cache is not found.
var ErrNotFound = errors.New("Requested object not found")

// An AppContext is the interface to generate new RequestContext upon each new
// in-bound HTTP connections.
//
// Not much can be done by the app itself, all actions are done on behalf of an
// inbound request. In-bound requests can be generated automatically by a cron
// job or a task queue. See Tasker for a technique to trigger in-bound task
// queue requests.
type AppContext interface {
	// NewContext returns a new RequestContext for the current http.Request
	// running on this AppContext.
	//
	// This RequestContext holds context to be able to access the DB, logging and
	// user and do out-going HTTP requests on behalf of the application.
	NewContext(r *http.Request) RequestContext

	// InjectContext adds a gorilla context to the http.Request.
	//
	// This must be called at the initial router level.
	InjectContext(handler http.HandlerFunc) http.HandlerFunc
}

// AppIdentity exposes the application's identity.
type AppIdentity interface {
	AppID() string
	AppVersion() string
}

// Connectivity exposes both unauthenticated and authenticated out-bound HTTP
// connections.
type Connectivity interface {
	// HttpClient returns an *http.Client for outgoing connections that are
	// bound to this incoming request. Note that the RoundTripper may enforce a
	// limit on the data size.
	HttpClient() (*http.Client, error)
	// OAuth2HttpClient returns an *http.Client that can be used to send RPCs to a
	// remote service like Google CloudStorage with the Application's identity.
	OAuth2HttpClient(scope string) (*http.Client, error)
}

// Context for a single HTTP request.
type RequestContext interface {
	AppIdentity
	Connectivity
}

// GetContext returns the framework Context associated with the request.
func GetContext(r *http.Request) RequestContext {
	return gorillaContext.Get(r, contextKey).(RequestContext)
}

// Internal stuff.

const (
	contextKey contextKeyType = 0
)

type contextKeyType int
