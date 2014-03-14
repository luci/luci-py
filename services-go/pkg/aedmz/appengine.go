// Copyright 2013 The Swarming Authors. All rights reserved.
// Use of this source code is governed by the Apache v2.0 license that can be
// found in the LICENSE file.

// +build appengine

package aedmz

// AppEngine abstraction layer.

import (
	"appengine"
	"appengine/log"
	"appengine/urlfetch"
	"code.google.com/p/swarming/services-go/third_party/code.google.com/p/goauth2/oauth"
	gorillaContext "code.google.com/p/swarming/services-go/third_party/github.com/gorilla/context"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"
)

// Interface that describes the necessary mocks to load a unit test AppContext.
//
// This exposes internal details. It is meant for use for testing only by
// aedmztest.NewAppMock()
type AppContextImpl interface {
	NewContext(r *http.Request) appengine.Context
}

// Real implementation.
type appContextImpl struct{}

func (i appContextImpl) NewContext(r *http.Request) appengine.Context {
	return appengine.NewContext(r)
}

// appContext is a singleton that holds all the details of the currently
// running application.
//
// A mock instance can be created with aedmztest.NewAppMock().
type appContext struct {
	lock       sync.Mutex
	appID      string
	appVersion string
	impl       AppContextImpl
}

// NewApp creates a new Application context.
func NewApp(appID, appVersion string) AppContext {
	return NewAppInternal(appID, appVersion, appContextImpl{})
}

// NewAppInternal returns a new Application context.
//
// This exposes internal details. It is meant for use for testing only by
// aedmztest.NewAppMock()
func NewAppInternal(appID, appVersion string, impl AppContextImpl) AppContext {
	return &appContext{
		appID:      appID,
		appVersion: appVersion,
		impl:       impl,
	}
}

func (a *appContext) NewContext(r *http.Request) RequestContext {
	return &requestContext{
		Context: a.impl.NewContext(r),
		app:     a,
	}
}

func (a *appContext) InjectContext(handler http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		c := a.NewContext(r)
		defer func() {
			// This is necessary for unit tests.
			if closer, ok := c.(io.Closer); ok {
				closer.Close()
			}
		}()
		gorillaContext.Set(r, contextKey, c)
		handler(w, r)
	}
}

// requestContext holds the local references for a single HTTP request.
type requestContext struct {
	appengine.Context // Implements the Logger interface so it needs to be embedded as is.
	app               *appContext
}

// RequestContextAppengine adds functions that are only relevant when running
// on AppEngine.
type RequestContextAppengine interface {
	RequestContext
	AppengineContext() appengine.Context
}

func (r *requestContext) AppengineContext() appengine.Context {
	return r.Context
}

func (r *requestContext) AppID() string {
	// The value is automatically cached to reduce the number of RPC.
	app := r.app
	app.lock.Lock()
	appID := app.appID
	app.lock.Unlock()

	if appID == "" {
		// Lazy load this value because this function call does an RPC under the
		// hood.
		appID = appengine.AppID(r.Context)
		app.lock.Lock()
		app.appID = appID
		app.lock.Unlock()
	}
	return appID
}

func (r *requestContext) AppVersion() string {
	// The value is automatically cached to reduce the number of RPC.
	app := r.app
	app.lock.Lock()
	appVersion := app.appVersion
	app.lock.Unlock()

	if appVersion == "" {
		// Lazy load this value because this function call does an RPC under the
		// hood.
		appVersion = strings.Split(appengine.VersionID(r.Context), ".")[0]
		app.lock.Lock()
		app.appVersion = appVersion
		app.lock.Unlock()
	}
	return appVersion
}

// Connectivity

func (r *requestContext) getTransport() http.RoundTripper {
	return &urlfetch.Transport{Context: r}
}

// Currently valid access token for this instance for this request.
type accessToken struct {
	token      string
	expiration time.Time
}

// AccessToken returns an oauth2 token on behalf of the service account of this
// application.
func (r *requestContext) getAccessToken(scope string) (*oauth.Token, error) {
	// While this function call requires an appengine.Context due to it doing
	// RPCs under the hood, the access token is not connection or user specific.
	a, e, err := appengine.AccessToken(r.Context, scope)
	if err != nil {
		return nil, err
	}
	return &oauth.Token{AccessToken: a, Expiry: e}, nil
}

func (r *requestContext) HttpClient() (*http.Client, error) {
	return &http.Client{Transport: r.getTransport()}, nil
}

func (r *requestContext) OAuth2HttpClient(scope string) (*http.Client, error) {
	token, err := r.getAccessToken(scope)
	if err != nil {
		return nil, err
	}
	transport := &oauth.Transport{
		Token:     token,
		Transport: r.getTransport(),
	}
	return transport.Client(), nil
}

// Logging: Nothing to implement, the interface is implemented by
// appengine.Context.

// LogService.

type AppLog log.AppLog
type Record log.Record

func (r *requestContext) run(q *log.Query, entries chan<- *Record) {
	result := q.Run(r.Context)
	for {
		record, _ := result.Next()
		if record == nil {
			break
		}
		entries <- (*Record)(record)
	}
}

func (r *requestContext) ScanLogs(start, end time.Time, minLevel int, versions []string, entries chan<- *Record) {
	r.run(
		&log.Query{
			StartTime:     start,
			EndTime:       end,
			Incomplete:    true,
			AppLogs:       true,
			ApplyMinLevel: (minLevel >= 0),
			MinLevel:      minLevel,
			Versions:      versions,
		},
		entries)
}

func (r *requestContext) GetLogEntry(requestIDs []string, entries chan<- *Record) {
	r.run(
		&log.Query{
			Incomplete: true,
			AppLogs:    true,
			RequestIDs: requestIDs,
		},
		entries)
}
