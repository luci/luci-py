// Copyright 2013 The Swarming Authors. All rights reserved.
// Use of this source code is governed by the Apache v2.0 license that can be
// found in the LICENSE file.

// +build !appengine

// AppEngine abstraction layer when running locally.

package aedmz

import (
	"code.google.com/p/swarming/services-go/pkg/ofh"
	leveldbdb "code.google.com/p/swarming/services-go/third_party/code.google.com/p/leveldb-go/leveldb/db"
	gorillaContext "code.google.com/p/swarming/services-go/third_party/github.com/gorilla/context"
	"io"
	"net/http"
	"runtime"
	"sync"
	"time"
)

// Interface that describes the necessary mocks to load a unit test AppContext.
//
// This exposes internal details. It is meant for use for testing only by
// aedmztest.NewAppMock()
type AppContextImpl interface {
	Now() time.Time
}

// Real implementation.
type appContextImpl struct{}

func (a appContextImpl) Now() time.Time { return time.Now().UTC() }

// AppContextLocal adds functions that are only relevant when running locally,
// not on AppEngine.
type AppContextLocal interface {
	AppContext
	// WaitForOngoingRequests waits for all ongoing requests to terminate. It is
	// meant to be used in standalone server only.
	WaitForOngoingRequests()
}

// appContext is a singleton that holds all the details (context) of the
// currently running application.
//
// A mock instance can be created with aedmztest.NewAppMock().
type appContext struct {
	appID           string
	appVersion      string
	out             io.Writer
	ongoingRequests sync.WaitGroup // To enable orderly shutdown.
	oauth2          ofh.OAuth2ClientProvider
	impl            AppContextImpl
}

// NewApp returns a new AppContextLocal.
//
// This function is specific to stand alone server. AppEngine servers get their
// identity from AppEngine itself.
//
// out is meant to os.Stderr or a log file. settings defines the application's
// oauth2 credentials.
func NewApp(appID, appVersion string, out io.Writer, p ofh.OAuth2ClientProvider, db leveldbdb.DB) AppContextLocal {
	return NewAppInternal(appID, appVersion, out, p, db, appContextImpl{})
}

// NewAppInternal returns a new Application context.
//
// This exposes internal details. It is meant for use for testing only by
// aedmztest.NewAppMock()
func NewAppInternal(appID, appVersion string, out io.Writer, p ofh.OAuth2ClientProvider, db leveldbdb.DB, impl AppContextImpl) AppContextLocal {
	return &appContext{
		appID:      appID,
		appVersion: appVersion,
		out:        out,
		oauth2:     p,
		impl:       impl,
	}
}

func (a *appContext) newContext(r *http.Request) *requestContext {
	return &requestContext{
		app: a,
	}
}

func (a *appContext) NewContext(r *http.Request) RequestContext {
	return a.newContext(r)
}

func (a *appContext) InjectContext(handler http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		a.ongoingRequests.Add(1)
		defer a.ongoingRequests.Done()

		// If any of these statements fail, it means the process is extremely low
		// in memory. In that case the defer right after would fail too, so it is
		// not worth handling the case where c == nil.
		// Save a copy, it could be modified in-place.
		c := a.newContext(r)

		w2 := &responseWriter{ResponseWriter: w, Status: 200}
		defer func() {
			// Make sure Done() is called even if this function panics itself.
			if err := recover(); err != nil {
				buf := make([]byte, 4096)
				buf = buf[:runtime.Stack(buf, false)]
				// Return HTTP 503.
				// BUG(maruel): Sadly returning HTTP 503 on panic won't work if
				// .Write() was already called unless the full reply is buffered in
				// memory. An option?
				w2.WriteHeader(503)
				w2.Write([]byte("Panicked"))
			}
		}()

		gorillaContext.Set(r, contextKey, RequestContext(c))
		handler(w2, r)
	}
}

func (a *appContext) WaitForOngoingRequests() {
	a.ongoingRequests.Wait()
}

// requestContext holds the local references for a single HTTP request.
type requestContext struct {
	app  *appContext
	lock sync.Mutex
	r    http.RoundTripper
}

// responseWriter implements http.ResponseWriter and caches the response length
// and the Status code for logging.
type responseWriter struct {
	http.ResponseWriter
	Len    int64
	Status int
}

func (w *responseWriter) Write(b []byte) (int, error) {
	n, err := w.ResponseWriter.Write(b)
	w.Len += int64(n)
	return n, err
}

func (w *responseWriter) WriteHeader(s int) {
	w.Status = s
	w.ResponseWriter.WriteHeader(s)
}

func (r *requestContext) AppID() string {
	return r.app.appID
}

func (r *requestContext) AppVersion() string {
	return r.app.appVersion
}

// Connectivity

func (r *requestContext) getTransport() http.RoundTripper {
	r.lock.Lock()
	defer r.lock.Unlock()
	if r.r == nil {
		r.r = http.DefaultTransport
	}
	return r.r
}

func (r *requestContext) HttpClient() (*http.Client, error) {
	return &http.Client{Transport: r.getTransport()}, nil
}

func (r *requestContext) OAuth2HttpClient(scope string) (*http.Client, error) {
	return r.app.oauth2.GetClient(scope, r.getTransport())
}
