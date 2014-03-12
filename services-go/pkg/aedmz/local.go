// Copyright 2013 The Swarming Authors. All rights reserved.
// Use of this source code is governed by the Apache v2.0 license that can be
// found in the LICENSE file.

// +build !appengine

// AppEngine abstraction layer when running locally.

package aedmz

import (
	"bytes"
	"code.google.com/p/swarming/services-go/pkg/ofh"
	leveldbdb "code.google.com/p/swarming/services-go/third_party/code.google.com/p/leveldb-go/leveldb/db"
	gorillaContext "code.google.com/p/swarming/services-go/third_party/github.com/gorilla/context"
	"fmt"
	"io"
	"net/http"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode"
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
	logService      logServiceContext
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
		logService: logServiceContext{records: make([]*Record, 0)},
	}
}

func (a *appContext) newContext(r *http.Request) *requestContext {
	return &requestContext{
		app:    a,
		record: &Record{AppID: a.appID, VersionID: a.appVersion, RequestID: a.logService.NewID()},
	}
}

func (a *appContext) NewContext(r *http.Request) RequestContext {
	return a.newContext(r)
}

func (a *appContext) InjectContext(handler http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		timestamp := a.impl.Now()

		a.ongoingRequests.Add(1)
		defer a.ongoingRequests.Done()

		// If any of these statements fail, it means the process is extremely low
		// in memory. In that case the defer right after would fail too, so it is
		// not worth handling the case where c == nil.
		// Save a copy, it could be modified in-place.
		c := a.newContext(r)
		c.record.Resource = r.URL.String()
		c.record.Method = r.Method
		c.record.IP = strings.Split(r.RemoteAddr, ":")[0]
		c.record.StartTime = timestamp

		w2 := &responseWriter{ResponseWriter: w, Status: 200}
		defer func() {
			// Make sure Done() is called even if this function panics itself.
			if err := recover(); err != nil {
				buf := make([]byte, 4096)
				buf = buf[:runtime.Stack(buf, false)]
				c.Errorf("aedmz: panic serving %v: %v\n%s", r.RemoteAddr, err, buf)
				// Return HTTP 503.
				// BUG(maruel): Sadly returning HTTP 503 on panic won't work if
				// .Write() was already called unless the full reply is buffered in
				// memory. An option?
				w2.WriteHeader(503)
				w2.Write([]byte("Panicked"))
			}
			c.record.Status = int32(w2.Status)
			c.record.ResponseSize = w2.Len
			c.record.EndTime = time.Now().UTC()
			io.WriteString(c.app.out, c.record.String())

			c.app.logService.Append(c.record)
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
	app    *appContext
	lock   sync.Mutex
	record *Record
	r      http.RoundTripper
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

// Logger: The API is designed against Go GAE's one.

// Returns the text as lines with empty lines at the beginning or the end
// stripped.
func asLines(text string) []string {
	lines := strings.Split(text, "\n")
	start := 0
	end := 0
	for i, line := range lines {
		isEmpty := len(strings.TrimSpace(line)) == 0
		if start == i && isEmpty {
			start++
		}
		if !isEmpty {
			end = i
		}
	}
	return lines[start : end+1]
}

func levelToStr(level int) string {
	switch level {
	case LOG_LEVEL_DEBUG:
		return "D"
	case LOG_LEVEL_INFO:
		return "I"
	case LOG_LEVEL_WARNING:
		return "W"
	case LOG_LEVEL_ERROR:
		return "E"
	case LOG_LEVEL_CRITICAL:
		return "C"
	default:
		return "x"
	}
}

func (r *requestContext) Debugf(format string, args ...interface{}) {
	r.record.logf(LOG_LEVEL_DEBUG, format, args...)
}

func (r *requestContext) Infof(format string, args ...interface{}) {
	r.record.logf(LOG_LEVEL_INFO, format, args...)
}

func (r *requestContext) Warningf(format string, args ...interface{}) {
	r.record.logf(LOG_LEVEL_WARNING, format, args...)
}

func (r *requestContext) Errorf(format string, args ...interface{}) {
	r.record.logf(LOG_LEVEL_ERROR, format, args...)
}

// LogService.

type logServiceContext struct {
	lock sync.Mutex
	// BUG(maruel): logServiceContext is currently a memory leak. It will have to
	// save its logs to disk instead of keeping it in memory. The
	// yet-to-be-saved-to-disk part could be implemented with leveldb.memDB.
	records []*Record
	lastID  int64
}

func (l *logServiceContext) NewID() string {
	v := atomic.AddInt64(&l.lastID, 1)
	return strconv.FormatInt(v, 10)
}

func (l *logServiceContext) Append(r *Record) {
	l.lock.Lock()
	defer l.lock.Unlock()
	// TODO(maruel): Flush l.records to disk. Stop using a single array.
	l.records = append(l.records, r)
}

func (l *logServiceContext) getSafeRecords() []*Record {
	l.lock.Lock()
	defer l.lock.Unlock()
	records := make([]*Record, len(l.records))
	// TODO(maruel): This doesn't make sense long term.
	copy(records, l.records)
	return records
}

func (l *logServiceContext) ScanLogs(start, end time.Time, minLevel int, versions []string, entries chan<- *Record) {
	for _, record := range l.getSafeRecords() {
		if record.StartTime.Before(start) || record.StartTime.After(end) {
			continue
		}
		if minLevel >= 0 {
			ok := false
			for _, l := range record.AppLogs {
				if l.Level >= minLevel {
					ok = true
					break
				}
			}
			if !ok {
				continue
			}
		}
		if len(versions) > 0 {
			ok := false
			for _, v := range versions {
				if record.VersionID == v {
					ok = true
					break
				}
			}
			if !ok {
				continue
			}
		}
		entries <- record
	}
}

func (l *logServiceContext) GetLogEntry(requestIDs []string, entries chan<- *Record) {
	ids := map[string]bool{}
	for _, id := range requestIDs {
		ids[id] = true
	}
	for _, record := range l.getSafeRecords() {
		for id := range ids {
			if record.RequestID == id {
				entries <- record
				delete(ids, id)
				if len(ids) == 0 {
					return
				}
				break
			}
		}
	}
}

// Record is an entry for a single HTTP request.
//
// It is compatible with appegnine.log/Record.
type Record struct {
	AppID        string
	VersionID    string
	RequestID    string
	IP           string
	StartTime    time.Time
	EndTime      time.Time
	Method       string
	Resource     string
	Status       int32
	ResponseSize int64
	Referrer     string
	UserAgent    string
	Finished     bool
	lock         sync.Mutex
	AppLogs      []AppLog
}

// timeToStr returns a string reprenseting a time.Time with a deterministic
// length of 23. time.Time.Format is annoying because it strips trailing zeros.
func timeToStr(t time.Time) string {
	timestamp := t.Format("2006/01/02 15:04:05.999")
	if len(timestamp) < 20 {
		timestamp += "."
	}
	if len(timestamp) < 23 {
		timestamp += strings.Repeat("0", 23-len(timestamp))
	}
	return timestamp
}

func (r *Record) String() string {
	b := bytes.NewBuffer(make([]byte, 0, 256))
	fmt.Fprintf(b, "%23s %3d %-4s %6db %15s %s\n", timeToStr(r.StartTime), r.Status, r.Method, r.ResponseSize, r.IP, r.Resource)
	r.getLogs(b)
	return b.String()
}

func (r *Record) logf(level int, format string, args ...interface{}) {
	l := AppLog{time.Now().UTC(), level, fmt.Sprintf(format, args...)}
	r.lock.Lock()
	defer r.lock.Unlock()
	r.AppLogs = append(r.AppLogs, l)
}

// Returns indented logs.
func (r *Record) getLogs(b io.Writer) {
	r.lock.Lock()
	defer r.lock.Unlock()
	for _, e := range r.AppLogs {
		if len(e.Message) != 0 {
			lines := asLines(e.Message)
			for i, l := range lines {
				l = strings.TrimRightFunc(l, unicode.IsSpace)
				if i == 0 {
					fmt.Fprintf(b, " %s:%s\n", levelToStr(e.Level), l)
				} else {
					fmt.Fprintf(b, "   %s\n", l)
				}
			}
		}
	}
}

// AppLog is an entry in the Logger service.
//
// It is compatible with appengine/log.AppLog.
type AppLog struct {
	Time    time.Time
	Level   int
	Message string
}

func (r *requestContext) ScanLogs(start, end time.Time, minLevel int, versions []string, entries chan<- *Record) {
	r.app.logService.ScanLogs(start, end, minLevel, versions, entries)
}

func (r *requestContext) GetLogEntry(requestIDs []string, entries chan<- *Record) {
	r.app.logService.GetLogEntry(requestIDs, entries)
}
