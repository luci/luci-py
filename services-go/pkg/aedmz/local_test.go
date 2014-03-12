// Copyright 2013 The Swarming Authors. All rights reserved.
// Use of this source code is governed by the Apache v2.0 license that can be
// found in the LICENSE file.

// +build !appengine

package aedmz

import (
	"bytes"
	"code.google.com/p/swarming/services-go/pkg/ofh"
	ut "code.google.com/p/swarming/services-go/pkg/utiltest"
	"code.google.com/p/swarming/services-go/third_party/code.google.com/p/leveldb-go/leveldb/memdb"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

type appContextImplMock struct {
	now time.Time
}

func (a appContextImplMock) Now() time.Time {
	return a.now
}

// newAppMock returns an *appContext to be used in unit tests.
//
// It has AppID "Yo" and version "v1".
//
// It is impossible to import aedmztest here because it would cause an import
// cycle.
func newAppMock(a AppContextImpl) *appContext {
	if a == nil {
		a = &appContextImplMock{}
	}
	return NewAppInternal("Yo", "v1", &bytes.Buffer{}, ofh.MakeStubProvider(http.DefaultClient), memdb.New(nil), a).(*appContext)
}

func CloseRequest(r RequestContext) {
	// Closing is not necessary when running standalone.
}

func TestLog(t *testing.T) {
	impl := &appContextImplMock{}
	app := newAppMock(impl)
	req, err := http.NewRequest("GET", "/", &bytes.Buffer{})
	req.RemoteAddr = "AremoteHost:123"
	if err != nil {
		t.Fatal(err)
	}
	resp := httptest.NewRecorder()
	handler := func(w http.ResponseWriter, r *http.Request) {
		c := GetContext(r)
		c.Debugf("d")
		c.Infof("i")
		c.Warningf("w")
		c.Errorf("e")
		w.WriteHeader(199)
		w.Write([]byte("Yay"))
	}
	impl.now = time.Date(2013, 12, 27, 20, 23, 50, 0, time.UTC)
	app.InjectContext(handler)(resp, req)
	ut.AssertEqual(t, "2013/12/27 20:23:50.000 199 GET       3b     AremoteHost /\n D:d\n I:i\n W:w\n E:e\n", app.out.(*bytes.Buffer).String())
	ut.AssertEqual(t, "Yay", resp.Body.String())
}

func TestLogPanic(t *testing.T) {
	impl := &appContextImplMock{}
	app := newAppMock(impl)
	req, err := http.NewRequest("GET", "/", &bytes.Buffer{})
	req.RemoteAddr = "AremoteHost:123"
	if err != nil {
		t.Fatal(err)
	}
	resp := httptest.NewRecorder()
	handler := func(w http.ResponseWriter, r *http.Request) {
		c := GetContext(r)
		c.Debugf("d")
		w.WriteHeader(199)
		w.Write([]byte("Yay"))
		panic("Fake")
	}
	impl.now = time.Date(2013, 12, 27, 20, 23, 50, 0, time.UTC)
	app.InjectContext(handler)(resp, req)
	out := app.out.(*bytes.Buffer).String()
	// The rest of the string is dependent on runtime environment.
	prefix := "2013/12/27 20:23:50.000 503 GET      11b     AremoteHost /\n D:d\n E:aedmz: panic serving AremoteHost:123: Fake\n   goroutine "
	ut.AssertEqual(t, prefix, out[:len(prefix)])
	// TODO(maruel): Remove the output in progress. Sadly this requires buffering
	// the complete Handler output in memory, which may not always be possible.
	// The fact 503 was properly returned may depend on either WriteHeader() was
	// called or not, as httptest.NewRecorder() seems to have slightly different
	// behavior.
	ut.AssertEqual(t, "YayPanicked", resp.Body.String())
}

func TestAsLines(t *testing.T) {
	data := []struct {
		input    string
		expected []string
	}{
		{"foo", []string{"foo"}},
		{"foo\nbar", []string{"foo", "bar"}},
		{"\n\nfoo\nbar", []string{"foo", "bar"}},
		{"\n  \nfoo\nbar", []string{"foo", "bar"}},
		{"foo\nbar\n\n", []string{"foo", "bar"}},
		{"foo\nbar\n  \n", []string{"foo", "bar"}},
		{"foo\n\nbar\n", []string{"foo", "", "bar"}},
		{"foo\n  \nbar\n", []string{"foo", "  ", "bar"}},
	}
	for i, line := range data {
		ut.AssertEqualIndex(t, i, line.expected, asLines(line.input))
	}
}

func TestTimeToStr(t *testing.T) {
	ut.AssertEqual(t, 23, len(timeToStr(time.Now())))
}
