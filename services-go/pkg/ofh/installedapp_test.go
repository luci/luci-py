// Copyright 2013 The Swarming Authors. All rights reserved.
// Use of this source code is governed by the Apache v2.0 license that can be
// found in the LICENSE file.

package ofh

import (
	"bytes"
	ut "code.google.com/p/swarming/services-go/pkg/utiltest"
	"io"
	"io/ioutil"
	"net/http"
	"testing"
)

type roundTripperStub struct {
	requests []*http.Request
	replies  []*http.Response
}

func (r *roundTripperStub) RoundTrip(req *http.Request) (resp *http.Response, err error) {
	r.requests = append(r.requests, req)
	resp = r.replies[0]
	r.replies = r.replies[1:]
	return
}

func asReader(v string) io.ReadCloser {
	return ioutil.NopCloser(bytes.NewBufferString(v))
}

func TestInstalledApp(t *testing.T) {
	i := &InstalledApp{
		ClientID:         "C",
		ClientSecret:     "S",
		AuthURL:          "http://localhost/auth",
		TokenURL:         "http://localhost/token",
		ScopedTokenCache: make(map[string]*TokenCache),
	}
	tokReply := `{"access_token":"a", "refresh_token": "r", "id_token": "i"}`
	resp := []*http.Response{
		&http.Response{StatusCode: 200, Body: asReader(tokReply)},
	}
	r := &roundTripperStub{[]*http.Request{}, resp}
	prompt := func(string) string {
		return "auth"
	}
	_, err := i.GetClientPrompt("scope", r, prompt)
	ut.AssertEqual(t, nil, err)
	ut.AssertEqual(t, 1, len(r.requests))
	i.Lock()
	defer i.Unlock()
	ut.AssertEqual(t, true, i.ShouldSave())
	i.ClearDirtyBit()
	ut.AssertEqual(t, false, i.ShouldSave())
}

func TestMakeInstalledApp(t *testing.T) {
	actual := MakeInstalledApp()
	expected := &InstalledApp{
		AuthURL:          "https://accounts.google.com/o/oauth2/auth",
		TokenURL:         "https://accounts.google.com/o/oauth2/token",
		ScopedTokenCache: make(map[string]*TokenCache),
	}
	ut.AssertEqual(t, expected, actual)
}
