// Copyright 2013 The Swarming Authors. All rights reserved.
// Use of this source code is governed by the Apache v2.0 license that can be
// found in the LICENSE file.

// +build appengine

package aedmz

import (
	"appengine"
	"appengine/aetest"
	"io"
	"net/http"
)

type appContextImplMock struct{}

func (a appContextImplMock) NewContext(r *http.Request) appengine.Context {
	// https://developers.google.com/appengine/docs/go/tools/localunittesting
	//
	// The call to aetest.NewContext will start dev_appserver.py in a subprocess,
	// which will be used to service API calls during the test. This subprocess
	// will be shutdown with the call to Close.
	//
	// This means it's costly.
	//
	// aetest.NewContext() won't use by default the 'application' specified in
	// app.yaml. newAppMock() prefills appContext.appID so appengine.AppID() is
	// never called in RequestContext.AppID(). If this becomes an issue, pass
	// aetest.Options{AppID:"Yo"} instead of nil.
	c, err := aetest.NewContext(nil)
	if err != nil {
		panic(err)
	}
	return c
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
	return NewAppInternal("Yo", "v1", a).(*appContext)
}

func CloseRequest(r RequestContext) {
	c, _ := r.(RequestContextAppengine).AppengineContext().(io.Closer)
	c.Close()
}
