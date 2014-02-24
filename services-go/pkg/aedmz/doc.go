// Copyright 2013 The Swarming Authors. All rights reserved.
// Use of this source code is governed by the Apache v2.0 license that can be
// found in the LICENSE file.

// Package aedmz is an AppEngine abstraction layer to run a service
// either locally or on AppEngine.
//
// This package contains code and interfaces to make it possible for a server
// to run on either AppEngine or as a local process. This permits not being
// locked in on AppEngine for a service coded using exclusively this package
// instead of using the "appengine" package directly.
package aedmz
