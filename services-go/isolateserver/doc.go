// Copyright 2013 The Swarming Authors. All rights reserved.
// Use of this source code is governed by the Apache v2.0 license that can be
// found in the LICENSE file.

// This project is both a standalone executable that runs the standalone
// Isolate Server and the AppEngine server code that needs to be deployed with
// goapp update.
//
// To run the standalone server, use either:
//
//   go build
//   go install
//
// and then execute:
//
//  isolateserver
//
// It will act as a local frontend to a CloudStorage bucket. A proper OAuth2
// client must be set up on first use. It is recommended to use a certificate.
//
// To use the AppEnginer server code, simply deploy it to an instance with:
//
//   tools/deploy
//
package main
