// Copyright 2013 The Swarming Authors. All rights reserved.
// Use of this source code is governed by the Apache v2.0 license that can be
// found in the LICENSE file.

// Package ofh is OAuth2 For Humans.
//
// It supports both 'installed app' and 'service account' flows. The user can
// use each of these seamlessly.
package ofh

import (
	"errors"
	"net/http"
)

// OAuth2ClientProvider is a reference to an OAuth2 enabled *http.Client
// provider.
//
// All of OAuth2Settings, InstalledApp and ServiceAccount implement this
// interface.
type OAuth2ClientProvider interface {
	// GetClient returns an *http.Client enabled for the corresponding scope on
	// the specified http.RoundTripper. If r is nil, a default transport will be
	// used.
	GetClient(scope string, r http.RoundTripper) (*http.Client, error)
}

// StubProvider implements OAuth2ClientProvider but doesn't do anything, it is
// only meant for testing.
type StubProvider struct {
	client *http.Client
	Scopes []string
}

// MakeStubProvider returns an initialized StubProvider.
func MakeStubProvider(client *http.Client) *StubProvider {
	return &StubProvider{client, []string{}}
}

func (s *StubProvider) GetClient(scope string, r http.RoundTripper) (*http.Client, error) {
	s.Scopes = append(s.Scopes, scope)
	if s.client == nil {
		return nil, errors.New("No client")
	}
	return s.client, nil
}
