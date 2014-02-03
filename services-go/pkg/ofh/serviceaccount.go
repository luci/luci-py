// Copyright 2013 The Swarming Authors. All rights reserved.
// Use of this source code is governed by the Apache v2.0 license that can be
// found in the LICENSE file.

package ofh

import (
	"code.google.com/p/swarming/services-go/third_party/code.google.com/p/goauth2/oauth"
	"code.google.com/p/swarming/services-go/third_party/code.google.com/p/goauth2/oauth/jwt"
	"fmt"
	"net/http"
	"sync"
)

// A ServiceAccount is a proper identity. This is why the private key must be
// kept secure since anyone having the private key can present itself as this
// service account. A service account is not strictly speaking 'a user'.
//
// A service account can impersonate a user but this requires a "prn" claim and
// the author of this document has no idea how to do this.
//
// https://developers.google.com/accounts/docs/OAuth2ServiceAccount describes
// how to use the server's identity.
//
// When obtaining a key from the Google API console it will be downloaded in a
// PKCS12 encoding. To use this key you will need to convert it to a PEM file.
// This can be achieved with openssl.
//
//   $ openssl pkcs12 -in <key.p12> -nocerts -passin pass:notasecret -nodes -out <key.pem>
type ServiceAccount struct {
	ClientID     string // Currently not used.
	EmailAddress string
	PrivateKey   string
}

type transport struct {
	token     *jwt.Token
	prjectID  string
	transport http.RoundTripper

	lock        sync.Mutex
	accessToken *oauth.Token
}

// cloneRequest returns a clone of the provided *http.Request.
// The clone is a shallow copy of the struct and its Header map.
//
// Copy pasted from oauth.go
func cloneRequest(r *http.Request) *http.Request {
	// shallow copy of the struct
	r2 := &http.Request{}
	*r2 = *r
	// deep copy of the Header
	r2.Header = make(http.Header)
	for k, s := range r.Header {
		r2.Header[k] = s
	}
	return r2
}

// Expiration will stall all the new requests.
func (t *transport) renewIfExpired() error {
	t.lock.Lock()
	defer t.lock.Unlock()
	if t.accessToken.Expired() {
		accessToken, err := t.token.Assert(&http.Client{Transport: t.transport})
		if err != nil {
			return fmt.Errorf("Failed to assert oauth2 token while refreshing: %s", err)
		}
		t.accessToken = accessToken
	}
	return nil
}

func (t *transport) RoundTrip(req *http.Request) (*http.Response, error) {
	if err := t.renewIfExpired(); err != nil {
		return nil, err
	}
	req = cloneRequest(req)
	req.Header.Set("Authorization", "OAuth "+t.accessToken.AccessToken)
	req.Header.Set("x-goog-api-version", "2")
	return t.transport.RoundTrip(req)
}

// GetClient returns an OAuth2 authenticated http.Client that uses the identity
// of the server.
//
// A private key is required to assert the identity.
func (s *ServiceAccount) GetClient(scope string, r http.RoundTripper) (*http.Client, error) {
	if r == nil {
		r = http.DefaultTransport
	}
	tok := jwt.NewToken(s.EmailAddress, scope, []byte(s.PrivateKey))

	// Get the access token right away by doing an HTTP request.
	accessToken, err := tok.Assert(&http.Client{Transport: r})
	if err != nil {
		return nil, fmt.Errorf("Failed to assert new oauth2 token: %s", err)
	}
	t := &transport{
		token:       tok,
		accessToken: accessToken,
		transport:   r,
	}
	return &http.Client{Transport: t}, nil
}
