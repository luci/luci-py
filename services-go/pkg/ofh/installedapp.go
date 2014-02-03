// Copyright 2013 The Swarming Authors. All rights reserved.
// Use of this source code is governed by the Apache v2.0 license that can be
// found in the LICENSE file.

package ofh

import (
	"bufio"
	"code.google.com/p/swarming/services-go/third_party/code.google.com/p/goauth2/oauth"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"
)

// PromptFunc is used by installed client applications for authentication
// without a web browser. This is useful for example when using an application
// on an headless machine (e.g. without a graphical interface).
//
// This function will be called during the authentication phase.
type PromptFunc func(authUrl string) string

// Prompts the user for authentication code for InstalledApp workflow.
//
// BUG(maruel): This is cheezy to prompt the user during an HTTP request.
func Prompt(authUrl string) string {
	fmt.Printf("Visit URL %s\n", authUrl)
	for {
		fmt.Printf("\nType the code here: ")
		line, _, err := bufio.NewReader(os.Stdin).ReadLine()
		if err == nil {
			code := strings.TrimSpace(string(line))
			if len(code) > 10 {
				return code
			}
		}
	}
}

// TokenCache specifies the methods that implements an oauth2.Token cache. An
// oauth.Token contains the AccessToken and RefreshToken, so that as soon as a
// connection is authenticated, other connections using the same cache will be
// able to reuse the token as-is.
//
// It is exported so it can be serialized safely.
type TokenCache struct {
	CachedToken *oauth.Token
	dirty       bool
}

func (t *TokenCache) Token() (*oauth.Token, error) {
	if t.CachedToken == nil {
		return nil, errors.New("Not found")
	}
	return t.CachedToken, nil
}

func (t *TokenCache) PutToken(tok *oauth.Token) error {
	t.dirty = true
	t.CachedToken = tok
	return nil
}

// An InstalledApp OAuth2 credential is not a proper identity by itself. It is
// a token to be used to denote that the application or server is secure just
// enough to ask a third party (a user) to lend his identity to the application
// on his behalf. It can be used in multiple ways but the most frequent should
// be:
//
// - A web service application wanting to identify the user or to access a
// third party service on the user's behalf, e.g. accessing his Google Drive
// files. See https://developers.google.com/accounts/docs/OAuth2WebServer for
// details.
//
// - An installed application (e.g. mobile app or CLI app) to do the same thing
// as a web service application but as an app running on the user's machine.
// See https://developers.google.com/accounts/docs/OAuth2InstalledApp for
// details.
//
// This struct also stores a cache of tokens for serialization so creating
// multiple entries will reuse the same token automatically.
type InstalledApp struct {
	ClientID     string
	ClientSecret string
	AuthURL      string
	TokenURL     string

	lock             sync.Mutex
	locked           bool
	ScopedTokenCache map[string]*TokenCache // The cache is important to reduce redundant requests.
}

func (i *InstalledApp) ShouldSave() bool {
	if !i.locked {
		panic("Must be called with Lock held.")
	}
	for _, v := range i.ScopedTokenCache {
		if v.dirty {
			return true
		}
	}
	return false
}

func (i *InstalledApp) ClearDirtyBit() {
	if !i.locked {
		panic("Must be called with Lock held.")
	}
	for _, v := range i.ScopedTokenCache {
		v.dirty = false
	}
}

// Lock is to be used during serialization of the object.
func (i *InstalledApp) Lock() {
	i.lock.Lock()
	i.locked = true
}

// Unlock is to be used during serialization of the object.
func (i *InstalledApp) Unlock() {
	i.locked = false
	i.lock.Unlock()
}

// GetClient returns an OAuth2 enabled *http.Client using the installed app
// workflow.
//
// It asks a third party (a user) to lends his personality for this application
// to use on his behalf. This prompts the user to generate an authentication
// code.
//
// Redirecting to localhost is currently not supported.
func (i *InstalledApp) GetClient(scope string, r http.RoundTripper) (*http.Client, error) {
	return i.GetClientPrompt(scope, r, Prompt)
}

// GetClientPrompt permits specifying a Prompt function over GetClient.
func (i *InstalledApp) GetClientPrompt(scope string, r http.RoundTripper, prompt PromptFunc) (*http.Client, error) {
	if r == nil {
		r = http.DefaultTransport
	}
	tokenCache := i.getToken(scope)
	token, _ := tokenCache.Token()
	config := &oauth.Config{
		ClientId:     i.ClientID,
		ClientSecret: i.ClientSecret,
		Scope:        scope,
		AuthURL:      i.AuthURL,
		TokenURL:     i.TokenURL,
		TokenCache:   tokenCache,
		RedirectURL:  "urn:ietf:wg:oauth:2.0:oob", // BUG(maruel): Add option of http://localhost:<port> to remove the need of copy-pasting.
	}
	transport := &oauth.Transport{
		Config:    config,
		Token:     token,
		Transport: r,
	}
	if token == nil {
		code := prompt(config.AuthCodeURL(""))
		// Exchange() automatically calls .TokenCache.PutToken(token)
		if _, err := transport.Exchange(code); err != nil {
			return nil, fmt.Errorf("Failed to exchange OAuth2 token: %s", err)
		}
	}
	return transport.Client(), nil
}

func (i *InstalledApp) getToken(scope string) *TokenCache {
	i.lock.Lock()
	defer i.lock.Unlock()
	if i.ScopedTokenCache == nil {
		i.ScopedTokenCache = make(map[string]*TokenCache)
	}
	if _, ok := i.ScopedTokenCache[scope]; !ok {
		i.ScopedTokenCache[scope] = &TokenCache{}
	}
	return i.ScopedTokenCache[scope]
}

// MakeInstalledApp returns an initialized InstalledApp instance with
// commonly used parameters.
func MakeInstalledApp() *InstalledApp {
	return &InstalledApp{
		AuthURL:          "https://accounts.google.com/o/oauth2/auth",
		TokenURL:         "https://accounts.google.com/o/oauth2/token",
		ScopedTokenCache: make(map[string]*TokenCache),
	}
}
