// Copyright 2018 The LUCI Authors. All rights reserved.
// Use of this source code is governed under the Apache License, Version 2.0
// that can be found in the LICENSE file.

/** @module swarming-ui/modules/oauth-login */

/** oauth-login is a small widget that handles an OAuth 2.0 flow with Google.
 *
 * <p>
 *  This widget either a sign in button or displays the info of the
 *  logged-in user and a sign out button. The widget fires an event
 *  when the user has logged in and clients should be able to make
 *  authenticated requests.
 * </p>
 *
 * @attr client_id - The Client ID for authenticating via OAuth.
 * @attr testing_offline - If true, the real OAuth flow won't be used.
 *    Instead, dummy data will be used. Ideal for local testing.
 *
 * @evt log-in The event has a detail of the form:
 *
 * <pre>
 * {
 *   auth_header: "Bearer abc12d",
 * }
 * </pre>
 *
 * @param {string} auth_header A string that should be used as the
 *    "Authorization" header for authenticated requests.
 *
 */

import { html, render } from 'lit-html/lib/lit-extended'
import { upgradeProperty } from 'elements-sk/upgradeProperty'
import { errorMessage } from 'common-sk/modules/errorMessage'

const template = (ele) => {
  if (ele.auth_header) {
    return html`
<div>
  <img class=center id=avatar src="${ele.profile.imageURL}" width=30 height=30>
  <span class=center>${ele.profile.email}</span>
  <span class=center>|</span>
  <a class=center on-click=${()=>ele._logOut()} href="#">Sign out</a>
</div>`;
  } else {
    return html`
<div>
  <a on-click=${()=>ele._logIn()} href="#">Sign in</a>
</div>`;
  }
};

window.customElements.define('oauth-login', class extends HTMLElement {

  connectedCallback() {
    upgradeProperty(this, 'client_id');
    upgradeProperty(this, 'testing_offline');
    this.auth_header = '';
    if (this.testing_offline) {
      // For local testing, set a profile here.  A real profile would be null at this point
      this.profile = {
        email: 'missing@chromium.org',
        imageURL: 'http://storage.googleapis.com/gd-wagtail-prod-assets/original_images/logo_google_fonts_color_2x_web_64dp.png',
      };
    } else {
      this.profile = null;
      document.addEventListener('oauth-lib-loaded', ()=>{
        gapi.auth2.init({
          client_id: this.client_id,
        }).then(() => {
          this._maybeFireLoginEvent();
          this._render();
        }, (error) => {
          console.error(error);
          errorMessage(`Error initializing oauth: ${JSON.stringify(error)}`, 10000);
        });
      });

    }
    this._render();
  }

  static get observedAttributes() {
    return ['client_id', 'testing_offline'];
  }

  get client_id() { return this.getAttribute('client_id');}
  set client_id(val) { return this.setAttribute('client_id', val);}

  get testing_offline() { return this.getAttribute('testing_offline');}
  set testing_offline(val) {
    // If testing_offline gets passed through multiple layers, it ends up the
    // string 'false'
    if (val && val !== 'false') {
      this.setAttribute('testing_offline', true);
    } else {
      this.removeAttribute('testing_offline');
    }
  }

  _maybeFireLoginEvent() {
    let user = gapi.auth2.getAuthInstance().currentUser.get();
    if (user.isSignedIn()) {
      let profile = user.getBasicProfile();
      this.profile = {
        email: profile.getEmail(),
        imageURL: profile.getImageUrl()
      };
      // Need the true here to get an access_token on the response.
      let auth = user.getAuthResponse(true);

      let header = `${auth.token_type} ${auth.access_token}`
      this.dispatchEvent(new CustomEvent('log-in', {
        detail: {
          'auth_header': header,
        },
        bubbles: true,
      }));
      this.auth_header = header;
      return true;
    } else {
      this.profile = null;
      this.auth_header = '';
      return false;
    }
  }

  _logIn() {
    if (this.testing_offline) {
        this.auth_header = 'Bearer 12345678910-boomshakalaka';
        this.dispatchEvent(new CustomEvent('log-in', {
          detail: {
            'auth_header': this.auth_header,
          },
          bubbles: true,
        }));
        this._render();
      } else {
        let auth = gapi.auth2.getAuthInstance();
        if (auth) {
          auth.signIn({
            scope: 'email',
            prompt: 'select_account',
          }).then(() => {
            if (!this._maybeFireLoginEvent()) {
              console.warn('login was not successful; maybe user canceled');
            }
            this._render();
          });
        }
      }
  }

  _logOut() {
    if (this.testing_offline) {
      this.auth_header = '';
      this._render();
      // reload the page to clear any sensitive data being displayed.
      window.location.reload();
    } else {
      let auth = gapi.auth2.getAuthInstance();
      if (auth) {
        auth.signOut().then(() => {
          this.auth_header = '';
          this.profile = null;
          // reload the page to clear any sensitive data being displayed.
          window.location.reload();
        });
      }
    }
  }

  _render() {
    render(template(this), this);
  }

  attributeChangedCallback(attrName, oldVal, newVal) {
    this._render();
  }

});
