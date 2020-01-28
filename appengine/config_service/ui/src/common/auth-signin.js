/**
 * The `auth-signin` element displays sign-in/sign-out button, user email and
 * avatar.
 *
 * It has a google-signin/google-signin-aware element under the hood that
 * handles the actual OAuth logic.
 *
 * Usage:
 *   <auth-signin></auth-signin>
 * Properties:
 *   * auth_headers: Object, Use this as an argument to sk.request to set oauth2
 *       headers.
 *   * auth_response: Object, The raw gapi.auth2.AuthResponse object.
 *   * client_id: String, The client id to authenticate
 *   * profile: Object, Read Only, The email address and imageurl of the logged
 *       in user.
 *   * signed_in: Boolean, Read Only, if the user is logged in.
 * Methods:
 *   * signIn(): Signs the user in by popping up the authorization dialog.
 *   * signOut(): Signs the user out.
 * Events:
 *   * auth-signin: Fired when the oauth handshake has completed and a user has
 *       logged in.
 *
 * @license
 * Copyright 2020 The LUCI Authors. All rights reserved.
 * Use of this source code is governed under the Apache License, Version 2.0
 * that can be found in the LICENSE file.
*/

import "@google-web-components/google-signin/google-signin-aware.js"
import '@polymer/polymer/lib/elements/dom-if.js';
import { PolymerElement, html } from '@polymer/polymer/polymer-element.js';


class AuthSignin extends PolymerElement {
  static get template() {
    return html`
    <style>
      #avatar { border-radius: 5px; }

      .center { vertical-align: middle; }

      .typeface {
        font-size: 75%;
        color: white;
      }
    </style>

    <google-signin-aware id="aware"
      client-id="[[client_id]]"
      initialized="{{initialized}}"
      scopes="email"
      on-google-signin-aware-success="_onSignin"
      on-google-signin-aware-signed-out="_onSignout">
    </google-signin-aware>

    <template is="dom-if" if="[[!signed_in]]">
      <div id="signinContainer">
        <a class="typeface" on-tap="signIn" href="/#/">Sign in</a>
      </div>
    </template>

    <template is="dom-if" if="[[signed_in]]">
      <img class="center" id="avatar"
        src="[[profile.imageUrl]]" width="30" height="30">
      <span class="center typeface">[[profile.email]]</span>
      <span class="center typeface">|</span>
      <!-- TODO(cwpayton): Before official deployment, change href to "/#/" -->
      <a class="center typeface" on-tap="signOut" href="/#/">Sign out</a>
    </template>
    `;
  }

  static get is() { return 'auth-signin'; }

  static get properties() {
    return {
      auth_headers: {
        type: Object,
        value: () => null,
        notify: true
      },
      auth_response: {
        type: Object,
        notify: true,
        observer: '_makeHeader'
      },
      client_id: {
        type: String
      },
      initialized: {
        type: Boolean,
        value: false,
        notify: true,
        observer: '_onInitialized'
      },
      profile: {
        type: Object,
        notify: true
      },
      signed_in: {
        type: Boolean,
        value: false,
        notify: true
      },
      user: {
        type: Object,
        value: () => null,
      }
    }
  }

  ready() {
    super.ready()
    if (!this.client_id) {
      return;
    }
  }

  _onInitialized() {
    if (this.initialized) {
      this.set('user', null);
      this.set('user', gapi.auth2.getAuthInstance().currentUser.get());
      if (!(this.user && this.user.getAuthResponse().access_token)) {
        this.dispatchEvent(new CustomEvent('fetch-configs', {
          bubbles: true,
          composed: true}));
      }
    }
  }

  _onSignin() {
    this._signingIn = true;
    this.set('user', null);
    this.set('user', gapi.auth2.getAuthInstance().currentUser.get());
    var profile = { email: this.user.getBasicProfile().getEmail(),
                    imageUrl: this.user.getBasicProfile().getImageUrl() };
    this.set('profile', null);
    this.set('profile', profile);
    this.set('auth_response', this.user.getAuthResponse());
    this.signed_in = true;
    // The credential will expire after a while (usually an hour)
    // so we need to reload it.
    setTimeout(function(){
      console.log("reloading credentials");
      this.user.reloadAuthResponse();
      this._onSignin();
    }, this.auth_response.expires_in * 1000)// convert seconds to ms
    this.dispatchEvent(new CustomEvent('fetch-configs', {
      bubbles: true,
      composed: true}));
    this._signingIn = false;
  }

  _onSignout(e) {
    this.signed_in = false;
    this.set('profile', null);
  }

  _makeHeader() {
    if (!this.auth_response) {
      this.set('auth_headers', null);
    }
    this.set('auth_headers',
        {
          "authorization": this.auth_response.token_type + " " +
                          this.auth_response.access_token
        });
  }

  signIn() {
    this.$.aware.signIn();
  }

  signOut() {
    this.$.aware.signOut();
    window.location.reload();
  }
}

window.customElements.define(AuthSignin.is, AuthSignin);