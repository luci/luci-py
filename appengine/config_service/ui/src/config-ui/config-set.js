/**
 * @license
 * Copyright 2020 The LUCI Authors. All rights reserved.
 * Use of this source code is governed under the Apache License, Version 2.0
 * that can be found in the LICENSE file.
 */

import './config-file-card.js';

import { CommonBehavior } from "../common/common-behaviors.js";

import '@polymer/iron-ajax/iron-ajax.js';
import '@polymer/iron-icon/iron-icon.js';
import '@polymer/paper-spinner/paper-spinner.js';
import '@polymer/paper-tooltip/paper-tooltip.js';
import '@polymer/polymer/lib/elements/dom-if.js';
import 'web-animations-js/web-animations-next.min.js';

import { mixinBehaviors } from '@polymer/polymer/lib/legacy/class.js';
import { PolymerElement, html } from '@polymer/polymer/polymer-element.js';

class ConfigSet extends mixinBehaviors([CommonBehavior], PolymerElement) {
  static get template() {
    return html`
    <style>
      @media only screen and (min-width: 768px) {
        .center {
          width: 550px;
        }
      }

      .category {
        font-size: 100%;
        font-family: sans-serif;
      }

      .name {
        font-size: 200%;
        font-family: sans-serif;
        word-wrap: break-word;
      }

      .center {
        margin: auto;
        text-align: left;
      }

      .config-card {
        padding-bottom: 1%;
        animation: fadein 1.5s;
      }

      @keyframes fadein {
        from {opacity: 0}
        to {opacity: 1}
      }

      .title {
        padding-bottom: 1%;
        padding-top: 5%;
      }

      #refreshStatus { font-size: 80%; }

      .paper-green { color: var(--paper-green-600); }

      .paper-red { color: var(--paper-red-600); }

      .paper-grey { color: var(--paper-grey-600); }

      .spinner {
        text-align: center;
      }

    </style>

    <iron-ajax
        id="requestConfigs"
        url="/_ah/api/config/v1/config-sets?config_set=[[category]]/[[name]][[route.path]]&include_files=true&include_last_import_attempt=true"
        handle-as="json"
        on-error="_onRequestError"
        on-response="_onGotConfigFiles"
        headers="[[auth_headers]]">
    </iron-ajax>

    <iron-ajax
        id="refreshConfigs"
        url="/_ah/api/config/v1/reimport?config_set=[[category]]/[[name]][[route.path]]"
        method="POST"
        handle-as="json"
        on-error="_onRefreshError"
        on-response="_onCompleteRefresh"
        headers="[[auth_headers]]">
    </iron-ajax>

    <div class="center title">
      <div class="name">
        [[name]][[route.path]]
        <iron-icon id="launch"
                  icon="icons:launch"
                  class="paper-grey"
                  on-tap="_openConfigGitiles">
        </iron-icon>
        <paper-tooltip for="launch" offset="0">
          [[url]]
        </paper-tooltip>
        <template is="dom-if" if="[[_not(isLoading)]]">
          <template is="dom-if" if="[[lastImportAttempt]]">
            <template is="dom-if" if="[[lastImportAttempt.success]]">
              <iron-icon id="valid"
                icon="icons:check-circle" class="paper-green"></iron-icon>
            </template>
            <template is="dom-if" if="[[_not(lastImportAttempt.success)]]">
              <iron-icon id="invalid"
                icon="icons:warning" class="paper-red"></iron-icon>
            </template>
          </template>
          <template is="dom-if" if="[[_not(lastImportAttempt)]]">
            <iron-icon icon="icons:help" class="paper-grey"></iron-icon>
          </template>
        </template>
        <template is="dom-if" if="[[auth_headers]]">
          <iron-icon id="force-refresh"
                    icon="icons:file-download"
                    on-tap="_forceRefresh">
          </iron-icon>
          <paper-tooltip for="force-refresh" offset="0">
            Re-import the config-set from the repository.
          </paper-tooltip>
        </template>
      </div>
      <div class="category">
        <p>[[_formatCategory(category, route.path)]]</p>
        <template is="dom-if" if="[[_not(isLoading)]]">
          <template is="dom-if" if="[[lastImportAttempt]]">
            <template is="dom-if" if="[[_not(lastImportAttempt.success)]]">
              <div class="paper-red">
                Last import attempt failed: [[lastImportAttempt.message]]
              </div>
            </template>
            <template is="dom-if" if="[[lastImportAttempt.success]]">
              Last import succeeded.
            </template>
          </template>
          <template is="dom-if" if="[[_not(lastImportAttempt)]]">
            Last import attempt info not available.
          </template>
          <p>Revision: [[_getRevision(revision)]]</p>
          <p>Timestamp: [[_getExactTime(timestamp)]]</p>
        </template>
        <p id="refreshStatus">[[refreshMessage]]</p>
      </div>
    </div>
    <template is="dom-if" if="[[_not(errorMessage)]]">
      <template is="dom-if" if="[[isRefreshing]]">
        <div class="spinner">
          <paper-spinner active></paper-spinner>
        </div>
      </template>
      <template is="dom-if" if="[[_not(isRefreshing)]]">
        <template is="dom-if" if="[[isLoading]]">
          <div class="spinner">
            <paper-spinner active></paper-spinner>
          </div>
        </template>
        <template is="dom-if" if="[[_not(isLoading)]]">
          <template is="dom-if" if="[[_isEmpty(files)]]">
            <div class="center" style="font-family: sans-serif;">
              No config files found.
            </div>
          </template>
          <template is="dom-if" if="[[_not(_isEmpty(files))]]">
            <template is="dom-repeat" items="[[files]]" as="file">
              <div class="center config-card">
                <config-file-card
                    name="[[file.path]]" link="[[url]]/[[file.path]]">
                </config-file-card>
              </div>
            </template>
          </template>
        </template>
      </template>
    </template>
    <template is="dom-if" if="[[errorMessage]]">
      <div class="center">
        <p>[[errorMessage]]</p>
      </div>
    </template>
    `;
  }

  static get is() { return 'config-set'; }

  static get properties() {
    return {
      category: {
        type: String
      },
      errorMessage: {
        type: String,
        value: null
      },
      files: {
        type: Array
      },
      frontPageIsActive: {
        type: Boolean,
        observer: '_frontPageIsActive'
      },
      isLoading: {
        type: Boolean,
        value: true
      },
      isRefreshing: {
        type: Boolean,
        value: false
      },
      lastImportAttempt: {
        type: Object
      },
      name: {
        type: String
      },
      refreshMessage: {
        type: String,
        value: null
      },
      route: {
        type: Object,
        observer: '_routeChanged'
      },
      revision: {
        type: String,
        value: null
      },
      timestamp: {
        type: String,
        value: null
      },
      url: {
        type: String
      }
    }
  }

  _forceRefresh() {
    this.refreshMessage = null;
    this.$.refreshConfigs.generateRequest();
    this.isRefreshing = true;
  }

  _formatCategory(category, name) {
    if (name && name.includes("/refs")) return "Ref";
    if (category === "projects") return "Project";
    if (category === "services") return "Service";
    return "Cannot determine type of config set.";
  }

  _onCompleteRefresh() {
    this.isRefreshing = false;
    this.refreshMessage = "Reimport successful.";
    this.$.requestConfigs.generateRequest();
    this.dispatchEvent(new CustomEvent('refreshComplete', {
      bubbles: true,
      composed: true}));
  }

  _onGotConfigFiles(event) {
    var config_set = event.detail.response.config_sets[0];
    this.files = config_set.files || [];
    this.lastImportAttempt = config_set.last_import_attempt || null;
    if (this.lastImportAttempt && this.lastImportAttempt.success) {
      this.url = config_set.last_import_attempt.revision.url;
      this.revision = config_set.last_import_attempt.revision;
    } else if (config_set.revision) {
      this.url = config_set.revision.url || config_set.location;
      this.revision = config_set.revision;
    } else {
      this.url = config_set.location;
    }
    this.timestamp = this._getTimestamp(this.lastImportAttempt, this.revision);
    this.isLoading = false;
    this.errorMessage = null;
    this.dispatchEvent(new CustomEvent('processedConfigFiles', {
      bubbles: true,
      composed: true}));
  }

  _onRefreshError() {
    this.isRefreshing = false;
    this.refreshMessage = "Error: Files could not be reimported.";
    this.dispatchEvent(new CustomEvent('refreshError', {
      bubbles: true,
      composed: true}));
  }

  _onRequestError(event) {
    var error = parseInt(event.detail.error.message.match(/\d+/g));
    this.isLoading = false;
    if (error === 403) {
      if (!this.auth_headers) {
        this.errorMessage = "Access denied, please sign in.";
      } else {
        this.errorMessage = "Access denied, " + this.profile.email +
            " is not authorized to access this config set." +
            " Request access or sign in as a different user.";
      }
    } else if (500 <= error && error < 600) {
      this.errorMessage = "Internal server error.";
    } else {
      this.errorMessage = "Error occured. Try again later.";
    }
    this.dispatchEvent(new CustomEvent('fetchError', {
      bubbles: true,
      composed: true}));
  }

  _routeChanged() {
    this.isLoading = true;
    this.$.requestConfigs.generateRequest();
  }

  _openConfigGitiles() {
    window.open(this.url);
  }
}

window.customElements.define(ConfigSet.is, ConfigSet);