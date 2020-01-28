/**
 * @license
 * Copyright 2020 The LUCI Authors. All rights reserved.
 * Use of this source code is governed under the Apache License, Version 2.0
 * that can be found in the LICENSE file.
 */

import './config-set-card.js';

import { CommonBehavior } from "../common/common-behaviors.js";

import '@cwmr/paper-search/paper-search.js';
import '@polymer/iron-ajax/iron-ajax.js';
import '@polymer/paper-spinner/paper-spinner.js';
import '@polymer/paper-styles/shadow.js';
import '@polymer/polymer/lib/elements/dom-if.js';

import { mixinBehaviors } from '@polymer/polymer/lib/legacy/class.js';
import { PolymerElement, html } from '@polymer/polymer/polymer-element.js';

class FrontPage extends mixinBehaviors([CommonBehavior], PolymerElement) {
  static get template() {
    return html`
    <style>
      @media only screen and (min-width: 768px) {
        .center {
          width: 550px;
        }

        paper-search-bar {
          width: 900px;
        }
      }

      .loading { text-align: center; }

      .config-card {
        padding-bottom: 1%;
        animation: fadein 1.5s;
      }

      @keyframes fadein {
        from {opacity: 0}
        to {opacity: 1}
      }

      .search-bar {
        padding-top: 7%;
        padding-bottom: 2%;
      }

      .name {
        font-family: sans-serif;
        word-wrap: break-word;
        text-align: center;
      }

      .center {
        margin: auto;
      }

      paper-search-bar {
        @apply --shadow-elevation-4dp;
        height: 100%;
        margin: auto;
      }

    </style>

    <iron-ajax
        id="requestConfigs"
        url="/_ah/api/config/v1/config-sets?include_last_import_attempt=true"
        handle-as="json"
        on-error="_onRequestError"
        on-response="_onGotConfigSets"
        headers="[[auth_headers]]">
    </iron-ajax>

    <div class="search-bar">
      <paper-search-bar
          id = "searchBar"
          query="{{query}}"
          hide-filter-button="true"
          on-paper-search-search="_setURL"></paper-search-bar>
    </div>

    <div class="config-list">
      <template is="dom-if" if="[[isLoading]]">
        <div class="center loading">
          <paper-spinner active></paper-spinner>
        </div>
      </template>
      <template is="dom-if" if="[[_not(isLoading)]]">
        <template is="dom-if" if="[[_not(errorMessage)]]">
          <template is="dom-if" if="[[_isEmpty(searchResults)]]">
            <div class="center name">No config sets found.</div>
          </template>
          <template is="dom-if" if="[[_not(_isEmpty(searchResults))]]">
            <template is="dom-repeat" items="[[searchResults]]" as="config">
              <div class="center config-card">
                <config-set-card
                  name="[[config.config_set]]"
                  last-import-attempt="[[_getLastImportAttempt(config.last_import_attempt)]]"
                  revision="[[_getRevision(config.revision)]]"
                  link="[[_getURL(config)]]"
                  timestamp="[[_getTimestamp(config.last_import_attempt, config.revision)]]">
                </config-set-card>
              </div>
            </template>
          </template>
        </template>
        <template is="dom-if" if="[[errorMessage]]">
          <div class="center name">[[errorMessage]]</div>
        </template>
      </template>
    </div>
    `;
  }

  static get is() { return 'front-page'; }

  static get properties() {
    return {
      configSetList: {
        type: Array,
        value: () => []
      },
      errorMessage: {
        type: String,
        value: null
      },
      isLoading: {
        type: Boolean,
        value: true
      },
      query: {
        type: String,
        observer: '_updateSearchResults'
      },
      searchResults: {
        type: Array,
        value: () => []
      },
      signed_in: {
        type: Boolean,
        observer: '_onSignIn'
      }
    }
  }


  ready() {
    super.ready()
    if (!this.initialized) {
      document.addEventListener('fetch-configs', function() {
        this.$.requestConfigs.generateRequest();
      }.bind(this));
    } else {
      this.$.requestConfigs.generateRequest();
    }
    document.addEventListener('WebComponentsReady', function() {
      this.$['searchBar'].focus();
    }.bind(this));
  }

  _onSignIn() {
    if (this.signed_in) {
      this.isLoading = true;
      this.$.requestConfigs.generateRequest();
    }
  }

  _formatName(name) {
    var tempName = name.substring(name.indexOf("/") + 1);
    return tempName.includes("/") ?
        tempName.substring(0, tempName.indexOf("/")) : tempName;
  }

  _getLastImportAttempt(lastImportAttempt) {
    return lastImportAttempt || null;
  }

  _setURL() {
    window.location.href = "/#/q/" + this.query;
  }

  _getURL(config) {
    if (config.last_import_attempt && config.last_import_attempt.success) {
      return config.last_import_attempt.revision.url;
    } else if (config.revision && config.revision.url) {
      return config.revision.url;
    } else {
      return config.location;
    }
  }

  _onGotConfigSets(event) {
    this.configSetList = event.detail.response.config_sets;
    this._updateSearchResults();
    this.isLoading = false;
    this.dispatchEvent(new CustomEvent('processedConfigSets', {
      bubbles: true,
      composed: true}));
  }

  _onRequestError(event) {
    var error = parseInt(event.detail.error.message.match(/\d+/g));
    if (error === 403) {
      this.errorMessage = "Access denied.";
    } else if (500 <= error && error < 600) {
      this.errorMessage = "Internal server error. Please refresh or try again later.";
    } else {
      this.errorMessage = "Error occured. Please try again later.";
    }
    this.isLoading = false;
    this.dispatchEvent(new CustomEvent('fetchError', {
      bubbles: true,
      composed: true}));
  }

  _updateSearchResults() {
    // This method sorts search results by the name of the config set, that way
    // the list doesn't consist of all projects followed by all services due to
    // the path beginning with "projects/" or "services/"
    var tempResults = this.configSetList.filter(e => e.config_set.includes(this.query));
    tempResults.sort(function(a, b) {
      return this._formatName(a.config_set).localeCompare(this._formatName(b.config_set));
    }.bind(this));
    this.searchResults = tempResults;
  }
}

window.customElements.define(FrontPage.is, FrontPage);