/**
 * @license
 * Copyright 2020 The LUCI Authors. All rights reserved.
 * Use of this source code is governed under the Apache License, Version 2.0
 * that can be found in the LICENSE file.
 */

import { CommonBehavior } from "../common/common-behaviors.js"

import '@polymer/paper-card/paper-card.js';
import '@polymer/paper-tooltip/paper-tooltip.js';
import '@polymer/polymer/lib/elements/dom-if.js';

import { PolymerElement, html } from '@polymer/polymer/polymer-element.js';
import { mixinBehaviors } from '@polymer/polymer/lib/legacy/class.js';

class ConfigSetCard extends mixinBehaviors([CommonBehavior], PolymerElement) {
  static get template() {
    return html`
    <style>
      paper-card {
        width: 100%;
      }

      span {
        color: var(--paper-grey-600);
        word-wrap: break-word;
        font-size: 90%;
      }

      .config-title {
        @apply --paper-font-headline;
        word-wrap: break-word;
        font-size: 120%;
      }

      .validation {
        float: right;
        font-size: 15px;
        vertical-align: middle;
      }

      .paper-green { color: var(--paper-green-600); }

      .paper-red { color: var(--paper-red-600); }

      .paper-grey { color: var(--paper-grey-600); }

    </style>

    <paper-card elevation="2"
                on-tap="_openConfigPage">
      <div class="card-content">
        <div class="config-title">
          [[name]]
          <div class="validation">
            <iron-icon id="launch"
                      icon="icons:launch"
                      class="paper-grey"
                      on-tap="_openConfigGitiles">
            </iron-icon>
            <paper-tooltip for="launch" offset="0">
              [[link]]
            </paper-tooltip>
            <template is="dom-if" if="[[lastImportAttempt]]" restamp="true">
              <template is="dom-if"
                if="[[lastImportAttempt.success]]" restamp="true">
                <iron-icon id="successful-import"
                          icon="icons:check-circle"
                          class="paper-green">
                </iron-icon>
                <paper-tooltip for="successful-import" offset="0">
                  Last import succeeded.
                </paper-tooltip>
              </template>
              <template is="dom-if"
                if="[[_not(lastImportAttempt.success)]]" restamp="true">
                <iron-icon id="failed-import"
                          icon="icons:warning"
                          class="paper-red">
                </iron-icon>
                <paper-tooltip for="failed-import" offset="0">
                  Last import failed. Click for more info.
                </paper-tooltip>
              </template>
            </template>
            <template is="dom-if"
              if="[[_not(lastImportAttempt)]]" restamp="true">
              <iron-icon id="no-import"
                        icon="icons:help"
                        class="paper-grey">
              </iron-icon>
              <paper-tooltip for="no-import" offset="0">
                Last import attempt info not available.
              </paper-tooltip>
            </template>
          </div>
          <div>
            <span id="revision">Revision: [[_formatRevision(revision)]]</span>
            <paper-tooltip for="revision" offset="0">
              [[revision]]
            </paper-tooltip>
            <span id="timestamp">Timestamp: [[_formatDate(timestamp)]]</span>
            <paper-tooltip for="timestamp" offset="0">
              [[_getExactTime(timestamp)]]
            </paper-tooltip>
          </div>
        </div>
      </div>
    </paper-card>
    `;
  }

  static get is() { return 'config-set-card'; }

  _openConfigGitiles(event) {
    event.stopPropagation();
    window.open(this.link);
  }

  _openConfigPage() {
    window.location.href = "/#/" + this.name;
  }
}

window.customElements.define(ConfigSetCard.is, ConfigSetCard);