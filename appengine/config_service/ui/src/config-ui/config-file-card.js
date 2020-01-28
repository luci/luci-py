/**
 * @license
 * Copyright 2020 The LUCI Authors. All rights reserved.
 * Use of this source code is governed under the Apache License, Version 2.0
 * that can be found in the LICENSE file.
 */

import '@polymer/paper-card/paper-card.js';

import { PolymerElement, html } from '@polymer/polymer/polymer-element.js';

class ConfigFileCard extends PolymerElement {
  static get template() {
    return html`
    <style>
      a {
        text-decoration: none;
        color: inherit;
      }

      paper-card {
        width: 100%;
      }

    </style>

    <a href="[[link]]" target="_blank">
      <paper-card elevation="2" on-tap="_handleClick">
        <div class="card-content">
          [[name]]
        </div>
      </paper-card>
    </a>
    `;
  }

  static get is() { return 'config-file-card'; }

  static get properties() {
    return {
      name: {
        type: String
      },
      link: {
        type: String
      },
    }
  }
}

window.customElements.define(ConfigFileCard.is, ConfigFileCard);