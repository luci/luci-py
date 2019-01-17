// Copyright 2019 The LUCI Authors. All rights reserved.
// Use of this source code is governed under the Apache License, Version 2.0
// that can be found in the LICENSE file.

import { html, render } from 'lit-html'
import { upgradeProperty } from 'elements-sk/upgradeProperty'


/**
 * @module swarming-ui/modules/task-mass-cancel
 * @description <h2><code>task-mass-cancel<code></h2>
 *
 * <p>
 *   TODO
 * </p>
 *
 * @fires TODO
 */


const template = (ele) => html`
hello world`;

window.customElements.define('task-mass-cancel', class extends HTMLElement {

  constructor() {
    super();
  }

  connectedCallback() {
    this._render();
  }

  _render() {
    render(template(this), this);
  }

});