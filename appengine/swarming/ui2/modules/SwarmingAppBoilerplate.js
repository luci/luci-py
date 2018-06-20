// Copyright 2018 The LUCI Authors. All rights reserved.
// Use of this source code is governed under the Apache License, Version 2.0
// that can be found in the LICENSE file.

/** @module swarming-ui/SwarmingAppBoilerplate */

import { html, render } from 'lit-html/lib/lit-extended'
import { upgradeProperty } from 'elements-sk/upgradeProperty'

/** @classdesc
 * The SwarmingAppBoilerplate class deduplicates much of the boilerplate
 * that all top-level Swarming apps (e.g. bot-list, task-page) have.
 *
 * To use, extend SwarmingAppBoilerplate, call <code>super(template)</code> in the
 * constructor with the main template that should be rendered, and call
 * <code>super.connectedCallback()</code> in the <code>connectedCallback()</code>
 * function to get access to the attributes.
 *
 * @example
 * const template = (ele) => html`<h1>Hello ${ele.foo}</h1>`
 *
 * window.customElements.define('my-page', class extends SwarmingAppBoilerplate {
 *
 *  constructor() {
 *    super(template);
 *    this.foo = 'World';
 *  }
 *
 *  connectedCallback() {
 *   super.connectedCallback();
 *   console.log('client_id is' + this.client_id);
 *  }
 *}
 *
 * @attr client_id - The Client ID for authenticating via OAuth.
 * @attr testing_offline - If true, the real OAuth flow won't be used.
 *    Instead, dummy data will be used. Ideal for local testing.
 *
 */
export default class SwarmingAppBoilerplate extends HTMLElement {

  constructor(template) {
    super();
    this._template = template;
  }

  connectedCallback() {
    upgradeProperty(this, 'client_id');
    upgradeProperty(this, 'testing_offline');
  }

  static get observedAttributes() {
    return ['client_id', 'testing_offline'];
  }

  /** @prop {string} client_id Mirrors the attribute 'client_id'. */
  get client_id() { return this.getAttribute('client_id');}
  set client_id(val) {return this.setAttribute('client_id', val);}

  /** @prop {bool} testing_offline Mirrors the attribute 'testing_offline'. */
  get testing_offline() { return this.getAttribute('testing_offline')}
  set testing_offline(val) {
    // handle testing_offline=false "correctly"
    if (val && val !== 'false') {
      this.setAttribute('testing_offline', true);
    } else {
      this.removeAttribute('testing_offline');
    }
  }

  /** Re-renders the app, starting with the top level template. */
  render() {
    render(this._template(this), this);
  }

  attributeChangedCallback(attrName, oldVal, newVal) {
    this.render();
  }
}