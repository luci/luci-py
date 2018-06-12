// Copyright 2018 The LUCI Authors. All rights reserved.
// Use of this source code is governed under the Apache License, Version 2.0
// that can be found in the LICENSE file.

/** @module swarming-ui/modules/swarming-app */

/**
 * <p>
 *   A general application layout which includes a responsive
 *   side panel. This element is largely CSS, with a smattering of
 *   JS to toggle the side panel on/off when in small screen mode.
 *   A notable addition to the top panel is an &lt;oauth-login&gt; element
 *   to handle login. See the demo page for an example usage.
 * </p>
 *
 * <p>
 *   The swarming-app can be a central place for indicating to the user
 *   that the app is busy (e.g. RPCs). Simply use the addBusyTasks()
 *   and finishedTask() to indicate when work is starting and stopping.
 *   The 'busy-end' event will signal any time a finishedTask() drops the
 *   count of ongoing tasks to zero (or lower). See also the busy property.
 * </p>
 *
 * @evt busy-end This event is emitted when ever the app transitions from
 *               busy to not busy.
 *
 * @attr client_id - The Client ID for authenticating via OAuth.
 * @attr testing_offline - If true, the real OAuth flow won't be used.
 *    Instead, dummy data will be used. Ideal for local testing.
 *
 */

import { html, render } from 'lit-html/lib/lit-extended'
import { upgradeProperty } from 'elements-sk/upgradeProperty'

const button_template = document.createElement('template');
button_template.innerHTML =`
<button class=toggle-button>
  <icon-menu-sk>
  </icon-menu-sk>
</button>
`;

const spinner_template = document.createElement('template');
spinner_template.innerHTML =`
<div class=spinner-spacer>
  <spinner-sk></spinner-sk>
</div>
`;

const login_template = (ele) => html`
<oauth-login client_id=${ele.client_id} testing_offline=${ele.testing_offline}></oauth-login>
`;

window.customElements.define('swarming-app', class extends HTMLElement {

  connectedCallback() {
    upgradeProperty(this, 'client_id');
    upgradeProperty(this, 'testing_offline');
    this._busyTaskCount = 0;
    this._spinner = null;
    this._loginEle = null;
    this._addHTML();
    this._render();
  }

  static get observedAttributes() {
    return ['client_id', 'testing_offline'];
  }

  /** @prop {boolean} busy Indicates if there any on-going tasks (e.g. RPCs).
   *                  This also mirrors the status of the embedded spinner-sk.
   *                  Read-only. */
  get busy() { return !!this._busyTaskCount;}

  /** @prop {string} client_id Mirrors the attribute 'client_id'. */
  get client_id() { return this.getAttribute('client_id');}
  set client_id(val) { return this.setAttribute('client_id', val);}

  /** @prop {bool} testing_offline Mirrors the attribute 'testing_offline'. */
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

  /**
   * Indicate there are some number of tasks (e.g. RPCs) the app is waiting on
   * and should be in the "busy" state, if it isn't already.
   *
   * @param {Number} count - Number of tasks to wait for. Should be positive.
   */
  addBusyTasks(count) {
    this._busyTaskCount += count;
    if (this._spinner && this._busyTaskCount > 0) {
      this._spinner.active = true;
    }
  }

  /**
   * Removes one task from the busy count. If there are no more tasks to wait
   * for, the app will leave the "busy" state and emit the "busy-end" event.
   *
   */
  finishedTask() {
    this._busyTaskCount--;
    if (this._busyTaskCount <= 0) {
      this._busyTaskCount = 0;
      if (this._spinner) {
        this._spinner.active = false;
      }
      this.dispatchEvent(new CustomEvent('busy-end', {bubbles: true}));
    }
  }

  /**
   * As mentioned in the element description, the main point of this element
   * is to insert a little bit of CSS and a few HTML elements for consistent
   * theming and functionality.
   *
   * This function adds in the following:
   *   1. A button that will toggle the side panel on small screens (and will
   *      be hidden on large screens).
   *   2. The spinner that indicates the busy state.
   *   3. A spacer span to right-align the login element
   *   4. A placeholder in which to render the login-element.
   *
   * This function need only be called once, when the element is created.
   */
  _addHTML() {
    let header = this.querySelector('header');
    let sidebar = header && header.querySelector('aside');
    if (!(header && sidebar && sidebar.classList.contains('hideable'))) {
      return;
    }
    // Add the collapse button to the header as the first item.
    let btn = button_template.content.cloneNode(true);
    // btn is a document-fragment, so we need to insert it into the
    // DOM to make it "expand" into a real button. Then, and only then,
    // we can add a "click" listener.
    header.insertBefore(btn, header.firstElementChild);
    btn = header.firstElementChild;
    btn.addEventListener('click', (e) => this._toggleMenu(e, sidebar));

    // Add the spinner that will visually indicate the state of the
    // busy property.
    let spinner = spinner_template.content.cloneNode(true);
    header.insertBefore(spinner, sidebar);
    // The real spinner is a child of the template, so we need to grab it
    // from the header after the template has been expanded.
    this._spinner = header.querySelector('spinner-sk');

    let spacer = document.createElement('span');
    spacer.classList.add('grow');
    header.appendChild(spacer);

    // The placeholder for which the login element (the only dynamic content
    // swarming-app manages) will be rendered into. See _render() for when that
    // happens.
    this._loginEle = document.createElement('div');
    header.appendChild(this._loginEle);
  }

  _toggleMenu(e, sidebar) {
    sidebar.classList.toggle('shown');
  }

  _render() {
    if (this._loginEle) {
      render(login_template(this), this._loginEle);
    }
  }

  attributeChangedCallback(attrName, oldVal, newVal) {
    this._render();
  }

});
