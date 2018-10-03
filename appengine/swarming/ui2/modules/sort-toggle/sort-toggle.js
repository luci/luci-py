// Copyright 2018 The LUCI Authors. All rights reserved.
// Use of this source code is governed under the Apache License, Version 2.0
// that can be found in the LICENSE file.

import { html, render } from 'lit-html'
import { upgradeProperty } from 'elements-sk/upgradeProperty'

import 'elements-sk/icon/arrow-drop-down-icon-sk'
import 'elements-sk/icon/arrow-drop-up-icon-sk'

/**
 * @module swarming-ui/modules/sort-toggle
 * @description <h2><code>sort-toggle<code></h2>
 *
 * <p>
 *   An element that indicates sort direction of ascending, descending
 *   or none. They can be joined together via the <code>currentKey</code> property
 *   So that only one is 'active' at a time.
 * </p>
 *
 * @fires sort-change
 */

// Looks to properties set, then attributes to initialize ele[prop]
// using the proper setters.  Optionally removes the attr to avoid stale data.
function initPropertyFromAttrOrProperty(ele, prop, removeAttr=true) {
  upgradeProperty(ele, prop);
  if (ele[prop] === undefined && ele.hasAttribute(prop)) {
    ele[prop] = ele.getAttribute(prop);
    if (removeAttr) {
      ele.removeAttribute(prop);
    }
  }
}

const template = (ele) => html`
<arrow-drop-down-icon-sk ?hidden=${ele.key === ele.currentKey && ele.direction === 'asc'}>
</arrow-drop-down-icon-sk>
<arrow-drop-up-icon-sk ?hidden=${ele.key === ele.currentKey && ele.direction === 'desc'}>
</arrow-drop-up-icon-sk>`

window.customElements.define('sort-toggle', class extends HTMLElement {

  constructor() {
    super()
    // _currentKey, _key, _direction are private members
  }

  connectedCallback() {
    initPropertyFromAttrOrProperty(this, 'currentKey');
    initPropertyFromAttrOrProperty(this, 'key');
    initPropertyFromAttrOrProperty(this, 'direction');

    this.addEventListener('click', () => {
      this.toggle();
    });
  }

  /** @prop {string} currentKey - The currently selected sort key for a
   *                  group of sort-toggles. This should be set if a
   *                  sort-changed event from another sort-toggle was
   *                  observed.
   */
  get currentKey() { return this._currentKey; }
  set currentKey(val) { this._currentKey = val; this._render();}

  /** @prop {string} key - An arbitrary, unique string that this sort-toggle
   *                  represents.
   */
  get key() { return this._key; }
  set key(val) { this._key = val; this._render();}

  /** @prop {string} direction - Either 'asc' or 'desc' indicating which
   *                  direction the user indicated. Is ignored if currentKey
   *                  does not equal this.key.
   */
  get direction() { return this._direction; }
  set direction(val) { this._direction = val; this._render();}

  toggle() {
    if (this.direction === 'asc') {
      this.direction = 'desc';
    } else {
      this.direction = 'asc';
    }
    /**
     * Sort change event - a user has indicated the sort direction
     * should be changed.
     *
     * @event sort-change
     * @type {object}
     * @property {string} direction - 'asc' or 'desc' for
     *                    ascending/descending
     * @property {string} key - The key of the toggle that was clicked.
     */
    this.dispatchEvent(new CustomEvent('sort-change', {
      detail: {
        'direction': this.direction,
        'key': this.key,
      },
      bubbles: true,
    }));
  }

  _render() {
    render(template(this), this);
  }

});