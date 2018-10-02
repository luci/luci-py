// Copyright 2018 The LUCI Authors. All rights reserved.
// Use of this source code is governed under the Apache License, Version 2.0
// that can be found in the LICENSE file.

/** @module swarming-ui/util
 * @description
 *
 * <p>
 *  A general set of useful functions.
 * </p>
 */

/** Performs an in-place, stable sort on the passed-in array using
 * the passed in comparison function for all non-null and non-undefined
 * elements.
 * @param {Array} arr - The array to sort.
 * @param {Function} comp - A function that compares two elements
 *                   in the array and returns an integer indicating
 *                   whether a is greater than, less than or equal to b.
 */
export function stableSort(arr, comp) {
    if (!arr || !comp) {
      console.warn('missing arguments to stableSort', arr, comp);
      return;
    }
    // We can guarantee a potential non-stable sort (like V8's
    // Array.prototype.sort()) to be stable by first storing the index in the
    // original sorting and using that if the original compare was 0.
    arr.forEach((e, i) => {
      if (e !== undefined && e !== null) {
        e.__sortIdx = i;
      }
    });

    arr.sort((a, b) => {
      // undefined and null elements always go last.
      if (a === undefined || a === null) {
        if (b === undefined || b === null) {
          return 0;
        }
        return 1;
      }
      if (b === undefined || b === null) {
        return -1;
      }
      let c = comp(a, b);
      if (c === 0) {
        return a.__sortIdx - b.__sortIdx;
      }
      return c;
    });
}
