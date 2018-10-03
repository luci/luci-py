// Copyright 2018 The LUCI Authors. All rights reserved.
// Use of this source code is governed under the Apache License, Version 2.0
// that can be found in the LICENSE file.
import './index.js'

(function(){

let display = document.getElementById('display');
// listen to event
document.addEventListener('sort-change', (e) => {
  console.log('sort change');
  let scs = document.querySelectorAll('sort-toggle');
  for (let i = 0; i < scs.length; i++) {
    scs[i].currentKey = e.detail.key;
  }
});
})();