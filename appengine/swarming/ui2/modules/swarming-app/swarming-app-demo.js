// Copyright 2018 The LUCI Authors. All rights reserved.
// Use of this source code is governed under the Apache License, Version 2.0
// that can be found in the LICENSE file.

import './index.js'

(function(){

let btn = document.getElementById('test-button');
btn.addEventListener('click', () => {
  let swapp = document.getElementsByTagName('swarming-app');
  swapp[0].addBusyTasks(1);
});

})();