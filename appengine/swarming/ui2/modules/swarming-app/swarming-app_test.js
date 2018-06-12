// Copyright 2018 The LUCI Authors. All rights reserved.
// Use of this source code is governed under the Apache License, Version 2.0
// that can be found in the LICENSE file.

import 'modules/swarming-app'
(function(){

// A reusable HTML element in which we create our element under test.
let container = document.createElement('div');
document.body.appendChild(container);

afterEach(function() {
  container.innerHTML = '';
});

// calls the test callback with one element 'ele', a created <swarming-app>.
// We can't put the describes inside the whenDefined callback because
// that doesn't work on Firefox (and possibly other places).
function createElement(test) {
  return window.customElements.whenDefined('swarming-app').then(() => {
    container.innerHTML = `
        <swarming-app testing_offline=true>
          <header>
            <aside class=hideable>Menu option</aside>
          </header>
          <main></main>
          <footer></footer>
        </swarming-app>`;
    expect(container.firstElementChild).toBeTruthy();
    test(container.firstElementChild);
  });
}

describe('swarming-app', function() {

  describe('html injection to provided content', function() {

    it('injects login element and sidebar buttons', function(done) {
      createElement((ele) => {
        let button = ele.querySelector('header button');
        expect(button).toBeTruthy();
        let spacer = ele.querySelector('header .grow');
        expect(spacer).toBeTruthy();
        let login = ele.querySelector('header oauth-login');
        expect(login).toBeTruthy();
        let spinner = ele.querySelector('header spinner-sk');
        expect(spinner).toBeTruthy();
        done();
      });
    });

    it('does not inject content if missing .hideable', function(done) {
      window.customElements.whenDefined('swarming-app').then(() => {
        container.innerHTML = `
        <swarming-app testing_offline=true>
          <header>
            <aside>Menu option</aside>
          </header>
          <main></main>
          <footer></footer>
        </swarming-app>`;
        let ele = container.firstElementChild;
        expect(ele).toBeTruthy();
        let button = ele.querySelector('header button');
        expect(button).toBeNull();
        let spacer = ele.querySelector('header .grow');
        expect(spacer).toBeNull();
        let login = ele.querySelector('header oauth-login');
        expect(login).toBeNull();
        done();
      });
    });
  });  // end describe('html injection to provided content')

  describe('sidebar', function() {
    it('should toggle', function(done) {
      createElement((ele) => {
        let button = ele.querySelector('header button');
        expect(button).toBeTruthy();
        let sidebar = ele.querySelector('header aside');
        expect(sidebar).toBeTruthy();

        expect(sidebar.classList).not.toContain('shown');
        button.click();
        expect(sidebar.classList).toContain('shown');
        button.click();
        expect(sidebar.classList).not.toContain('shown');
        done();
      });
    });
  }); // end describe('sidebar')

  describe('spinner and busy property', function() {
    it('becomes busy while there are tasks to be done', function(done) {
      createElement((ele) => {
        expect(ele.busy).toBeFalsy();
        ele.addBusyTasks(2);
        expect(ele.busy).toBeTruthy();
        ele.finishedTask();
        expect(ele.busy).toBeTruthy();
        ele.finishedTask();
        expect(ele.busy).toBeFalsy();
        done();
      });
    });

    it('keeps spinner active while busy', function(done) {
      createElement((ele) => {
        let spinner = ele.querySelector('header spinner-sk');
        expect(spinner.active).toBeFalsy();
        ele.addBusyTasks(2);
        expect(spinner.active).toBeTruthy();
        ele.finishedTask();
        expect(spinner.active).toBeTruthy();
        ele.finishedTask();
        expect(spinner.active).toBeFalsy();
        done();
      });
    });

    it('emits a busy-end task when tasks finished', function(done) {
      createElement((ele) => {
        ele.addEventListener('busy-end', (e) => {
          e.stopPropagation();
          expect(ele.busy).toBeFalsy();
          done();
        });
        ele.addBusyTasks(1);

        setTimeout(()=>{
          ele.finishedTask();
        }, 10);
      });
    });
  }); // end describe('spinner and busy property')
});

})();