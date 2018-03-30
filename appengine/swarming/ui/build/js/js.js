/**
 * @license
 * Copyright (c) 2014 The Polymer Project Authors. All rights reserved.
 * This code may only be used under the BSD style license found at http://polymer.github.io/LICENSE.txt
 * The complete set of authors may be found at http://polymer.github.io/AUTHORS.txt
 * The complete set of contributors may be found at http://polymer.github.io/CONTRIBUTORS.txt
 * Code distributed by Google as part of the polymer project is also
 * subject to an additional IP rights grant found at http://polymer.github.io/PATENTS.txt
 */
// @version 0.7.24
!function(){window.WebComponents=window.WebComponents||{flags:{}};var e="webcomponents-lite.js",t=document.querySelector('script[src*="'+e+'"]'),n={};if(!n.noOpts){if(location.search.slice(1).split("&").forEach(function(e){var t,o=e.split("=");o[0]&&(t=o[0].match(/wc-(.+)/))&&(n[t[1]]=o[1]||!0)}),t)for(var o,r=0;o=t.attributes[r];r++)"src"!==o.name&&(n[o.name]=o.value||!0);if(n.log&&n.log.split){var i=n.log.split(",");n.log={},i.forEach(function(e){n.log[e]=!0})}else n.log={}}n.register&&(window.CustomElements=window.CustomElements||{flags:{}},window.CustomElements.flags.register=n.register),WebComponents.flags=n}(),function(e){"use strict";function t(e){return void 0!==h[e]}function n(){s.call(this),this._isInvalid=!0}function o(e){return""==e&&n.call(this),e.toLowerCase()}function r(e){var t=e.charCodeAt(0);return t>32&&t<127&&[34,35,60,62,63,96].indexOf(t)==-1?e:encodeURIComponent(e)}function i(e){var t=e.charCodeAt(0);return t>32&&t<127&&[34,35,60,62,96].indexOf(t)==-1?e:encodeURIComponent(e)}function a(e,a,s){function c(e){g.push(e)}var d=a||"scheme start",l=0,u="",w=!1,_=!1,g=[];e:for(;(e[l-1]!=p||0==l)&&!this._isInvalid;){var b=e[l];switch(d){case"scheme start":if(!b||!m.test(b)){if(a){c("Invalid scheme.");break e}u="",d="no scheme";continue}u+=b.toLowerCase(),d="scheme";break;case"scheme":if(b&&v.test(b))u+=b.toLowerCase();else{if(":"!=b){if(a){if(p==b)break e;c("Code point not allowed in scheme: "+b);break e}u="",l=0,d="no scheme";continue}if(this._scheme=u,u="",a)break e;t(this._scheme)&&(this._isRelative=!0),d="file"==this._scheme?"relative":this._isRelative&&s&&s._scheme==this._scheme?"relative or authority":this._isRelative?"authority first slash":"scheme data"}break;case"scheme data":"?"==b?(this._query="?",d="query"):"#"==b?(this._fragment="#",d="fragment"):p!=b&&"\t"!=b&&"\n"!=b&&"\r"!=b&&(this._schemeData+=r(b));break;case"no scheme":if(s&&t(s._scheme)){d="relative";continue}c("Missing scheme."),n.call(this);break;case"relative or authority":if("/"!=b||"/"!=e[l+1]){c("Expected /, got: "+b),d="relative";continue}d="authority ignore slashes";break;case"relative":if(this._isRelative=!0,"file"!=this._scheme&&(this._scheme=s._scheme),p==b){this._host=s._host,this._port=s._port,this._path=s._path.slice(),this._query=s._query,this._username=s._username,this._password=s._password;break e}if("/"==b||"\\"==b)"\\"==b&&c("\\ is an invalid code point."),d="relative slash";else if("?"==b)this._host=s._host,this._port=s._port,this._path=s._path.slice(),this._query="?",this._username=s._username,this._password=s._password,d="query";else{if("#"!=b){var y=e[l+1],E=e[l+2];("file"!=this._scheme||!m.test(b)||":"!=y&&"|"!=y||p!=E&&"/"!=E&&"\\"!=E&&"?"!=E&&"#"!=E)&&(this._host=s._host,this._port=s._port,this._username=s._username,this._password=s._password,this._path=s._path.slice(),this._path.pop()),d="relative path";continue}this._host=s._host,this._port=s._port,this._path=s._path.slice(),this._query=s._query,this._fragment="#",this._username=s._username,this._password=s._password,d="fragment"}break;case"relative slash":if("/"!=b&&"\\"!=b){"file"!=this._scheme&&(this._host=s._host,this._port=s._port,this._username=s._username,this._password=s._password),d="relative path";continue}"\\"==b&&c("\\ is an invalid code point."),d="file"==this._scheme?"file host":"authority ignore slashes";break;case"authority first slash":if("/"!=b){c("Expected '/', got: "+b),d="authority ignore slashes";continue}d="authority second slash";break;case"authority second slash":if(d="authority ignore slashes","/"!=b){c("Expected '/', got: "+b);continue}break;case"authority ignore slashes":if("/"!=b&&"\\"!=b){d="authority";continue}c("Expected authority, got: "+b);break;case"authority":if("@"==b){w&&(c("@ already seen."),u+="%40"),w=!0;for(var L=0;L<u.length;L++){var N=u[L];if("\t"!=N&&"\n"!=N&&"\r"!=N)if(":"!=N||null!==this._password){var M=r(N);null!==this._password?this._password+=M:this._username+=M}else this._password="";else c("Invalid whitespace in authority.")}u=""}else{if(p==b||"/"==b||"\\"==b||"?"==b||"#"==b){l-=u.length,u="",d="host";continue}u+=b}break;case"file host":if(p==b||"/"==b||"\\"==b||"?"==b||"#"==b){2!=u.length||!m.test(u[0])||":"!=u[1]&&"|"!=u[1]?0==u.length?d="relative path start":(this._host=o.call(this,u),u="",d="relative path start"):d="relative path";continue}"\t"==b||"\n"==b||"\r"==b?c("Invalid whitespace in file host."):u+=b;break;case"host":case"hostname":if(":"!=b||_){if(p==b||"/"==b||"\\"==b||"?"==b||"#"==b){if(this._host=o.call(this,u),u="",d="relative path start",a)break e;continue}"\t"!=b&&"\n"!=b&&"\r"!=b?("["==b?_=!0:"]"==b&&(_=!1),u+=b):c("Invalid code point in host/hostname: "+b)}else if(this._host=o.call(this,u),u="",d="port","hostname"==a)break e;break;case"port":if(/[0-9]/.test(b))u+=b;else{if(p==b||"/"==b||"\\"==b||"?"==b||"#"==b||a){if(""!=u){var T=parseInt(u,10);T!=h[this._scheme]&&(this._port=T+""),u=""}if(a)break e;d="relative path start";continue}"\t"==b||"\n"==b||"\r"==b?c("Invalid code point in port: "+b):n.call(this)}break;case"relative path start":if("\\"==b&&c("'\\' not allowed in path."),d="relative path","/"!=b&&"\\"!=b)continue;break;case"relative path":if(p!=b&&"/"!=b&&"\\"!=b&&(a||"?"!=b&&"#"!=b))"\t"!=b&&"\n"!=b&&"\r"!=b&&(u+=r(b));else{"\\"==b&&c("\\ not allowed in relative path.");var O;(O=f[u.toLowerCase()])&&(u=O),".."==u?(this._path.pop(),"/"!=b&&"\\"!=b&&this._path.push("")):"."==u&&"/"!=b&&"\\"!=b?this._path.push(""):"."!=u&&("file"==this._scheme&&0==this._path.length&&2==u.length&&m.test(u[0])&&"|"==u[1]&&(u=u[0]+":"),this._path.push(u)),u="","?"==b?(this._query="?",d="query"):"#"==b&&(this._fragment="#",d="fragment")}break;case"query":a||"#"!=b?p!=b&&"\t"!=b&&"\n"!=b&&"\r"!=b&&(this._query+=i(b)):(this._fragment="#",d="fragment");break;case"fragment":p!=b&&"\t"!=b&&"\n"!=b&&"\r"!=b&&(this._fragment+=b)}l++}}function s(){this._scheme="",this._schemeData="",this._username="",this._password=null,this._host="",this._port="",this._path=[],this._query="",this._fragment="",this._isInvalid=!1,this._isRelative=!1}function c(e,t){void 0===t||t instanceof c||(t=new c(String(t))),this._url=e,s.call(this);var n=e.replace(/^[ \t\r\n\f]+|[ \t\r\n\f]+$/g,"");a.call(this,n,null,t)}var d=!1;if(!e.forceJURL)try{var l=new URL("b","http://a");l.pathname="c%20d",d="http://a/c%20d"===l.href}catch(u){}if(!d){var h=Object.create(null);h.ftp=21,h.file=0,h.gopher=70,h.http=80,h.https=443,h.ws=80,h.wss=443;var f=Object.create(null);f["%2e"]=".",f[".%2e"]="..",f["%2e."]="..",f["%2e%2e"]="..";var p=void 0,m=/[a-zA-Z]/,v=/[a-zA-Z0-9\+\-\.]/;c.prototype={toString:function(){return this.href},get href(){if(this._isInvalid)return this._url;var e="";return""==this._username&&null==this._password||(e=this._username+(null!=this._password?":"+this._password:"")+"@"),this.protocol+(this._isRelative?"//"+e+this.host:"")+this.pathname+this._query+this._fragment},set href(e){s.call(this),a.call(this,e)},get protocol(){return this._scheme+":"},set protocol(e){this._isInvalid||a.call(this,e+":","scheme start")},get host(){return this._isInvalid?"":this._port?this._host+":"+this._port:this._host},set host(e){!this._isInvalid&&this._isRelative&&a.call(this,e,"host")},get hostname(){return this._host},set hostname(e){!this._isInvalid&&this._isRelative&&a.call(this,e,"hostname")},get port(){return this._port},set port(e){!this._isInvalid&&this._isRelative&&a.call(this,e,"port")},get pathname(){return this._isInvalid?"":this._isRelative?"/"+this._path.join("/"):this._schemeData},set pathname(e){!this._isInvalid&&this._isRelative&&(this._path=[],a.call(this,e,"relative path start"))},get search(){return this._isInvalid||!this._query||"?"==this._query?"":this._query},set search(e){!this._isInvalid&&this._isRelative&&(this._query="?","?"==e[0]&&(e=e.slice(1)),a.call(this,e,"query"))},get hash(){return this._isInvalid||!this._fragment||"#"==this._fragment?"":this._fragment},set hash(e){this._isInvalid||(this._fragment="#","#"==e[0]&&(e=e.slice(1)),a.call(this,e,"fragment"))},get origin(){var e;if(this._isInvalid||!this._scheme)return"";switch(this._scheme){case"data":case"file":case"javascript":case"mailto":return"null"}return e=this.host,e?this._scheme+"://"+e:""}};var w=e.URL;w&&(c.createObjectURL=function(e){return w.createObjectURL.apply(w,arguments)},c.revokeObjectURL=function(e){w.revokeObjectURL(e)}),e.URL=c}}(self),"undefined"==typeof WeakMap&&!function(){var e=Object.defineProperty,t=Date.now()%1e9,n=function(){this.name="__st"+(1e9*Math.random()>>>0)+(t++ +"__")};n.prototype={set:function(t,n){var o=t[this.name];return o&&o[0]===t?o[1]=n:e(t,this.name,{value:[t,n],writable:!0}),this},get:function(e){var t;return(t=e[this.name])&&t[0]===e?t[1]:void 0},"delete":function(e){var t=e[this.name];return!(!t||t[0]!==e)&&(t[0]=t[1]=void 0,!0)},has:function(e){var t=e[this.name];return!!t&&t[0]===e}},window.WeakMap=n}(),function(e){function t(e){b.push(e),g||(g=!0,m(o))}function n(e){return window.ShadowDOMPolyfill&&window.ShadowDOMPolyfill.wrapIfNeeded(e)||e}function o(){g=!1;var e=b;b=[],e.sort(function(e,t){return e.uid_-t.uid_});var t=!1;e.forEach(function(e){var n=e.takeRecords();r(e),n.length&&(e.callback_(n,e),t=!0)}),t&&o()}function r(e){e.nodes_.forEach(function(t){var n=v.get(t);n&&n.forEach(function(t){t.observer===e&&t.removeTransientObservers()})})}function i(e,t){for(var n=e;n;n=n.parentNode){var o=v.get(n);if(o)for(var r=0;r<o.length;r++){var i=o[r],a=i.options;if(n===e||a.subtree){var s=t(a);s&&i.enqueue(s)}}}}function a(e){this.callback_=e,this.nodes_=[],this.records_=[],this.uid_=++y}function s(e,t){this.type=e,this.target=t,this.addedNodes=[],this.removedNodes=[],this.previousSibling=null,this.nextSibling=null,this.attributeName=null,this.attributeNamespace=null,this.oldValue=null}function c(e){var t=new s(e.type,e.target);return t.addedNodes=e.addedNodes.slice(),t.removedNodes=e.removedNodes.slice(),t.previousSibling=e.previousSibling,t.nextSibling=e.nextSibling,t.attributeName=e.attributeName,t.attributeNamespace=e.attributeNamespace,t.oldValue=e.oldValue,t}function d(e,t){return E=new s(e,t)}function l(e){return L?L:(L=c(E),L.oldValue=e,L)}function u(){E=L=void 0}function h(e){return e===L||e===E}function f(e,t){return e===t?e:L&&h(e)?L:null}function p(e,t,n){this.observer=e,this.target=t,this.options=n,this.transientObservedNodes=[]}if(!e.JsMutationObserver){var m,v=new WeakMap;if(/Trident|Edge/.test(navigator.userAgent))m=setTimeout;else if(window.setImmediate)m=window.setImmediate;else{var w=[],_=String(Math.random());window.addEventListener("message",function(e){if(e.data===_){var t=w;w=[],t.forEach(function(e){e()})}}),m=function(e){w.push(e),window.postMessage(_,"*")}}var g=!1,b=[],y=0;a.prototype={observe:function(e,t){if(e=n(e),!t.childList&&!t.attributes&&!t.characterData||t.attributeOldValue&&!t.attributes||t.attributeFilter&&t.attributeFilter.length&&!t.attributes||t.characterDataOldValue&&!t.characterData)throw new SyntaxError;var o=v.get(e);o||v.set(e,o=[]);for(var r,i=0;i<o.length;i++)if(o[i].observer===this){r=o[i],r.removeListeners(),r.options=t;break}r||(r=new p(this,e,t),o.push(r),this.nodes_.push(e)),r.addListeners()},disconnect:function(){this.nodes_.forEach(function(e){for(var t=v.get(e),n=0;n<t.length;n++){var o=t[n];if(o.observer===this){o.removeListeners(),t.splice(n,1);break}}},this),this.records_=[]},takeRecords:function(){var e=this.records_;return this.records_=[],e}};var E,L;p.prototype={enqueue:function(e){var n=this.observer.records_,o=n.length;if(n.length>0){var r=n[o-1],i=f(r,e);if(i)return void(n[o-1]=i)}else t(this.observer);n[o]=e},addListeners:function(){this.addListeners_(this.target)},addListeners_:function(e){var t=this.options;t.attributes&&e.addEventListener("DOMAttrModified",this,!0),t.characterData&&e.addEventListener("DOMCharacterDataModified",this,!0),t.childList&&e.addEventListener("DOMNodeInserted",this,!0),(t.childList||t.subtree)&&e.addEventListener("DOMNodeRemoved",this,!0)},removeListeners:function(){this.removeListeners_(this.target)},removeListeners_:function(e){var t=this.options;t.attributes&&e.removeEventListener("DOMAttrModified",this,!0),t.characterData&&e.removeEventListener("DOMCharacterDataModified",this,!0),t.childList&&e.removeEventListener("DOMNodeInserted",this,!0),(t.childList||t.subtree)&&e.removeEventListener("DOMNodeRemoved",this,!0)},addTransientObserver:function(e){if(e!==this.target){this.addListeners_(e),this.transientObservedNodes.push(e);var t=v.get(e);t||v.set(e,t=[]),t.push(this)}},removeTransientObservers:function(){var e=this.transientObservedNodes;this.transientObservedNodes=[],e.forEach(function(e){this.removeListeners_(e);for(var t=v.get(e),n=0;n<t.length;n++)if(t[n]===this){t.splice(n,1);break}},this)},handleEvent:function(e){switch(e.stopImmediatePropagation(),e.type){case"DOMAttrModified":var t=e.attrName,n=e.relatedNode.namespaceURI,o=e.target,r=new d("attributes",o);r.attributeName=t,r.attributeNamespace=n;var a=e.attrChange===MutationEvent.ADDITION?null:e.prevValue;i(o,function(e){if(e.attributes&&(!e.attributeFilter||!e.attributeFilter.length||e.attributeFilter.indexOf(t)!==-1||e.attributeFilter.indexOf(n)!==-1))return e.attributeOldValue?l(a):r});break;case"DOMCharacterDataModified":var o=e.target,r=d("characterData",o),a=e.prevValue;i(o,function(e){if(e.characterData)return e.characterDataOldValue?l(a):r});break;case"DOMNodeRemoved":this.addTransientObserver(e.target);case"DOMNodeInserted":var s,c,h=e.target;"DOMNodeInserted"===e.type?(s=[h],c=[]):(s=[],c=[h]);var f=h.previousSibling,p=h.nextSibling,r=d("childList",e.target.parentNode);r.addedNodes=s,r.removedNodes=c,r.previousSibling=f,r.nextSibling=p,i(e.relatedNode,function(e){if(e.childList)return r})}u()}},e.JsMutationObserver=a,e.MutationObserver||(e.MutationObserver=a,a._isPolyfilled=!0)}}(self),function(){function e(e){switch(e){case"&":return"&amp;";case"<":return"&lt;";case">":return"&gt;";case" ":return"&nbsp;"}}function t(t){return t.replace(u,e)}var n="undefined"==typeof HTMLTemplateElement;/Trident/.test(navigator.userAgent)&&!function(){var e=document.importNode;document.importNode=function(){var t=e.apply(document,arguments);if(t.nodeType===Node.DOCUMENT_FRAGMENT_NODE){var n=document.createDocumentFragment();return n.appendChild(t),n}return t}}();var o=function(){if(!n){var e=document.createElement("template"),t=document.createElement("template");t.content.appendChild(document.createElement("div")),e.content.appendChild(t);var o=e.cloneNode(!0);return 0===o.content.childNodes.length||0===o.content.firstChild.content.childNodes.length}}(),r="template",i=function(){};if(n){var a=document.implementation.createHTMLDocument("template"),s=!0,c=document.createElement("style");c.textContent=r+"{display:none;}";var d=document.head;d.insertBefore(c,d.firstElementChild),i.prototype=Object.create(HTMLElement.prototype),i.decorate=function(e){if(!e.content){e.content=a.createDocumentFragment();for(var n;n=e.firstChild;)e.content.appendChild(n);if(e.cloneNode=function(e){return i.cloneNode(this,e)},s)try{Object.defineProperty(e,"innerHTML",{get:function(){for(var e="",n=this.content.firstChild;n;n=n.nextSibling)e+=n.outerHTML||t(n.data);return e},set:function(e){for(a.body.innerHTML=e,i.bootstrap(a);this.content.firstChild;)this.content.removeChild(this.content.firstChild);for(;a.body.firstChild;)this.content.appendChild(a.body.firstChild)},configurable:!0})}catch(o){s=!1}i.bootstrap(e.content)}},i.bootstrap=function(e){for(var t,n=e.querySelectorAll(r),o=0,a=n.length;o<a&&(t=n[o]);o++)i.decorate(t)},document.addEventListener("DOMContentLoaded",function(){i.bootstrap(document)});var l=document.createElement;document.createElement=function(){"use strict";var e=l.apply(document,arguments);return"template"===e.localName&&i.decorate(e),e};var u=/[&\u00A0<>]/g}if(n||o){var h=Node.prototype.cloneNode;i.cloneNode=function(e,t){var n=h.call(e,!1);return this.decorate&&this.decorate(n),t&&(n.content.appendChild(h.call(e.content,!0)),this.fixClonedDom(n.content,e.content)),n},i.fixClonedDom=function(e,t){if(t.querySelectorAll)for(var n,o,i=t.querySelectorAll(r),a=e.querySelectorAll(r),s=0,c=a.length;s<c;s++)o=i[s],n=a[s],this.decorate&&this.decorate(o),n.parentNode.replaceChild(o.cloneNode(!0),n)};var f=document.importNode;Node.prototype.cloneNode=function(e){var t=h.call(this,e);return e&&i.fixClonedDom(t,this),t},document.importNode=function(e,t){if(e.localName===r)return i.cloneNode(e,t);var n=f.call(document,e,t);return t&&i.fixClonedDom(n,e),n},o&&(HTMLTemplateElement.prototype.cloneNode=function(e){return i.cloneNode(this,e)})}n&&(window.HTMLTemplateElement=i)}(),function(e){"use strict";if(!window.performance||!window.performance.now){var t=Date.now();window.performance={now:function(){return Date.now()-t}}}window.requestAnimationFrame||(window.requestAnimationFrame=function(){var e=window.webkitRequestAnimationFrame||window.mozRequestAnimationFrame;return e?function(t){return e(function(){t(performance.now())})}:function(e){return window.setTimeout(e,1e3/60)}}()),window.cancelAnimationFrame||(window.cancelAnimationFrame=function(){return window.webkitCancelAnimationFrame||window.mozCancelAnimationFrame||function(e){clearTimeout(e)}}());var n=function(){var e=document.createEvent("Event");return e.initEvent("foo",!0,!0),e.preventDefault(),e.defaultPrevented}();if(!n){var o=Event.prototype.preventDefault;Event.prototype.preventDefault=function(){this.cancelable&&(o.call(this),Object.defineProperty(this,"defaultPrevented",{get:function(){return!0},configurable:!0}))}}var r=/Trident/.test(navigator.userAgent);if((!window.CustomEvent||r&&"function"!=typeof window.CustomEvent)&&(window.CustomEvent=function(e,t){t=t||{};var n=document.createEvent("CustomEvent");return n.initCustomEvent(e,Boolean(t.bubbles),Boolean(t.cancelable),t.detail),n},window.CustomEvent.prototype=window.Event.prototype),!window.Event||r&&"function"!=typeof window.Event){var i=window.Event;window.Event=function(e,t){t=t||{};var n=document.createEvent("Event");return n.initEvent(e,Boolean(t.bubbles),Boolean(t.cancelable)),n},window.Event.prototype=i.prototype}}(window.WebComponents),window.HTMLImports=window.HTMLImports||{flags:{}},function(e){function t(e,t){t=t||p,o(function(){i(e,t)},t)}function n(e){return"complete"===e.readyState||e.readyState===w}function o(e,t){if(n(t))e&&e();else{var r=function(){"complete"!==t.readyState&&t.readyState!==w||(t.removeEventListener(_,r),o(e,t))};t.addEventListener(_,r)}}function r(e){e.target.__loaded=!0}function i(e,t){function n(){c==d&&e&&e({allImports:s,loadedImports:l,errorImports:u})}function o(e){r(e),l.push(this),c++,n()}function i(e){u.push(this),c++,n()}var s=t.querySelectorAll("link[rel=import]"),c=0,d=s.length,l=[],u=[];if(d)for(var h,f=0;f<d&&(h=s[f]);f++)a(h)?(l.push(this),c++,n()):(h.addEventListener("load",o),h.addEventListener("error",i));else n()}function a(e){return u?e.__loaded||e["import"]&&"loading"!==e["import"].readyState:e.__importParsed}function s(e){for(var t,n=0,o=e.length;n<o&&(t=e[n]);n++)c(t)&&d(t)}function c(e){return"link"===e.localName&&"import"===e.rel}function d(e){var t=e["import"];t?r({target:e}):(e.addEventListener("load",r),e.addEventListener("error",r))}var l="import",u=Boolean(l in document.createElement("link")),h=Boolean(window.ShadowDOMPolyfill),f=function(e){return h?window.ShadowDOMPolyfill.wrapIfNeeded(e):e},p=f(document),m={get:function(){var e=window.HTMLImports.currentScript||document.currentScript||("complete"!==document.readyState?document.scripts[document.scripts.length-1]:null);return f(e)},configurable:!0};Object.defineProperty(document,"_currentScript",m),Object.defineProperty(p,"_currentScript",m);var v=/Trident/.test(navigator.userAgent),w=v?"complete":"interactive",_="readystatechange";u&&(new MutationObserver(function(e){for(var t,n=0,o=e.length;n<o&&(t=e[n]);n++)t.addedNodes&&s(t.addedNodes)}).observe(document.head,{childList:!0}),function(){if("loading"===document.readyState)for(var e,t=document.querySelectorAll("link[rel=import]"),n=0,o=t.length;n<o&&(e=t[n]);n++)d(e)}()),t(function(e){window.HTMLImports.ready=!0,window.HTMLImports.readyTime=(new Date).getTime();var t=p.createEvent("CustomEvent");t.initCustomEvent("HTMLImportsLoaded",!0,!0,e),p.dispatchEvent(t)}),e.IMPORT_LINK_TYPE=l,e.useNative=u,e.rootDocument=p,e.whenReady=t,e.isIE=v}(window.HTMLImports),function(e){var t=[],n=function(e){t.push(e)},o=function(){t.forEach(function(t){t(e)})};e.addModule=n,e.initializeModules=o}(window.HTMLImports),window.HTMLImports.addModule(function(e){var t=/(url\()([^)]*)(\))/g,n=/(@import[\s]+(?!url\())([^;]*)(;)/g,o={resolveUrlsInStyle:function(e,t){var n=e.ownerDocument,o=n.createElement("a");return e.textContent=this.resolveUrlsInCssText(e.textContent,t,o),e},resolveUrlsInCssText:function(e,o,r){var i=this.replaceUrls(e,r,o,t);return i=this.replaceUrls(i,r,o,n)},replaceUrls:function(e,t,n,o){return e.replace(o,function(e,o,r,i){var a=r.replace(/["']/g,"");return n&&(a=new URL(a,n).href),t.href=a,a=t.href,o+"'"+a+"'"+i})}};e.path=o}),window.HTMLImports.addModule(function(e){var t={async:!0,ok:function(e){return e.status>=200&&e.status<300||304===e.status||0===e.status},load:function(n,o,r){var i=new XMLHttpRequest;return(e.flags.debug||e.flags.bust)&&(n+="?"+Math.random()),i.open("GET",n,t.async),i.addEventListener("readystatechange",function(e){if(4===i.readyState){var n=null;try{var a=i.getResponseHeader("Location");a&&(n="/"===a.substr(0,1)?location.origin+a:a)}catch(e){console.error(e.message)}o.call(r,!t.ok(i)&&i,i.response||i.responseText,n)}}),i.send(),i},loadDocument:function(e,t,n){this.load(e,t,n).responseType="document"}};e.xhr=t}),window.HTMLImports.addModule(function(e){var t=e.xhr,n=e.flags,o=function(e,t){this.cache={},this.onload=e,this.oncomplete=t,this.inflight=0,this.pending={}};o.prototype={addNodes:function(e){this.inflight+=e.length;for(var t,n=0,o=e.length;n<o&&(t=e[n]);n++)this.require(t);this.checkDone()},addNode:function(e){this.inflight++,this.require(e),this.checkDone()},require:function(e){var t=e.src||e.href;e.__nodeUrl=t,this.dedupe(t,e)||this.fetch(t,e)},dedupe:function(e,t){if(this.pending[e])return this.pending[e].push(t),!0;return this.cache[e]?(this.onload(e,t,this.cache[e]),this.tail(),!0):(this.pending[e]=[t],!1)},fetch:function(e,o){if(n.load&&console.log("fetch",e,o),e)if(e.match(/^data:/)){var r=e.split(","),i=r[0],a=r[1];a=i.indexOf(";base64")>-1?atob(a):decodeURIComponent(a),setTimeout(function(){this.receive(e,o,null,a)}.bind(this),0)}else{var s=function(t,n,r){this.receive(e,o,t,n,r)}.bind(this);t.load(e,s)}else setTimeout(function(){this.receive(e,o,{error:"href must be specified"},null)}.bind(this),0)},receive:function(e,t,n,o,r){this.cache[e]=o;for(var i,a=this.pending[e],s=0,c=a.length;s<c&&(i=a[s]);s++)this.onload(e,i,o,n,r),this.tail();this.pending[e]=null},tail:function(){--this.inflight,this.checkDone()},checkDone:function(){this.inflight||this.oncomplete()}},e.Loader=o}),window.HTMLImports.addModule(function(e){var t=function(e){this.addCallback=e,this.mo=new MutationObserver(this.handler.bind(this))};t.prototype={handler:function(e){for(var t,n=0,o=e.length;n<o&&(t=e[n]);n++)"childList"===t.type&&t.addedNodes.length&&this.addedNodes(t.addedNodes)},addedNodes:function(e){this.addCallback&&this.addCallback(e);for(var t,n=0,o=e.length;n<o&&(t=e[n]);n++)t.children&&t.children.length&&this.addedNodes(t.children)},observe:function(e){this.mo.observe(e,{childList:!0,subtree:!0})}},e.Observer=t}),window.HTMLImports.addModule(function(e){function t(e){return"link"===e.localName&&e.rel===l}function n(e){var t=o(e);return"data:text/javascript;charset=utf-8,"+encodeURIComponent(t)}function o(e){return e.textContent+r(e)}function r(e){var t=e.ownerDocument;t.__importedScripts=t.__importedScripts||0;var n=e.ownerDocument.baseURI,o=t.__importedScripts?"-"+t.__importedScripts:"";return t.__importedScripts++,"\n//# sourceURL="+n+o+".js\n"}function i(e){var t=e.ownerDocument.createElement("style");return t.textContent=e.textContent,a.resolveUrlsInStyle(t),t}var a=e.path,s=e.rootDocument,c=e.flags,d=e.isIE,l=e.IMPORT_LINK_TYPE,u="link[rel="+l+"]",h={documentSelectors:u,importsSelectors:[u,"link[rel=stylesheet]:not([type])","style:not([type])","script:not([type])",'script[type="application/javascript"]','script[type="text/javascript"]'].join(","),map:{link:"parseLink",script:"parseScript",style:"parseStyle"},dynamicElements:[],parseNext:function(){var e=this.nextToParse();e&&this.parse(e)},parse:function(e){if(this.isParsed(e))return void(c.parse&&console.log("[%s] is already parsed",e.localName));var t=this[this.map[e.localName]];t&&(this.markParsing(e),t.call(this,e))},parseDynamic:function(e,t){this.dynamicElements.push(e),t||this.parseNext()},markParsing:function(e){c.parse&&console.log("parsing",e),this.parsingElement=e},markParsingComplete:function(e){e.__importParsed=!0,this.markDynamicParsingComplete(e),e.__importElement&&(e.__importElement.__importParsed=!0,this.markDynamicParsingComplete(e.__importElement)),this.parsingElement=null,c.parse&&console.log("completed",e)},markDynamicParsingComplete:function(e){var t=this.dynamicElements.indexOf(e);t>=0&&this.dynamicElements.splice(t,1)},parseImport:function(e){if(e["import"]=e.__doc,window.HTMLImports.__importsParsingHook&&window.HTMLImports.__importsParsingHook(e),e["import"]&&(e["import"].__importParsed=!0),this.markParsingComplete(e),e.__resource&&!e.__error?e.dispatchEvent(new CustomEvent("load",{bubbles:!1})):e.dispatchEvent(new CustomEvent("error",{bubbles:!1})),e.__pending)for(var t;e.__pending.length;)t=e.__pending.shift(),t&&t({target:e});this.parseNext()},parseLink:function(e){t(e)?this.parseImport(e):(e.href=e.href,this.parseGeneric(e))},parseStyle:function(e){var t=e;e=i(e),t.__appliedElement=e,e.__importElement=t,this.parseGeneric(e)},parseGeneric:function(e){this.trackElement(e),this.addElementToDocument(e)},rootImportForElement:function(e){for(var t=e;t.ownerDocument.__importLink;)t=t.ownerDocument.__importLink;return t},addElementToDocument:function(e){var t=this.rootImportForElement(e.__importElement||e);t.parentNode.insertBefore(e,t)},trackElement:function(e,t){var n=this,o=function(r){e.removeEventListener("load",o),e.removeEventListener("error",o),t&&t(r),n.markParsingComplete(e),n.parseNext()};if(e.addEventListener("load",o),e.addEventListener("error",o),d&&"style"===e.localName){var r=!1;if(e.textContent.indexOf("@import")==-1)r=!0;else if(e.sheet){r=!0;for(var i,a=e.sheet.cssRules,s=a?a.length:0,c=0;c<s&&(i=a[c]);c++)i.type===CSSRule.IMPORT_RULE&&(r=r&&Boolean(i.styleSheet))}r&&setTimeout(function(){e.dispatchEvent(new CustomEvent("load",{bubbles:!1}))})}},parseScript:function(t){var o=document.createElement("script");o.__importElement=t,o.src=t.src?t.src:n(t),e.currentScript=t,this.trackElement(o,function(t){o.parentNode&&o.parentNode.removeChild(o),e.currentScript=null}),this.addElementToDocument(o)},nextToParse:function(){return this._mayParse=[],!this.parsingElement&&(this.nextToParseInDoc(s)||this.nextToParseDynamic())},nextToParseInDoc:function(e,n){if(e&&this._mayParse.indexOf(e)<0){this._mayParse.push(e);for(var o,r=e.querySelectorAll(this.parseSelectorsForNode(e)),i=0,a=r.length;i<a&&(o=r[i]);i++)if(!this.isParsed(o))return this.hasResource(o)?t(o)?this.nextToParseInDoc(o.__doc,o):o:void 0}return n},nextToParseDynamic:function(){return this.dynamicElements[0]},parseSelectorsForNode:function(e){var t=e.ownerDocument||e;return t===s?this.documentSelectors:this.importsSelectors},isParsed:function(e){return e.__importParsed},needsDynamicParsing:function(e){return this.dynamicElements.indexOf(e)>=0},hasResource:function(e){return!t(e)||void 0!==e.__doc}};e.parser=h,e.IMPORT_SELECTOR=u}),window.HTMLImports.addModule(function(e){function t(e){return n(e,a)}function n(e,t){return"link"===e.localName&&e.getAttribute("rel")===t}function o(e){return!!Object.getOwnPropertyDescriptor(e,"baseURI")}function r(e,t){var n=document.implementation.createHTMLDocument(a);n._URL=t;var r=n.createElement("base");r.setAttribute("href",t),n.baseURI||o(n)||Object.defineProperty(n,"baseURI",{value:t});var i=n.createElement("meta");return i.setAttribute("charset","utf-8"),n.head.appendChild(i),n.head.appendChild(r),n.body.innerHTML=e,window.HTMLTemplateElement&&HTMLTemplateElement.bootstrap&&HTMLTemplateElement.bootstrap(n),n}var i=e.flags,a=e.IMPORT_LINK_TYPE,s=e.IMPORT_SELECTOR,c=e.rootDocument,d=e.Loader,l=e.Observer,u=e.parser,h={documents:{},documentPreloadSelectors:s,importsPreloadSelectors:[s].join(","),loadNode:function(e){f.addNode(e)},loadSubtree:function(e){var t=this.marshalNodes(e);f.addNodes(t)},marshalNodes:function(e){return e.querySelectorAll(this.loadSelectorsForNode(e))},loadSelectorsForNode:function(e){var t=e.ownerDocument||e;return t===c?this.documentPreloadSelectors:this.importsPreloadSelectors},loaded:function(e,n,o,a,s){if(i.load&&console.log("loaded",e,n),n.__resource=o,n.__error=a,t(n)){var c=this.documents[e];void 0===c&&(c=a?null:r(o,s||e),c&&(c.__importLink=n,this.bootDocument(c)),this.documents[e]=c),n.__doc=c}u.parseNext()},bootDocument:function(e){this.loadSubtree(e),this.observer.observe(e),u.parseNext()},loadedAll:function(){u.parseNext()}},f=new d(h.loaded.bind(h),h.loadedAll.bind(h));if(h.observer=new l,!document.baseURI){var p={get:function(){var e=document.querySelector("base");return e?e.href:window.location.href},configurable:!0};Object.defineProperty(document,"baseURI",p),Object.defineProperty(c,"baseURI",p)}e.importer=h,e.importLoader=f}),window.HTMLImports.addModule(function(e){var t=e.parser,n=e.importer,o={added:function(e){for(var o,r,i,a,s=0,c=e.length;s<c&&(a=e[s]);s++)o||(o=a.ownerDocument,r=t.isParsed(o)),i=this.shouldLoadNode(a),i&&n.loadNode(a),this.shouldParseNode(a)&&r&&t.parseDynamic(a,i)},shouldLoadNode:function(e){return 1===e.nodeType&&r.call(e,n.loadSelectorsForNode(e))},shouldParseNode:function(e){return 1===e.nodeType&&r.call(e,t.parseSelectorsForNode(e))}};n.observer.addCallback=o.added.bind(o);var r=HTMLElement.prototype.matches||HTMLElement.prototype.matchesSelector||HTMLElement.prototype.webkitMatchesSelector||HTMLElement.prototype.mozMatchesSelector||HTMLElement.prototype.msMatchesSelector}),function(e){function t(){window.HTMLImports.importer.bootDocument(o)}var n=e.initializeModules;e.isIE;if(!e.useNative){n();var o=e.rootDocument;"complete"===document.readyState||"interactive"===document.readyState&&!window.attachEvent?t():document.addEventListener("DOMContentLoaded",t)}}(window.HTMLImports),window.CustomElements=window.CustomElements||{flags:{}},function(e){var t=e.flags,n=[],o=function(e){n.push(e)},r=function(){n.forEach(function(t){t(e)})};e.addModule=o,e.initializeModules=r,e.hasNative=Boolean(document.registerElement),e.isIE=/Trident/.test(navigator.userAgent),e.useNative=!t.register&&e.hasNative&&!window.ShadowDOMPolyfill&&(!window.HTMLImports||window.HTMLImports.useNative)}(window.CustomElements),window.CustomElements.addModule(function(e){function t(e,t){n(e,function(e){return!!t(e)||void o(e,t)}),o(e,t)}function n(e,t,o){var r=e.firstElementChild;if(!r)for(r=e.firstChild;r&&r.nodeType!==Node.ELEMENT_NODE;)r=r.nextSibling;for(;r;)t(r,o)!==!0&&n(r,t,o),r=r.nextElementSibling;return null}function o(e,n){for(var o=e.shadowRoot;o;)t(o,n),o=o.olderShadowRoot}function r(e,t){i(e,t,[])}function i(e,t,n){if(e=window.wrap(e),!(n.indexOf(e)>=0)){n.push(e);for(var o,r=e.querySelectorAll("link[rel="+a+"]"),s=0,c=r.length;s<c&&(o=r[s]);s++)o["import"]&&i(o["import"],t,n);t(e)}}var a=window.HTMLImports?window.HTMLImports.IMPORT_LINK_TYPE:"none";e.forDocumentTree=r,e.forSubtree=t}),window.CustomElements.addModule(function(e){function t(e,t){return n(e,t)||o(e,t)}function n(t,n){return!!e.upgrade(t,n)||void(n&&a(t))}function o(e,t){g(e,function(e){if(n(e,t))return!0})}function r(e){L.push(e),E||(E=!0,setTimeout(i))}function i(){E=!1;for(var e,t=L,n=0,o=t.length;n<o&&(e=t[n]);n++)e();L=[]}function a(e){y?r(function(){s(e);
}):s(e)}function s(e){e.__upgraded__&&!e.__attached&&(e.__attached=!0,e.attachedCallback&&e.attachedCallback())}function c(e){d(e),g(e,function(e){d(e)})}function d(e){y?r(function(){l(e)}):l(e)}function l(e){e.__upgraded__&&e.__attached&&(e.__attached=!1,e.detachedCallback&&e.detachedCallback())}function u(e){for(var t=e,n=window.wrap(document);t;){if(t==n)return!0;t=t.parentNode||t.nodeType===Node.DOCUMENT_FRAGMENT_NODE&&t.host}}function h(e){if(e.shadowRoot&&!e.shadowRoot.__watched){_.dom&&console.log("watching shadow-root for: ",e.localName);for(var t=e.shadowRoot;t;)m(t),t=t.olderShadowRoot}}function f(e,n){if(_.dom){var o=n[0];if(o&&"childList"===o.type&&o.addedNodes&&o.addedNodes){for(var r=o.addedNodes[0];r&&r!==document&&!r.host;)r=r.parentNode;var i=r&&(r.URL||r._URL||r.host&&r.host.localName)||"";i=i.split("/?").shift().split("/").pop()}console.group("mutations (%d) [%s]",n.length,i||"")}var a=u(e);n.forEach(function(e){"childList"===e.type&&(N(e.addedNodes,function(e){e.localName&&t(e,a)}),N(e.removedNodes,function(e){e.localName&&c(e)}))}),_.dom&&console.groupEnd()}function p(e){for(e=window.wrap(e),e||(e=window.wrap(document));e.parentNode;)e=e.parentNode;var t=e.__observer;t&&(f(e,t.takeRecords()),i())}function m(e){if(!e.__observer){var t=new MutationObserver(f.bind(this,e));t.observe(e,{childList:!0,subtree:!0}),e.__observer=t}}function v(e){e=window.wrap(e),_.dom&&console.group("upgradeDocument: ",e.baseURI.split("/").pop());var n=e===window.wrap(document);t(e,n),m(e),_.dom&&console.groupEnd()}function w(e){b(e,v)}var _=e.flags,g=e.forSubtree,b=e.forDocumentTree,y=window.MutationObserver._isPolyfilled&&_["throttle-attached"];e.hasPolyfillMutations=y,e.hasThrottledAttached=y;var E=!1,L=[],N=Array.prototype.forEach.call.bind(Array.prototype.forEach),M=Element.prototype.createShadowRoot;M&&(Element.prototype.createShadowRoot=function(){var e=M.call(this);return window.CustomElements.watchShadow(this),e}),e.watchShadow=h,e.upgradeDocumentTree=w,e.upgradeDocument=v,e.upgradeSubtree=o,e.upgradeAll=t,e.attached=a,e.takeRecords=p}),window.CustomElements.addModule(function(e){function t(t,o){if("template"===t.localName&&window.HTMLTemplateElement&&HTMLTemplateElement.decorate&&HTMLTemplateElement.decorate(t),!t.__upgraded__&&t.nodeType===Node.ELEMENT_NODE){var r=t.getAttribute("is"),i=e.getRegisteredDefinition(t.localName)||e.getRegisteredDefinition(r);if(i&&(r&&i.tag==t.localName||!r&&!i["extends"]))return n(t,i,o)}}function n(t,n,r){return a.upgrade&&console.group("upgrade:",t.localName),n.is&&t.setAttribute("is",n.is),o(t,n),t.__upgraded__=!0,i(t),r&&e.attached(t),e.upgradeSubtree(t,r),a.upgrade&&console.groupEnd(),t}function o(e,t){Object.__proto__?e.__proto__=t.prototype:(r(e,t.prototype,t["native"]),e.__proto__=t.prototype)}function r(e,t,n){for(var o={},r=t;r!==n&&r!==HTMLElement.prototype;){for(var i,a=Object.getOwnPropertyNames(r),s=0;i=a[s];s++)o[i]||(Object.defineProperty(e,i,Object.getOwnPropertyDescriptor(r,i)),o[i]=1);r=Object.getPrototypeOf(r)}}function i(e){e.createdCallback&&e.createdCallback()}var a=e.flags;e.upgrade=t,e.upgradeWithDefinition=n,e.implementPrototype=o}),window.CustomElements.addModule(function(e){function t(t,o){var c=o||{};if(!t)throw new Error("document.registerElement: first argument `name` must not be empty");if(t.indexOf("-")<0)throw new Error("document.registerElement: first argument ('name') must contain a dash ('-'). Argument provided was '"+String(t)+"'.");if(r(t))throw new Error("Failed to execute 'registerElement' on 'Document': Registration failed for type '"+String(t)+"'. The type name is invalid.");if(d(t))throw new Error("DuplicateDefinitionError: a type with name '"+String(t)+"' is already registered");return c.prototype||(c.prototype=Object.create(HTMLElement.prototype)),c.__name=t.toLowerCase(),c["extends"]&&(c["extends"]=c["extends"].toLowerCase()),c.lifecycle=c.lifecycle||{},c.ancestry=i(c["extends"]),a(c),s(c),n(c.prototype),l(c.__name,c),c.ctor=u(c),c.ctor.prototype=c.prototype,c.prototype.constructor=c.ctor,e.ready&&v(document),c.ctor}function n(e){if(!e.setAttribute._polyfilled){var t=e.setAttribute;e.setAttribute=function(e,n){o.call(this,e,n,t)};var n=e.removeAttribute;e.removeAttribute=function(e){o.call(this,e,null,n)},e.setAttribute._polyfilled=!0}}function o(e,t,n){e=e.toLowerCase();var o=this.getAttribute(e);n.apply(this,arguments);var r=this.getAttribute(e);this.attributeChangedCallback&&r!==o&&this.attributeChangedCallback(e,o,r)}function r(e){for(var t=0;t<y.length;t++)if(e===y[t])return!0}function i(e){var t=d(e);return t?i(t["extends"]).concat([t]):[]}function a(e){for(var t,n=e["extends"],o=0;t=e.ancestry[o];o++)n=t.is&&t.tag;e.tag=n||e.__name,n&&(e.is=e.__name)}function s(e){if(!Object.__proto__){var t=HTMLElement.prototype;if(e.is){var n=document.createElement(e.tag);t=Object.getPrototypeOf(n)}for(var o,r=e.prototype,i=!1;r;)r==t&&(i=!0),o=Object.getPrototypeOf(r),o&&(r.__proto__=o),r=o;i||console.warn(e.tag+" prototype not found in prototype chain for "+e.is),e["native"]=t}}function c(e){return _(N(e.tag),e)}function d(e){if(e)return E[e.toLowerCase()]}function l(e,t){E[e]=t}function u(e){return function(){return c(e)}}function h(e,t,n){return e===L?f(t,n):M(e,t)}function f(e,t){e&&(e=e.toLowerCase()),t&&(t=t.toLowerCase());var n=d(t||e);if(n){if(e==n.tag&&t==n.is)return new n.ctor;if(!t&&!n.is)return new n.ctor}var o;return t?(o=f(e),o.setAttribute("is",t),o):(o=N(e),e.indexOf("-")>=0&&g(o,HTMLElement),o)}function p(e,t){var n=e[t];e[t]=function(){var e=n.apply(this,arguments);return w(e),e}}var m,v=(e.isIE,e.upgradeDocumentTree),w=e.upgradeAll,_=e.upgradeWithDefinition,g=e.implementPrototype,b=e.useNative,y=["annotation-xml","color-profile","font-face","font-face-src","font-face-uri","font-face-format","font-face-name","missing-glyph"],E={},L="http://www.w3.org/1999/xhtml",N=document.createElement.bind(document),M=document.createElementNS.bind(document);m=Object.__proto__||b?function(e,t){return e instanceof t}:function(e,t){if(e instanceof t)return!0;for(var n=e;n;){if(n===t.prototype)return!0;n=n.__proto__}return!1},p(Node.prototype,"cloneNode"),p(document,"importNode"),document.registerElement=t,document.createElement=f,document.createElementNS=h,e.registry=E,e["instanceof"]=m,e.reservedTagList=y,e.getRegisteredDefinition=d,document.register=document.registerElement}),function(e){function t(){i(window.wrap(document)),window.CustomElements.ready=!0;var e=window.requestAnimationFrame||function(e){setTimeout(e,16)};e(function(){setTimeout(function(){window.CustomElements.readyTime=Date.now(),window.HTMLImports&&(window.CustomElements.elapsed=window.CustomElements.readyTime-window.HTMLImports.readyTime),document.dispatchEvent(new CustomEvent("WebComponentsReady",{bubbles:!0}))})})}var n=e.useNative,o=e.initializeModules;e.isIE;if(n){var r=function(){};e.watchShadow=r,e.upgrade=r,e.upgradeAll=r,e.upgradeDocumentTree=r,e.upgradeSubtree=r,e.takeRecords=r,e["instanceof"]=function(e,t){return e instanceof t}}else o();var i=e.upgradeDocumentTree,a=e.upgradeDocument;if(window.wrap||(window.ShadowDOMPolyfill?(window.wrap=window.ShadowDOMPolyfill.wrapIfNeeded,window.unwrap=window.ShadowDOMPolyfill.unwrapIfNeeded):window.wrap=window.unwrap=function(e){return e}),window.HTMLImports&&(window.HTMLImports.__importsParsingHook=function(e){e["import"]&&a(wrap(e["import"]))}),"complete"===document.readyState||e.flags.eager)t();else if("interactive"!==document.readyState||window.attachEvent||window.HTMLImports&&!window.HTMLImports.ready){var s=window.HTMLImports&&!window.HTMLImports.ready?"HTMLImportsLoaded":"DOMContentLoaded";window.addEventListener(s,t)}else t()}(window.CustomElements),function(e){var t=document.createElement("style");t.textContent="body {transition: opacity ease-in 0.2s; } \nbody[unresolved] {opacity: 0; display: block; overflow: hidden; position: relative; } \n";var n=document.querySelector("head");n.insertBefore(t,n.firstChild)}(window.WebComponents);(function(){var customUtils = {};
/**
 * Return an array with the numbers from 0 to n-1, in a random order
 */
function getRandomArray (n) {
  var res, next;

  if (n === 0) { return []; }
  if (n === 1) { return [0]; }

  res = getRandomArray(n - 1);
  next = Math.floor(Math.random() * n);
  res.splice(next, 0, n - 1);   // Add n-1 at a random position in the array

  return res;
};
customUtils.getRandomArray = getRandomArray;


/*
 * Default compareKeys function will work for numbers, strings and dates
 */
function defaultCompareKeysFunction (a, b) {
  if (a < b) { return -1; }
  if (a > b) { return 1; }
  if (a === b) { return 0; }

  throw { message: "Couldn't compare elements", a: a, b: b };
}
customUtils.defaultCompareKeysFunction = defaultCompareKeysFunction;


/**
 * Check whether two values are equal (used in non-unique deletion)
 */
function defaultCheckValueEquality (a, b) {
  return a === b;
}
customUtils.defaultCheckValueEquality = defaultCheckValueEquality;
/**
 * Simple binary search tree
 */
 


/**
 * Constructor
 * @param {Object} options Optional
 * @param {Boolean}  options.unique Whether to enforce a 'unique' constraint on the key or not
 * @param {Key}      options.key Initialize this BST's key with key
 * @param {Value}    options.value Initialize this BST's data with [value]
 * @param {Function} options.compareKeys Initialize this BST's compareKeys
 */
function BinarySearchTree (options) {
  options = options || {};

  this.left = null;
  this.right = null;
  this.parent = options.parent !== undefined ? options.parent : null;
  if (options.hasOwnProperty('key')) { this.key = options.key; }
  this.data = options.hasOwnProperty('value') ? [options.value] : [];
  this.unique = options.unique || false;

  this.compareKeys = options.compareKeys || customUtils.defaultCompareKeysFunction;
  this.checkValueEquality = options.checkValueEquality || customUtils.defaultCheckValueEquality;
}


// ================================
// Methods used to test the tree
// ================================


/**
 * Get the descendant with max key
 */
BinarySearchTree.prototype.getMaxKeyDescendant = function () {
  if (this.right) {
    return this.right.getMaxKeyDescendant();
  } else {
    return this;
  }
};


/**
 * Get the maximum key
 */
BinarySearchTree.prototype.getMaxKey = function () {
  return this.getMaxKeyDescendant().key;
};


/**
 * Get the descendant with min key
 */
BinarySearchTree.prototype.getMinKeyDescendant = function () {
  if (this.left) {
    return this.left.getMinKeyDescendant()
  } else {
    return this;
  }
};


/**
 * Get the minimum key
 */
BinarySearchTree.prototype.getMinKey = function () {
  return this.getMinKeyDescendant().key;
};


/**
 * Check that all nodes (incl. leaves) fullfil condition given by fn
 * test is a function passed every (key, data) and which throws if the condition is not met
 */
BinarySearchTree.prototype.checkAllNodesFullfillCondition = function (test) {
  if (!this.hasOwnProperty('key')) { return; }

  test(this.key, this.data);
  if (this.left) { this.left.checkAllNodesFullfillCondition(test); }
  if (this.right) { this.right.checkAllNodesFullfillCondition(test); }
};


/**
 * Check that the core BST properties on node ordering are verified
 * Throw if they aren't
 */
BinarySearchTree.prototype.checkNodeOrdering = function () {
  var self = this;

  if (!this.hasOwnProperty('key')) { return; }

  if (this.left) {
    this.left.checkAllNodesFullfillCondition(function (k) {
      if (self.compareKeys(k, self.key) >= 0) {
        throw 'Tree with root ' + self.key + ' is not a binary search tree';
      }
    });
    this.left.checkNodeOrdering();
  }

  if (this.right) {
    this.right.checkAllNodesFullfillCondition(function (k) {
      if (self.compareKeys(k, self.key) <= 0) {
        throw 'Tree with root ' + self.key + ' is not a binary search tree';
      }
    });
    this.right.checkNodeOrdering();
  }
};


/**
 * Check that all pointers are coherent in this tree
 */
BinarySearchTree.prototype.checkInternalPointers = function () {
  if (this.left) {
    if (this.left.parent !== this) { throw 'Parent pointer broken for key ' + this.key; }
    this.left.checkInternalPointers();
  }

  if (this.right) {
    if (this.right.parent !== this) { throw 'Parent pointer broken for key ' + this.key; }
    this.right.checkInternalPointers();
  }
};


/**
 * Check that a tree is a BST as defined here (node ordering and pointer references)
 */
BinarySearchTree.prototype.checkIsBST = function () {
  this.checkNodeOrdering();
  this.checkInternalPointers();
  if (this.parent) { throw "The root shouldn't have a parent"; }
};


/**
 * Get number of keys inserted
 */
BinarySearchTree.prototype.getNumberOfKeys = function () {
  var res;

  if (!this.hasOwnProperty('key')) { return 0; }

  res = 1;
  if (this.left) { res += this.left.getNumberOfKeys(); }
  if (this.right) { res += this.right.getNumberOfKeys(); }

  return res;
};



// ============================================
// Methods used to actually work on the tree
// ============================================

/**
 * Create a BST similar (i.e. same options except for key and value) to the current one
 * Use the same constructor (i.e. BinarySearchTree, AVLTree etc)
 * @param {Object} options see constructor
 */
BinarySearchTree.prototype.createSimilar = function (options) {
  options = options || {};
  options.unique = this.unique;
  options.compareKeys = this.compareKeys;
  options.checkValueEquality = this.checkValueEquality;

  return new this.constructor(options);
};


/**
 * Create the left child of this BST and return it
 */
BinarySearchTree.prototype.createLeftChild = function (options) {
  var leftChild = this.createSimilar(options);
  leftChild.parent = this;
  this.left = leftChild;

  return leftChild;
};


/**
 * Create the right child of this BST and return it
 */
BinarySearchTree.prototype.createRightChild = function (options) {
  var rightChild = this.createSimilar(options);
  rightChild.parent = this;
  this.right = rightChild;

  return rightChild;
};


/**
 * Insert a new element
 */
BinarySearchTree.prototype.insert = function (key, value) {
  // Empty tree, insert as root
  if (!this.hasOwnProperty('key')) {
    this.key = key;
    this.data.push(value);
    return;
  }

  // Same key as root
  if (this.compareKeys(this.key, key) === 0) {
    if (this.unique) {
      throw { message: "Can't insert key " + key + ", it violates the unique constraint"
            , key: key
            , errorType: 'uniqueViolated'
            };
    } else {
      this.data.push(value);
    }
    return;
  }

  if (this.compareKeys(key, this.key) < 0) {
    // Insert in left subtree
    if (this.left) {
      this.left.insert(key, value);
    } else {
      this.createLeftChild({ key: key, value: value });
    }
  } else {
    // Insert in right subtree
    if (this.right) {
      this.right.insert(key, value);
    } else {
      this.createRightChild({ key: key, value: value });
    }
  }
};


/**
 * Search for all data corresponding to a key
 */
BinarySearchTree.prototype.search = function (key) {
  if (!this.hasOwnProperty('key')) { return []; }

  if (this.compareKeys(this.key, key) === 0) { return this.data; }

  if (this.compareKeys(key, this.key) < 0) {
    if (this.left) {
      return this.left.search(key);
    } else {
      return [];
    }
  } else {
    if (this.right) {
      return this.right.search(key);
    } else {
      return [];
    }
  }
};


/**
 * Return a function that tells whether a given key matches a lower bound
 */
BinarySearchTree.prototype.getLowerBoundMatcher = function (query) {
  var self = this;

  // No lower bound
  if (!query.hasOwnProperty('$gt') && !query.hasOwnProperty('$gte')) {
    return function () { return true; };
  }

  if (query.hasOwnProperty('$gt') && query.hasOwnProperty('$gte')) {
    if (self.compareKeys(query.$gte, query.$gt) === 0) {
      return function (key) { return self.compareKeys(key, query.$gt) > 0; };
    }

    if (self.compareKeys(query.$gte, query.$gt) > 0) {
      return function (key) { return self.compareKeys(key, query.$gte) >= 0; };
    } else {
      return function (key) { return self.compareKeys(key, query.$gt) > 0; };
    }
  }

  if (query.hasOwnProperty('$gt')) {
    return function (key) { return self.compareKeys(key, query.$gt) > 0; };
  } else {
    return function (key) { return self.compareKeys(key, query.$gte) >= 0; };
  }
};


/**
 * Return a function that tells whether a given key matches an upper bound
 */
BinarySearchTree.prototype.getUpperBoundMatcher = function (query) {
  var self = this;

  // No lower bound
  if (!query.hasOwnProperty('$lt') && !query.hasOwnProperty('$lte')) {
    return function () { return true; };
  }

  if (query.hasOwnProperty('$lt') && query.hasOwnProperty('$lte')) {
    if (self.compareKeys(query.$lte, query.$lt) === 0) {
      return function (key) { return self.compareKeys(key, query.$lt) < 0; };
    }

    if (self.compareKeys(query.$lte, query.$lt) < 0) {
      return function (key) { return self.compareKeys(key, query.$lte) <= 0; };
    } else {
      return function (key) { return self.compareKeys(key, query.$lt) < 0; };
    }
  }

  if (query.hasOwnProperty('$lt')) {
    return function (key) { return self.compareKeys(key, query.$lt) < 0; };
  } else {
    return function (key) { return self.compareKeys(key, query.$lte) <= 0; };
  }
};


// Append all elements in toAppend to array
function append (array, toAppend) {
  var i;

  for (i = 0; i < toAppend.length; i += 1) {
    array.push(toAppend[i]);
  }
}


/**
 * Get all data for a key between bounds
 * Return it in key order
 * @param {Object} query Mongo-style query where keys are $lt, $lte, $gt or $gte (other keys are not considered)
 * @param {Functions} lbm/ubm matching functions calculated at the first recursive step
 */
BinarySearchTree.prototype.betweenBounds = function (query, lbm, ubm) {
  var res = [];

  if (!this.hasOwnProperty('key')) { return []; }   // Empty tree

  lbm = lbm || this.getLowerBoundMatcher(query);
  ubm = ubm || this.getUpperBoundMatcher(query);

  if (lbm(this.key) && this.left) { append(res, this.left.betweenBounds(query, lbm, ubm)); }
  if (lbm(this.key) && ubm(this.key)) { append(res, this.data); }
  if (ubm(this.key) && this.right) { append(res, this.right.betweenBounds(query, lbm, ubm)); }

  return res;
};


/**
 * Delete the current node if it is a leaf
 * Return true if it was deleted
 */
BinarySearchTree.prototype.deleteIfLeaf = function () {
  if (this.left || this.right) { return false; }

  // The leaf is itself a root
  if (!this.parent) {
    delete this.key;
    this.data = [];
    return true;
  }

  if (this.parent.left === this) {
    this.parent.left = null;
  } else {
    this.parent.right = null;
  }

  return true;
};


/**
 * Delete the current node if it has only one child
 * Return true if it was deleted
 */
BinarySearchTree.prototype.deleteIfOnlyOneChild = function () {
  var child;

  if (this.left && !this.right) { child = this.left; }
  if (!this.left && this.right) { child = this.right; }
  if (!child) { return false; }

  // Root
  if (!this.parent) {
    this.key = child.key;
    this.data = child.data;

    this.left = null;
    if (child.left) {
      this.left = child.left;
      child.left.parent = this;
    }

    this.right = null;
    if (child.right) {
      this.right = child.right;
      child.right.parent = this;
    }

    return true;
  }

  if (this.parent.left === this) {
    this.parent.left = child;
    child.parent = this.parent;
  } else {
    this.parent.right = child;
    child.parent = this.parent;
  }

  return true;
};


/**
 * Delete a key or just a value
 * @param {Key} key
 * @param {Value} value Optional. If not set, the whole key is deleted. If set, only this value is deleted
 */
BinarySearchTree.prototype.delete = function (key, value) {
  var newData = [], replaceWith
    , self = this
    ;

  if (!this.hasOwnProperty('key')) { return; }

  if (this.compareKeys(key, this.key) < 0) {
    if (this.left) { this.left.delete(key, value); }
    return;
  }

  if (this.compareKeys(key, this.key) > 0) {
    if (this.right) { this.right.delete(key, value); }
    return;
  }

  if (!this.compareKeys(key, this.key) === 0) { return; }

  // Delete only a value
  if (this.data.length > 1 && value !== undefined) {
    this.data.forEach(function (d) {
      if (!self.checkValueEquality(d, value)) { newData.push(d); }
    });
    self.data = newData;
    return;
  }

  // Delete the whole node
  if (this.deleteIfLeaf()) {
    return;
  }
  if (this.deleteIfOnlyOneChild()) {
    return;
  }

  // We are in the case where the node to delete has two children
  if (Math.random() >= 0.5) {   // Randomize replacement to avoid unbalancing the tree too much
    // Use the in-order predecessor
    replaceWith = this.left.getMaxKeyDescendant();

    this.key = replaceWith.key;
    this.data = replaceWith.data;

    if (this === replaceWith.parent) {   // Special case
      this.left = replaceWith.left;
      if (replaceWith.left) { replaceWith.left.parent = replaceWith.parent; }
    } else {
      replaceWith.parent.right = replaceWith.left;
      if (replaceWith.left) { replaceWith.left.parent = replaceWith.parent; }
    }
  } else {
    // Use the in-order successor
    replaceWith = this.right.getMinKeyDescendant();

    this.key = replaceWith.key;
    this.data = replaceWith.data;

    if (this === replaceWith.parent) {   // Special case
      this.right = replaceWith.right;
      if (replaceWith.right) { replaceWith.right.parent = replaceWith.parent; }
    } else {
      replaceWith.parent.left = replaceWith.right;
      if (replaceWith.right) { replaceWith.right.parent = replaceWith.parent; }
    }
  }
};


/**
 * Execute a function on every node of the tree, in key order
 * @param {Function} fn Signature: node. Most useful will probably be node.key and node.data
 */
BinarySearchTree.prototype.executeOnEveryNode = function (fn) {
  if (this.left) { this.left.executeOnEveryNode(fn); }
  fn(this);
  if (this.right) { this.right.executeOnEveryNode(fn); }
};


/**
 * Pretty print a tree
 * @param {Boolean} printData To print the nodes' data along with the key
 */
BinarySearchTree.prototype.prettyPrint = function (printData, spacing) {
  spacing = spacing || "";

  console.log(spacing + "* " + this.key);
  if (printData) { console.log(spacing + "* " + this.data); }

  if (!this.left && !this.right) { return; }

  if (this.left) {
    this.left.prettyPrint(printData, spacing + "  ");
  } else {
    console.log(spacing + "  *");
  }
  if (this.right) {
    this.right.prettyPrint(printData, spacing + "  ");
  } else {
    console.log(spacing + "  *");
  }
};




// Interface
 
/**
* Created by Hanwen on 01.07.2014.
*/

 
/**
 * Constructor
 * @param {Object} [options] config map
 * @param {Number}  options.minLength the shortest length of the fragment.
 * @param {Number}  options.minOccurrence the minimum occurrence of the fragment.
 * @param {Boolean}  options.debug whether show the console message and set the minLength and minOccurrence.
 * @constructor
 */
function SuffixTrie(options) {
    this.options = options || {};

    this.structure = {};
    this.debug = this.options.debug === undefined ? false : options.debug;


    this.minLENGTH = this.options.minLength === undefined ? 3 : options.minLength;
    this.minOccurrence = this.options.minOccurrence === undefined ? 2 : options.minOccurrence;
    this.isByLength = this.options.byLength === undefined ? false : options.byLength;
    
    //this.options.limit is a number
    this.save = [];
    this.array = null;
    this.labelArray = null;
    this.fragmentsArray = null;
    this.fragmentTrie = {};
    this.rebuildArray;
    //default test environment

}

//help function to clear the trie
SuffixTrie.prototype.refresh = function(){
    this.structure = {};
    this.save = [];
    this.array = null;
    this.labelArray = null;
    this.fragmentsArray = null;
    this.fragmentTrie = {};
    this.rebuildArray;
};


//help function for quick weight
SuffixTrie.prototype.weigh = function(){
    if(this.isByLength){
        return this.weighByMax()
    }else{
        return this.weighByAverage()
    }
}

//    ======================================================================================================================
//    ==================================================== Public Functions ================================================
//    ======================================================================================================================
/**
 * add fragment string to the trie
 * @param array {Array} adding word
 */
SuffixTrie.prototype.build = function (array) {
    var debug = this.debug;
    this.buildLabelTrie(array);
    this.optimize(this.structure);
    if(debug) console.log('after optimization our label trie of array length ' + this.array.length + ' is ', this.structure);

    this.listLabel();
    if(debug) console.log('get the compressed label array (without duplicate fragments ) of array length ' + this.array.length + ' is ', this.labelArray);
    if(debug) console.log('and fragments array of length ' + this.fragmentsArray.length + ' is ', this.fragmentsArray);

    this.clearRedundantFragment();
    if(debug) console.log('get the cleared label array (without duplicate fragments ) of array length ' + this.array.length + ' is ', this.labelArray);
    if(debug) console.log('and cleared fragments array of length ' + this.fragmentsArray.length + ' is ', this.fragmentsArray);

    this.rebuild();
    if(debug) console.log('rebuild ended' , this.rebuildArray);
};

SuffixTrie.prototype.buildLabelTrie = function (array) {
    this.array = array;
    var root = this.structure;
    var LENGTH = this.minLENGTH;

    array.forEach(function (word, index) {
        var first = true;
        //used to store the loop element, for connect the suffixes, e.g. 'foob' next is 'oob', by this connection, we get all the substrings.
        var last = {};

        //loop every suffix in one word, e.g. 'oss' in 'boss'
        for (var i = 0, l = word.length; i <= l - LENGTH; i++) {
            var cur = root;
            var suffix = word.substring(i);
            var letters = suffix.split("");

            //loop every letter in the suffix, e.g. 'o' in 'oss'
            for (var j = 0; j < letters.length; j++) {
                var letter = letters[j], pos = cur[ letter ];

                if (j === letters.length - 1) {
                    if (pos == null) {
                        //create new node and add the information.
                        cur[letter] = {source: [index], listed: false};
                    } else if (pos.hasOwnProperty('source')) {
                        //just add occurrence
                        pos["source"].push(index);
                    } else {
                        //node already existed, add information.
                        cur[letter]['source'] = [index];
                        cur[letter]['listed'] = false;
                    }
                    cur = cur[letter];

                    if (!first) {
                        last['next'] = suffix;
                    } else {
                        first = false;
                    }

                    last = cur;
                } else if (pos == null) {
                    //create node
                    cur = cur[letter] = {};
                } else {
                    //no creation, loop to next node
                    cur = cur[ letter ];
                }
            }
        }
    });

};

/**
 * add the origin for the split node and trunk node && integrate prefix in each branch
 * @param {Object} root the start node
 * @param {Number} [rootLevel] the start level of the algorithm, should be -1
 * @returns {Array} array with the origin of the word
 *
 */
SuffixTrie.prototype.optimize = function (root, rootLevel) {
    var occurrence = this.minOccurrence;
    //self origin indexes
    var self_save = [];
    rootLevel = rootLevel === undefined ? -1 : rootLevel;
    rootLevel++;
    //whether the word is long enough. ignored the short part.
    var is_allowed = rootLevel >= this.minLENGTH;

    for (var child in root) {

        if (root.hasOwnProperty(child)) { //&& child !== 'next' && child !== 'level'
            //loop to new child and combine the index information
            if (child.length === 1) {

                // returned indexes from children (iterate from the leaf)
                var children_save = this.optimize(root[child], rootLevel);
                if (is_allowed) {
                    self_save = self_save.concat(children_save);
                }
            } else if (child === 'source' && is_allowed) {
                self_save = self_save.concat(root['source']);
            }
        }
    }

    // this part is to test if the fragment fulfil the occurrence requirement, delete if not, which reduce time significantly.
    self_save = uniqueArray(self_save);
    var isEnoughOccurred = self_save.length >= occurrence;
    is_allowed = is_allowed && isEnoughOccurred;

    if (is_allowed) {
        //leaf will at least has property of 'listed' and 'next'
        var is_SoleNode = Object.keys(root).length === 1;
        if (!is_SoleNode) {
            //this is a split node or a flaged node, add information include source here
            root['source'] = self_save;   //update it
            root['level'] = rootLevel;
            root['listed'] = false;     //indicate this node should be check
            root['weight'] = self_save.length * rootLevel;
            self_save = [];     //clear the array: this is important if we don't want the node with former indexes!
        } else {
            //this is just a sole node with one child, do nothing
        }
    }
    else {
        //it is lower than the request level , or it is a leaf node, so not calculate here
        delete root['source'];
    }

    return self_save;
};

/**
 * list the repeat part with certain order && integrate suffix in 'next' branch
 * @param {Number} [start] start point in the array [0 - array.length]
 */
SuffixTrie.prototype.listLabel = function (start) {
    var array = this.array;
    var root = this.structure;
    var label_array = [];
    var fragments_array = [];
    var length = this.minLENGTH;
    var occurrence = this.minOccurrence;
    start = start === undefined ? 1 : start;

    //loop from the certain index, lead to different rebuild array.
    for (var index = start - 1, i = 0; i < array.length; index++, index = index % (array.length), i++) {
        var word = array[index];

        var fragments = {};
        //skip the short word, just push the empty object.
        if (word.length < length) {
            label_array.push(fragments);
            continue;
        }

        findFragments(word, fragments, root, length, occurrence, false);

        //accumulate the fragment to another array.
        for (var fragment in fragments) {
            if (fragments.hasOwnProperty(fragment)) {
                //get all the origin label index and push them into fragment array with default order
                var fragmentsArrayIndex = fragments_array.push(fragments[fragment]) - 1;
                fragments_array[fragmentsArrayIndex]['name'] = fragment;
            }
        }

        //build another array to map each label and all of its fragments.
        label_array.push(fragments);
    }
    this.labelArray = label_array;
    this.fragmentsArray = fragments_array;
    return label_array;
};

/**
 * rebuild the label array, make sure every label has all of its own fragments
 */
SuffixTrie.prototype.rebuild = function () {
    var rebuildArray = JSON.parse(JSON.stringify(this.labelArray));//deep copy array
    rebuildArray.forEach(function (object, index) {
        for (var fragment in object) {
            if (object.hasOwnProperty(fragment)) {
                object[fragment]['source'].forEach(function (labelIndex) {
                    if (labelIndex > index) {
                        rebuildArray[labelIndex][fragment] = object[fragment];
                    }
                });
            }
        }
    });
    this.rebuildArray = rebuildArray;
};

/**
 * weight the fragments and order them by the product of their occurrence and length,
 * should use this function after the build and rebuild.
 */
SuffixTrie.prototype.weighByAverage = function () {
    var debug = this.debug;
    var fragmentsArray = this.fragmentsArray;
    var fragments_Num = this.fragmentsArray.length;
    fragmentsArray.sort(function (f1, f2) {
        return f2['weight'] - f1['weight'];
    });
    if (debug) console.log('weigh by average:  result of length ' + fragments_Num + ' is', fragmentsArray);

    if(typeof this.options.limit == "number")
        fragments_Num = this.options.limit < fragments_Num ? this.options.limit : fragments_Num;
    
    return fragmentsArray.slice(0, fragments_Num);
};

/**
 * weight the fragments and order them by max label length.
 */
SuffixTrie.prototype.weighByMax = function () {
    var debug = this.debug;
    
    buildFragmentTrie(this.fragmentTrie, this.fragmentsArray);

    var rebuildArray = JSON.parse(JSON.stringify(this.rebuildArray));
//        var rebuildArray = this.rebuildArray;
    var fragmentsArray = [];
    var fragmentsTrie = this.fragmentTrie;
    var fragments_Num = this.fragmentsArray.length;
    var label_Lengths = this.array.map(function (s) {
        return s.length;
    });

    var label_Tree = new BinarySearchTree();
    label_Lengths.forEach(function (length, index) {
        label_Tree.insert(length, index);
    });
    
    if (typeof this.options.limit == "number") 
        fragments_Num = this.options.limit < fragments_Num ? this.options.limit : fragments_Num;

    while (fragmentsArray.length < fragments_Num) {
        var maxKey = label_Tree.getMaxKey();

        var labels_In_Key = label_Tree.search(maxKey);

        if (labels_In_Key.length === 0) {
            label_Tree.delete(maxKey);
            continue;
        }
        //choose a random labels index with the biggest length
        var longest_Label = labels_In_Key[0];

        //get all the fragments in this label
        //in case the there is no fragments under it.
        var fragments = Object.keys(rebuildArray[longest_Label]);
        if (fragments.length > 0) {
            //then find the longest fragment
            fragments.sort(function (f1, f2) {
                return f2.length - f1.length;
            });
            removeTrie(fragments[0], fragmentsTrie, fragmentsArray, label_Lengths, rebuildArray, label_Tree);
        }
        //in case there is no this label in fragments array -- which could be possible
        label_Tree.delete(maxKey, longest_Label);
    }

    this.fragmentsArray = fragmentsArray;
    if (debug) console.log('weigh by max:  result of length ' + fragmentsArray.length + ' is', fragmentsArray);

    return fragmentsArray.slice(0, fragments_Num);
};

/**
 *  delete the redundant fragment, always keep the longer one.
 */
SuffixTrie.prototype.clearRedundantFragment = function () {
    var fragments = this.fragmentsArray;
    var occurrence = this.minOccurrence;
    var newFragmentArray = [];
    var labelArray = this.labelArray;
    //sort the fragments array from short to long.
    fragments.sort(function (a, b) {
        return a.name.length - b.name.length;
    });

    fragments.forEach(function (fragment, index) {
        //backup fragment's occurrence.
        var backupsource = fragment.source.slice();
        //check if the longer contain duplicated occurrence, filter such occurrence
        for (var i = index + 1; i < fragments.length; i++) {
            var longerFragment = fragments[i];
            if (longerFragment.name.indexOf(fragment.name) !== -1) {
                fragment.source = fragment.source.filter(function (i) {
                    return this.indexOf(i) === -1;
                }, longerFragment.source);
            }
        }

        //build the new fragment array with no duplication
        if (fragment.source.length >= occurrence) {
            newFragmentArray.push(fragment);
        }
        else {
            if (backupsource.length !== fragment.source.length) {
                deleteFragmentInLabelArray(backupsource, fragment.name, labelArray);
            }
        }
    });
    this.fragmentsArray = newFragmentArray;
};

//    ======================================================================================================================
//    ==================================================== Helper Functions ================================================
//    ======================================================================================================================

/**
 * Most Important Helper function
 * help function to find the all the fragment of one word from the root of the tree, and finally link to another suffix
 * @param word searching objects
 * @param fragments an object to store the sources of the common fragments in the path to find the word
 * @param root start point
 * @param length minimum length requirement
 * @param occurrence minimum occurrence requirement
 * @param [iterate] {Boolean} whether it is the complete word or suffix
 */
function findFragments(word, fragments, root, length, occurrence, iterate) {

    var cur = root;
    var letters = word.split("");
    //loop every letter in the suffix, e.g. 'o' in 'oss'
    for (var j = 0; j < letters.length; j++) {
        var letter = letters[j], pos = cur[ letter ];
        if (j + 1 >= length) {
            var fragment = word.substring(0, j + 1);

            if (pos.hasOwnProperty('listed')) {
                if (!pos['listed']) {
                    //if it is a node fulfil our requirement
                    if (pos.hasOwnProperty('source')) {
                        fragments[fragment] = {};
                        fragments[fragment]['source'] = pos.source;
                        fragments[fragment]['weight'] = pos['weight'];

                        //debug whether we include all the situation.
                        if (pos['weight'] == null) {
                            console.warn('escape case! fragment does not has weight property, node is ', fragments[fragment]);
                        }
                        if (fragments.hasOwnProperty(fragment)) {
                            //if this is not the full word (but a suffix) we check the duplication
                            if (iterate) {
                                for (var property in fragments) {
                                    if (property !== fragment && fragments.hasOwnProperty(property) && fragments.hasOwnProperty(fragment)) {
                                        if (property.indexOf(fragment) !== -1) {
                                            fragments[fragment]['source'] = fragments[fragment]['source'].filter(function (i) {
                                                return this.indexOf(i) < 0
                                            }, fragments[property].source);
                                        }
                                    }
                                }

                                if (fragments[fragment]['source'].length < occurrence) {
                                    delete fragments[fragment];
                                }
                            }
                        }
                        pos['listed'] = true;
                    }
                    //iterate to its suffix from root
                    if (pos.hasOwnProperty('next')) {
                        findFragments(pos['next'], fragments, root, length, occurrence, true);
                    }
                }
            }
        }
        cur = pos;
    }
}

/**
 * A helper function to delete the reference in Label Array
 * @param {Array} source fragment.source
 * @param {String} name fragment.name
 * @param labelArray LabelArray
 */
function deleteFragmentInLabelArray(source, name, labelArray) {
    var label = labelArray[source[0]];
    if (label.hasOwnProperty(name)) {
        delete label[name];
    }
}

/**
 * Helper function for build a dictionary trie for searching the fragment.
 * @param trie the fragment trie will be build in this variable.
 * @param array the fragment array used for building trie.
 */
function buildFragmentTrie(trie, array) {
    for (var i = 0; i < array.length; i++) {

        var word = array[i]['name'], letters = word.split(""), cur = trie;

        // Loop through the letters
        for (var j = 0; j < letters.length; j++) {
            var letter = letters[j];

            if (!cur.hasOwnProperty(letter)) {
                cur[ letter ] = {};
            }
            cur = cur[letter];
            if (j === letters.length - 1) {
                cur['source'] = array[i]['source'];
                cur['name'] = array[i]['name'];
                cur['weight'] = array[i]['weight'];
                cur['list'] = false;
            }

        }
    }
}

/**
 * Helper function to remove the fragment from the trie and list it in the result array (fragmentsArray).
 * @param word the word to be searched for.
 * @param cur the node where the search start.
 * @param array the array to store the result fragment
 * @param labelLengthArray the array map to the length of the label
 * @param rebuildArray this.rebuildArray
 * @param label_trie the binary search trie where store all the labels with their length.
 * @returns {Number|Boolean} if find return the length of fragment, else return false.
 */
function removeTrie(word, cur, array, labelLengthArray, rebuildArray, label_trie) {
    for (var node in cur) {

        if (cur.hasOwnProperty(node) && node.length === 1) {

            if (word.indexOf(node) === 0) {
                //if this is the leaf of the trie
                if (word.length === 1) {
                    if (cur[node].hasOwnProperty('list')) {
                        var fragment = {};
                        fragment['source'] = cur[node]['source'];
                        var fragmentName = cur[node]['name'];
                        fragment['name'] = fragmentName;
                        fragment['weight'] = cur[node]['weight'];
                        delete cur[node]['list'];
                        //store it into our result
                        array.push(fragment);

                        //for other node who has same fragment
                        cur[node]['source'].forEach(function (indexLabel) {
                            //delete the fragment reference in the label
                            delete rebuildArray[indexLabel][fragmentName];
                            //update the label length in the trie
                            var labelLength = labelLengthArray[indexLabel];
                            label_trie.delete(labelLength, indexLabel);
                            labelLength -= fragmentName.length;
                            label_trie.insert(labelLength, indexLabel);
                        });
                    }
                } else {
                    removeTrie(word.slice(1), cur[node], array, labelLengthArray, rebuildArray, label_trie);
                }
            }

        }
    }
}

/**
 * Helper function to remove the repeat elements in an array
 * @param {Array} array
 * @returns {Array} the result array without duplicate elments
 */
function uniqueArray(array) {
    var a = array.concat();
    for (var i = 0; i < a.length; ++i) {
        for (var j = i + 1; j < a.length; ++j) {
            if (a[i] === a[j])
                a.splice(j--, 1);
        }
    }
    return a;
}

 
window.Substrings = SuffixTrie;})();
/**
 * common.js is a set of common functions used across all of skiaperf.
 *
 * Everything is scoped to 'sk' except $$ and $$$ which are global since they
 * are used so often.
 *
 */

/**
 * $$ returns a real JS array of DOM elements that match the CSS query selector.
 *
 * A shortcut for jQuery-like $ behavior.
 **/
function $$(query, ele) {
  if (!ele) {
    ele = document;
  }
  return Array.prototype.map.call(ele.querySelectorAll(query), function(e) { return e; });
}


/**
 * $$$ returns the DOM element that match the CSS query selector.
 *
 * A shortcut for document.querySelector.
 **/
function $$$(query, ele) {
  if (!ele) {
    ele = document;
  }
  return ele.querySelector(query);
}


this.sk = this.sk || {};

(function(sk) {
  "use strict";

  /**
   * app_config is a place for applications to store app specific
   * configuration variables.
  **/
  sk.app_config = {};

  /**
   * clearChildren removes all children of the passed in node.
   */
  sk.clearChildren = function(ele) {
    while (ele.firstChild) {
      ele.removeChild(ele.firstChild);
    }
  }

  /**
   * findParent returns either 'ele' or a parent of 'ele' that has the nodeName of 'nodeName'.
   *
   * Note that nodeName is all caps, i.e. "DIV" or "PAPER-BUTTON".
   *
   * The return value is null if no containing element has that node name.
   */
  sk.findParent = function(ele, nodeName) {
    while (ele != null) {
      if (ele.nodeName == nodeName) {
        return ele;
      }
      ele = ele.parentElement;
    }
    return null;
  }

  /**
   * errorMessage dispatches an event with the error message in it.
   * message is expected to be an object with either a field response
   * (e.g. server response) or message (e.g. message of a typeError)
   * that is a String.
   *
   * See <error-toast-sk> for an element that listens for such events
   * and displays the error messages.
   *
   */
  sk.errorMessage = function(message, duration) {
    if (typeof message === 'object') {
      message = message.response || // for backwards compatibility
          message.message || // for handling Errors {name:String, message:String}
          JSON.stringify(message); // for everything else
    }
    var detail = {
      message: message,
    }
    detail.duration = duration;
    document.dispatchEvent(new CustomEvent('error-sk', {detail: detail, bubbles: true}));
  }

  /**
   * Importer simplifies importing HTML Templates from HTML Imports.
   *
   * Just instantiate an instance in the HTML Import:
   *
   *    importer = new sk.Importer();
   *
   * Then import templates via their id:
   *
   *    var node = importer.import('#foo');
   */
  sk.Importer = function() {
    if ('currentScript' in document) {
      this.importDoc_ = document.currentScript.ownerDocument;
    } else {
      this.importDoc_ = document._currentScript.ownerDocument;
    }
  }

  sk.Importer.prototype.import = function(id) {
    return document.importNode($$$(id, this.importDoc_).content, true);
  }

  // elePos returns the position of the top left corner of given element in
  // client coordinates.
  //
  // Returns an object of the form:
  // {
  //   x: NNN,
  //   y: MMM,
  // }
  sk.elePos = function(ele) {
    var bounds = ele.getBoundingClientRect();
    return {x: bounds.left, y: bounds.top};
  }

  // Returns a Promise that uses XMLHttpRequest to make a request with the given
  // method to the given URL with the given headers and body.
  sk.request = function(method, url, body, headers, withCredentials) {
    // Return a new promise.
    return new Promise(function(resolve, reject) {
      // Do the usual XHR stuff
      var req = new XMLHttpRequest();
      req.open(method, url);
      if (headers) {
        for (var k in headers) {
          req.setRequestHeader(k, headers[k]);
        }
      }

      if (withCredentials) {
        req.withCredentials = true;
      }

      req.onload = function() {
        // This is called even on 404 etc
        // so check the status
        if (req.status == 200) {
          // Resolve the promise with the response text
          resolve(req.response);
        } else {
          // Otherwise reject with an object containing the status text and
          // response code, which will hopefully be meaningful error
          reject({
            response: req.response,
            status: req.status,
          });
        }
      };

      // Handle network errors
      req.onerror = function() {
        reject({
            response: Error("Network Error")
          });
      };

      // Make the request
      req.send(body);
    });
  }

  // Returns a Promise that uses XMLHttpRequest to make a request to the given URL.
  sk.get = function(url, withCredentials) {
    return sk.request('GET', url, null, null, withCredentials);
  }


  // Returns a Promise that uses XMLHttpRequest to make a POST request to the
  // given URL with the given JSON body. The content_type is optional and
  // defaults to "application/json".
  sk.post = function(url, body, content_type, withCredentials) {
    if (!content_type) {
      content_type = "application/json";
    }
    return sk.request('POST', url, body, {"Content-Type": content_type}, withCredentials);
  }

  // Returns a Promise that uses XMLHttpRequest to make a DELETE request to the
  // given URL.
  sk.delete = function(url, body, withCredentials) {
    return sk.request('DELETE', url, body, null, withCredentials);
  }

  // A Promise that resolves when DOMContentLoaded has fired.
  sk.DomReady = new Promise(function(resolve, reject) {
      if (document.readyState != 'loading') {
        // If readyState is already past loading then
        // DOMContentLoaded has already fired, so just resolve.
        resolve();
      } else {
        document.addEventListener('DOMContentLoaded', resolve);
      }
    });

  // A Promise that resolves when Polymer has fired polymer-ready.
  sk.WebComponentsReady = new Promise(function(resolve, reject) {
    window.addEventListener('polymer-ready', resolve);
  });

  // _Mailbox is an object that allows distributing, possibly in a time
  // delayed manner, values to subscribers to mailbox names.
  //
  // For example, a series of large objects may need to be distributed across
  // a DOM tree in a way that doesn't easily fit with normal data binding.
  // Instead each element can subscribe to a mailbox name where the data will
  // be placed, and receive a callback when the data is updated. Note that
  // upon first subscribing to a mailbox the callback will be triggered
  // immediately with the value there, which may be the default of null.
  //
  // There is no order required for subscribe and send calls. You can send to
  // a mailbox with no subscribers, and a subscription can be registered for a
  // mailbox that has not been sent any data yet.
  var _Mailbox = function() {
    this.boxes = {};
  };

  // Subscribe to a mailbox of the name 'addr'. The callback 'cb' will
  // be called each time the mailbox is updated, including the very first time
  // a callback is registered, possibly with the default value of null.
  _Mailbox.prototype.subscribe = function(addr, cb) {
    var box = this.boxes[addr] || { callbacks: [], value: null };
    box.callbacks.push(cb);
    cb(box.value);
    this.boxes[addr] = box;
  };

  // Remove a callback from a subscription.
  _Mailbox.prototype.unsubscribe = function(addr, cb) {
    var box = this.boxes[addr] || { callbacks: [], value: null };
    // Use a countdown loop so multiple removals is safe.
    for (var i = box.callbacks.length-1; i >= 0; i--) {
      if (box.callbacks[i] == cb) {
        box.callbacks.splice(i, 1);
      }
    }
  };

  // Send data to a mailbox. All registered callbacks will be triggered
  // synchronously.
  _Mailbox.prototype.send = function(addr, value) {
    var box = this.boxes[addr] || { callbacks: [], value: null };
    box.value = value;
    this.boxes[addr] = box;
    box.callbacks.forEach(function(cb) {
      cb(value);
    });
  };

  // sk.Mailbox is an instance of sk._Mailbox, the only instance
  // that should be needed.
  sk.Mailbox = new _Mailbox();


  // Namespace for utilities for working with human i/o.
  sk.human = {};

  var TIME_DELTAS = [
    { units: "w", delta: 7*24*60*60 },
    { units: "d", delta:   24*60*60 },
    { units: "h", delta:      60*60 },
    { units: "m", delta:         60 },
    { units: "s", delta:          1 },
  ];

  sk.KB = 1024;
  sk.MB = sk.KB * 1024;
  sk.GB = sk.MB * 1024;
  sk.TB = sk.GB * 1024;
  sk.PB = sk.TB * 1024;

  var BYTES_DELTAS = [
    { units: " PB", delta: sk.PB},
    { units: " TB", delta: sk.TB},
    { units: " GB", delta: sk.GB},
    { units: " MB", delta: sk.MB},
    { units: " KB", delta: sk.KB},
    { units: " B",  delta:     1},
  ];

  /**
   * Pad zeros in front of the specified number.
   */
  sk.human.pad = function(num, size) {
    var str = num + "";
    while (str.length < size) str = "0" + str;
    return str;
  }

  /**
   * Returns a human-readable format of the given duration in seconds.
   *
   * For example, 'strDuration(123)' would return "2m 3s".
   *
   * Negative seconds is treated the same as positive seconds.
   */
  sk.human.strDuration = function(seconds) {
    if (seconds < 0) {
      seconds = -seconds;
    }
    if (seconds == 0) { return '  0s'; }
    var rv = "";
    for (var i=0; i<TIME_DELTAS.length; i++) {
      if (TIME_DELTAS[i].delta <= seconds) {
        var s = Math.floor(seconds/TIME_DELTAS[i].delta)+TIME_DELTAS[i].units;
        while (s.length < 4) {
          s = ' ' + s;
        }
        rv += s;
        seconds = seconds % TIME_DELTAS[i].delta;
      }
    }
    return rv;
  };

  /**
   * Returns the difference between the current time and 's' as a string in a
   * human friendly format.
   * If 's' is a number it is assumed to contain the time in milliseconds
   * otherwise it is assumed to contain a time string.
   *
   * For example, a difference of 123 seconds between 's' and the current time
   * would return "2m".
   */
  sk.human.diffDate = function(s) {
    var ms = (typeof(s) == "number") ? s : Date.parse(s);
    var diff = (ms - Date.now())/1000;
    if (diff < 0) {
      diff = -1.0 * diff;
    }
    return humanize(diff, TIME_DELTAS);
  }

  /**
   * Formats the amount of bytes in a human friendly format.
   * unit may be supplied to indicate b is not in bytes, but in something
   * like kilobytes (sk.KB) or megabytes (sk.MB)

   * For example, a 1234 bytes would be displayed as "1 KB".
   */
  sk.human.bytes = function(b, unit) {
    if (Number.isInteger(unit)) {
      b = b * unit;
    }
    return humanize(b, BYTES_DELTAS);
  }

  function humanize(n, deltas) {
    for (var i=0; i<deltas.length-1; i++) {
      // If n would round to '60s', return '1m' instead.
      var nextDeltaRounded =
          Math.round(n/deltas[i+1].delta)*deltas[i+1].delta;
      if (nextDeltaRounded/deltas[i].delta >= 1) {
        return Math.round(n/deltas[i].delta)+deltas[i].units;
      }
    }
    var i = deltas.length-1;
    return Math.round(n/deltas[i].delta)+deltas[i].units;
  }

  // localeTime formats the provided Date object in locale time and appends the timezone to the end.
  sk.human.localeTime = function(date) {
    // caching timezone could be buggy, especially if times from a wide range
    // of dates are used. The main concern would be crossing over Daylight
    // Savings time and having some times be erroneously in EST instead of
    // EDT, for example
    var str = date.toString();
    var timezone = str.substring(str.indexOf("("));
    return date.toLocaleString() + " " + timezone;
  }

  // Gets the epoch time in seconds.  This is its own function to make it easier to mock.
  sk.now = function() {
    return Math.round(new Date().getTime() / 1000);
  }

  // Namespace for utilities for working with arrays.
  sk.array = {};

  /**
   * Returns true if the two arrays are equal.
   *
   * Notes:
   *   Presumes the arrays are already in the same order.
   *   Compares equality using ===.
   */
  sk.array.equal = function(a, b) {
    if (a.length != b.length) {
      return false;
    }
    for (var i = 0, len = a.length; i < len; i++) {
      if (a[i] !== b[i]) {
        return false;
      }
    }
    return true;
  }

  /**
   * Formats the given string, replacing newlines with <br/> and auto-linkifying URLs.
   * References to bugs like "skia:123" and "chromium:123" are also converted into links.
   *
   * If linksInNewWindow is true, links are created with target="_blank".
   */
  sk.formatHTML = function(s, linksInNewWindow) {
    var sub = '<a href="$&">$&</a>';
    if (linksInNewWindow) {
      sub = '<a href="$&" target="_blank">$&</a>';
    }
    s = s.replace(/https?:(\/\/|&#x2F;&#x2F;)[^ \t\n<]*/g, sub).replace(/(?:\r\n|\n|\r)/g, '<br/>');
    return sk.linkifyBugs(s);
  }

  var PROJECTS_TO_ISSUETRACKERS = {
    'chromium': 'http://crbug.com/',
    'skia': 'http://skbug.com/',
  }

  /**
   * Formats bug references like "skia:123" and "chromium:123" into links.
   */
  sk.linkifyBugs = function(s) {
    for (var project in PROJECTS_TO_ISSUETRACKERS) {
      var re = new RegExp(project + ":[0-9]+", "g");
      var found_bugs = s.match(re);
      if (found_bugs) {
        found_bugs.forEach(function(found_bug) {
          var bug_number = found_bug.split(":")[1];
          var bug_link = '<a href="' + PROJECTS_TO_ISSUETRACKERS[project] +
                         bug_number + '" target="_blank">' + found_bug +
                         '</a>';
          s = s.replace(found_bug, bug_link);
        });
      }
    }
    return s;
  }

  sk.isGoogler = function(email) {
    return email && email.endsWith("@google.com");
  };

  // Namespace for utilities for working with URL query strings.
  sk.query = {};


  // fromParamSet encodes an object of the form:
  //
  // {
  //   a:["2", "4"],
  //   b:["3"]
  // }
  //
  // to a query string like:
  //
  // "a=2&a=4&b=3"
  //
  // This function handles URI encoding of both keys and values.
  sk.query.fromParamSet = function(o) {
    if (!o) {
      return "";
    }
    var ret = [];
    var keys = Object.keys(o).sort();
    keys.forEach(function(key) {
      o[key].forEach(function(value) {
        ret.push(encodeURIComponent(key) + '=' + encodeURIComponent(value));
      });
    });
    return ret.join('&');
  }

  // toParamSet parses a query string into an object with
  // arrays of values for the values. I.e.
  //
  //   "a=2&b=3&a=4"
  //
  // decodes to
  //
  //   {
  //     a:["2", "4"],
  //     b:["3"],
  //   }
  //
  // This function handles URI decoding of both keys and values.
  sk.query.toParamSet = function(s) {
    s = s || '';
    var ret = {};
    var vars = s.split("&");
    for (var i=0; i<vars.length; i++) {
      var pair = vars[i].split("=", 2);
      if (pair.length == 2) {
        var key = decodeURIComponent(pair[0]);
        var value = decodeURIComponent(pair[1]);
        if (ret.hasOwnProperty(key)) {
          ret[key].push(value);
        } else {
          ret[key] = [value];
        }
      }
    }
    return ret;
  }


  // fromObject takes an object and encodes it into a query string.
  //
  // The reverse of this function is toObject.
  sk.query.fromObject = function(o) {
    var ret = [];
    Object.keys(o).sort().forEach(function(key) {
      if (Array.isArray(o[key])) {
        o[key].forEach(function(value) {
          ret.push(encodeURIComponent(key) + '=' + encodeURIComponent(value));
        })
      } else if (typeof(o[key]) == 'object') {
          ret.push(encodeURIComponent(key) + '=' + encodeURIComponent(sk.query.fromObject(o[key])));
      } else {
        ret.push(encodeURIComponent(key) + '=' + encodeURIComponent(o[key]));
      }
    });
    return ret.join('&');
  }


  // toObject decodes a query string into an object
  // using the 'target' as a source for hinting on the types
  // of the values.
  //
  //   "a=2&b=true"
  //
  // decodes to:
  //
  //   {
  //     a: 2,
  //     b: true,
  //   }
  //
  // When given a target of:
  //
  //   {
  //     a: 1.0,
  //     b: false,
  //   }
  //
  // Note that a target of {} would decode
  // the same query string into:
  //
  //   {
  //     a: "2",
  //     b: "true",
  //   }
  //
  // Only Number, String, Boolean, Object, and Array of String hints are supported.
  sk.query.toObject = function(s, target) {
    var target = target || {};
    var ret = {};
    var vars = s.split("&");
    for (var i=0; i<vars.length; i++) {
      var pair = vars[i].split("=", 2);
      if (pair.length == 2) {
        var key = decodeURIComponent(pair[0]);
        var value = decodeURIComponent(pair[1]);
        if (target.hasOwnProperty(key)) {
          switch (typeof(target[key])) {
            case 'boolean':
              ret[key] = value=="true";
              break;
            case 'number':
              ret[key] = Number(value);
              break;
            case 'object': // Arrays report as 'object' to typeof.
              if (Array.isArray(target[key])) {
                var r = ret[key] || [];
                r.push(value);
                ret[key] = r;
              } else {
                ret[key] = sk.query.toObject(value, target[key]);
              }
              break;
            case 'string':
              ret[key] = value;
              break;
            default:
              ret[key] = value;
          }
        } else {
          ret[key] = value;
        }
      }
    }
    return ret;
  }

  // splitAmp returns the given query string as a newline
  // separated list of key value pairs. If sepator is not
  // provided newline will be used.
  sk.query.splitAmp = function(queryStr, separator) {
    separator = (separator) ? separator : '\n';
    queryStr = queryStr || "";
    return queryStr.split('&').join(separator);
  };

  // Namespace for utilities for working with Objects.
  sk.object = {};

  // Returns true if a and b are equal, covers Boolean, Number, String and
  // Arrays and Objects.
  sk.object.equals = function(a, b) {
    if (typeof(a) != typeof(b)) {
      return false
    }
    var ta = typeof(a);
    if (ta == 'string' || ta == 'boolean' || ta == 'number') {
      return a === b
    }
    if (ta == 'object') {
      if (Array.isArray(ta)) {
        return JSON.stringify(a) == JSON.stringify(b)
      } else {
        return sk.query.fromObject(a) == sk.query.fromObject(b)
      }
    }
  }

  // Returns an object with only values that are in o that are different
  // from d.
  //
  // Only works shallowly, i.e. only diffs on the attributes of
  // o and d, and only for the types that sk.object.equals supports.
  sk.object.getDelta = function (o, d) {
    var ret = {};
    Object.keys(o).forEach(function(key) {
      if (!sk.object.equals(o[key], d[key])) {
        ret[key] = o[key];
      }
    });
    return ret;
  };

  // Returns a copy of object o with values from delta if they exist.
  sk.object.applyDelta = function (delta, o) {
    var ret = {};
    Object.keys(o).forEach(function(key) {
      if (delta.hasOwnProperty(key)) {
        ret[key] = JSON.parse(JSON.stringify(delta[key]));
      } else {
        ret[key] = JSON.parse(JSON.stringify(o[key]));
      }
    });
    return ret;
  };

  // Returns a shallow copy (top level keys) of the object.
  sk.object.shallowCopy = function(o) {
    var ret = {};
    for(var k in o) {
      if (o.hasOwnProperty(k)) {
        ret[k] = o[k];
      }
    }
    return ret;
  };

  // Namespace for utilities for working with structured keys.
  //
  // See /go/query for a description of structured keys.
  sk.key = {};

  // Returns true if paramName=paramValue appears in the given structured key.
  sk.key.matches = function(key, paramName, paramValue) {
    return key.indexOf("," + paramName + "=" + paramValue + ",") >= 0;
  };

  // Parses the structured key and returns a populated object with all
  // the param names and values.
  sk.key.toObject = function(key) {
    var ret = {};
    key.split(",").forEach(function(s, i) {
      if (i == 0 ) {
        return
      }
      if (s === "") {
        return;
      }
      var parts = s.split("=");
      if (parts.length != 2) {
        return
      }
      ret[parts[0]] = parts[1];
    });
    return ret;
  };

  // Track the state of a page and reflect it to and from the URL.
  //
  // page - An object with a property 'state' where the state to be reflected
  //        into the URL is stored. We need the level of indirection because
  //        JS doesn't have pointers.
  //
  //        The 'state' must be on Object and all the values in the Object
  //        must be Number, String, Boolean, Object, or Array of String.
  //        Doesn't handle NaN, null, or undefined.
  // cb   - A callback of the form function() that is called when state has been
  //        changed by a change in the URL.
  sk.stateReflector = function(page, cb) {
    // The default state of the page. Used to calculate diffs to state.
    var defaultState = JSON.parse(JSON.stringify(page.state));

    // The last state of the page. Used to determine if the page state has changed recently.
    var lastState = JSON.parse(JSON.stringify(page.state));

    // Watch for state changes and reflect them in the URL by simply
    // polling the object and looking for differences from defaultState.
    setInterval(function() {
      if (Object.keys(sk.object.getDelta(lastState, page.state)).length > 0) {
        lastState = JSON.parse(JSON.stringify(page.state));
        var q = sk.query.fromObject(sk.object.getDelta(page.state, defaultState));
        history.pushState(null, "", window.location.origin + window.location.pathname + "?" +  q);
      }
    }, 100);

    // stateFromURL should be called when the URL has changed, it updates
    // the page.state and triggers the callback.
    var stateFromURL = function() {
      var delta = sk.query.toObject(window.location.search.slice(1), defaultState);
      page.state = sk.object.applyDelta(delta, defaultState);
      lastState = JSON.parse(JSON.stringify(page.state));
      cb();
    }

    // When we are loaded we should update the state from the URL.
    //
    // We check to see if we are running Polymer 0.5, in which case
    // we need to wait for Polymer to finish initializing, otherwise
    // we can just wait for DomReady.
    if (window["Polymer"] && Polymer.version[0] == "0") {
      sk.WebComponentsReady.then(stateFromURL);
    } else {
      sk.DomReady.then(stateFromURL);
    }

    // Every popstate event should also update the state.
    window.addEventListener('popstate', stateFromURL);
  }

  // Find a "round" number in the given range. Attempts to find numbers which
  // consist of a multiple of one of the following, order of preference:
  // [5, 2, 1], followed by zeroes.
  //
  // TODO(borenet): It would be nice to support other multiples, for example,
  // when dealing with time data, it'd be nice to round to seconds, minutes,
  // hours, days, etc.
  sk.getRoundNumber = function(min, max, base) {
    if (min > max) {
      throw ("sk.getRoundNumber: min > max! (" + min + " > " + max + ")");
    }
    var multipleOf = [5, 2, 1];

    var val = (max + min) / 2;
    // Determine the number of digits left of the decimal.
    if (!base) {
      base = 10;
    }
    var digits = Math.floor(Math.log(Math.abs(val)) / Math.log(base)) + 1;

    // Start with just the most significant digit and attempt to round it to
    // multiples of the above numbers, gradually including more digits until
    // a "round" value is found within the given range.
    for (var shift = 0; ; shift++) {
      // Round by shifting digits and dividing by a multiplier, then performing
      // the round function, then multiplying and shifting back.
      var shiftDiv = Math.pow(base, (digits - shift));
      for (var i = 0; i < multipleOf.length; i++) {
        var f = shiftDiv * multipleOf[i];
        // Actually perform the rounding. The 10s are included to intentionally
        // reduce precision to round off floating point error.
        var newVal = ((Math.round(val / f) * 10) * f) / 10;
        if (newVal >= min && newVal <= max) {
          return newVal;
        }
      }
    }

    console.error("sk.getRoundNumber Couldn't find appropriate rounding " +
                  "value. Returning midpoint.");
    return val;
  }

  // Sort the given array of strings, ignoring case.
  sk.sortStrings = function(s) {
    return s.sort(function(a, b) {
      return a.localeCompare(b, "en", {"sensitivity": "base"});
    });
  }

  // Capitalize each word in the string.
  sk.toCapWords = function(s) {
    return s.replace(/\b\w/g, function(firstLetter) {
      return firstLetter.toUpperCase();
    });
  }

  // Truncate the given string to the given length. If the string was
  // shortened, change the last three characters to ellipsis.
  sk.truncate = function(str, len) {
    if (str.length > len) {
      var ellipsis = "..."
      return str.substring(0, len - ellipsis.length) + ellipsis;
    }
    return str
  }

  // Return a 32 bit hash for the given string.
  //
  // This is a super simple hash (h = h * 31 + x_i) currently used
  // for things like assigning colors to graphs based on trace ids. It
  // shouldn't be used for anything more serious than that.
  sk.hashString = function(s) {
    var hash = 0;
    for (var i = s.length - 1; i >= 0; i--) {
      hash = ((hash << 5) - hash) + s.charCodeAt(i);
      hash |= 0;
    }
    return Math.abs(hash);
  }

  // Returns the string with all instances of &,<,>,",',/
  // replaced with their html-safe equivilents.
  // See OWASP doc https://goto.google.com/hyaql
  sk.escapeHTML = function(s) {
    return s.replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#x27;')
    .replace(/\//g, '&#x2F;');

  }

  // Returns true if the sorted arrays a and b
  // contain at least one element in common
  sk.sharesElement = function(a, b) {
    var i = 0;
    var j = 0;
    while (i < a.length && j < b.length) {
      if (a[i] < b[j]) {
        i++;
      } else if (b[j] < a[i]) {
        j++;
      } else {
        return true;
      }
    }
    return false;
  }


  // robust_get finds a sub object within 'obj' by following the path
  // in 'idx'. It will not throw an error if any sub object is missing
  // but instead return 'undefined'. 'idx' has to be an array.
  sk.robust_get = function(obj, idx) {
    if (!idx || !obj) {
      return;
    }

    for(var i=0, len=idx.length; i<len; i++) {
      if ((typeof obj === 'undefined') || (typeof idx[i] === 'undefined')) {
        return;  // returns 'undefined'
      }

      obj = obj[idx[i]];
    }

    return obj;
  };

  // Utility function for colorHex.
  function _hexify(i) {
    var s = i.toString(16).toUpperCase();
    // Pad out to two hex digits if necessary.
    if (s.length < 2) {
      s = '0' + s;
    }
    return s;
  }

  // colorHex returns a hex representation of a given color pixel as a string.
  // 'colors' is an array of bytes that contain pixesl in  RGBA format.
  // 'offset' is the offset of the pixel of interest.
  sk.colorHex = function(colors, offset) {
    return '#'
      + _hexify(colors[offset+0])
      + _hexify(colors[offset+1])
      + _hexify(colors[offset+2])
      + _hexify(colors[offset+3]);
  };

  // colorRGB returns the given RGBA pixel as a 4-tupel of decimal numbers.
  // 'colors' is an array of bytes that contain pixesl in  RGBA format.
  // 'offset' is the offset of the pixel of interest.
  // 'rawAlpha' will return the alpha value directly if true. Otherwise it will
  //            be normalized to [0...1].
  sk.colorRGB = function(colors, offset, rawAlpha) {
    var scaleAlpha = (rawAlpha) ? 1 : 255;
    return "rgba(" + colors[offset] + ", " +
              colors[offset + 1] + ", " +
              colors[offset + 2] + ", " +
              colors[offset + 3] / scaleAlpha + ")";
  };

  // Polyfill for String.startsWith from
  // https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/String/startsWith#Polyfill
  // returns true iff the string starts with the given prefix
  if (!String.prototype.startsWith) {
    String.prototype.startsWith = function(searchString, position) {
      position = position || 0;
      return this.indexOf(searchString, position) === position;
    };
  }

})(sk);
/*
 * Natural Sort algorithm for Javascript - Version 0.7 - Released under MIT license
 * Author: Jim Palmer (based on chunking idea from Dave Koelle)
 */
/*jshint unused:false */
window.naturalSort = function (a, b) {
	"use strict";
	var re = /(^([+\-]?(?:0|[1-9]\d*)(?:\.\d*)?(?:[eE][+\-]?\d+)?)?$|^0x[0-9a-f]+$|\d+)/gi,
		sre = /(^[ ]*|[ ]*$)/g,
		dre = /(^([\w ]+,?[\w ]+)?[\w ]+,?[\w ]+\d+:\d+(:\d+)?[\w ]?|^\d{1,4}[\/\-]\d{1,4}[\/\-]\d{1,4}|^\w+, \w+ \d+, \d{4})/,
		hre = /^0x[0-9a-f]+$/i,
		ore = /^0/,
		i = function(s) { return naturalSort.insensitive && ('' + s).toLowerCase() || '' + s; },
		// convert all to strings strip whitespace
		x = i(a).replace(sre, '') || '',
		y = i(b).replace(sre, '') || '',
		// chunk/tokenize
		xN = x.replace(re, '\0$1\0').replace(/\0$/,'').replace(/^\0/,'').split('\0'),
		yN = y.replace(re, '\0$1\0').replace(/\0$/,'').replace(/^\0/,'').split('\0'),
		// numeric, hex or date detection
		xD = parseInt(x.match(hre), 16) || (xN.length !== 1 && x.match(dre) && Date.parse(x)),
		yD = parseInt(y.match(hre), 16) || xD && y.match(dre) && Date.parse(y) || null,
		oFxNcL, oFyNcL;
	// first try and sort Hex codes or Dates
	if (yD) {
		if ( xD < yD ) { return -1; }
		else if ( xD > yD ) { return 1; }
	}
	// natural sorting through split numeric strings and default strings
	for(var cLoc=0, numS=Math.max(xN.length, yN.length); cLoc < numS; cLoc++) {
		// find floats not starting with '0', string or 0 if not defined (Clint Priest)
		oFxNcL = !(xN[cLoc] || '').match(ore) && parseFloat(xN[cLoc]) || xN[cLoc] || 0;
		oFyNcL = !(yN[cLoc] || '').match(ore) && parseFloat(yN[cLoc]) || yN[cLoc] || 0;
		// handle numeric vs string comparison - number < string - (Kyle Adams)
		if (isNaN(oFxNcL) !== isNaN(oFyNcL)) { return (isNaN(oFxNcL)) ? 1 : -1; }
		// rely on string comparison if different types - i.e. '02' < 2 != '02' < '2'
		else if (typeof oFxNcL !== typeof oFyNcL) {
			oFxNcL += '';
			oFyNcL += '';
		}
		if (oFxNcL < oFyNcL) { return -1; }
		if (oFxNcL > oFyNcL) { return 1; }
	}
	return 0;
};
// Copyright 2016 The LUCI Authors. All rights reserved.
// Use of this source code is governed under the Apache License, Version 2.0
// that can be found in the LICENSE file.

this.swarming = this.swarming || function() {

  var swarming = {};

  // return the longest string in an array
  swarming.longest = function(arr) {
      var most = "";
      for(var i = 0; i < arr.length; i++) {
        if (arr[i] && arr[i].length > most.length) {
          most = arr[i];
        }
      }
      return most;
    };

  swarming.stableSort = function(arr, comp) {
    if (!arr || !comp) {
      console.log("missing arguments to stableSort", arr, comp);
      return;
    }
    // We can guarantee a potential non-stable sort (like V8's
    // Array.prototype.sort()) to be stable by first storing the index in the
    // original sorting and using that if the original compare was 0.
    arr.forEach(function(e, i){
      if (e !== undefined && e !== null) {
        e.__sortIdx = i;
      }
    });

    arr.sort(function(a, b){
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
      var c = comp(a, b);
      if (c === 0) {
        return a.__sortIdx - b.__sortIdx;
      }
      return c;
    });
  }

  // postWithToast makes a post request and updates the error-toast
  // element with the response, regardless of failure.  See error-toast.html
  // for more information. The body param should be an object or undefined.
  swarming.postWithToast = function(url, msg, auth_headers, body) {
    // Keep toast displayed until we hear back from the request.
    sk.errorMessage(msg, 0);

    auth_headers["content-type"] = "application/json; charset=UTF-8";
    if (body) {
      body = JSON.stringify(body);
    }

    return sk.request("POST", url, body, auth_headers).then(function(response) {
      // Assumes response is a stringified json object
      sk.errorMessage("Request sent.  Response: "+response, 3000);
      return response;
    }).catch(function(r) {
      // Assumes r is something like
      // {response: "{\"error\":{\"message\":\"User ... \"}}", status: 403}
      var err = JSON.parse(r.response);
      console.log("Request failed", err);
      var humanReadable = (err.error && err.error.message) || JSON.stringify(err);
      sk.errorMessage("Request failed.  Reason: "+ humanReadable, 5000);
      return Promise.reject(err);
    });
  }

  // sanitizeAndHumanizeTime parses a date string or ms_since_epoch into a JS
  // Date object, assuming UTC time. It also creates a human readable form in
  // the obj under a key with a human_ prefix.  E.g.
  // swarming.sanitizeAndHumanizeTime(foo, "some_ts")
  // parses the string/int at foo["some_ts"] such that foo["some_ts"] is now a
  // Date object and foo["human_some_ts"] is the human formated version from
  // sk.human.localeTime.
  swarming.sanitizeAndHumanizeTime = function(obj, key) {
    obj["human_"+key] = "‑‑";
    if (obj[key]) {
      if (obj[key].endsWith && !obj[key].endsWith('Z')) {
        // Timestamps from the server are missing the 'Z' that specifies Zulu
        // (UTC) time. If that's not the case, add the Z. Otherwise, some
        // browsers interpret this as local time, which throws off everything.
        // TODO(kjlubick): Should the server output milliseconds since the
        // epoch?  That would be more consistent.
        // See http://crbug.com/714599
        obj[key] += 'Z';
      }
      obj[key] = new Date(obj[key]);

      // Extract the timezone.
      var str = obj[key].toString();
      var timezone = str.substring(str.indexOf("("));

      // If timestamp is today, skip the date.
      var now = new Date();
      if (obj[key].getDate() == now.getDate() &&
          obj[key].getMonth() == now.getMonth() &&
          obj[key].getYear() == now.getYear()) {
        obj["human_"+key] = obj[key].toLocaleTimeString() + " " + timezone;
      } else {
        obj["human_"+key] = obj[key].toLocaleString() + " " + timezone;
      }
    }
  }

  // parseDuration parses a duration string into an integer number of seconds.
  // e.g:
  // swarming.parseDuration("40s") == 40
  // swarming.parseDuration("2m") == 120
  // swarming.parseDuration("1h") == 3600
  // swarming.parseDuration("foo") == null
  swarming.parseDuration = function(duration) {
    var number = duration.slice(0, -1);
    if (!/[1-9][0-9]*/.test(number)) {
      return null;
    }
    number = Number(number);

    var unit = duration.slice(-1);
    switch (unit) {
      // the fallthroughs here are intentional
      case 'h':
        number *= 60;
      case 'm':
        number *= 60;
      case 's':
        break;
      default:
        return null;
    }
    return number;
  }

  return swarming;
}();
// Copyright 2016 The LUCI Authors. All rights reserved.
// Use of this source code is governed under the Apache License, Version 2.0
// that can be found in the LICENSE file.

// TODO(kjlubick): add tests for this code

this.swarming = this.swarming || {};
this.swarming.alias = this.swarming.alias || (function(){
  var ANDROID_ALIASES = {
    "angler": "Nexus 6p",
    "athene": "Moto G4",
    "bullhead": "Nexus 5X",
    "dragon": "Pixel C",
    "flo": "Nexus 7 [2013]",
    "flounder": "Nexus 9",
    "foster": "NVIDIA Shield",
    "fugu": "Nexus Player",
    "gce_x86": "Android on GCE",
    "goyawifi": "Galaxy Tab 3",
    "grouper": "Nexus 7 [2012]",
    "hammerhead": "Nexus 5",
    "herolte": "Galaxy S7 [Global]",
    "heroqlteatt": "Galaxy S7 [AT&T]",
    "j5xnlte": "Galaxy J5",
    "m0": "Galaxy S3",
    "mako": "Nexus 4",
    "manta": "Nexus 10",
    "marlin": "Pixel XL",
    "sailfish": "Pixel",
    "shamu": "Nexus 6",
    "sprout": "Android One",
    "zerofltetmo": "Galaxy S6",
  };

  var UNKNOWN = "unknown";

  var GPU_ALIASES = {
    "1002":      "AMD",
    "1002:6613": "AMD Radeon R7 240",
    "1002:6646": "AMD Radeon R9 M280X",
    "1002:6779": "AMD Radeon HD 6450/7450/8450",
    "1002:679e": "AMD Radeon HD 7800",
    "1002:6821": "AMD Radeon HD 8870M",
    "1002:683d": "AMD Radeon HD 7770/8760",
    "1002:9830": "AMD Radeon HD 8400",
    "1002:9874": "AMD Carrizo",
    "102b":      "Matrox",
    "102b:0522": "Matrox MGA G200e",
    "102b:0532": "Matrox MGA G200eW",
    "102b:0534": "Matrox G200eR2",
    "10de":      "NVIDIA",
    "10de:08a4": "NVIDIA GeForce 320M",
    "10de:08aa": "NVIDIA GeForce 320M",
    "10de:0a65": "NVIDIA GeForce 210",
    "10de:0fe9": "NVIDIA GeForce GT 750M Mac Edition",
    "10de:0ffa": "NVIDIA Quadro K600",
    "10de:104a": "NVIDIA GeForce GT 610",
    "10de:11c0": "NVIDIA GeForce GTX 660",
    "10de:1244": "NVIDIA GeForce GTX 550 Ti",
    "10de:1401": "NVIDIA GeForce GTX 960",
    "10de:1ba1": "NVIDIA GeForce GTX 1070",
    "10de:1cb3": "NVIDIA Quadro P400",
    "8086":      "Intel",
    "8086:0046": "Intel Ironlake HD Graphics",
    "8086:0102": "Intel Sandy Bridge HD Graphics 2000",
    "8086:0116": "Intel Sandy Bridge HD Graphics 3000",
    "8086:0166": "Intel Ivy Bridge HD Graphics 4000",
    "8086:0412": "Intel Haswell HD Graphics 4600",
    "8086:041a": "Intel Haswell HD Graphics",
    "8086:0a16": "Intel Haswell HD Graphics 4400",
    "8086:0a26": "Intel Haswell HD Graphics 5000",
    "8086:0a2e": "Intel Haswell Iris Graphics 5100",
    "8086:0d26": "Intel Haswell Iris Pro Graphics 5200",
    "8086:0f31": "Intel Bay Trail HD Graphics",
    "8086:1616": "Intel Broadwell HD Graphics 5500",
    "8086:161e": "Intel Broadwell HD Graphics 5300",
    "8086:1626": "Intel Broadwell HD Graphics 6000",
    "8086:162b": "Intel Broadwell Iris Graphics 6100",
    "8086:1912": "Intel Skylake HD Graphics 530",
    "8086:1926": "Intel Skylake Iris 540/550",
    "8086:193b": "Intel Skylake Iris Pro 580",
    "8086:22b1": "Intel Braswell HD Graphics",
    "8086:591e": "Intel Kaby Lake HD Graphics 615",
    "8086:5926": "Intel Kaby Lake Iris Plus Graphics 640",
  }

  // Taken from http://developer.android.com/reference/android/os/BatteryManager.html
  var BATTERY_HEALTH_ALIASES = {
    1: "Unknown",
    2: "Good",
    3: "Overheated",
    4: "Dead",
    5: "Over Voltage",
    6: "Unspecified Failure",
    7: "Too Cold",
  }

  var BATTERY_STATUS_ALIASES = {
    1: "Unknown",
    2: "Charging",
    3: "Discharging",
    4: "Not Charging",
    5: "Full",
  }

  // Created from experience and cross-referenced with
  // https://www.theiphonewiki.com/wiki/Models
  var DEVICE_ALIASES = {
    "iPad4,1":   "iPad Air",
    "iPad5,1":   "iPad mini 4",
    "iPad6,3":   "iPad Pro [9.7 in]",
    "iPhone7,2": "iPhone 6",
    "iPhone9,1": "iPhone 7",
  }

  // For consistency, all aliases are displayed like:
  // Nexus 5X (bullhead)
  // This regex matches a string like "ALIAS (ORIG)", with ORIG as group 1.
  var ALIAS_REGEXP = /.+ \((.*)\)/;

  var alias = {};

  alias.DIMENSIONS_WITH_ALIASES = ["device_type", "gpu", "battery_health", "battery_status", "device"];

  alias.android = function(dt) {
    return ANDROID_ALIASES[dt] || UNKNOWN;
  };

  alias.battery_health = function(bh) {
    return BATTERY_HEALTH_ALIASES[bh] || UNKNOWN;
  };

  alias.battery_status = function(bs) {
    return BATTERY_STATUS_ALIASES[bs] || UNKNOWN;
  };

  alias.device = function(dt) {
    return DEVICE_ALIASES[dt] || UNKNOWN;
  };

  alias.gpu = function(gpu) {
    if (!gpu || !gpu.split) {
      return UNKNOWN;
    }
    gpu = gpu.split("-")[0];
    return GPU_ALIASES[gpu] || UNKNOWN;
  };


  // alias.apply tries to alias the string "orig" based on what "type" it is.
  // If type is in DIMENSIONS_WITH_ALIASES, the appropriate alias (e.g. gpu)
  // is automatically applied.  Otherwise, "type" is treated as the alias.
  // If type is known, but there is no matching alias (e.g. for gpu: FOOBAR)
  // the original will be returned.
  alias.apply = function(orig, type) {
    var aliaser = aliasMap[type];
    if (!aliaser) {
      // treat type as the actual alias
      return type + " ("+orig+")";
    }
    var alias = aliaser(orig);
    if (alias !== UNKNOWN) {
      return alias + " ("+orig+")";
    }
    return orig;
  };

  alias.has = function(type) {
    return !!aliasMap[type];
  };

  // alias.unapply will return the base dimension/state with its alias removed
  // if it had one.  This is handy for sorting and filtering.
  alias.unapply = function(str) {
    var match = ALIAS_REGEXP.exec(str);
    if (match) {
      return match[1];
    }
    return str;
  };

  var aliasMap = {
    "battery_health": alias.battery_health,
    "battery_status": alias.battery_status,
    "device": alias.device,
    "device_type": alias.android,
    "gpu": alias.gpu,
  }

  return alias;
})();