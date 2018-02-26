// vim: set et sw=2 ts=2:
"use strict";
var env = process.env;
var url = require('url');
var Promise = require('bluebird');
var Phantom = Promise.promisifyAll(require('node-phantom-simple'));

var login = {
  begin: url.parse(env['ORACLE_LOGIN_BEGIN'] || "https://www.oracle.com/webapps/redirect/signon?nexturl=https://www.oracle.com/favicon.ico"),
  end:   url.parse(env['ORCALE_LOGIN_END']   || "https://www.oracle.com/favicon.ico"),
};
delete env['ORACLE_LOGIN_BEGIN'];
delete env['ORACLE_LOGIN_END'];

var credentials = Object.keys(env)
  .filter(function (key) { return key.indexOf('ORACLE_LOGIN_') == 0 })
  .map(function (key) { return [key.substr(13), env[key]] });

if (credentials.length <= 0) {
  console.error("Missing ORACLE_LOGIN environment variables!");
  process.exit(1);
}

Phantom.createAsync({ parameters: { 'ssl-protocol': 'tlsv1' } }).then(function (browser) {
  browser = Promise.promisifyAll(browser, { suffix: 'Promise' });
  browser.addCookie({'name': 'oraclelicense', 'value': "accept-" + env['ORACLE_COOKIE'] + "-cookie", 'domain': '.oracle.com' });

  // Open a tab, configure it
  return browser.createPagePromise().then(function (page) {
    page = Promise.promisifyAll(page, { suffix: 'Promise' });

    var received = "";
    page.onNavigationRequested = function () { console.info("%s %j", (new Date()).toISOString(), arguments["0"]); };
    page.onResourceError = console.error.bind(console);
    page.onResourceReceived = function (response) { if (response.stage == "end") received = response.url; };
    page.set('settings.loadImages', false);

    return page
    .setPromise('settings.userAgent', env['USER_AGENT']) // PhantomJS configures the UA per tab

    // Begin login, wait for the login page
    .then(function () {
      return page.openPromise(login.begin.href).then(function (status) {
        if (status != 'success') throw "Unable to connect to " + login.begin.host;

        return new Promise(function (resolve, reject) {
          var deadline = Date.now() + 6000;
          var interval = 100;

          var check = function () {
            if (deadline < Date.now()) return reject("Timeout waiting for form");

            page.evaluate(function () {
              return window['jQuery'] && document.querySelectorAll('input[type=password]').length;
            }, function (err, result) {
              if (result) { resolve(); } else { setTimeout(check, interval); }
            });
          };

          check();
        });
      })
      .tapCatch(function (err) {
        return page.getPromise('plainText').then(function (text) {
          console.error("Unable to load login page. Last response was:\n" + text);
        });
      });
    })

    // Submit the login form
    .then(function () {
      return page.evaluatePromise(function (credentials) {
        var $form = jQuery(document.forms[0]);
        return credentials.filter(function (tuple) {
          return $form.find("[name='"+tuple[0]+"']").val(tuple[1]).length == 0;
        })
        .map(function (tuple) { return tuple[0]; });
      }, credentials)
      .then(function (unapplied) {
        if (unapplied.length > 0) {
          console.warn("Unable to use all ORACLE_LOGIN environment variables: %j", unapplied);
        }
        return page.evaluatePromise(function () {
          jQuery(function () { document.forms[0].submit(); });
        });
      });
    })

    // Wait for login result
    .then(function () {
      return new Promise(function (resolve, reject) {
        var deadline = Date.now() + 6000;
        var interval = 100;

        var check = function () {
          if (deadline < Date.now()) return reject("Timeout waiting for " + login.end.href);
          if (received == login.end.href) { resolve(); } else { setTimeout(check, interval); }
        };

        check();
      })
      .tapCatch(function (err) {
        return page.getPromise('plainText').then(function (text) {
          console.error("Unable to load login result. Last response was:\n" + text);
        });
      });
    })

    // Export cookies for cURL
    .then(function () {
      return browser.getPromise('cookies').then(function (cookies) {
        var data = "";
        for (var i = 0; i < cookies.length; ++i) {
          var cookie = cookies[i];
          data += cookie.domain + "\tTRUE\t" + cookie.path + "\t"
            + (cookie.secure ? "TRUE" : "FALSE") + "\t0\t"
            + cookie.name + "\t" + cookie.value + "\n";
        }
        return Promise.promisify(require('fs').writeFile)(env['COOKIES'], data);
      });
    })

    // Download file using cURL
    .then(function () {
      return browser.exitPromise().then(function () {
        var cmd = ['curl', [
          '--cookie', env['COOKIES'],
          '--cookie-jar', env['COOKIES'],
          '--location',
          '--output', env['ORACLE_DOWNLOAD_FILE'],
          '--user-agent', env['USER_AGENT'],
          "https://edelivery.oracle.com/akam/otn/linux/" + env['ORACLE_FILE']
        ]];

        console.info("Executing %j", cmd);

        var child_process = require('child_process');
        var child = child_process.spawn.apply(child_process, cmd.concat({ stdio: [0, 1, 2] }));
        child.on('exit', process.exit);
      });
    })
    .catch(function (err) {
      console.error(err);
      browser.on('exit', function () { process.exit(1); });
      browser.exit();
    });
  });
})
.catch(function (err) {
  console.error(err);
  process.exit(1);
});
