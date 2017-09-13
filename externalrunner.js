var tokenize = function (data) {
    var astr = data.split(" ");
    var res = [];
    var token = "";
    for (var i = 0; i < astr.length; i++) {
        token += (token.length > 0 ? " " : "") + astr[i];
        if (token.length > 3)
            token = token.replace(/^"(.*[^\\])"$/, "$1");
        if (token.length > 0) {
            if (token.charAt(0) != '"') {
                res.push(token.replace(/\\"/g, '"'));
                token = "";
            }
        }
    }
    if (token.length > 0)
        res.push(token.substring(1).replace(/\\"/g, '"'));
    return res;
};

//noinspection FunctionWithInconsistentReturnsJS
var ExternalRunner = function (conf) {
    if (!(this instanceof ExternalRunner)) return new ExternalRunner(conf);

    var spawn = require('child_process').spawn;
    var split = require("split");
    var self = this;

    conf.start = conf.start || function () {
        };
    conf.stop = conf.stop || function () {
        };
    conf.stdout = conf.stdout || function () {
        };
    conf.stderr = conf.stderr || function () {
        };

    conf.autoflush = ("autoflush" in conf) ? conf.autoflush : true;

    self.conf = conf;

    self.go = function (restarted) {
        self.options = conf.options;
        self.cmd = conf.cmd;
        if ((typeof self.options) === "undefined") {
            self.options = tokenize(self.cmd);
            self.cmd = self.options.splice(0, 1)[0];
        }
        if ((typeof self.options) === 'string') {
            self.options = tokenize(self.options);
        }

        var r = spawn(self.cmd, self.options);

        r.stdout.pipe(split(/\r?\n|\r/)).on("data", function (data) {
            conf.stdout(data);
        });
        r.stderr.pipe(split(/\r?\n|\r/)).on("data", function (data) {
            conf.stderr(data);
        });

        r.on("close", function (code, signal) {
            //console.log(code);
            conf.stop(code, signal, function () {
                self.child = self.go(true);
            });
        });
        r.on("exit", function (code) {
            // console.log("EXIT: "+code);
        });
        r.on("error", function (err) {
            console.log("ExternalRunner error: " + err);
        });

        var stdin = {
            stdin: r.stdin,
            write: function (data) {
                try {
                    r.stdin.write(data);
                }
                catch (e) {
                    console.log("khm.." + e);
                }
            }
        };
        stdin.writeln = function (data) {
            stdin.write(data + "\n");
        };

        conf.start(self.cmd, self.options, stdin, restarted);

        return r;
    };

    self.start = function () {
        self.child = self.go(false);
    };

    self.stop = function (code) {
        code = code || "SIGINT";
        if (self.child) {
            self.child.kill(code);
        }
    }
};

module.exports = ExternalRunner;
