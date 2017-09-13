var path = require('path');
var findRemoveSync = require('find-remove');
var winston = require("winston");
var fs = require("fs-extra");
var request = require('request');
var http = require("http");
var url = require("url");
var querystring = require('querystring');
var express = require('express');
var socketio = require('socket.io');

//-------------------------------------------- functions

// this block of helper functions is ideal - they never fail ;)
//noinspection JSUnusedGlobalSymbols
var f = {
    swap: function(o1,a,b,c) {
        var o2 = o1;
        if (c != undefined) {
            o2 = b;
            b = c;
        }
        if (o1 == undefined || o2 == undefined)
            return;

        var temp = o1[a];
        o1[a] = o2[b];
        o2[b] = temp;
    },
    two : function(a)  { if (a<10) return "0"+a; return ""+a; },
    three: function(a) { if (a<10) return "00"+a; if (a < 100) return "0"+a; return ""+a;},
    shorttimeFunc : function(t) {
        t = t || new Date();
        return f.two(t.getHours()) + ':' + f.two(t.getMinutes()) + ":" + f.two(t.getSeconds()) + "." + f.three(t.getMilliseconds());
    },
    file_get_contents: function(url, timeout, callback) {
        callback = callback || function(err, response, data, url) {};
        timeout = timeout || 0;
        if (url.indexOf("http") == 0)
            request({ uri: url, timeout: timeout} , function(err, response, data) {
                callback(err, response, data, url);
            });
        else
            fs.readFile(url, function(err, data) {
                callback(err, { statusCode: (err ? 500 : 200) }, data, url);
            });
    },
    trace : function(a,b) {
        console.log("TRACE: "+a);
        if (b === Object(b))
            console.log(b);
    },
    isIp: function(x) {
        return x ? x.match(/^(?:[0-9]{1,3}\.){3}[0-9]{1,3}$/) : false;
    },
    isNumber:  function(n) {
        return !isNaN(parseFloat(n)) && isFinite(n);
    },
    parse: function(x) {
        if (f.isNumber(x))
            return parseFloat(x);
        if (x=="true") return true;
        if (x=="false") return false;
        if (x.length > 1 && ((x.charAt(0) == "{" && x.charAt(x.length-1)=="}") || (x.charAt(0) == "[" && x.charAt(x.length-1)=="]")))
            return f.json_decode(x);
        return x;
    },
    setDeep: function(e, path, val) {
        var z = path.shift();
        if (f.ifin(e, z)) {
            if (path.length == 1) {
                e[z][path[0]] = val;
                return true;
            }
            else
                return f.setDeep(e[z], path, val);
        }
        return false;
    },
    undef: function(x) { return typeof(x) === "undefined"},
    def: function(x,y) {
        if (f.undef(y))
            return !f.undef(x);
        return f.undef(x) ? y : x;
    },
    findKeyByValue : function(x, val ) {
        for(var prop in x)
            if(x.hasOwnProperty(prop))
                if(x[prop] == val)
                    return prop;
        return undefined;
    },
    error: function(code, text) { return { e: code, d : text }; },
    iserror: function(a) { return f.ifin(a,"e"); },
    each: function(arr, ff) {
        if (Object.prototype.toString.call( arr ) === '[object Array]' ) {
            for (var i = 0; i < arr.length; i++)
                if(ff(arr[i])=="stop")
                    break;
        } else
            for (var key in arr)
                if (arr.hasOwnProperty(key) && ff(key, arr[key])=="stop")
                    break;
    },
    arrayToMap : function(array, keyfield) {
        var len = array.length;
        var res = {};
        var elem;
        for (var i = 0; i < len; i++) {
            elem = array[i];
            res[elem[keyfield]] = elem;
        }
        return res;
    },
    printMap: function(a) {
        var res = "";
        f.each(a, function(k,v) { res+=k+"="+v+", " });
        return res.substring(0, res.length-2);
    },
    json_decode: function(text) {
        try { return JSON.parse(text); }
        catch(e) { return {} }
    },
    /// dead simple cloning via serialization-deserialization
    /// will not work for circular structures
    json_clone: function(o) {
        return o == undefined ? undefined : JSON.parse(JSON.stringify(o));
    },
    random_int: function(min, max) {
        var x = Math.floor(Math.random() * (max - min + 1)) + min; //noinspection JSUnresolvedVariable
        return (x== f.random_int.last_random &&max>min) ? f.random_int(min,max) : x;
    },
    filter: function(a, filters) {
        var res = {};
        f.each(a, function(k,v) {
            var ok = 1;
            f.each(filters, function(fname,fval){
                if (v[fname] != fval) {
                    ok = 0;
                    return "stop";
                }
                return "";
            });
            if (ok==1)
                res[k]=v;
        });
        return res;
    },

    count: function(a) {
        if (a instanceof Array) return a.length;
        if (typeof a === 'object') return Object.keys(a).length;
        return 0;
    },
    ifin: function(a, b) { return a.hasOwnProperty(b); },
    notin: function(a, b) {return !f.ifin(a,b);},
    enrich: function(a,b, override, enrichchilds) {
        if (f.undef(enrichchilds)) enrichchilds = {};
        if (f.undef(override)) override = true;
        f.each(b, function(key, val) {
            if (f.notin(a,key))
                a[key] = val;
            else if (override) {
                a[key] = ((f.ifin(enrichchilds,key) || (f.ifin(enrichchilds,"all")) && (typeof a[key] === 'object')))
                    ? f.enrich(a[key], val, true,
                    ((f.ifin(enrichchilds,key) && enrichchilds[key]=="deep") ? {all:"deep"} : enrichchilds))
                    : val;
            }
        });
        return a;
    },
    readJsonFile: function(filename, handler) {
        fs.readFile(filename, function(err,data) {
            handler(err, f.json_decode(data));
        });
    },
    protectedReplaceFile: function(filename, data, fileSavingConfig ) {
        var c = fileSavingConfig;
        var unlinkingsync = false;
        try {
            var temp_filename = filename+"."+Date.now();
            c = c || { raw: false, uglyJson: false, failCountLimit: 2, errorHandler: undefined};

            // write to temp file
            var str = c.raw ? data :
                (c.uglyJson ? JSON.stringify(data) : JSON.stringify(data, undefined, 4));

            fs.writeFileSync(temp_filename, str);//, function(err) {
            unlinkingsync = true;
            if (fs.existsSync(filename))
                fs.unlinkSync(filename);
            unlinkingsync = false;

            // remove prev temp file
            if (c.prevTempFile && fs.existsSync(c.prevTempFile)) {
                var prev = c.prevTempFile;
                fs.unlink(prev, function(err) {
                    if (err)
                        setTimeout(function() {
                            fs.unlink(prev, function(err2) { if (err2) console.warn("failed to delete tmp state file..: "+prev); /* nobody cares */});
                        }, 1000);
                });
            }
            c.prevTempFile = temp_filename;

            // rename temp file to most fresh file
            fs.renameSync(temp_filename, filename);
            c.failCount = 0;
        }
        catch (e) {
            if (unlinkingsync)
                setTimeout(function khm() {
                    fs.unlink(filename, function(err) { console.log(err); if (err) setTimeout(khm, 2000); }); // try to delete some time later..
                }, 2000);
            // that sometimes happens on my windows machine, so if it will happen too often... then log

            c.failCount = c.failCount || 0;
            if (++c.failCount > (c.failCountLimit || 2))
                if (c.errorHandler)
                    c.errorHandler(e);
        }
    },
    // useful for Amazon actually
    iterateToEnd : function(execFunc, callbackFunc, extractFunc, params, collection, nextToken, nextTokenName) {
        nextTokenName = nextTokenName || "NextToken";
        callbackFunc = callbackFunc || function(err, collection) {};
        params = params || {}; // nothing here
        collection = collection || [];
        if (nextToken)
            params[nextTokenName] = nextToken;
        else
            delete params[nextTokenName];

        execFunc(params, function(err, data) {
            if (err)
                callbackFunc(err, collection);
            else {
                extractFunc(collection, data);
                if (data.hasOwnProperty(nextTokenName))
                    f.iterateToEnd(execFunc, callbackFunc, extractFunc, params, collection, data[nextTokenName]);
                else
                    callbackFunc(undefined, collection);
            }
        });
    },

    convertPathToAbsolute: function (dirPath) {
        var resultDirPath = "";
        resultDirPath = path.normalize(dirPath + "/")
        if (path.resolve(resultDirPath) + "/" != resultDirPath) {
            resultDirPath = __dirname + "/" + resultDirPath;
        }
        return resultDirPath;
    },

    createDirIfNotExists: function (dirFullPath) {
        if (!fs.existsSync(dirFullPath)) {
            fs.mkdirSync(dirFullPath);
        }
    },

    clearOldFiles: function (storageDir, period) {
        var period = period || 3600; // 1 hour
        var result = findRemoveSync(storageDir, {
            extensions: ['.png', '.mp4', '.flv'],
            age: {seconds: period},
            // test: true
        });
        // console.log(result)
    }
};

//----------------------------------------------- logging

var LoggerBase = function(processFunc) {
    this.f = processFunc;
    this.debug = function(msg) { this.aux('debug',msg); };
    this.info = function(msg) { this.aux('info',msg); };
    this.error = function(msg) { this.aux("error", msg); };
    this.warn = function(msg) { this.aux("warn", msg); };
    this.lastLogSecond = 0;
    this.aux = function(level, msg) {
        var currentLogSecond = Math.ceil(Date.now()/1000);
        if (currentLogSecond != this.lastLogSecond) {
            console.log("");
            this.lastLogSecond = currentLogSecond;
        }

        this.f(level, msg);
    };
};

var createHttpLoggerFunc = function(log) {
    return function(req) {
        log.info("request["+req.ip+"]: "+req.url);
    };
};

var createConsoleLogger = function(prefix) {
    prefix = prefix || "";
    return new LoggerBase(function(level,msg) {
        console.log(prefix+level, msg);
    });
};
var createSilentLogger = function() {
    return new LoggerBase(function(level,msg) {
        //console.log(level, msg);
    });
};

var createLogger = function(params) {
    var me = new LoggerBase(undefined);

    me.logPath = params.logPath || "./logs";
    me.logFileName = params.logFileName || "app.log";
    me.memoryLimit = params.memoryLimit || 10000;

    if (!fs.existsSync(me.logPath))
        fs.mkdirsSync(me.logPath);

    me.memory = [];
    me.io = undefined;

    // winston setup
    winston.add(winston.transports.DailyRotateFile, {
        filename: me.logPath+"/"+me.logFileName,
        datePattern: "_yyyyMMdd.log"
    });
    winston.remove(winston.transports.Console);
    winston.add(winston.transports.Console, {'timestamp':f.shorttimeFunc});

    me.f = function(level, msg) {
        winston.log(level,msg);
        var io_msg = {
            l: level,
            m: (typeof msg === 'object' ? msg.message : msg),
            t: Date.now()
        };
        if (me.io)
            me.io.of("/serverlogs").emit("log", io_msg);

        me.memory.push(io_msg);
        while (me.memory.length > me.memoryLimit)
            me.memory.shift();
    };

    return me;
};

//---------------------------------------------- configuration

// local conf, state, and dyn conf
var ModernConf = function(_params) {
    var me = this;
    var params = {
        localFileName: "localconf.json",
        stateFileName: "state.json",
        dynUpdateUrl: "dyn.json",
        savePeriodMs: 1000,
        dynUpdatePeriodMs : 5000,
        log: createConsoleLogger(),
        dynConfigUpdatedFirstHandler: function() { params.log.error("dynConfigUpdatedFirstHandler not specified");},
        dynConfigUpdatedHandler: function() {params.log.error("dynConfigUpdatedHandler not specified");}
    };
    f.enrich(params, _params); // add missing features only
    me.params = params;

    me.state = params.state || {}; // it is ok because should be objects in any case
    me.local = params.local || {};
    me.dyn = { static: {} };

    me.loadLocal = function(handler) {
        if (params.localFileName)
            f.readJsonFile(params.localFileName, function(err,data) {
                f.enrich(me.local, data);
                handler(err);
            });
        else
            handler();
    };

    me.loadState = function(handler) {
        f.readJsonFile(params.stateFileName, function(err,data) {
            f.enrich(me.state, data);
            handler(err);
        } );
    };

    me.saveStateConfig = {
        errorHandler: function(e) {
            params.log.error(e);
        }
    };
    me.saveState = function() {
        f.protectedReplaceFile(params.stateFileName, me.state, me.saveStateConfig);
    };

    me.startStateSavingCycle = function() {
        setTimeout(function() {
            me.saveState();
            me.startStateSavingCycle();
        }, params.savePeriodMs);
    };

    me.loadDyn = function(logTheWholeUpdatedDynConf) {
        var lf = me.loadDyn;
        f.file_get_contents(params.dynUpdateUrl, params.dynUpdatePeriodMs, function (error, response, body) {
            if (!error && response && response.statusCode == 200) {
                if (lf.wasError) {
                    params.log.info('connection to dynamic config is restored now');
                    lf.wasError = false;
                }
                body = body.toString();
                if (lf.prev != body) {
                    lf.prev = body;
                    var obj = f.json_decode(body);
                    if (f.count(obj)>0) {
                        if (logTheWholeUpdatedDynConf)
                            params.log.info("got dyn conf update: "+JSON.stringify(obj));
                        var olddyn = {};
                        f.each(me.dyn, function(k,v) { olddyn[k] = v; });
                        f.enrich(me.dyn, obj);
                        var first = false;
                        if (!lf._isInit) {
                            lf._isInit = first = true;
                            params.dynConfigUpdatedFirstHandler();
                        }
                        //noinspection JSCheckFunctionSignatures
                        params.dynConfigUpdatedHandler(first, olddyn);
                    }
                    else
                        params.log.error("failed parsing json ["+params.dynUpdateUrl+"]: "+body);
                }
            }
            else {
                params.log.error("failed reading ["+params.dynUpdateUrl+"]: "+error+" / "+(response ? response.statusCode : "-"));
                lf.wasError = true;
            }
        });
    };

    me.startDynLoadingCycle = function() {
        me.loadDyn();
        setTimeout(function() {
            me.startDynLoadingCycle();
        }, params.dynUpdatePeriodMs);
    };

    me.loadAll = function(log) {
        log = log || createConsoleLogger();
        me.loadLocal(function(err) {
            if (err)
                log.error("failed loading local file: "+err);
            else
            {
                log.info("local loaded!");
                me.loadState(function(err2) {
                    if (err2)
                        log.warn("failed loading state file: "+err2);
                    else
                        log.info("state loaded!");
                    log.info("starting state saving loop and dynconf loading loop");
                    me.startStateSavingCycle();
                    me.startDynLoadingCycle();
                });
            }
        });
    };
};

var HttpServer = function(port, log, secret, accessLogFunc) {
    var me = this;
    var app = express();

    //noinspection JSValidateTypes
    app.configure(function() {
        app.set('views', __dirname + '/modernviews');
        app.set('view engine', 'jade'); //noinspection JSUnresolvedFunction
        app.use(express.responseTime()); //noinspection JSUnresolvedFunction
        app.use(express.favicon()); //noinspection JSUnresolvedFunction
        app.use(express.cookieParser()); //noinspection JSUnresolvedFunction
        app.use(express.session({ secret: secret || 'THE_SECRET' }));//noinspection JSUnresolvedFunction
        app.use(express.urlencoded());
        app.use(express.json());
        app.use(express.query());
        app.use(function(req, resp, next){
            if (accessLogFunc)
                accessLogFunc(req);
            next();
        });
        app.use(app.router); //noinspection JSUnresolvedFunction
        app.use("/static", express.static(__dirname + '/modernstatic'));
    });

    app.configure('development', function() { //noinspection JSUnresolvedFunction
        app.use(express.errorHandler({ dumpExceptions: true, showStack: true }));
        app.locals.pretty = true;
    });

    app.configure('production', function() { //noinspection JSUnresolvedFunction
        app.use(express.errorHandler());
    });

    var httpServer = http.createServer(app);
    var io = socketio.listen(httpServer);
    io.set("log level",1);

    if (log) {
        if(log.memory) {
            io.of("/serverlogs").on('connection', function (socket) {
                socket.emit("history", log.memory);
            });
        }
        log.io = io;
    }

    me.app = app;
    me.io = io;
    me.httpServer = httpServer;

    me.start = function() {
        httpServer.listen(port);
    }
};

//------------------------------------------------ special "proxy"

// it will connect to frontend server and listen for requests to make local curl
var ProxyIoConnector = function(host, port, room, reconnectTimeout, log) {
    if (!log)
        log = createSilentLogger();
    //reconnectTimeout = reconnectTimeout || 2000;
    room = room || "specialproxy";
    var self = this;
    self.proxyIoClient = require('socket.io-client');

    var socket = self.proxyIoSocket = self.proxyIoClient.connect('http://'+host+":"+port+"/"+room, {
        port: port,
        'connect timeout': 5000,
        reconnect: true,
        // 'force new connection' : false,
        'max reconnection attempts': 5
    });

    socket.on('connect', function(){
        log.info("proxy client connected");
    });

    socket.on('curl', function(data){
        log.info("curl "+JSON.stringify(data));
        var port = data.port || 80;
        var url = data.url || "/";
        var id = data.id || "fail";
        var timeout = data.timeout || 5000;
        request({uri: 'http://localhost:'+port+url, timeout: timeout, encoding: null}, function(error, response, body) {
            //console.log(body);
            //noinspection JSCheckFunctionSignatures
            socket.emit("response", {
                id : id,
                r: {
                    error: error,
                    response: response,
                    body: (body instanceof Buffer ? body : new Buffer(0)).toString("binary")
                }
            });
        });
    });

    socket.on('disconnect', function(){ // nothing to do
        console.log("disconnect");
        //socket.socket.reconnect();
    });
    socket.on('connect_failed', function(){ // nothing to do
        console.log("connect_failed");
    });
    socket.on('reconnect_failed', function(){ // nothing to do
        console.log("reconnect_failed");
    });
    var i = 0;
    socket.on('reconnecting', function(){
        console.log("reconnecting event");
        if (++i > 2) {
            i = 0;
            socket.disconnect();
            socket.socket.reconnect();
        }
    });
    socket.on('error', function(){ // nothing to do
        console.log("error");
        socket.socket.reconnect();
    });


};

// we should run it on frontend as listener to io, so it will accept connections from backend
// activates on construction!
var ProxyIoListener = function(io, room, log) {
    if (!log)
        log = createSilentLogger();
    room = room || "specialproxy";
    var connection = undefined;
    var awaiting = {};
    io.of("/"+room).on('connection', function (socket) {
        log.info("io accepted proxy connection from: "+socket.handshake.address.address);
        connection = socket;

        socket.on('response', function(data){
            log.info("proxy got response for: "+data.id);
            var id = ""+data.id;
            if (awaiting.hasOwnProperty(id)) {
                var a = awaiting[id];
                delete awaiting[id];
                var r = data.r;
                r.response = r.response || {statusCode: 500, headers: {}};
                f.each(r.response.headers, function(k,v) { a.resp.setHeader(k,v); }); // copy headers
                //noinspection JSCheckFunctionSignatures
                r.body = new Buffer(r.body, "binary");
                log.info("writing buffer of "+ r.body.length+" bytes with status code: "+ r.response.statusCode);
                a.resp.send(r.response.statusCode, r.body);
            }
        });

        socket.on('disconnect', function(){
            connection = undefined;
        });
    });
    this.curl = function(req, res, port, timeoutms) {
        var id = ""+Math.random();
        var a = {
            id: id,
            resp: res
        };
        var curl = {
            id: id,
            port: port || 80,
            url: req.url
        };
        if (connection) {
            awaiting[id] = a;
            connection.emit("curl", curl);
            timeoutms = timeoutms || 5000;
            setTimeout(function() {
                if (awaiting.hasOwnProperty(id)) {
                    delete awaiting[id];
                    res.send("timeout", 500);
                }
            }, timeoutms);
        }
        else
            res.send("no connection", 500);
    }
};

var createProxyIoHttpListenerFunc = function(proxyIoListener, port) {
    return function(req, res) {
        proxyIoListener.curl(req, res, port);
    };
};

//------------------------------------------------- comparer

var compareObjects = function(oldo, newo, funcs, reportfunc) {
    funcs = funcs!="test" ? funcs : {deleted: function(a){}, created: function(a,v){}, modified: function(a,v2){}, unchanged: function(a){}};
    var rdel = {}, rleft = {}, rnew = {}, rmod = {}, runc = {};

    f.each(oldo, function(k,v) {
        if (f.undef(newo[k]))
            rdel[k] = v;
        else
            rleft[k] = v;
    });
    f.each(newo, function(k,v) {
        if (f.undef(rleft[k]))
            rnew[k] = v;
    });
    f.each(rleft, function(k,v) {
        var newval = newo[k];
        var eq = JSON.stringify(v) == JSON.stringify(newval);
        if (eq)
            runc[k]= v;
        else
            rmod[k] = newval;
    });

    if (reportfunc) {
        f.each(runc, function(k,v) {reportfunc("unchanged",k,v);});
        f.each(rdel, function(k,v) {reportfunc("deleted",k,v);});
        f.each(rmod, function(k,v) {reportfunc("modified",k,v);});
        f.each(rnew, function(k,v) {reportfunc("created",k,v);});
    }

    if (funcs) {
        if (funcs.unchanged) f.each(runc, function(k) {funcs.unchanged(k);});
        if (funcs.deleted) f.each(rdel, function(k) {funcs.deleted(k);});
        if (funcs.created) f.each(rnew, function(k,v) {funcs.created(k,v);});
        if (funcs.modified) f.each(rmod, function(k,v) {funcs.modified(k,v);});
    }

    return {
        deleted: rdel,
        created: rnew,
        modified: rmod,
        unchanged: runc
    };
};

//------------------------------------------------- unit testing

var prepareUsefulTestingStuff = function() {
    Object.prototype.yo = function(){}; // again to avoid //noinspection BadExpressionStatementJS
    Array.prototype.testEach = function(f) {
        var a = this;
        for (var i = 0; i < a.length; i++)
            eval("a[i]."+f);
    };
};

//------------------------------------------------- http queue

var HttpQueue = function(log) {
    var file_get_contents = require('request');
    this.queues = {};
    var me = this;
    this.curl = function(p) { //url, params, queueId, retries, timeoutMs, onEnd) {
        p = f.enrich({
            queueId : "default",
            timeoutMs: 2000,
            retries: 2,
            silent: false,
            onEnd: function(err, response, data, url, p) {} //res, p) {}
        }, p);
        if (!p.url)
            throw new Error("HttpQueue called without url param");

        // create queue
        if (f.notin(me.queues, p.queueId))
            me.queues[p.queueId] = [];
        var q = me.queues[p.queueId];
        q.push(p);
        if (q.length == 1) {
            var forLog = p.url+ "-->" + JSON.stringify(p.params);
            var next = function(res,p) {
                p.onEnd(res.e, res.response, res.body, p.url, p);
                q.shift();
                if (q.length != 0)
                    getter(q[0]);
            };
            var getter = function(p) {
                file_get_contents({url: p.url, qs: p.params, timeout: p.timeoutMs}, function(err, response, body){
                    if (err) {
                        log.error("failed to curl: "+forLog+" ("+err+"), "+ p.retries+" retries left");
                        if (p.retries != 0) {
                            p.retries--;
                            getter(p);
                        }
                        else
                            next({e: err},p);
                    }
                    else {
                        if (!p.silent)
                            log.info("curl ["+forLog+"] got response: "+body);
                        next({response: response, body: body},p);
                    }
                });
            };
            getter(p);
        }
    };
};

//------------------------------------------------- final exporting

module.exports = {
    createLogger: createLogger,
    createConsoleLogger : createConsoleLogger,
    createSilentLogger : createSilentLogger,
    createHttpLoggerFunc: createHttpLoggerFunc,
    f: f,
    ModernConf: ModernConf,
    HttpServer: HttpServer,

    ProxyIoConnector: ProxyIoConnector,
    ProxyIoListener: ProxyIoListener,
    createProxyIoHttpListenerFunc : createProxyIoHttpListenerFunc,

    compareObjects: compareObjects,
    HttpQueue: HttpQueue,

    prepareUsefulTestingStuff: prepareUsefulTestingStuff
};
