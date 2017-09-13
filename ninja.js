// author: htplbc@gmail.com

var fs = require('fs-extra');
var mkdirp = require('mkdirp');
var express = require('express');
var app = express();
var externalrunner = require('./externalrunner.js');
var modern = require('./helpers.js');
var dateFormat = require('dateformat');
var winston = require('winston');
var debug = require('debug');
var fdebug = debug('ffmpeg');
var util = require('util');
var AWS = require('aws-sdk');
const path = require('path');
var rdebug = (function () {
    var recorderDebug = debug('recorder');
    return function (msg) {
        recorderDebug(dateFormat(new Date(), "HH:MM:ss") + " " + msg);
    };
})();
var f = modern.f;

// ==================================

// global variables
var currentRecordings = {};
var currentSnapshots = {};
var storageDir = '';
var snapshotsDir = '';
var recordingsDir = '';
var snapReconnectTime = '';
var recordReconnectTime = '';

var logger = modern.createLogger({
    logPath: "./logs",
    logFileName: "ninja",
    memoryLimit: 10000
});


logger.info("Explore Ninja starting...");

var config = new modern.ModernConf({
    localFileName: "./config/local.json",
    stateFileName: "./config/state.json",
    dynUpdateUrl: "./config/dyn.json",
    savePeriodMs: 5000,
    dynUpdatePeriodMs : 25000,
    log: logger,
    local: {
        muxgroup: "default"
    },
    dynConfigUpdatedFirstHandler: function() {
        logger.info("dyn conf loaded first!");
        config.dyn = config.dyn || {
            "main": {
                settings: {
                    "httpport": 2115
                }
            }
        };

        configsLoaded(); // 3
    },
    dynConfigUpdatedHandler: function (firsttime, olddyn) {
        logger.info("dyn config loaded");
        if (confChangedHandler && !firsttime)
            confChangedHandler(olddyn, config.dyn);
    }
});

var confChangedHandler;
config.loadAll(logger);

// 3 do preparations
function configsLoaded() {
    logger.info("ok, everything seems to be loaded");
    mkdirp('./logs', function () {
        logsDir = f.convertPathToAbsolute('./logs');

        snapshotLogs = logsDir + config.dyn.snapcontroller.settings.logsDir;
        recordingLogs = logsDir + config.dyn.recordcontroller.settings.logsDir;

        fs.ensureDir(snapshotLogs, err => { console.log(err); });
        fs.ensureDir(recordingLogs, err => { console.log(err); });

        logger.info("log folders created");
        externalrunner({
            cmd: "killall ffmpeg", // i will enable this functionality if ffmpegs will be not killed on their parent death
            stop: function () {
                //log.info("ffmpegs should be killed...");
                //prepS3Client();
                startHttp(); // 4
                snapshotsPrepared(); // 5
                recordingsPrepared(); // 6
                configHandler(); // 7
            }
        }).start();
    }); // mkdirp
}

var activeSnap = {};
var activeRecord = {};

var restartSnap = function (chid) {
};
var restartRecord = function (chid) {
};

var stopSnap = function (chid) {
};
var stopRecord = function (chid) {
};


var s3client = '';

// 4
function startHttp() {
    logger.info("starting http server..");
    var http = new modern.HttpServer(config.local.main.settings.httpport, logger, "THE_SEEECRET", modern.createHttpLoggerFunc(logger));
    http.start();
    http.app.get('/ffmpegsjson', function (req, res) {
        res.send(JSON.stringify({
            ffmpegs: active,
            now_ms: Date.now()
        }, function (key, value) {
            if (key == "runner")
                return undefined;
            return value;
        }));
        //res.json(active);
    });
    http.app.get('/restartffmpeg', function (req, res) {
        var chid = req.query.id;
        restart(chid);
        res.send("ok");
    });
    http.app.get('/snapshot', function (req, res) {
    if (req.query.streamname) {
        var streamname = req.query.streamname;
        var streamnameRegex = streamname.match(/^[^\/]+\/([^\/]+\/)?([^\/]+)$/);
        if (streamnameRegex && streamnameRegex[0] == streamname) {
            var snapshotUrl = config.get('snapshotsWowzaAddr') + streamname;
            makeSnapshotFromUrl(snapshotUrl, function (snapshotErr, snapshotFile) {
                if (snapshotErr) {
                    res.status(404).send(snapshotErr + " Check streamname, please");
                } else {
                    var readFile = fs.createReadStream(snapshotFile);
                    res.writeHead(200, {'Content-Type': 'image/png'});
                    readFile.pipe(res);
                }
            });
        } else {
            logger.error("Cannot resolve streamname (for snapshots) " + streamname);
            res.status(500).end('Cannot resolve streamname...');
        }
    } else {
        res.status(500).end('Cannot find important parameters...');
    }
});
}

function rxExtract(rx, string) {
    if (!string)
        return undefined;
    var arr = rx.exec(string);
    return (arr && arr.length > 1) ? arr[1] : undefined;
}

function getsnaps(dyn) {
    var snapgroup = 'default';
    if (config.local.snapcontroller && config.local.snapcontroller.snapgroup)
        snapgroup = config.local.snapcontroller.snapgroup || 'default';
    if (dyn.snapcontroller && dyn.snapcontroller.groups && dyn.snapcontroller.groups[snapgroup] && dyn.snapcontroller.groups[snapgroup].streams)
        return dyn.snapcontroller.groups[snapgroup].streams;
    return dyn? dyn.streams : undefined;
}

function getrecordings(dyn) {
    var recordgroup = 'default';
    if (config.local.recordcontroller && config.local.recordcontroller.recordgroup)
        recordgroup = config.local.recordcontroller.recordgroup || 'default';
    if (dyn.recordcontroller && dyn.recordcontroller.groups && dyn.recordcontroller.groups[recordgroup] && dyn.recordcontroller.groups[recordgroup].streams)
        return dyn.recordcontroller.groups[recordgroup].streams;
    return dyn? dyn.streams : undefined;
}

// 5
function snapshotsPrepared() {
    logger.info("snapshots prepared!");

    storageDir = f.convertPathToAbsolute(config.dyn.snapcontroller.settings.storageDir);
    snapshotsDir = storageDir + 'snapshots/';

    fs.ensureDir(snapshotsDir, err => { console.log(err); });

    snapReconnectTime = config.dyn.snapcontroller.settings.reconnectTime;

    restartSnap = function(chid) {
        var a = stopSnap(chid);

        var thesnap = getsnaps(config.dyn)[chid];
        if (thesnap.disabled)
            return;

        a = a || {
                id: chid,
                logfile: undefined,
                pretty: chid,
                glog: function(s) {
                    logger.info(a.pretty+": "+s);
                    a.chlog(s);
                },
                chlog: function(s) {
                    if (a.logfile)
                        fs.appendFile(a.logfile, f.shorttimeFunc()+" "+s+"\n", emptyErrorHandler);
                    //console.log("***"+s);
                }
            };

        a.config = thesnap;
        a.pretty = "ch"+chid;//+"("+ a.conf.name+")";
        a.logfile = "./logs/"+config.dyn.snapcontroller.settings.logsDir+"/"+ a.pretty;
        a.glog("will start..");

        a.state = "smallsleep";
        a.started = Date.now();
        a.lastlogtime = 0;
        activeSnap[chid] = a;

        logger.info("Will start periodical snapshots for: " + a.config.videoURL);
        var makePeriodicalSnapshots = function () {
            if (f.ifin(activeSnap, chid)) {
                var startSnapshotTime = Date.now();
                makeSnapshotFromUrl(a.config.videoURL, function (snapshotErr, snapshotFile) {
                    if (snapshotErr) {
                        logger.error("Cannot find stream for snapshots: " + a.config.videoURL + ". Will retry in " + snapReconnectTime + " sec...");
                        setTimeout(function () {
                            snapshotsInterval = makePeriodicalSnapshots();
                        }, snapReconnectTime * 1000);
                    } else {
                        var timeoutTime = startSnapshotTime + a.config.snapshotsPeriod * 1000 - Date.now();
                        if (timeoutTime <= 0) {
                            timeoutTime = 0;
                        }
                        setTimeout(function () {
                            //uploadFileToS3(snapshotFile, chid);
                            //logger.info("snapshotfile created! "+snapshotFile);
                            makePeriodicalSnapshots();
                        }, timeoutTime);
                    }
                });
            }
        };
        makePeriodicalSnapshots();
    };

    stopSnap = function(chid) {
        if (f.notin(activeSnap, chid))
            return undefined;
        var a = activeSnap[chid];
        console.log("a =" + util.inspect(a, {showHidden: false, depth: null}));
        console.log("a.config = " + util.inspect(a.config, {showHidden: false, depth: null}));
        var b = currentSnapshots[a.config.videoURL];
        console.log("currentSnapshots = " + util.inspect(currentSnapshots, {showHidden: false, depth: null}));
        console.log("b = " + util.inspect(b, {showHidden: false, depth: null}));
        a.glog("stopping worker");
        delete activeSnap[chid];
        return a;
    };
}

// 6

function recordingsPrepared() {
    logger.info("recordings prepared!");

    storageDir = f.convertPathToAbsolute(config.dyn.recordcontroller.settings.storageDir);
    recordingsDir = storageDir + 'recordings/';

    fs.ensureDir(recordingsDir, err => { console.log(err); });

    recordReconnectTime = config.dyn.recordcontroller.settings.reconnectTime;

    restartRecord = function(chid) {
        var a = stopRecord(chid);

        var therecord = getrecordings(config.dyn)[chid];
        if (therecord.disabled)
            return;

        a = a || {
                id: chid,
                logfile: undefined,
                pretty: chid,
                glog: function(s) {
                    logger.info(a.pretty+": "+s);
                    a.chlog(s);
                },
                chlog: function(s) {
                    if (a.logfile)
                        fs.appendFile(a.logfile, f.shorttimeFunc()+" "+s+"\n", emptyErrorHandler);
                    //console.log("***"+s);
                }
            };

        a.config = therecord;
        a.pretty = "ch"+chid;//+"("+ a.conf.name+")";
        a.logfile = "./logs/"+config.dyn.recordcontroller.settings.logsDir+"/"+ a.pretty;
        a.glog("will start..");

        a.state = "smallsleep";
        a.started = Date.now();
        a.lastlogtime = 0;
        activeRecord[chid] = a;

        logger.info("Will start recording: " + a.config.videoURL);

        var makePeriodicalRecordings = function () {
            if (f.ifin(activeRecord, chid)) {
                var startRecordTime = Date.now();
                startStreamRecording(a.config.videoURL, function (recordingErr, recordingFile) {
                    if (recordingErr) {
                        logger.error("Cannot find stream for recording: " + a.config.videoURL + ". Will retry in " + recordReconnectTime + " sec...");
                        setTimeout(function () {
                            recordingInterval = makePeriodicalRecordings();
                        }, recordReconnectTime * 1000);
                    } else {
                        var timeoutTime = startRecordTime + a.config.recordingTime * 1000 - Date.now();
                        if (timeoutTime <= 0) {
                            timeoutTime = 0;
                        }
                        setTimeout(function () {
                            makePeriodicalRecordings();
                        }, timeoutTime);
                    }
                });
            }
        };
        makePeriodicalRecordings();
    };

    stopRecord = function(chid) {
        if (f.notin(activeRecord, chid))
            return undefined;
        var a = activeRecord[chid];
        console.log("a =" + util.inspect(a, {showHidden: false, depth: null}));
        console.log("a.config = " + util.inspect(a.config, {showHidden: false, depth: null}));
        var b = currentRecordings[a.config.videoURL];
        console.log("currentRecordings = " + util.inspect(currentRecordings, {showHidden: false, depth: null}));
        console.log("b = " + util.inspect(b, {showHidden: false, depth: null}));
        a.glog("stopping worker");
        delete activeRecord[chid];
        return a;
    };
}

// 7

function configHandler() {
    confChangedHandler = function(oldc, newc) {
        var oldsnap = getsnaps(oldc) || {};
        var oldrecordings = getrecordings(oldc) || {};

        modern.compareObjects(oldsnap, getsnaps(newc), {
            created: function(k) {
                logger.info("created snap["+k+"]");
                restartSnap(k);
            },
            deleted: function(k) {
                logger.info("deleted snap["+k+"]");
                stopSnap(k);
            },
            modified: function(k) {
                logger.info("modified snap["+k+"]");
                restartSnap(k);
            }
        }, function(event, k) {
            if (event != "unchanged")
                logger.info("snap["+k+"] is "+event);
        });

        modern.compareObjects(oldrecordings, getrecordings(newc), {
            created: function(k) {
                logger.info("created recording["+k+"]");
                restartRecord(k);
            },
            deleted: function(k) {
                logger.info("deleted recording["+k+"]");
                stopRecord(k);
            },
            modified: function(k) {
                logger.info("modified recording["+k+"]");
                restartRecord(k);
            }
        }, function(event, k) {
            if (event != "unchanged")
                logger.info("recording["+k+"] is "+event);
        });
    };

    confChangedHandler({},config.dyn); // initial dyn reading start
}

function makeSnapshotFromUrl(snapshotUrl, callback) {
    var urlRegex = snapshotUrl.match(/^.+:\/\/[^\/]+\/([^\/]+)\/([^\/]+\/)?([^\/]+)$/);
    if (urlRegex && urlRegex[0] === snapshotUrl) {
        if (currentSnapshots[snapshotUrl]) {
            logger.warn("Snapshot is still in progress for " + snapshotUrl);

            if (currentSnapshots[snapshotUrl].config) {
                currentSnapshots[snapshotUrl].config.addSnapshotCallback(callback);
            }
            return;
        }

        var snapshotFileName = snapshotUrl.replace(/^(rtmp|https?):\/\//, '').replace(/[^a-zA-Z0-9\-\.]/g, "_");
        var snapshotFile = snapshotsDir + urlRegex[1] + "-" + (Math.floor(Date.now()/1000)) + ".jpg";

        var runner = new externalrunner({
            snapshotCallbacks: [callback],
            addSnapshotCallback: function (cb) {
                this.snapshotCallbacks.push(cb);
            },
            sendAllCallbacks: function (cbErr, cbRes) {
                this.snapshotCallbacks.forEach(function (currentCb) {
                    currentCb(cbErr, cbRes);
                });
            },
            // standart parameters
            cmd: config.dyn.snapcontroller.settings.ffmpegCmd,
            options: [
                "-y", "-i", snapshotUrl, "-vframes", "1", snapshotFile
            ],
            start: function (cmd, options, stdin, restarted) {
                logger.debug("info", "Starting ffmpeg for snapshot: " + snapshotUrl);
                logger.debug("info", cmd + " " + options.join(" "));
            },
            stop: function (code, signal, restartfunc) {
                var self = this;
                if (code !== 0) {
                    logger.debug("error", "Wrong " + this.cmd + " response code: " + code);
                }
                if (fs.existsSync(snapshotFile)) {
                    logger.debug("info", "Created snapshot file: " + snapshotFile);
                    self.sendAllCallbacks(null, snapshotFile);
                } else {
                    logger.debug("error", "Cannot find snapshot: " + snapshotFile);
                    self.sendAllCallbacks("Snapshot generation failed");
                }

                delete currentSnapshots[snapshotUrl];
            },
            stdout: function (line) {
                fdebug("*" + line);
            },
            stderr: function (line) {
                fdebug("+" + line);
            }
        });

        currentSnapshots[snapshotUrl] = runner;
        runner.start();
    } else {
        logger.error("Cannot resolve url (for snapshots) " + snapshotUrl);
        callback("Cannot resolve url");
    }

}

function startStreamRecording(videoURL, callback) {
    var urlRegex = videoURL.match(/^.+:\/\/[^\/]+\/([^\/]+)\/([^\/]+\/)?([^\/]+)$/);
    if (urlRegex && urlRegex[0] === videoURL && urlRegex[1] && urlRegex[3]) {
        // check if recording already exists
        if (currentRecordings[videoURL]) {
            logger.warn("Recording already exists! Will not start recording for periodical " + videoURL);
            currentRecordings[videoURL].config.addRecordingCallback(callback);
            return;
        }

        var recordingsSubDir = recordingsDir + urlRegex[1] + "-" + urlRegex[3] + "/";
        fs.ensureDir(recordingsSubDir, err => { console.log(err); });

        var recordingFileName = videoURL.replace(/^(rtmp|https?):\/\//, '').replace(/[^a-zA-Z0-9\-\.]/g, "_");
        var recordingFile = recordingsSubDir + urlRegex[3] + "_" + Date.now() + (config.dyn.recordcontroller.settings.ext || ".mp4");

        var getRunnerOptions = function () {
            var ffmpegParams = config.dyn.recordcontroller.settings.ffmpegParams.slice(0); // cloning config instead reference
            var recordingsFilename = recordingFile;
            ffmpegParams.push(recordingsFilename);
            if (ffmpegParams.indexOf("{{videoURL}}") != -1) {
                ffmpegParams.splice(ffmpegParams.indexOf("{{videoURL}}"), 1, videoURL);
            }
            return ffmpegParams;
        };

        var runnerTimeout = null;

        var runner = new externalrunner({
            recordingCallbacks: [callback],
            addRecordingCallback: function (cb) {
                this.recodingCallbacks.push(cb);
            },
            sendAllCallbacks: function (cbErr, cbRes) {
                this.recordingCallbacks.forEach(function (currentCb) {
                    currentCb(cbErr, cbRes);
                });
            },
            // standart parameters

            cmd: config.dyn.recordcontroller.settings.ffmpegCmd,
            options: getRunnerOptions(),
            start: function (cmd, options, stdin, restarted) {
                logger.debug("info", "Starting ffmpeg for recording: " + videoURL);
                logger.debug("info", cmd + " " + options.join(" "));
            },
            stop: function (code, signal, restartfunc) {
                var self = this;
                if (code !== 0) {
                    logger.debug("error", "Wrong " + this.cmd + " response code: " + code);
                }
                if (fs.existsSync(recordingFile)) {
                    logger.debug("info", "Created recording file: " + recordingFile);
                    self.sendAllCallbacks(null, recordingFile);
                } else {
                    logger.debug("error", "Cannot find recording: " + recordingFile);
                    self.sendAllCallbacks("Recording generation failed");
                }

                delete currentRecordings[videoURL];
            },
            stdout: function (line) {
                fdebug("*" + line);
            },
            stderr: function (line) {
                fdebug("+" + line);
            }
        });

        currentRecordings[videoURL] = runner;
        runner.start();
    } else {
        logger.error("Cannot resolve url (for recording) " + videoURL);
        callback("Cannot resolve url");
    }
}

function prepS3Client() {
    AWS.config.update(
        {
            accessKeyId: config.local.main.settings.aws_accesskey,
            secretAccessKey:  config.local.main.settings.aws_secretkey,
            region: config.local.main.settings.aws_region
        });
    s3client = new AWS.S3({apiVersion: '2006-03-01'});
}

function uploadFileToS3(fileToUpload, awskeyName) {
    var fileBuffer = fs.readFileSync(fileToUpload);
    var metaData = getContentTypeByFile(fileToUpload);

    var filename = path.basename(fileToUpload);
    var keyName = awskeyName + "/" + filename;

    var params = {
        localFile: fileToUpload,
        s3Params: {
            Bucket: config.local.main.settings.aws_bucket,
            Key: keyName
        }
    };

    s3client.putObject({
        ACL: 'public-read',
        Bucket: config.local.main.settings.aws_bucket,
        Key: keyName,
        Body: fileBuffer,
        ContentType: metaData
    }, function(error, response) {
        console.log('uploaded file[' + filename + '] to [' + keyName + '] as [' + metaData + ']');
        console.log(arguments);
    });
}

function getContentTypeByFile(fileName) {
  var rc = 'application/octet-stream';
  var fileNameLowerCase = fileName.toLowerCase();

  if (fileNameLowerCase.indexOf('.html') >= 0) rc = 'text/html';
  else if (fileNameLowerCase.indexOf('.css') >= 0) rc = 'text/css';
  else if (fileNameLowerCase.indexOf('.json') >= 0) rc = 'application/json';
  else if (fileNameLowerCase.indexOf('.js') >= 0) rc = 'application/x-javascript';
  else if (fileNameLowerCase.indexOf('.png') >= 0) rc = 'image/png';
  else if (fileNameLowerCase.indexOf('.jpg') >= 0) rc = 'image/jpg';

  return rc;
}

function emptyErrorHandler() { }
