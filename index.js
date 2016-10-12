var redis = require('redis');
var args = require('minimist')(process.argv);

var client = redis.createClient();
var id = Math.random();

function getMessage() {
    this.cnt = this.cnt || 0;
    return this.cnt++;
}

function startEmit() {
    console.log('Start emitting');
    client.set('emitter', id);
    client.expire('emitter', 5);

    setInterval(function() {
        client.set('emitter', id);
        client.expire('emitter', 5);
    }, 4000);

    setInterval(function() {
        client.rpush('messages', getMessage());
    }, 500);
}

function eventHandler(msg, cb) {
    function onComplete() {
        var error = Math.random() > 0.85;
        cb(error, msg);
    }

    setTimeout(onComplete, Math.floor(Math.random() * 1000));
}

function checkEmitter(cb) {
    client.get('emitter', function(err, emitter) {
        cb(!!emitter);
    });
}

function getMessages() {
    client.blpop('messages', 1, function(err, reply) {
        if (!err && reply) {
            var msg = +reply[1];
            console.log('Got message: ' + msg);

            if (msg) {
                eventHandler(msg, function(err, msg) {
                    if (err) {
                        console.log('error in msg ' + msg);
                        client.rpush('errors', msg);
                    };

                    checkEmitter(function(isEmitterPresent) {
                        if (!isEmitterPresent) {
                            startEmit();
                        } else {
                            getMessages();
                        }
                    });
                });
                return;
            };
        };

        checkEmitter(function(isEmitterPresent) {
            if (!isEmitterPresent) {
                startEmit();
            } else {
                getMessages();
            }
        });
    });
}

if (args.getErrors) {
    client.lrange('errors', 0, -1, function(err, reply) {
        console.log('List of errors:');
        console.log(reply.join('\n'));
        client.del('errors', function(err) {
            client.quit();
        });
    });
} else {
    client.get('emitter', function(err, emitter) {
        if (!emitter) {
            startEmit();
        } else {
            getMessages();
        }
    });
}


