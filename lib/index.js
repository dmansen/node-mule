var childProcess = require('child_process'),
    events = require('events'),
    os = require('os'),
    util = require('util');


WorkerState = {
    STARTING: 'STARTING',
    READY:    'READY',
    BUSY:     'BUSY'
};
module.exports.WorkerState = WorkerState;

/**
 * Encapsulates a worker process. Manages state and notifying listeners of
 * changes in state.
 */
function Worker(workerScript) {
    this.process = childProcess.fork(workerScript);
    this.pid = this.process.pid;
    this.status = WorkerState.STARTING;

    this.process.once('message', this.onReady.bind(this));

    events.EventEmitter.call(this);
}
util.inherits(Worker, events.EventEmitter);

Worker.prototype.onReady = function (message) {
    if (this.status === WorkerState.STARTING) {
        console.log('Worker ' + this.pid + ' ready.');
        this.status = WorkerState.READY;
        this.emit('ready', this);
    }
};

Worker.prototype.onMessage = function (callback, message) {
    callback(message);

    this.status = WorkerState.READY;
    this.emit('ready', this);
};

Worker.prototype.send = function (message, callback) {
    this.status = WorkerState.BUSY;
    this.emit('busy');

    this.process.once('message', this.onMessage.bind(this, callback));
    this.process.send(message);
};
module.exports.Worker = Worker;


function WorkQueue(workerScript, nWorkers, options) {
    this.workers = [];
    this.queue = [];
    this.options = options || {};
    this.workerScript = workerScript;

    nWorkers = nWorkers || os.cpus().length;
    console.log('Starting ' + nWorkers + ' workers..');
    for (var i = 0; i < nWorkers; i++) {
        this.fork(workerScript);
    }
}

WorkQueue.prototype.fork = function() {
    var self = this;

    var worker = new Worker(this.workerScript);

    worker.on('ready', self._run.bind(self));

    worker.process.on('exit', function (code, signal) {
        if (code !== 0) { // Code will be non-zero if process dies suddenly
            console.warn('Worker process ' + worker.pid + ' died with code ' + code + '. Respawning...');
            for (var i = 0; i < self.workers.length; i++) {
                if (self.workers[i].pid === worker.pid) {
                    self.workers.splice(i, 1); // Remove dead worker from pool.
                }
            }
            self.fork(this.workerScript); // FTW!
        } else {
            console.warn("Dead on purpose, code: " + code);
        }
    });

    self.workers.push(worker);
}

/**
 * Enqueue a task for a worker process to handle. A task can be any type of var,
 * as long your worker script knows what to do with it.
 */
WorkQueue.prototype.enqueue = function (task, timeout, callback) {
    if(!callback) {
        callback = timeout;
        timeout = null;
    }

    this.queue.push({ task: task, callback: callback, timeout: timeout });
    process.nextTick(this._run.bind(this));
};

WorkQueue.prototype._run = function (worker) {
    if (this.queue.length === 0) {
        return; // nothing to do
    }

    if (!worker) {
        // Find the first available worker.
        for (var i = 0; i < this.workers.length; i++) {
            if (this.workers[i].status === WorkerState.READY) {
                worker = this.workers[i];
                break;
            }
        }
    }

    if (!worker) {
        if(this.options.autoexpand) {
            // if max_workers is set, make sure we don't exceed it
            if(this.options.max_workers && this.workers.length > this.options.max_workers) return;

            console.warn("Ran out of workers, forking another");
            this.fork(this.workerScript);
            process.nextTick(this._run.bind(this));
            return;
        } else {
            return; // there are no workers available to handle requests. Leave queue as is.
        }
    }

    var queued = this.queue.shift();
    var callback = null;

    if(queued.timeout) {
        var timeoutId = setTimeout(function() {
            worker.process.emit('exit', 1);
        }, queued.timeout);

        callback = function() {
            clearTimeout(timeoutId);

            queued.callback.apply(this, arguments);
        };
    } else {
        callback = queued.callback;
    }

    worker.send(queued.task, callback);
};
module.exports.WorkQueue = WorkQueue;
