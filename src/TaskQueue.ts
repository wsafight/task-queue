import { EventEmitter } from 'events'

export default class TaskQueue extends EventEmitter {

  process: Function
  filter: Function
  merge: Function
  precondition: Function
  setImmediate: any
  id: string
  priority: null | any = null
  cancelIfRunning: boolean
  autoResume: boolean
  failTaskOnProcessException: any
  filo: boolean
  batchSize: number;
  batchDelay: number;
  batchDelayTimeout: number;
  afterProcessDelay: number
  concurrent: number;
  maxTimeout: number
  maxRetries: number;

  retryDelay: number
  storeMaxRetries: number;
  storeRetryTimeout: number
  preconditionRetryTimeout: number;

  // Statuses
  _queuedPeak: number = 0
  _queuedTime: Record<string, any> = {}
  _processedTotalElapsed: number = 0;
  _processedAverage: number = 0;
  _processedTotal: number = 0;
  _failedTotal: number = 0;
  length: number = 0;
  _stopped: boolean = false;
  _saturated: boolean = false;

  _preconditionRetryTimeoutId = null;
  _batchTimeoutId = null;
  _batchDelayTimeoutId = null;
  _connected = false;
  _storeRetries = 0;

  // Locks
  _hasMore = false;
  _isWriting = false;
  _writeQueue = [];
  _writing = {};
  _tasksWaitingForConnect = [];

  _calledDrain = true;
  _calledEmpty = true;
  _fetching = 0;
  _running = 0;  // Active running tasks
  _retries = {}; // Map of taskId => retries
  _workers = {}; // Map of taskId => active job
  _tickets = {}; // Map of taskId => tickets

  _store: any


  constructor(process: any, opts: Record<string, any> = {}) {
    super();
    if (typeof process === 'object') {
      opts = process || {};
    }
    if (typeof process === 'function') {
      opts.process = process;
    }
    if (!opts.process) {
      throw new Error("Queue has no process function.");
    }

    opts = opts || {};

    this.process = opts.process || function (task, cb) {
      cb(null, {})
    };
    this.filter = opts.filter || function (input, cb) {
      cb(null, input)
    };
    this.merge = opts.merge || function (oldTask, newTask, cb) {
      cb(null, newTask)
    };
    this.precondition = opts.precondition || function (cb) {
      cb(null, true)
    };
    this.setImmediate = opts.setImmediate || setImmediate;
    this.id = opts.id || 'id';
    this.priority = opts.priority || null;

    this.cancelIfRunning = (opts.cancelIfRunning === undefined ? false : !!opts.cancelIfRunning);
    this.autoResume = (opts.autoResume === undefined ? true : !!opts.autoResume);
    this.failTaskOnProcessException = (opts.failTaskOnProcessException === undefined ? true : !!opts.failTaskOnProcessException);
    this.filo = opts.filo || false;
    this.batchSize = opts.batchSize || 1;
    this.batchDelay = opts.batchDelay || 0;
    this.batchDelayTimeout = opts.batchDelayTimeout || Infinity;
    this.afterProcessDelay = opts.afterProcessDelay || 0;
    this.concurrent = opts.concurrent || 1;
    this.maxTimeout = opts.maxTimeout || Infinity;
    this.maxRetries = opts.maxRetries || 0;
    this.retryDelay = opts.retryDelay || 0;
    this.storeMaxRetries = opts.storeMaxRetries || Infinity;
    this.storeRetryTimeout = opts.storeRetryTimeout || 1000;
    this.preconditionRetryTimeout = opts.preconditionRetryTimeout || 1000;


    // Initialize Storage
    this.use(opts.store || 'memory');
    if (!this._store) {
      throw new Error('Queue cannot continue without a valid store.')
    }

  }

  use(store, opts?: any) {
    const loadStore = function (store) {
      let Store;
      try {
        Store = require('better-queue-' + store);
      } catch (e) {
        throw new Error('Attempting to require better-queue-' + store + ', but failed.\nPlease ensure you have this store installed via npm install --save better-queue-' + store)
      }
      return Store;
    }
    if (typeof store === 'string') {
      const Store = loadStore(store);
      this._store = new Store(opts);
    } else if (typeof store === 'object' && typeof store.type === 'string') {
      const Store = loadStore(store.type);
      this._store = new Store(store);
    } else if (typeof store === 'object' && store.putTask && store.getTask && ((this.filo && store.takeLastN) || (!this.filo && store.takeFirstN))) {
      this._store = store;
    } else {
      throw new Error('unknown_store');
    }
    this._connected = false;
    this._tasksWaitingForConnect = [];
    this._connectToStore();
  }

  private _connectToStore() {
    if (this._connected) return;
    if (this._storeRetries >= this.storeMaxRetries) {
      return this.emit('error', new Error('failed_connect_to_store'));
    }
    this._storeRetries++;
    this._store.connect(function (err, len) {
      if (err) return setTimeout(function () {
        this._connectToStore();
      }, this.storeRetryTimeout);
      if (len === undefined || len === null) {
        throw new Error("store_not_returning_length");
      }
      this.length = parseInt(len);
      if (isNaN(self.length)) {
        throw new Error("length_is_not_a_number");
      }
      if (this.length) {
        this._calledDrain = false;
      }
      this._connected = true;
      this._storeRetries = 0;
      this._store.getRunningTasks(function (err, running) {
        if (!this._stopped && this.autoResume) {
          Object.keys(running).forEach(function (lockId) {
            this._running++;
            this._startBatch(running[lockId], {}, lockId);
          })
          this.resume();
        }
        for (var i = 0; i < this._tasksWaitingForConnect.length; i++) {
          this.push(this._tasksWaitingForConnect[i].input, this._tasksWaitingForConnect[i].ticket);
        }
      })
    })
  }

  resume() {
    this._stopped = false;
    this._getWorkers().forEach((worker) => {
      if (typeof worker.resume === 'function') {
        worker.resume();
      }
    })
    setTimeout(function () {
      this._processNextAfterTimeout();
    }, 0)
  }

  private _getWorkers() {
    const workers = [];
    Object.keys(this._workers).forEach((taskId) => {
      var worker = this._workers[taskId];
      if (worker && !workers.includes(worker)) {
        workers.push(worker);
      }
    })
    return workers;
  }
}