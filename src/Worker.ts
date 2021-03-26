import { EventEmitter } from 'events';
import ETA from 'node-eta'

export default class Worker extends EventEmitter {
  fn: any
  batch: boolean
  single: boolean
  active = false;
  cancelled = false;
  failTaskOnProcessException: any

  private _taskIds: string[]
  _process: Record<string, any>
  _waiting: Record<string, any>
  progress: Record<string, any>
  _eta: ETA
  counts: any;
  status: string;

  constructor(opts: Record<string, any>) {
    super()
    this.fn = opts.fn
    this.batch = opts.batch
    this.single = opts.single
    this.failTaskOnProcessException = opts.failTaskOnProcessException
  }

  setup() {


    // Internal
    this._taskIds = Object.keys(this.batch);
    this._process = {};
    this._waiting = {};
    this._eta = new ETA();

    // Task counts
    this.counts = {
      finished: 0,
      failed: 0,
      completed: 0,
      total: this._taskIds.length,
    };

    // Progress
    this.status = 'ready';
    this.progress = {
      tasks: {},
      complete: 0,
      total: this._taskIds.length,
      eta: '',
    };

    // Setup
    this._taskIds.forEach((taskId, id) => {
      this._waiting[id] = true;
      this.progress.tasks[id] = {
        pct: 0,
        complete: 0,
        total: 1,
      }
    })
  }

  start() {
    if (this.active) {
      return;
    }

    this.setup();
    this._eta.count = this.progress.total;
    this._eta.start();

    this.active = true;
    this.status = 'in-progress';
    let tasks: any = this._taskIds.map((taskId) => this.batch[taskId]);
    if (this.single) {
      tasks = tasks[0]
    }
    try {
      this._process = this.fn.call(this, tasks, function (err, result) {
        if (!this.active) {
          return;
        }
        if (err) {
          this.failedBatch(err);
        } else {
          this.finishBatch(result);
        }
      })
    } catch (err) {
      if (this.failTaskOnProcessException) {
        this.failedBatch(err);
      } else {
        throw new Error(err);
      }
    }
    this._process = this._process || {};
  }

  end() {
    if (!this.active) {
      return
    }
    this.status = 'finished';
    this.active = false;
    this.emit('end');
  }

  resume() {
    if (typeof this._process.resume === 'function') {
      this._process.resume();
    }
    this.status = 'in-progress';
  }

  pause() {
    if (typeof this._process.pause === 'function') {
      this._process.pause();
    }
    this.status = 'paused';
  }


  cancel() {
    this.cancelled = true;
    if (typeof this._process.cancel === 'function') {
      this._process.cancel();
    }
    if (typeof this._process.abort === 'function') {
      this._process.abort();
    }
    this.failedBatch('cancelled');
  }

  failedBatch(msg) {
    if (!this.active) {
      return;
    }
    Object.keys(this._waiting).forEach((id) => {
      if (!this._waiting[id]) return;
      this.failedTask(id, msg);
    })
    this.emit('failed', msg);
    this.end();
  }

  failedTask(id: string, msg: string) {
    if (!this.active) return;
    if (this._waiting[id]) {
      this._waiting[id] = false;
      this.counts.failed++;
      this.counts.completed++;
      this.emit('task_failed', id, msg);
    }
  }

  finishBatch(result) {
    if (!this.active) {
      return;
    }
    Object.keys(this._waiting).forEach((id) => {
      if (!this._waiting[id]) return;
      this.finishTask(id, result);
    })
    this.emit('finish', result);
    this.end();
  }


  finishTask(id, result) {
    if (!this.active) {
      return;
    }
    if (this._waiting[id]) {
      this._waiting[id] = false;
      this.counts.finished++;
      this.counts.completed++;
      this.emit('task_finish', id, result);
    }
  }

  progressBatch = function (complete, total, msg) {
    if (!this.active) {
      return;
    }
    Object.keys(this._waiting).forEach((id) => {
      if (!this._waiting[id]) return;
      this.progressTask(id, complete, total, msg);
    })
    this.progress.complete = 0;
    this._taskIds.forEach((taskId, id) => {
      this.progress.complete += this.progress.tasks[id].pct;
    })
    this._eta.done = this.progress.complete;
    this.progress.eta = this._eta.format('{{etah}}')
    this.progress.message = msg || '';
    this.emit('progress', this.progress);
  }

  progressTask = function (id, complete, total, msg) {
    if (!this.active) {
      return
    }
    if (this._waiting[id]) {
      this.progress.tasks[id].complete = complete;
      this.progress.tasks[id].total = this.progress.tasks[id].total || total;
      this.progress.tasks[id].message = this.progress.tasks[id].message || msg;
      this.progress.tasks[id].pct = Math.max(0, Math.min(1, complete / total));
      this.emit('task_progress', id, this.progress.tasks[id]);
    }
  }

}