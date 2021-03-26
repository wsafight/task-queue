import { EventEmitter } from 'events'
import ETA from 'node-eta'

export default class Ticket extends EventEmitter {
  public isAccepted: boolean = false;
  public isQueued = false;
  public isStarted = false;
  public isFailed = false;
  public isFinished = false;
  public result = null;
  public status = 'created';
  public eta = new ETA();

  accept() {
    this.status = 'accepted';
    this.isAccepted = true;
    this.emit('accepted');
  }

  queued() {
    this.status = 'queued';
    this.isQueued = true;
    this.emit('queued');
  }

  unqueued() {
    this.status = 'accepted';
    this.isQueued = false;
    this.emit('unqueued');
  }

  started() {
    this.eta.count = 1;
    this.eta.start();
    this.isStarted = true;
    this.status = 'in-progress';
    this.emit('started');
  }

  failed(msg) {
    this.isFailed = true;
    this.isFinished = true;
    this.status = 'failed';
    this.emit('failed', msg);
  }

  finish(result) {
    this.eta.done = this.eta.count;
    this.isFinished = true;
    this.status = 'finished';
    this.result = result;
    this.emit('finish', this.result);
  }


  stopped() {
    this.eta = new ETA();
    this.isFinished = false;
    this.isStarted = false;
    this.status = 'queued';
    this.result = null;
    this.emit('stopped');
  }

  progress(progress) {
    this.eta.done = progress.complete;
    this.eta.count = progress.total;
    this.emit('progress', {
      complete: this.eta.done,
      total: this.eta.count,
      pct: (this.eta.done / this.eta.count) * 100,
      eta: this.eta.format('{{etah}}'),
      message: progress.message
    });
  }
}