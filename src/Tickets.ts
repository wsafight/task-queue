// @ts-ignore
import Ticket from './Ticket.ts'

export default class Tickets {
  public tickets: Ticket[] = []

  push(ticket: Ticket) {
    if (ticket instanceof Tickets) {
      return ticket.tickets.forEach((ticket) => {
        this.push(ticket)
      })
    } else if (ticket instanceof Ticket) {
      if (!this.tickets.includes(ticket)) {
        this.tickets.push(ticket);
      }
    }
  }

  private _apply(fn: string, args?: any[]) {
    this.tickets.forEach((ticket) => {
      ticket[fn]?.(args);
    })
  }

  accept() {
    this._apply('accept')
  }

  queued() {
    this._apply('queue')
  }

  unqueued() {
    this._apply('unqueued')
  }

  started() {
    this._apply('started')
  }

  failed(msg) {
    this._apply('started', msg)
  }

  finish(result) {
    this._apply('finish', result)
  }


  stopped() {
    this._apply('stopped')
  }

  progress(progress) {
    this._apply('progress', progress)
  }
}