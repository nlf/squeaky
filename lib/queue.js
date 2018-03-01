'use strict'

const { EventEmitter } = require('events')

class Queue extends EventEmitter {
  constructor ({ action }) {
    super()

    this._pulse = this._pulse.bind(this)

    this.items = []
    this.running = false
    this.drained = false
    this.action = action
  }

  add (task, first) {
    const envelope = {}
    envelope.promise = new Promise((resolve, reject) => {
      envelope.resolve = resolve
      envelope.reject = reject
    })
    envelope.task = task

    if (first) {
      this.items.unshift(envelope)
    } else {
      this.items.push(envelope)
    }

    if (this.running && this.drained) {
      this.drained = false
      this._pulse()
    }

    return envelope.promise
  }

  start () {
    this.running = true
    this.drained = false
    this._pulse()
  }

  stop () {
    if (!this.running || this.drained) {
      return Promise.resolve()
    }
    this.running = false

    return new Promise((resolve) => this.once('stopped', resolve))
  }

  _pulse () {
    if (!this.running) {
      this.emit('stopped')
      return
    }

    if (!this.items.length) {
      this.emit('drain')
      this.drained = true
      return
    }

    const envelope = this.items.shift()
    return this.action(envelope.task).then((res) => {
      envelope.resolve(res)
      return this._pulse()
    }).catch((err) => {
      envelope.reject(err)
      return this._pulse()
    })
  }
}

module.exports = Queue
