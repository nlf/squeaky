'use strict'

const Assert = require('assert')
const { EventEmitter } = require('events')

const { Connecting, Closing, Polling, Ready } = require('./states')
const Connection = require('./connection')
const defaults = require('./defaults')
const http = require('./http')

class Subscriber extends EventEmitter {
  constructor (options) {
    super()

    this.options = Object.assign({}, defaults.subscriber, options)
    this.options.lookup = [].concat(this.options.lookup)
    this.debug = this.options.debug

    Assert(this.options.topic, 'Must specify a topic')
    Assert(this.options.channel, 'Must specify a channel')

    this.connections = new Map()
    this.state = Connecting

    if (this.options.lookup.length) {
      this.lookups = this.options.lookup.map(url => !url.startsWith('http') ? `http://${url}` : url)
      this._poll()
    } else {
      const { host, port } = this.options
      this.connections.set(`${host}:${port}`, this._createConnection(host, port))
      this._distributeReady()
    }
  }

  _createConnection (host, port) {
    const options = Object.assign({}, this.options, { host, port })
    const connection = new Connection(options)

    connection.on('error', (err) => this.emit('error', Object.assign(err, { host, port })))
    connection.on('close', () => this.emit('close', { host, port }))
    connection.on('ready', () => this.emit('ready', { host, port }))
    connection.on('connect', () => this.emit('connect', { host, port }))
    connection.on('reconnect', () => this.emit('reconnect', { host, port }))
    connection.on('disconnect', () => this.emit('disconnect', { host, port }))
    connection.on('drain', () => this.emit('drain', { host, port }))
    connection.on('message', (msg) => {
      connection._lastMessage = Date.now()
      this.emit('message', msg)
    })

    connection.subscribe(this.options.topic, this.options.channel)
    // don't cover unref
    /* istanbul ignore next */
    if (this._unref) {
      connection.unref()
    }

    return connection
  }

  async _poll () {
    if (this.state === Closing) {
      return
    }

    this.state = Polling
    this.debug('starting poll')
    const desired = new Set()

    for (const lookup of this.lookups) {
      try {
        const res = await http.get(`${lookup}/lookup?topic=${this.options.topic}`)
        for (const producer of res.producers) {
          const host = producer.broadcast_address
          const port = producer.tcp_port
          desired.add(`${host}:${port}`)
        }
      } catch (err) {
        this.emit('error', Object.assign(err, { code: 'ELOOKUPERROR', host: lookup }))
      }
    }

    for (const [name, connection] of this.connections) {
      if (!desired.has(name)) {
        await connection.close()
        this.connections.delete(name)
        const port = Number(name.split(':').pop())
        const host = name.slice(0, name.lastIndexOf(':'))
        this.emit('removed', { host, port })
      }
    }

    for (const name of desired) {
      if (!this.connections.has(name)) {
        const port = Number(name.split(':').pop())
        const host = name.slice(0, name.lastIndexOf(':'))
        this.connections.set(name, this._createConnection(host, port))
      }
    }

    await this._distributeReady()

    clearTimeout(this._pollTimer)
    this._pollTimer = setTimeout(() => {
      this._poll()
    }, this.options.discoverFrequency)
    this.debug('poll finished')
    this.emit('pollComplete')
  }

  async _distributeReady () {
    if (this.options.concurrency < this.connections.size) {
      let unused = []
      let used = []

      for (const connection of this.connections.values()) {
        if (connection._ready > 0) {
          used.push(connection)
        } else {
          unused.push(connection)
        }
      }

      unused = unused.sort((a, b) => a._lastMessage - b._lastMessage).slice(0, this.options.concurrency)
      // coverage disabled here because Array.prototype.sort doesn't run unless length > 1
      // and i'm not inclined to force having two nsq servers available for tests
      /* istanbul ignore next */
      used = used.sort((a, b) => a._lastMessage - b._lastMessage).slice(0, unused.length)

      for (const connection of used) {
        await connection.ready(0)
      }

      for (const connection of unused) {
        await connection.ready(1)
      }
    } else {
      for (const connection of this.connections.values()) {
        await connection.ready(Math.floor(this.options.concurrency / this.connections.size))
      }
    }

    this.state = Ready
  }

  async close () {
    if (this.state === Polling) {
      await new Promise((resolve) => this.once('pollComplete', resolve))
    }

    clearTimeout(this._pollTimer)
    this.state = Closing

    for (const connection of this.connections.values()) {
      await connection.close()
    }
  }

  // don't cover unref
  /* istanbul ignore next */
  unref () {
    this._unref = true
    for (const connection of this.connections.values()) {
      connection.unref()
    }
  }
}

module.exports = Subscriber
