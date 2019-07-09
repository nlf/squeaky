'use strict'

const Assert = require('assert')
const { EventEmitter } = require('events')
const { URL } = require('url')

const { Connecting, Closing, Disconnected, Polling, Ready } = require('./states')
const Connection = require('./connection')
const defaults = require('./defaults')
const http = require('./http')

class Subscriber extends EventEmitter {
  constructor (options) {
    super()

    this.options = Object.assign({}, defaults.subscriber)
    if (typeof options === 'string') {
      const hosts = options.split(',')
      const opts = {}

      const first = new URL(hosts.shift())
      for (const key in this.options) {
        let value
        switch (key) {
          case 'host':
            if (first.protocol === 'nsq:') {
              value = first.hostname
            }
            break
          case 'port':
            if (first.protocol === 'nsq:') {
              value = +first.port
            }
            break
          case 'topic':
            value = first.pathname.replace(/^\//, '')
            // coverage disabled here because tests always use ephemeral topics
            /* istanbul ignore next */
            if (first.searchParams.has('ephemeral') || first.searchParams.has('ephemeralTopic')) {
              value += '#ephemeral'
            }
            break
          case 'channel':
            value = first.searchParams.get('channel')
            // coverage disabled here because tests always use ephemeral topics
            /* istanbul ignore next */
            if (first.searchParams.has('ephemeral') || first.searchParams.has('ephemeralChannel')) {
              value += '#ephemeral'
            }
            break
          case 'lookup':
            if (first.protocol === 'nsqlookup:') {
              // coverage disabled so i don't have to set up ssl for tests
              /* istanbul ignore next */
              const proto = first.searchParams.has('ssl') ? 'https' : 'http'
              value = [`${proto}://${first.host}`]
            }
            break
          case 'debug':
            break
          default:
            if (first.searchParams.has(key)) {
              value = first.searchParams.get(key)
              // skipping coverage because right now we only have numerical options
              // but we may have something else in the future and i don't want to
              // footgun accidentally so this guard stays
              /* istanbul ignore next */
              if (typeof defaults.publisher[key] === 'number') {
                value = +value
              }
            }
        }

        if (value) {
          opts[key] = value
        }
      }

      // coverage disabled because i didn't feel like setting up two phony lookupds in tests
      /* istanbul ignore next */
      if (hosts.length && opts.lookup) {
        for (const host of hosts) {
          const parsed = new URL(host)
          if (parsed.protocol === 'nsqlookup:') {
            const proto = first.searchParams.has('ssl') ? 'https' : 'http'
            opts.lookup.push(`${proto}://${parsed.host}`)
          }
        }
      }

      Object.assign(this.options, opts)
    } else {
      Object.assign(this.options, options)
    }

    if (!Array.isArray(this.options.lookup)) {
      this.options.lookup = this.options.lookup ? [].concat(this.options.lookup) : []
    }

    this.debug = this.options.debug
    this.concurrency = this.options.concurrency

    Assert(this.options.topic, 'Must specify a topic')
    Assert(this.options.channel, 'Must specify a channel')

    this.connections = new Map()
    this.state = Disconnected

    this.on('newListener', async (event) => {
      if (!this._started && event === 'message') {
        if (this.state !== Ready) {
          if (this.lookups && this.lookups.length) {
            await new Promise((resolve) => this.once('pollComplete', resolve))
          } else {
            await new Promise((resolve) => this.once('ready', resolve))
          }
        }

        this._started = true
        await this._distributeReady()
      }
    })

    if (this.options.autoConnect) {
      this.connect()
    }
  }

  async connect () {
    if (this.state !== Disconnected) {
      return Promise.reject(new Error('A connection has already been established'))
    }

    this.state = Connecting
    if (this.options.lookup.length) {
      this.lookups = this.options.lookup.map(url => !url.startsWith('http') ? `http://${url}` : url)
      this._poll()
      await new Promise((resolve) => this.once('pollComplete', resolve))
    } else {
      const { host, port } = this.options
      this.connections.set(`${host}:${port}`, this._createConnection(host, port))
      await new Promise((resolve) => this.once('ready', resolve))
    }

    if (this._started) {
      await this._distributeReady()
    }
  }

  async pause () {
    this.concurrency = 0
    await this._distributeReady()
  }

  async unpause () {
    this.concurrency = this.options.concurrency
    await this._distributeReady()
  }

  _createConnection (host, port) {
    const options = Object.assign({}, this.options, { host, port })
    const connection = new Connection(options)

    connection.on('error', (err) => this.emit('error', Object.assign(err, { host, port })))
    connection.on('close', () => this.emit('close', { host, port }))
    connection.on('ready', () => {
      this.emit('ready', { host, port })
      if (!this.options.lookup.length) {
        this.state = Ready
      }
    })
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
    this.state = Polling
    this.emit('pollBegin')
    this.debug('starting poll')
    const desired = new Set()

    this.debug('asking lookups for hosts')
    for (const lookup of this.lookups) {
      try {
        const res = await http.get(`${lookup}/lookup?topic=${this.options.topic}`)
        for (const producer of res.producers) {
          const host = producer.broadcast_address
          const port = producer.tcp_port
          desired.add(`${host}:${port}`)
        }
      } catch (err) {
        this.emit('warn', Object.assign(err, { code: 'ELOOKUPERROR', host: lookup }))
      }
    }

    this.debug('determining which connections to remove')
    for (const [name, connection] of this.connections) {
      if (!desired.has(name)) {
        await connection.close()
        this.connections.delete(name)
        const port = Number(name.split(':').pop())
        const host = name.slice(0, name.lastIndexOf(':'))
        this.emit('removed', { host, port })
      }
    }

    this.debug('determining new connections to be created')
    for (const name of desired) {
      if (!this.connections.has(name)) {
        const port = Number(name.split(':').pop())
        const host = name.slice(0, name.lastIndexOf(':'))
        this.connections.set(name, this._createConnection(host, port))
      }
    }

    this.debug('scheduling next poll run')
    clearTimeout(this._pollTimer)
    this._pollTimer = setTimeout(() => {
      this._poll()
    }, this.options.discoverFrequency)
    this.debug('poll finished')
    this.state = Ready
    this.emit('pollComplete')

    if (this._started) {
      await this._distributeReady()
    }
  }

  async _distributeReady () {
    this.emit('distributeBegin')
    if (this.concurrency < this.connections.size) {
      let unused = []
      let used = []

      for (const connection of this.connections.values()) {
        if (connection._ready > 0) {
          used.push(connection)
        } else {
          unused.push(connection)
        }
      }

      unused = unused.sort((a, b) => a._lastMessage - b._lastMessage).slice(0, this.concurrency)
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
        await connection.ready(Math.floor(this.concurrency / this.connections.size))
      }
    }
    this.emit('distributeComplete')
  }

  async close () {
    if (this.state === Polling) {
      await new Promise((resolve) => this.once('pollComplete', resolve))
    }

    clearTimeout(this._pollTimer)
    this.state = Closing

    for (const [name, connection] of this.connections) {
      await connection.close()
      this.connections.delete(name)
      const port = Number(name.split(':').pop())
      const host = name.slice(0, name.lastIndexOf(':'))
      this.emit('removed', { host, port })
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
