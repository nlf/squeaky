'use strict'

const { EventEmitter } = require('events')

const each = require('./each')
const Connection = require('./connection')
const Discoverer = require('./discoverer')

const defaults = {
  host: '127.0.0.1',
  port: 4150,
  lookup: [],
  concurrency: 1,
  timeout: 60000,
  discoverFrequency: 1000 * 60 * 5 // 5 minutes
}

class Squeaky extends EventEmitter {
  constructor (options = {}) {
    super()

    options = Object.assign({}, defaults, options)
    options.lookup = [].concat(options.lookup)
    this.options = options
    this.connections = new Map()
    this.subscriptions = new WeakMap()
  }

  _addListeners (connection, name) {
    connection.on('end', () => {
      this.emit(`${name}.end`)
    })

    connection.on('closed', () => {
      this.emit(`${name}.closed`)
    })

    connection.on('ready', () => {
      this.emit(`${name}.ready`)
    })

    connection.on('error', (err) => {
      if (this.listenerCount('error')) {
        err.connection = name
        this.emit('error', err)
      }
    })
  }

  publish (topic, data, delay) {
    if (!this.connections.has('writer')) {
      const connection = new Connection(this.options)
      this._addListeners(connection, 'writer')
      this.connections.set('writer', connection)
    }

    return this.connections.get('writer').publish(topic, data, delay)
  }

  subscribe (topic, channel, fn) {
    const name = `${topic}.${channel}`

    let result = Promise.resolve()
    let connection
    if (this.connections.has(name)) {
      connection = this.connections.get(name)
    } else {
      connection = this.options.lookup.length ? new Discoverer(this.options) : new Connection(this.options)
      this._addListeners(connection, name)

      connection.on('message', (msg) => {
        this.emit(`${name}.message`, msg)

        const subscriptions = this.subscriptions.get(connection) || new Set()
        return each(subscriptions.values(), (subscription) => {
          return subscription(msg)
        })
      })

      connection.on('added', (address) => {
        this.emit('added', address)
      })

      connection.on('removed', (address) => {
        this.emit('removed', address)
      })

      this.connections.set(name, connection)
      result = result.then(() => {
        return connection.subscribe(topic, channel)
      }).then(() => {
        connection.ready(this.options.concurrency)
      })
    }

    if (fn) {
      const subscriptions = this.subscriptions.get(connection) || new Set()
      subscriptions.add(fn)
      this.subscriptions.set(connection, subscriptions)
    }

    return result.then(() => {
      return connection
    })
  }

  close (...names) {
    return each(this.connections.entries(), ([name, connection]) => {
      if (!names.length || names.includes(name)) {
        this.connections.delete(name)
        return connection.close()
      }
    })
  }

  unref () {
    this.options.unref = true
    for (const connection of this.connections.values()) {
      connection.unref()
    }
  }
}

module.exports = Squeaky
