'use strict'

const { EventEmitter } = require('events')

const Connection = require('./connection')

const defaults = {
  host: '127.0.0.1',
  port: 4150,
  concurrency: 1
}

class Squeaky extends EventEmitter {
  constructor (options = {}) {
    super()

    options = Object.assign({}, defaults, options)
    this.options = options
    this.connections = new Map()
    this.subscriptions = new WeakMap()
  }

  publish (topic, data, delay) {
    if (!this.connections.has('writer')) {
      const connection = new Connection(this.options)

      connection.on('end', () => {
        this.emit('end', 'writer')
      })

      connection.on('error', (err) => {
        if (this.listenerCount('error')) {
          err.connection = 'writer'
          this.emit('error', err)
        }
      })

      connection.on('ready', () => {
        this.emit('writer.ready')
      })

      this.connections.set('writer', connection)
    }

    return this.connections.get('writer').publish(topic, data, delay)
  }

  async subscribe (topic, channel, fn) {
    const name = `${topic}.${channel}`

    let connection
    if (this.connections.has(name)) {
      connection = this.connections.get(name)
    } else {
      connection = new Connection(this.options)

      connection.on('end', () => {
        this.emit('end', name)
      })

      connection.on('error', (err) => {
        if (this.listenerCount('error')) {
          err.connection = name
          this.emit('error', err)
        }
      })

      connection.on('ready', () => {
        this.emit(`${name}.ready`)
      })

      connection.on('message', async (msg) => {
        this.emit(`${name}.message`, msg)

        const subscriptions = this.subscriptions.get(connection) || new Set()
        for (const subscription of subscriptions.values()) {
          await subscription(msg)
        }
      })

      await connection.subscribe(topic, channel)
      connection.ready(this.options.concurrency)
      this.connections.set(name, connection)
    }

    if (fn) {
      const subscriptions = this.subscriptions.get(connection) || new Set()
      subscriptions.add(fn)
      this.subscriptions.set(connection, subscriptions)
    }

    return connection
  }

  async close (...names) {
    for (const [name, connection] of this.connections.entries()) {
      if (!names.length || names.includes(name)) {
        this.connections.delete(name)
        await connection.close()
      }
    }
  }
}

module.exports = Squeaky
