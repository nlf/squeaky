'use strict'

const { EventEmitter } = require('events')

const { Connecting, Closing, Disconnected, Ready } = require('./states')
const Connection = require('./connection')
const defaults = require('./defaults')

class Publisher extends EventEmitter {
  constructor (options) {
    super()

    this.options = Object.assign({}, defaults.publisher, options)
    this.state = Disconnected
    if (this.options.autoConnect) {
      this.connect()
    }
  }

  connect () {
    if (this.state !== Disconnected) {
      return Promise.reject(new Error('A connection has already been established'))
    }

    this.state = Connecting
    this.connection = new Connection(this.options)

    this.connection.on('error', (err) => this.emit('error', err))
    this.connection.on('close', () => this.emit('close'))
    this.connection.on('ready', () => {
      this.state = Ready
      this.emit('ready')
    })
    this.connection.on('connect', () => this.emit('connect'))
    this.connection.on('reconnect', () => this.emit('reconnect'))
    this.connection.on('disconnect', () => this.emit('disconnect'))
    this.connection.on('drain', () => this.emit('drain'))

    return new Promise((resolve) => this.once('ready', resolve))
  }

  publish (topic, data, delay) {
    return this.connection.publish(topic, data, delay)
  }

  async close () {
    this.state = Closing
    await this.connection.close()
    this.state = Disconnected
  }

  // don't cover unref
  /* istanbul ignore next */
  unref () {
    return this.connection.unref()
  }
}

module.exports = Publisher
