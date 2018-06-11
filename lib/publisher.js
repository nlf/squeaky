'use strict'

const { EventEmitter } = require('events')

const Connection = require('./connection')
const defaults = require('./defaults')

class Publisher extends EventEmitter {
  constructor (options) {
    super()

    this.options = Object.assign({}, defaults.publisher, options)
    this.connection = new Connection(this.options)

    this.connection.on('error', (err) => this.emit('error', err))
    this.connection.on('close', () => this.emit('close'))
    this.connection.on('ready', () => this.emit('ready'))
    this.connection.on('connect', () => this.emit('connect'))
    this.connection.on('reconnect', () => this.emit('reconnect'))
    this.connection.on('disconnect', () => this.emit('disconnect'))
    this.connection.on('drain', () => this.emit('drain'))
  }

  publish (topic, data, delay) {
    return this.connection.publish(topic, data, delay)
  }

  close () {
    return this.connection.close()
  }

  // don't cover unref
  /* istanbul ignore next */
  unref () {
    return this.connection.unref()
  }
}

module.exports = Publisher
