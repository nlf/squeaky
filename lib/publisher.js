'use strict'

const { EventEmitter } = require('events')
const { URL } = require('url')

const { Connecting, Closing, Disconnected, Ready } = require('./states')
const Connection = require('./connection')
const defaults = require('./defaults')

class Publisher extends EventEmitter {
  constructor (options) {
    super()

    this.options = Object.assign({}, defaults.publisher)
    if (typeof options === 'string') {
      const parsed = new URL(options)
      const opts = {}
      for (const key in this.options) {
        let value
        switch (key) {
          case 'host':
            value = parsed.hostname
            break
          case 'port':
            value = +parsed.port
            break
          case 'topic':
            const trimmed = parsed.pathname.replace(/^\//, '')
            if (trimmed) {
              value = trimmed
              // coverage disabled here because tests always use ephemeral topics
              /* istanbul ignore next */
              if (parsed.searchParams.has('ephemeral')) {
                value += '#ephemeral'
              }
            }
            break
          case 'debug':
            break
          default:
            if (parsed.searchParams.has(key)) {
              value = parsed.searchParams.get(key)
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

      Object.assign(this.options, opts)
    } else {
      Object.assign(this.options, options)
    }

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

  publish (...args) {
    let topic, data, delay
    if (this.options.topic) {
      topic = this.options.topic
      ;[data, delay] = args
    } else {
      ;[topic, data, delay] = args
    }

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
