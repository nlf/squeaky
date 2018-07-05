'use strict'

const { Socket } = require('net')

class ReconnectingSocket extends Socket {
  constructor (opts) {
    super(Object.assign({ allowHalfOpen: true }, opts))

    this.setNoDelay(true)
    this.options = opts
    this._connectAttempts = 0
    this.debug = opts.debug
    this._reconnect = this._reconnect.bind(this)
    this._disconnect = this._disconnect.bind(this)
    this._emitReconnect = this._emitReconnect.bind(this)
    this._connectListeners = []
    this._retryTimer = null
    this.finished = false

    this.on('close', this._reconnect)
  }

  _disableReconnects (retrying) {
    this.debug('disabling reconnects, retrying:', retrying)
    clearTimeout(this._retryTimer)
    this.removeListener('close', this._reconnect)
    if (!retrying) {
      this.on('close', this._disconnect)
    }
  }

  _disconnect () {
    this.emit('disconnect')
  }

  _reconnect () {
    this.emit('disconnect')

    this._disableReconnects(true)

    if (this._connectAttempts + 1 >= this.options.maxConnectAttempts) {
      this.emit('error', new Error('Maximum reconnect attempts exceeded'))
      this.emit('failed')
      this.finished = true
      return
    }

    this._retryTimer = setTimeout(() => {
      ++this._connectAttempts
      this.removeListener('close', this._disconnect)
      this.removeListener('connect', this._emitReconnect)
      this._connectListeners = this.listeners('connect')
      this.removeAllListeners('connect')
      this.on('close', this._reconnect)
      this.once('connect', this._emitReconnect)
      this.emit('reconnectAttempt')
      this.connect(...this._connection)
    }, Math.min(this._connectAttempts * this.options.reconnectDelayFactor, this.options.maxReconnectDelay))
  }

  _emitReconnect () {
    this.emit('reconnect')
    for (const listener of this._connectListeners) {
      this.on('connect', listener)
    }

    this._connectListeners = []
  }

  connect (...args) {
    this._connection = args
    return super.connect(...args)
  }

  end (...args) {
    this.debug('end')
    this._disableReconnects(false)
    this.finished = true
    return super.end(...args)
  }

  writeAsync (data) {
    this.debug(`writing ${data.length} bytes`)
    return new Promise((resolve) => {
      const res = this.write(data, () => resolve(res))
    })
  }
}

module.exports = ReconnectingSocket
