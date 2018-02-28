'use strict'

const { EventEmitter } = require('events')
const { Socket } = require('net')
const os = require('os')
const util = require('util')

const { Closed, Connecting, Identifying, Reconnecting, Ready } = require('./states')
const Message = require('./message')
const pkg = require('../package')

const encodeToBuffer = function (value) {
  if (Buffer.isBuffer(value)) {
    return value
  } else if (typeof value === 'string') {
    return Buffer.from(value)
  } else if (typeof value === 'object') {
    return Buffer.from(JSON.stringify(value))
  } else {
    return Buffer.from(`${value}`)
  }
}

const appendPayload = function (data, bits) {
  if (Array.isArray(data)) {
    const inner = []
    let size = 0
    for (const chunk of data) {
      size += appendPayload(chunk, inner)
    }

    const header = Buffer.alloc(8)
    header.writeInt32BE(size, 0)
    header.writeInt32BE(data.length, 4)
    bits.push(header, ...inner)
    return header.byteLength + size
  } else {
    const payload = encodeToBuffer(data)
    const header = Buffer.alloc(4)
    header.writeInt32BE(payload.byteLength, 0)
    bits.push(header, payload)
    return header.byteLength + payload.byteLength
  }
}

class Connection extends EventEmitter {
  constructor (options) {
    super()

    this.options = options

    this.connectAttempts = 0
    this.subscribed = false
    this.readyCount = 0
    this.state = Connecting
    this.inflight = new Map()
    this.buffer = Buffer.alloc(0)
    this.sending = new Promise((resolve) => this.once('ready', resolve))
    this.waiting = []

    this._identify = this._identify.bind(this)
    this._receive = this._receive.bind(this)
    this._reconnect = this._reconnect.bind(this)
    this._dispatchResult = this._dispatchResult.bind(this)
    this._dispatchError = this._dispatchError.bind(this)

    this.on('error-response', this._dispatchError)
    this.on('response', this._dispatchResult)
    this.on('message', this._dispatchResult)

    this.socket = new Socket()
    this.socket.writeAsync = util.promisify(this.socket.write.bind(this.socket))
    this.socket.on('error', this._reconnect)
    this.socket.on('connect', () => {
      this.socket.on('data', this._receive)
      this.socket.on('close', this._reconnect)
      this.socket.on('end', this._reconnect)
      this._identify()
    })

    this.socket.connect({
      host: this.options.host,
      port: this.options.port
    })

    if (this.options.unref) {
      this.unref()
    }
  }

  unref () {
    this.socket.unref()
  }

  _dispatchResult (msg) {
    if (this.waiting.length) {
      this.waiting.shift().resolver(msg)
    }
  }

  _dispatchError (err) {
    if (this.waiting.length) {
      this.waiting.shift().rejecter(err)
    }
  }

  _reconnect (err) {
    clearTimeout(this._nextReconnect)
    this.socket.removeListener('data', this._receive)
    this.socket.removeListener('close', this._reconnect)
    this.socket.removeListener('end', this._reconnect)
    this.socket.removeListener('error', this._reconnect)
    this.socket.destroy()

    if (err && err instanceof Error) {
      this.emit('error', err)
    }

    if (this.state === Closed) {
      this.emit('end')
      return
    }

    if (this.state === Ready) {
      this.emit('disconnect')
    }

    if (this.connectAttempts + 1 >= this.options.maxConnectAttempts) {
      this.emit('error', new Error('Maximum reconnection attempts exceeded'))
      this.emit('end')
      return
    }

    this.state = Reconnecting
    this._nextReconnect = setTimeout(() => {
      // skipping coverage here, this block is intended to prevent the reconnect
      // timer from firing if an error happens to trigger during the close cycle
      /* istanbul ignore if */
      if (this.state !== Reconnecting) {
        return
      }

      ++this.connectAttempts
      this.state = Connecting

      if (this.subscribed) {
        this.once('ready', () => {
          return this.subscribe(this.topic, this.channel).then(() => {
            return this.ready(this.readyCount)
          })
        })
        this.subscribed = false
      }

      this.socket.on('error', this._reconnect)
      this.socket.connect({
        host: this.options.host,
        port: this.options.port
      })
    }, Math.min(this.connectAttempts * this.options.reconnectDelayFactor, this.options.maxReconnectDelay))
  }

  _receive (chunk) {
    /* istanbul ignore next */
    this.buffer = this.buffer.byteLength ? Buffer.concat([this.buffer, chunk]) : chunk

    let frame = this._consumeFrame()
    while (frame) {
      this._processFrame(frame)
      frame = this._consumeFrame()
    }
  }

  _consumeFrame () {
    /* istanbul ignore if */
    if (this.buffer.byteLength < 4) {
      return
    }

    const size = 4 + this.buffer.readInt32BE(0)
    /* istanbul ignore if */
    if (this.buffer.byteLength < size) {
      return
    }

    const type = this.buffer.readInt32BE(4)
    const data = this.buffer.slice(8, size)

    this.buffer = this.buffer.slice(size)
    return { size, type, data }
  }

  _processFrame (frame) {
    switch (frame.type) {
      case 0:
        const response = frame.data.toString()
        // skipping coverage to avoid waiting for heartbeats in tests
        /* istanbul ignore next */
        if (response === '_heartbeat_') {
          return this._send('NOP')
        }

        this.emit('response', response)
        break
      case 1:
        const msg = frame.data.toString()
        const code = msg.split(/\s+/)[0]
        const err = new Error(`Received error response for "${this._last}": ${msg}`)
        this.emit('error-response', err)
        if (!['E_REQ_FAILED', 'E_FIN_FAILED', 'E_TOUCH_FAILED'].includes(code)) {
          this.socket.end()
        }
        break
      case 2:
        const message = new Message(frame.data, this)

        const inflight = {}
        inflight.promise = new Promise((resolve) => {
          inflight.resolve = resolve
        })

        inflight.timer = setTimeout(() => {
          inflight.resolve()
          this.inflight.delete(message.id)
        }, this.features.msg_timeout)
        this.inflight.set(message.id, inflight)

        this.emit('message', message)
        break
    }
  }

  _send (command, payload, needsResponse, isIdentify) {
    const send = (command, payload) => {
      this._last = isIdentify ? 'IDENTIFY' : command
      const bits = [Buffer.from(`${command}\n`)]
      if (payload) {
        appendPayload(payload, bits)
      }

      if (needsResponse) {
        const envelope = {}
        envelope.promise = new Promise((resolve, reject) => {
          envelope.resolver = resolve
          envelope.rejecter = reject
        })
        this.waiting.push(envelope)
      }

      const next = () => {
        if (needsResponse) {
          return this.waiting[this.waiting.length - 1].promise
        }
      }

      return this.socket.writeAsync(Buffer.concat(bits)).then(next, next)
    }

    if (isIdentify) {
      return send(command, payload)
    }

    this.sending = this.sending.then(() => {
      return send(command, payload)
    })

    return this.sending
  }

  _identify () {
    if (this.state !== Connecting) {
      return Promise.reject(new Error('Attempted to identify during an invalid state'))
    }

    this.state = Identifying
    return this._send('  V2IDENTIFY', {
      feature_negotiation: true,
      user_agent: `${pkg.name}/${pkg.version}`,
      hostname: os.hostname(),
      msg_timeout: this.options.timeout
    }, true, true).then((response) => {
      this.features = JSON.parse(response)
      this.state = Ready
      this.emit('ready')
    }).catch((err) => {
      this.emit('error', err)
      this.state = Closed
      this.socket.end()
      return new Promise((resolve) => this.socket.once('close', resolve))
    })
  }

  close () {
    const cleanup = this.subscribed
        ? this.sending.then(() => this.cls()).then(() => Promise.all(Array.from(this.inflight.values()).map(msg => msg.promise)))
        : this.sending

    const finish = () => {
      this.state = Closed
      this.socket.end()
      return new Promise((resolve) => this.socket.once('close', resolve))
    }

    return cleanup.then(finish).catch(finish)
  }

  publish (topic, data, delay) {
    if (delay && Array.isArray(data)) {
      return Promise.reject(new Error('Cannot delay a multi publish'))
    }

    return this._send(
      Array.isArray(data) ? `MPUB ${topic}` : (delay ? `DPUB ${topic} ${delay}` : `PUB ${topic}`),
      data,
      true
    )
  }

  subscribe (topic, channel) {
    if (this.subscribed) {
      return Promise.reject(new Error(`This connection is already subscribed to ${this.subscribed}`))
    }

    this.subscribed = true
    this.topic = topic
    this.channel = channel
    return this._send(`SUB ${topic} ${channel}`, null, true)
  }

  cls () {
    return this._send('CLS', null, true)
  }

  ready (count) {
    this.readyCount = count
    return this._send(`RDY ${count}`)
  }

  finish (id) {
    return this._send(`FIN ${id}`).then(() => {
      if (this.inflight.has(id)) {
        const inflight = this.inflight.get(id)
        clearTimeout(inflight.timer)
        inflight.resolve()
        this.inflight.delete(id)
      }
    })
  }

  requeue (id, delay = 0) {
    return this._send(`REQ ${id} ${delay}`).then(() => {
      if (this.inflight.has(id)) {
        const inflight = this.inflight.get(id)
        clearTimeout(inflight.timer)
        inflight.resolve()
        this.inflight.delete(id)
      }
    })
  }

  touch (id) {
    return this._send(`TOUCH ${id}`).then(() => {
      if (this.inflight.has(id)) {
        const inflight = this.inflight.get(id)
        clearTimeout(inflight.timer)
        inflight.timer = setTimeout(() => {
          inflight.resolve()
          this.inflight.delete(id)
        }, this.features.msg_timeout)
      }
    })
  }
}

module.exports = Connection
