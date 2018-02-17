'use strict'

const { EventEmitter } = require('events')
const net = require('net')
const os = require('os')

const each = require('./each')
const { Closed, Connecting, Identifying, Ready } = require('./states')
const Message = require('./message')
const pkg = require('../package.json')

const createEnvelope = function (command, data, skipPromise) {
  const envelope = { command, data }

  if (!skipPromise) {
    envelope.promise = new Promise((resolve, reject) => {
      envelope.resolver = resolve
      envelope.rejecter = reject
    })

    envelope.completed = new Promise((resolve) => {
      envelope.completer = resolve
    })
  }

  return envelope
}

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
    this.queue = new Set()
    this.socket = new net.Socket()
    this._receive = this._receive.bind(this)
    this._reconnect = this._reconnect.bind(this)
    this._connectAttempts = 0

    this.subscribed = false
    this.state = Connecting
    this._inflight = new Map()
    this._buffer = Buffer.from([])
    this._pulsing = new Promise((resolve) => this.once('ready', resolve))

    this.socket.on('connect', () => {
      this._connectAttempts = 0
      this.socket.on('close', this._reconnect)
      this.socket.on('end', this._reconnect)
      this.socket.on('data', this._receive)
      this._identify()
    })

    this.socket.on('error', this._reconnect)
    this.socket.connect({
      host: options.host,
      port: options.port
    })

    if (this.options.unref) {
      this.unref()
    }
  }

  _reconnect (err) {
    if (err) {
      this.emit('error', err)
    }

    this.emit('end')
    this.socket.removeListener('close', this._reconnect)
    this.socket.removeListener('end', this._reconnect)
    this.socket.removeListener('error', this._reconnect)
    this.socket.removeListener('data', this._receive)

    if (this.state === Closed) {
      return
    }

    if (this._connectAttempts + 1 >= this.options.maxConnectAttempts) {
      this.emit('error', new Error('Maximum reconnection attempts exceeded'))
      return
    }

    setTimeout(() => {
      ++this._connectAttempts
      this.state = Connecting
      this.socket.on('error', this._reconnect)
      this.socket.connect({
        host: this.options.host,
        port: this.options.port
      })
    }, Math.min(this._connectAttempts * this.options.reconnectDelayFactor, this.options.maxReconnectDelay))
  }

  _send (command, data) {
    this._last = command
    const bits = [Buffer.from(`${command}\n`)]
    if (data) {
      appendPayload(data, bits)
    }

    this.socket.write(Buffer.concat(bits))
  }

  _receive (data) {
    // disabling coverage in 3 places so we don't have to mock an nsq server to force packet splitting
    /* istanbul ignore next */
    this._buffer = this._buffer.byteLength ? Buffer.concat([this._buffer, data]) : data

    while (this._buffer.byteLength) {
      /* istanbul ignore if */
      if (this._buffer.byteLength < 4) {
        return
      }

      const frameSize = 4 + this._buffer.readInt32BE(0)
      /* istanbul ignore if */
      if (this._buffer.byteLength < frameSize) {
        return
      }

      const frameType = this._buffer.readInt32BE(4)
      const frameData = this._buffer.slice(8, frameSize)

      this._processFrame(frameSize, frameType, frameData)
      this._buffer = this._buffer.slice(frameSize)
    }
  }

  _processFrame (frameSize, frameType, frameData) {
    // FrameTypeResponse
    if (frameType === 0) {
      const message = frameData.toString()
      // coverage skipped here so we don't have to wait for the server to send a heartbeat
      /* istanbul ignore if */
      if (message === '_heartbeat_') {
        const envelope = createEnvelope('NOP', null, true)
        this.queue.add(envelope)
        this._pulse()
        return
      }

      if (this.state === Identifying) {
        this.features = JSON.parse(frameData)
        this.state = Ready
        this.emit('ready')
      }

      if (this._envelope) {
        this._envelope.resolver(message)
        this._envelope.completer()
        this._envelope = undefined
      }
      return
    }

    // FrameTypeError
    if (frameType === 1) {
      const err = new Error(`Received error response for "${this._last}": ${frameData.toString()}`)
      this.emit('error', err)

      if (this.state === Identifying) {
        this.state = Closed
        this.socket.destroy()
        this.emit('closed')
      }

      if (this._envelope) {
        this._envelope.rejecter(err)
        this._envelope.completer()
        this._envelope = undefined
      }
      return
    }

    // FrameTypeMessage
    // coverage skipped here because testing invalid message types being dropped lacks merit
    /* istanbul ignore else */
    if (frameType === 2) {
      const message = new Message(frameData, this)
      const inflight = {}
      inflight.promise = new Promise((resolve) => {
        inflight.resolve = resolve
      })

      inflight.timer = setTimeout(() => {
        inflight.resolve()
        this._inflight.delete(message.id)
      }, this.features.msg_timeout)

      this._inflight.set(message.id, inflight)

      this.emit('message', message)
    }
  }

  _identify () {
    if (this.state !== Connecting) {
      throw new Error('Attempted to identify during an invalid state')
    }

    this.state = Identifying
    this.socket.write('  V2')
    this._send('IDENTIFY', {
      feature_negotiation: true,
      user_agent: `${pkg.name}/${pkg.version}`,
      hostname: os.hostname(),
      msg_timeout: this.options.timeout
    })
  }

  _pulse () {
    this._pulsing = this._pulsing.then(() => {
      return each(this.queue.values(), (envelope) => {
        this.queue.delete(envelope)
        this._send(envelope.command, envelope.data)
        if (envelope.promise) {
          this._envelope = envelope
          return envelope.completed
        }
      })
    })
  }

  unref () {
    this.socket.unref()
  }

  close () {
    const cleanup = this.subscribed
      ? Promise.resolve(this.ready(0)).then(() => {
        return Promise.all(Array.from(this._inflight.values()).map(value => value.promise))
      }).then(() => {
        return this._pulsing
      })
      : this._pulsing

    return cleanup.then(() => {
      this.state = Closed
      this.socket.destroy()
      this.emit('closed')
    })
  }

  publish (topic, data, delay) {
    if (delay && Array.isArray(data)) {
      return Promise.reject(new Error('Cannot delay a multi publish'))
    }

    const envelope = createEnvelope(
      Array.isArray(data) ? `MPUB ${topic}` : (delay ? `DPUB ${topic} ${delay}` : `PUB ${topic}`),
      data
    )

    this.queue.add(envelope)
    this._pulse()

    return envelope.promise
  }

  subscribe (topic, channel) {
    if (this.subscribed) {
      return Promise.reject(new Error(`This connection is already subscribed to ${this.subscribed}`))
    }

    this.subscribed = `${topic}.${channel}`
    const envelope = createEnvelope(`SUB ${topic} ${channel}`)
    this.queue.add(envelope)
    this._pulse()

    return envelope.promise
  }

  ready (count) {
    const envelope = createEnvelope(`RDY ${count}`, null, true)
    this.queue.add(envelope)
    this._pulse()
  }

  finish (id) {
    const envelope = createEnvelope(`FIN ${id}`, null, true)
    this.queue.add(envelope)
    this._pulse()
    if (!this._inflight.has(id)) {
      return
    }

    const inflight = this._inflight.get(id)
    clearTimeout(inflight.timer)
    inflight.resolve()
    this._inflight.delete(id)
  }

  requeue (id, delay = 0) {
    const envelope = createEnvelope(`REQ ${id} ${delay}`, null, true)
    this.queue.add(envelope)
    this._pulse()
    if (!this._inflight.has(id)) {
      return
    }

    const inflight = this._inflight.get(id)
    clearTimeout(inflight.timer)
    inflight.resolve()
    this._inflight.delete(id)
  }

  touch (id) {
    const envelope = createEnvelope(`TOUCH ${id}`, null, true)
    this.queue.add(envelope)
    this._pulse()
    if (!this._inflight.has(id)) {
      return
    }

    const inflight = this._inflight.get(id)
    clearTimeout(inflight.timer)
    inflight.timer = setTimeout(() => {
      inflight.resolve()
      this._inflight.delete(id)
    }, this.features.msg_timeout)
  }
}

module.exports = Connection
