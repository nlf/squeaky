'use strict'

const { EventEmitter } = require('events')
const net = require('net')
const os = require('os')
const Promise = require('bluebird')

const Message = require('./message')
const pkg = require('../package.json')

const Connecting = Symbol('Connecting')
const Identifying = Symbol('Identifying')
const Ready = Symbol('Ready')

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
    for (const chunk of data) {
      appendPayload(chunk, inner)
    }

    const payload = Buffer.concat(inner)
    const header = Buffer.alloc(8)
    header.writeInt32BE(payload.byteLength, 0)
    header.writeInt32BE(data.length, 4)
    bits.push(header, payload)
  } else {
    const payload = encodeToBuffer(data)
    const header = Buffer.alloc(4)
    header.writeInt32BE(payload.byteLength, 0)
    bits.push(header, payload)
  }
}

class Connection extends EventEmitter {
  constructor (options) {
    super()

    this.queue = new Set()
    this.socket = new net.Socket()
    this.socket.on('data', this._receive.bind(this))
    this.socket.on('connect', this._identify.bind(this))
    this.socket.on('end', () => this.emit('end'))

    this.on('ready', () => {
      if (this._completer) {
        this._completer()
        this._completer = undefined
      }

      this._resolver = undefined
      this._rejecter = undefined

      this.state = Ready
    })

    this.subscribed = false
    this.state = Connecting
    this._buffer = Buffer.from([])
    this._pulsing = Promise.resolve()

    this.socket.connect({
      host: options.host,
      port: options.port
    })
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
    this._buffer = Buffer.concat([this._buffer, data])

    while (this._buffer.byteLength) {
      if (this._buffer.byteLength < 4) {
        return
      }

      let frameSize = 4 + this._buffer.readInt32BE(0)
      if (this._buffer.byteLength < frameSize) {
        return
      }

      let frameType = this._buffer.readInt32BE(4)
      let frameData = this._buffer.slice(8, frameSize)

      this._processFrame(frameSize, frameType, frameData)
      this._buffer = this._buffer.slice(frameSize)
    }

    this.emit('ready')
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
      }

      if (this._resolver) {
        this._resolver(message)
      }

      return
    }

    // FrameTypeError
    if (frameType === 1) {
      const err = new Error(`Received error response for "${this._last}": ${frameData.toString()}`)
      this.emit('error', err)
      if (this._rejecter) {
        this._rejecter(err)
      }

      return
    }

    // FrameTypeMessage
    // coverage skipped here because testing invalid message types being dropped lacks merit
    /* istanbul ignore else */
    if (frameType === 2) {
      this.emit('message', new Message(frameData, this))
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
      hostname: os.hostname()
    })
  }

  _pulse () {
    this._pulsing = this._pulsing.then(() => {
      if (this.state !== Ready) {
        return new Promise((resolve) => this.once('ready', resolve))
      }
    }).then(() => {
      return Promise.each(this.queue.values(), (envelope) => {
        this.queue.delete(envelope)
        this._send(envelope.command, envelope.data)
        if (envelope.promise) {
          this._resolver = envelope.resolver
          this._rejecter = envelope.rejecter
          this._completer = envelope.completer
          return envelope.completed
        }
      })
    })
  }

  close () {
    return this._pulsing.then(() => {
      this.socket.destroy()
      this.socket.removeAllListeners()
      this.removeAllListeners()
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
  }

  requeue (id, delay = 0) {
    const envelope = createEnvelope(`REQ ${id} ${delay}`, null, true)
    this.queue.add(envelope)
    this._pulse()
  }

  touch (id) {
    const envelope = createEnvelope(`TOUCH ${id}`, null, true)
    this.queue.add(envelope)
    this._pulse()
  }
}

module.exports = Connection
