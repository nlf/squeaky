'use strict'

const { EventEmitter } = require('events')
const net = require('net')
const os = require('os')
const Promise = require('bluebird')

const { Connecting, Identifying, Ready } = require('./states')
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

    this.queue = new Set()
    this.socket = new net.Socket()
    this.socket.on('data', this._receive.bind(this))
    this.socket.on('connect', this._identify.bind(this))
    this.socket.on('end', () => this.emit('end'))

    this.subscribed = false
    this.state = Connecting
    this._buffer = Buffer.from([])
    this._pulsing = new Promise((resolve) => this.once('ready', resolve))

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
      return Promise.each(this.queue.values(), (envelope) => {
        this.queue.delete(envelope)
        this._send(envelope.command, envelope.data)
        if (envelope.promise) {
          this._envelope = envelope
          return envelope.completed
        }
      })
    })
  }

  close () {
    return this._pulsing.then(() => {
      this.socket.destroy()
      this.socket.removeAllListeners()
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
