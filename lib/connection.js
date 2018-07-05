'use strict'

const { EventEmitter } = require('events')
const crypto = require('crypto')
const os = require('os')

const { Closed, Connecting, Failure, Identifying, Pulsing, Ready } = require('./states')
const Message = require('./message')
const Protocol = require('./protocol')
const Socket = require('./socket')
const pkg = require('../package.json')

class Connection extends EventEmitter {
  constructor (options) {
    super()

    this.options = options
    this.debug = this.options.debug
    this.socket = new Socket(this.options)
    this.queue = []

    this._buffer = Buffer.alloc(0)

    this._pulse = this._pulse.bind(this)
    this._connect = this._connect.bind(this)
    this._disconnect = this._disconnect.bind(this)
    this._receive = this._receive.bind(this)
    this._reconnect = this._reconnect.bind(this)

    this.state = Connecting

    this.on('ready', () => {
      this.debug('connection ready')
      this._pulse()
    })
    this.on('drain', () => {
      this.debug('connection drained')
      this.state = Ready
    })

    this.socket.on('disconnect', this._disconnect)
    this.socket.on('connect', this._connect)
    this.socket.on('reconnect', this._reconnect)
    this.socket.on('data', this._receive)
    this.socket.on('failed', () => {
      this.state = Failure
      this.emit('close')
    })
    this.socket.on('error', (err) => {
      if (this._waitFor) {
        this._waitFor.reject(err)
      } else {
        this.emit('error', err)
      }
    })

    this.socket.connect({
      host: options.host,
      port: options.port
    })
  }

  async _connect () {
    this.emit('connect')
    if (this.state === Closed) {
      return
    }
    this.debug('connected')
    await this._identify()
  }

  async _reconnect () {
    this.emit('reconnect')
    this.debug('reconnected')
    await this._identify()
    if (this.subscribed) {
      await this._write(Protocol.sub(this.subscribed.topic, this.subscribed.channel))
    }
  }

  _disconnect () {
    this.debug('disconnected')
    this.emit('disconnect')
    this.state = Connecting
  }

  async _receive (buf) {
    this.debug(`received ${buf.byteLength} bytes`)
    const decoded = Protocol.decodeFrames(Buffer.concat([this._buffer, buf]))
    this._buffer = decoded.remainder
    for (const frame of decoded.frames) {
      await this._processFrame(frame)
    }
  }

  async _processFrame (frame) {
    // FrameTypeResponse
    if (frame.type === 0) {
      const message = frame.data.toString()
      // coverage disabled here so we don't have to wait on a heartbeat in tests
      /* istanbul ignore if */
      if (message === '_heartbeat_') {
        this.debug('_heartbeat_')
        return this.socket.writeAsync(Protocol.nop())
      }

      if (this.state === Identifying) {
        this.features = JSON.parse(message)
        this.debug('got identify response')
        this.state = Ready
        this.emit('ready')
        return
      }

      if (this._waitFor) {
        this._waitFor.resolve(message)
      }

      return
    }

    // FrameTypeError
    if (frame.type === 1) {
      const message = frame.data.toString()
      const err = new Error(`Received error response: ${message}`)
      err.code = message.split(/\s+/)[0]
      this.debug('ERROR:', message)
      // coverage disabled here because we don't care to test non critical errors
      /* istanbul ignore else */
      if (!['E_REQ_FAILED', 'E_FIN_FAILED', 'E_TOUCH_FAILED'].includes(err.code)) {
        this.close()
      }

      if (this._waitFor) {
        return this._waitFor.reject(err)
      } else {
        return this.emit('error', err)
      }
    }

    // FrameTypeMessage
    // coverage disabled here because this guard only exists in case nsq introduces another frame type
    /* istanbul ignore else */
    if (frame.type === 2) {
      const message = new Message(frame.data, this)
      this.debug('emitted message')
      this.emit('message', message)
    }
  }

  async _write (payload, wait = false) {
    if (this.socket.finished) {
      const err = new Error('The connection has been terminated')
      // disabling coverage because building up a queue and then getting the connection to fail in a test is annoying
      /* istanbul ignore next */
      while (this.queue.length) {
        const envelope = this.queue.shift()
        envelope.rejector(err)
      }
      return Promise.reject(err)
    }

    const envelope = { payload, wait }
    envelope.finished = new Promise((resolve, reject) => {
      envelope.finisher = resolve
      envelope.rejector = reject
    })

    this.queue.push(envelope)
    this._pulse()
    return envelope.finished
  }

  async _pulse () {
    this.debug(`_pulse`, this.state, this.queue.length)
    if (this.state !== Ready || !this.queue.length) {
      return
    }

    this.state = Pulsing
    while (this.queue.length) {
      const envelope = this.queue.shift()
      this.debug('processing item')

      if (envelope.wait) {
        this._waitFor = {}
        this._waitFor.promise = new Promise((resolve, reject) => {
          this._waitFor.resolve = resolve
          this._waitFor.reject = reject
        })
      }

      const res = await this.socket.writeAsync(envelope.payload)
      this.debug('_pulse response:', res)

      let result
      if (envelope.wait) {
        this.debug('waiting for envelope')
        try {
          result = await this._waitFor.promise
        } catch (err) {
          result = err
        }
        this.debug('done waiting, continuing...')
        this._waitFor = null
      }

      if (result instanceof Error) {
        envelope.rejector(result)
      } else {
        envelope.finisher(result)
      }
    }

    this.emit('drain')
  }

  async _identify () {
    const clientId = crypto.randomBytes(16).toString('hex')
    this.debug('_identify', clientId)
    this.state = Identifying

    await this.socket.writeAsync('  V2')
    return this.socket.writeAsync(Protocol.identify({
      client_id: clientId,
      feature_negotiation: true,
      user_agent: `${pkg.name}/${pkg.version}`,
      hostname: os.hostname(),
      msg_timeout: this.options.timeout
    }))
  }

  // don't cover unref
  /* istanbul ignore next */
  unref () {
    this.socket.unref()
  }

  async close () {
    this.debug('close')
    if (this._waitFor) {
      await this._waitFor
    }

    if (this.subscribed && this._ready > 0 && [Ready, Pulsing].includes(this.state)) {
      this.debug('close: setting ready state to 0')
      await this.ready(0)
    }

    if (this.state === Connecting) {
      this.state = Closed
      await new Promise((resolve) => this.once('connect', resolve))
      this.socket._disableReconnects(false)
      this.socket.destroy()
    } else if (this.state !== Failure) {
      this.state = Closed
      const ended = new Promise((resolve) => this.socket.once('close', resolve))
      this.socket.end()
      await ended
    }

    this.emit('close')
    this.debug('close: finished')
  }

  publish (topic, data, delay) {
    if (delay && Array.isArray(data)) {
      const err = new Error('Cannot delay a multi publish')
      return Promise.reject(err)
    }

    let payload
    if (Array.isArray(data)) {
      payload = Protocol.mpub(topic, data)
      this.debug(`mpublish ${topic}`)
    } else if (delay) {
      payload = Protocol.dpub(topic, delay, data)
      this.debug(`dpublish ${topic} ${delay}`)
    } else {
      payload = Protocol.pub(topic, data)
      this.debug(`publish ${topic}`)
    }

    return this._write(payload, true)
  }

  subscribe (topic, channel) {
    this.debug(`subscribe ${topic}.${channel}`)
    this.subscribed = { topic, channel }
    this._ready = this._ready || 0
    const payload = Protocol.sub(topic, channel)
    return this._write(payload)
  }

  ready (count) {
    this.debug(`rdy ${count}`)
    this._ready = count
    const payload = Protocol.rdy(count)
    return this._write(payload)
  }

  finish (id) {
    this.debug(`fin ${id}`)
    const payload = Protocol.fin(id)
    return this._write(payload)
  }

  requeue (id, delay = 0) {
    this.debug(`req ${id} ${delay}`)
    const payload = Protocol.req(id, delay)
    return this._write(payload)
  }

  touch (id) {
    this.debug(`touch ${id}`)
    const payload = Protocol.touch(id)
    return this._write(payload)
  }
}

module.exports = Connection
