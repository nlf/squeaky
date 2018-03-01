'use strict'

const { EventEmitter } = require('events')
const http = require('http')

const each = require('./each')
const Connection = require('./connection')
const { Ready } = require('./states')

const get = function (url) {
  return new Promise((resolve, reject) => {
    const client = http.get(url, (res) => {
      if (res.statusCode !== 200) {
        return reject(new Error(`Got unexpected statusCode: ${res.statusCode}`))
      }

      const chunks = []
      res.on('data', (chunk) => {
        chunks.push(chunk)
      })

      res.on('end', () => {
        try {
          const payload = JSON.parse(Buffer.concat(chunks))
          return resolve(payload)
        } catch (err) {
          return reject(err)
        }
      })
    })

    client.on('error', reject)
  })
}

class Discoverer extends EventEmitter {
  constructor (options) {
    super()

    this.subscribed = false
    this.options = options
    this.connections = new Map()
    this._desired = new Set()
    this._addresses = new Set(options.lookup.map((address) => {
      if (!address.startsWith('http')) {
        address = `http://${address}`
      }
      return address
    }))
  }

  _cleanup () {
    if (this._closing) {
      return
    }

    return each(this.connections.keys(), (address) => {
      if (!this._desired.has(address)) {
        const connection = this.connections.get(address)
        return connection.close().then(() => {
          this.connections.delete(address)
          this.emit('removed', address)
        })
      }
    })
  }

  _reconcile () {
    if (this._closing) {
      return
    }

    return each(this._desired, (address) => {
      if (this.connections.has(address)) {
        return
      }

      const [host, port] = address.split(':')
      const connection = new Connection(Object.assign({ host, port }, this.options))

      connection.on('message', (msg) => {
        this.emit('message', msg)
      })

      connection.on('error', (err) => {
        err.address = address
        this.emit('error', err)
      })

      connection.on('ready', () => {
        // disabling coverage here so we don't have to fiddle with timing in tests
        /* istanbul ignore else */
        if (!Array.from(this.connections.values()).some((connection) => connection.state !== Ready)) {
          this.emit('ready')
        }
      })

      return connection.subscribe(this._topic, this._channel).then(() => {
        this.connections.set(address, connection)
        this.emit('added', address)
      })
    })
  }

  discover () {
    if (this._closing) {
      return
    }

    // Accumulate list of nsqd addresses
    this._desired.clear()
    return each(this._addresses.values(), (address) => {
      return get(`${address}/lookup?topic=${this._topic}`).then((res) => {
        for (const producer of res.producers) {
          this._desired.add(`${producer.broadcast_address}:${producer.tcp_port}`)
        }
      }).catch((err) => {
        // Lookup failed, emit an error and keep going
        this.emit('error', err)
      })
    }).then(() => {
      // Remove undesired connections
      return this._cleanup()
    }).then(() => {
      // Add new desired connections
      return this._reconcile()
    }).then(() => {
      clearTimeout(this._timer)
      this._timer = setTimeout(() => {
        this.discover()
      }, this.options.discoverFrequency)

      return this
    })
  }

  subscribe (topic, channel) {
    if (this.subscribed) {
      return Promise.reject(new Error(`This connection is already subscribed to ${this.subscribed}`))
    }

    this.subscribed = `${topic}.${channel}`
    this._topic = topic
    this._channel = channel

    return this.discover()
  }

  ready () {
    let wanted = this.options.concurrency
    const remainder = this.options.concurrency % this.connections.size
    const split = (this.options.concurrency - remainder) / (this.connections.size - 1)

    for (const connection of this.connections.values()) {
      connection.ready(wanted > split ? split : wanted)
      wanted -= split
    }
  }

  close () {
    this._closing = true
    clearTimeout(this._timer)
    return each(this.connections.values(), (connection) => {
      return connection.close().then(() => {
        this.connections.delete(connection)
      })
    })
  }

  unref () {
    for (const connection of this.connections.values()) {
      connection.unref()
    }
  }
}

module.exports = Discoverer
