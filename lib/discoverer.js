'use strict'

const { EventEmitter } = require('events')
const Promise = require('bluebird')
const http = require('http')

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
    this._addresses = new Set(options.lookup.map((address) => {
      if (!address.startsWith('http')) {
        address = `http://${address}`
      }
      return address
    }))
  }

  discover () {
    const desired = new Set()

    // Accumulate list of nsqd addresses
    return Promise.each(this._addresses.values(), (address) => {
      return get(`${address}/lookup?topic=${this._topic}`).then((res) => {
        for (const producer of res.producers) {
          desired.add(`${producer.broadcast_address}:${producer.tcp_port}`)
        }
      }).catch((err) => {
        // Lookup failed, emit an error and keep going
        this.emit('error', err)
      })
    }).then(() => {
      // Remove undesired connections
      return Promise.each(this.connections.keys(), (address) => {
        if (!desired.has(address)) {
          this.emit('removed', address)
          const connection = this.connections.get(address)
          this.connections.delete(address)
          return connection.close()
        }
      })
    }).then(() => {
      // Add new desired connections
      return Promise.each(desired, (address) => {
        if (this.connections.has(address)) {
          return
        }

        const connection = new Connection(this.options)
        connection.on('message', (msg) => {
          this.emit('message', msg)
        })

        connection.on('error', (err) => {
          this.emit('error', err)
        })

        connection.on('ready', () => {
          // disabling coverage here so we don't have to fiddle with timing in tests
          /* istanbul ignore else */
          if (!Array.from(this.connections.values()).some((connection) => connection.state !== Ready)) {
            this.emit('ready')
          }
        })

        this.connections.set(address, connection)
        this.emit('added', address)
        return connection.subscribe(this._topic, this._channel)
      })
    }).then(() => {
      clearTimeout(this._timer)
      this._timer = setTimeout(() => {
        this.discover()
      }, this.options.discoverFrequency)
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
    // Roughly evenly distribute the ready count
    const readyCount = this.connections.size > this.options.concurrency ? 1 : Math.ceil(this.options.concurrency / this.connections.size)
    for (const connection of this.connections.values()) {
      connection.ready(readyCount)
    }
  }

  close () {
    clearTimeout(this._timer)
    return Promise.each(this.connections.values(), (connection) => {
      this.connections.delete(connection)
      return connection.close()
    })
  }
}

module.exports = Discoverer
