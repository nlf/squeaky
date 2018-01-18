'use strict'

class Message {
  constructor (buffer, connection) {
    this.timestamp = new Date(parseInt(buffer.toString('hex', 0, 8), 16) / 1000000)
    this.attempts = buffer.readUInt16BE(8)
    this.id = buffer.toString('ascii', 10, 26)
    const value = buffer.slice(26)
    try {
      this.body = JSON.parse(value)
    } catch (err) {
      this.body = value
    }

    Object.defineProperty(this, 'connection', {
      enumerable: false,
      writable: false,
      value: connection
    })
  }

  finish () {
    this.connection.finish(this.id)
  }

  requeue (delay) {
    this.connection.requeue(this.id, delay)
  }

  touch () {
    Object.defineProperty(this, 'touched', {
      enumerable: false,
      writable: true,
      value: Date.now()
    })

    this.connection.touch(this.id)
  }

  get expiresIn () {
    return this.touched
      ? (this.touched + this.connection.features.msg_timeout - Date.now())
      : (this.timestamp.getTime() + this.connection.features.msg_timeout - Date.now())
  }
}

module.exports = Message
