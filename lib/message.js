'use strict'

class Message {
  constructor (buffer, connection) {
    this.timestamp = new Date()
    this.published = new Date(parseInt(buffer.toString('hex', 0, 8), 16) / 1000000)
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

    Object.defineProperty(this, 'touched', {
      enumerable: false,
      writable: true,
      value: this.timestamp.getTime()
    })
  }

  finish () {
    return this.connection.finish(this.id)
  }

  requeue (delay) {
    return this.connection.requeue(this.id, delay)
  }

  async touch () {
    const res = await this.connection.touch(this.id)
    this.touched = Date.now()
    return res
  }

  get expiresIn () {
    return this.touched + this.connection.features.msg_timeout - Date.now()
  }
}

module.exports = Message
