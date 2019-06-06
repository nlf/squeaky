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

    Object.defineProperty(this, 'timer', {
      enumerable: false,
      writable: true,
      value: null
    })
  }

  finish () {
    clearTimeout(this.timer)
    return this.connection.finish(this.id)
  }

  requeue (delay) {
    clearTimeout(this.timer)
    return this.connection.requeue(this.id, delay)
  }

  async touch () {
    const res = await this.connection.touch(this.id)
    this.touched = Date.now()
    return res
  }

  async keepalive () {
    const now = Date.now()
    // the else condition is not covered because it depends on the max_msg_timeout expiring, which defaults to 15 minutes and is not configurable by the requester
    /* istanbul ignore else */
    if (!this.expired) {
      await this.touch()
      this.timer = setTimeout(() => this.keepalive(), this.touched + this.connection.features.msg_timeout - now - this.connection.options.keepaliveOffset)
    }
  }

  get expired () {
    const now = Date.now()
    return this.touched + this.connection.features.msg_timeout < now || this.published.getTime() + this.connection.features.max_msg_timeout < now
  }

  get expiresIn () {
    const now = Date.now()
    // if the timer is set, we're keeping the message alive and the value returned here will not account for the msg_timeout
    if (this.timer) {
      return this.published.getTime() + this.connection.features.max_msg_timeout - now
    }

    // if the timer is not set, we return whichever timeout will occur first
    return Math.min(this.touched + this.connection.features.msg_timeout - now, this.published.getTime() + this.connection.features.max_msg_timeout - now)
  }
}

module.exports = Message
