'use strict'

// coverage disabled here because testing a noop is silly
/* istanbul ignore next */
const noop = () => {}

const defaults = {
  host: '127.0.0.1',
  port: 4150,
  timeout: 60000,
  maxConnectAttempts: 5,
  reconnectDelayFactor: 1000,
  maxReconnectDelay: 120000,
  debug: noop
}

const publisher = Object.assign({}, defaults)

const subscriber = Object.assign({}, defaults, {
  lookup: [],
  concurrency: 1,
  discoverFrequency: 1000 * 60 * 5 // 5 minutes
})

module.exports = {
  publisher,
  subscriber
}
