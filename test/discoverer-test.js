'use strict'

const http = require('http')
const { test } = require('tap')

const { getTopic, getSubDebugger } = require('./utils')
const Squeaky = require('../')

const getServer = function () {
  const payload = {
    topics: [],
    producers: [{
      broadcast_address: '127.0.0.1',
      tcp_port: 4150
    }]
  }

  const server = http.createServer((req, res) => {
    res.writeHead(200, { 'Content-Type': 'application/json' })
    res.write(JSON.stringify(payload))
    res.end()
  })

  server.listen(41611, '127.0.0.1')
  server.stop = () => new Promise((resolve) => {
    server.once('close', resolve)
    server.close()
  })

  return server
}

test('can subscribe with a lookup host', async (assert) => {
  const server = getServer()
  const topic = getTopic()
  const subscriber = new Squeaky.Subscriber({ lookup: ['http://127.0.0.1:41611'], topic, channel: 'test#ephemeral', ...getSubDebugger() })

  await new Promise((resolve) => subscriber.once('ready', ({ host, port }) => {
    assert.equals(host, '127.0.0.1')
    assert.equals(port, 4150)
    resolve()
  }))

  assert.equals(subscriber.connections.size, 1)

  await Promise.all([
    subscriber.close(),
    server.stop()
  ])
})

test('discoverer refreshes connections on defined interval', async (assert) => {
  const topic = getTopic()
  const payload = {
    topics: [],
    producers: [{
      broadcast_address: '127.0.0.1',
      tcp_port: 4150
    }, {
      broadcast_address: 'localhost',
      tcp_port: 4150
    }]
  }

  const server = http.createServer((req, res) => {
    res.writeHead(200, { 'Content-Type': 'application/json' })
    res.write(JSON.stringify(payload))
    res.end()

    if (payload.producers.length === 2) {
      payload.producers.pop()
    }
  })

  server.listen(41616)

  const subscriber = new Squeaky.Subscriber({ lookup: ['127.0.0.1:41616'], discoverFrequency: 100, topic, channel: 'test#ephemeral', ...getSubDebugger() })

  await Promise.all([
    new Promise((resolve) => subscriber.on('ready', ({ host, port }) => {
      if (host === '127.0.0.1' && port === 4150) {
        resolve()
      }
    })),
    new Promise((resolve) => subscriber.on('ready', ({ host, port }) => {
      if (host === 'localhost' && port === 4150) {
        resolve()
      }
    })),
    new Promise((resolve) => {
      subscriber.on('removed', ({ host, port }) => {
        assert.equals(host, 'localhost')
        assert.equals(port, 4150)
        resolve()
      })
    })
  ])

  await Promise.all([
    subscriber.close(),
    new Promise((resolve) => server.close(resolve))
  ])
})

test('discoverer distributes ready state appropriately', async (assert) => {
  const topic = getTopic()
  const payload = {
    topics: [],
    producers: [{
      broadcast_address: '127.0.0.1',
      tcp_port: 4150
    }, {
      broadcast_address: 'localhost',
      tcp_port: 4150
    }]
  }

  const server = http.createServer((req, res) => {
    res.writeHead(200, { 'Content-Type': 'application/json' })
    res.write(JSON.stringify(payload))
    res.end()
  })

  server.listen(41616)

  const subscriber = new Squeaky.Subscriber({ lookup: ['127.0.0.1:41616'], discoverFrequency: 100, topic, channel: 'test#ephemeral', ...getSubDebugger() })

  await Promise.all([
    new Promise((resolve) => subscriber.on('ready', ({ host, port }) => {
      if (host === '127.0.0.1' && port === 4150) {
        resolve()
      }
    })),
    new Promise((resolve) => subscriber.on('ready', ({ host, port }) => {
      if (host === 'localhost' && port === 4150) {
        resolve()
      }
    })),
    new Promise((resolve) => subscriber.once('pollComplete', resolve))
  ])

  assert.equals(subscriber.connections.get('127.0.0.1:4150')._ready, 1)
  assert.equals(subscriber.connections.get('localhost:4150')._ready, 0)

  await new Promise((resolve) => subscriber.once('pollComplete', resolve))

  assert.equals(subscriber.connections.get('127.0.0.1:4150')._ready, 0)
  assert.equals(subscriber.connections.get('localhost:4150')._ready, 1)

  await Promise.all([
    subscriber.close(),
    new Promise((resolve) => server.close(resolve))
  ])
})

test('discoverer skips lookup hosts that 404', async (assert) => {
  const topic = getTopic()
  const server = http.createServer((req, res) => {
    res.writeHead(404)
    res.end()
  })

  server.listen(41616)

  const subscriber = new Squeaky.Subscriber({ lookup: ['127.0.0.1:41616'], discoverFrequency: 100, topic, channel: 'test#ephemeral', ...getSubDebugger() })

  await new Promise((resolve) => subscriber.on('error', (err) => {
    assert.equals(err.code, 'ELOOKUPERROR')
    assert.equals(err.host, 'http://127.0.0.1:41616')
    resolve()
  }))

  await Promise.all([
    subscriber.close(),
    new Promise((resolve) => server.close(resolve))
  ])
})

test('discoverer skips lookup hosts that return invalid json', async (assert) => {
  const topic = getTopic()
  const server = http.createServer((req, res) => {
    res.writeHead(200)
    res.write('{"broken":"json')
    res.end()
  })

  server.listen(41616)

  const subscriber = new Squeaky.Subscriber({ lookup: ['127.0.0.1:41616'], discoverFrequency: 100, topic, channel: 'test#ephemeral', ...getSubDebugger() })

  await new Promise((resolve) => subscriber.on('error', (err) => {
    assert.equals(err.code, 'ELOOKUPERROR')
    assert.equals(err.host, 'http://127.0.0.1:41616')
    resolve()
  }))

  await Promise.all([
    subscriber.close(),
    new Promise((resolve) => server.close(resolve))
  ])
})

test('discoverer skips lookup hosts that cannot be reached', async (assert) => {
  const topic = getTopic()

  const subscriber = new Squeaky.Subscriber({ lookup: ['127.0.0.1:41616'], discoverFrequency: 100, topic, channel: 'test#ephemeral', ...getSubDebugger() })

  await new Promise((resolve) => subscriber.on('error', (err) => {
    assert.equals(err.code, 'ELOOKUPERROR')
    assert.equals(err.host, 'http://127.0.0.1:41616')
    resolve()
  }))

  await subscriber.close()
})
