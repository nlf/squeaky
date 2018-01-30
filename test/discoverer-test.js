'use strict'

const crypto = require('crypto')
const http = require('http')
const { test } = require('tap')

const Discoverer = require('../lib/discoverer')
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

  server.listen(41611)
  server.stop = () => new Promise((resolve) => server.close(resolve))

  return server
}

test('can subscribe with a lookup host', async (assert) => {
  const server = getServer()
  const client = new Squeaky({ lookup: 'http://127.0.0.1:41611' })

  assert.equals(client.connections.size, 0, 'client should not have any connections')

  await client.subscribe('test', 'channel')

  assert.equals(client.connections.size, 1, 'client should have one connection')
  assert.ok(client.connections.has('test.channel'), 'client should have named connection correctly')
  const connection = client.connections.get('test.channel')
  assert.ok(connection instanceof Discoverer, 'client should be a discoverer')
  assert.equals(connection.connections.size, 1, 'discoverer should have one connection')

  await client.close('test.channel')
  await server.stop()
})

test('discoverer prepends protocol to lookup hosts and skips hosts that error', async (assert) => {
  const server = getServer()
  const client = new Squeaky({ lookup: ['127.0.0.1:41611', 'test.test:4161'] })

  await client.subscribe('test', 'channel')

  assert.equals(client.connections.size, 1, 'client should have one connection')
  assert.ok(client.connections.has('test.channel'), 'client should have named connection correctly')
  const connection = client.connections.get('test.channel')
  assert.ok(connection instanceof Discoverer, 'client should be a discoverer')
  assert.equals(connection.connections.size, 1, 'discoverer should have one connection')

  await client.close('test.channel')
  await server.stop()
})

test('trying to subscribe twice errors', async (assert) => {
  const server = getServer()
  const client = new Squeaky({ lookup: '127.0.0.1:41611' })

  await client.subscribe('test', 'channel')
  const conn = client.connections.get('test.channel')

  try {
    await conn.subscribe('test', 'channel')
  } catch (err) {
    assert.match(err, {
      message: 'This connection is already subscribed to test.channel'
    }, 'should throw')
  }

  await client.close('test.channel')
  await server.stop()
})

test('discoverer actually receives messages', async (assert) => {
  const server = getServer()
  const client = new Squeaky({ lookup: '127.0.0.1:41611' })
  const topic = crypto.randomBytes(16).toString('hex')

  await client.subscribe(topic, 'channel')

  const promise = new Promise((resolve) => {
    client.once(`${topic}.channel.message`, (msg) => {
      assert.match(msg, {
        body: { test: 'subscribe' }
      }, 'should receive the correct message')
      msg.finish()
      resolve()
    })
  })

  await client.publish(topic, { test: 'subscribe' })
  await promise

  await client.close('writer', `${topic}.channel`)
  await server.stop()
})

test('skips error events on main client when no listener exists', async (assert) => {
  const server = getServer()
  const client = new Squeaky({ lookup: '127.0.0.1:41611' })

  await client.subscribe('test', 'channel')
  const discoverer = client.connections.get('test.channel')
  const conn = discoverer.connections.get('127.0.0.1:4150')

  const promise = new Promise((resolve) => {
    conn.once('error', (err) => {
      assert.match(err, {
        message: 'Received error response for "RDY invalid": E_INVALID RDY could not parse count invalid'
      }, 'should receive correct error')
      resolve()
    })
  })

  conn.ready('invalid')
  await promise

  await client.close('test.channel')
  await server.stop()
})

test('fires error events on main client when a listener exists', async (assert) => {
  const server = getServer()
  const client = new Squeaky({ lookup: '127.0.0.1:41611' })

  const promise = new Promise((resolve) => {
    client.once('error', (err) => {
      assert.match(err, {
        message: 'Received error response for "RDY invalid": E_INVALID RDY could not parse count invalid'
      }, 'should receive correct error')
      resolve()
    })
  })

  await client.subscribe('test', 'channel')
  const discoverer = client.connections.get('test.channel')
  const conn = discoverer.connections.get('127.0.0.1:4150')
  conn.ready('invalid')
  await promise

  await client.close('test.channel')
  await server.stop()
})

test('discoverer refreshes connections on defined interval', async (assert) => {
  const topic = crypto.randomBytes(16).toString('hex')
  const payload = {
    topics: [topic],
    producers: [{
      broadcast_address: '127.0.0.1',
      tcp_port: 4150
    }, {
      broadcast_address: 'localhost',
      tcp_port: 4150
    }]
  }

  let listenerResolver
  const listenerPromise = new Promise((resolve) => {
    listenerResolver = resolve
  })

  let count = 0
  const server = http.createServer((req, res) => {
    res.writeHead(200, { 'Content-Type': 'application/json' })
    res.write(JSON.stringify(payload))
    res.end()

    if (payload.producers.length === 2) {
      payload.producers.pop()
    }
    if (++count === 3) {
      listenerResolver()
    }
  })

  server.listen(41616)

  const client = new Squeaky({ lookup: '127.0.0.1:41616', discoverFrequency: 1 })

  const removedPromise = new Promise((resolve) => {
    client.on('removed', (address) => {
      assert.equals(address, 'localhost:4150')
      resolve()
    })
  })

  await client.subscribe(topic, 'channel')

  assert.equals(client.connections.get(`${topic}.channel`).connections.size, 2, 'discoverer should have 2 connections')

  await removedPromise
  await listenerPromise

  assert.equals(client.connections.get(`${topic}.channel`).connections.size, 1, 'discoverer should have 1 connection')

  await client.close('writer', `${topic}.channel`)
  await new Promise((resolve) => server.close(resolve))
})

test('discoverer skips invalid statusCodes when polling nsqlookupd', async (assert) => {
  const server = http.createServer((req, res) => {
    res.writeHead(404)
    res.write(JSON.stringify({ message: 'TOPIC_NOT_FOUND' }))
    res.end()
  })

  server.listen(41616)

  const client = new Squeaky({ lookup: '127.0.0.1:41616' })
  const promise = new Promise((resolve) => {
    client.once('error', (err) => {
      assert.match(err, {
        message: 'Got unexpected statusCode: 404'
      }, 'should detect invalid statusCode')
      resolve()
    })
  })

  await client.subscribe('somerandomtopicthatshouldnotexist', 'channel')
  await promise

  assert.ok(client.connections.has('somerandomtopicthatshouldnotexist.channel'), 'discoverer is created')
  assert.equals(client.connections.get('somerandomtopicthatshouldnotexist.channel').connections.size, 0, 'no connections exist on the discoverer')

  await client.close('somerandomtopicthatshouldnotexist.channel')
  await new Promise((resolve) => server.close(resolve))
})

test('discoverer skips lookupd hosts that return invalid json', async (assert) => {
  const server = http.createServer((req, res) => {
    res.writeHead(200, { 'Content-Type': 'application/json' })
    res.write('{"something":"invalid')
    res.end()
  })

  server.listen(41616)

  const client = new Squeaky({ lookup: '127.0.0.1:41616' })
  const promise = new Promise((resolve) => {
    client.once('error', (err) => {
      assert.match(err, {
        message: 'Unexpected end of JSON input'
      }, 'should detect invalid json')
      resolve()
    })
  })

  await client.subscribe('somerandomtopicthatshouldnotexist', 'channel')
  await promise

  assert.ok(client.connections.has('somerandomtopicthatshouldnotexist.channel'), 'discoverer is created')
  assert.equals(client.connections.get('somerandomtopicthatshouldnotexist.channel').connections.size, 0, 'no connections exist on the discoverer')

  await client.close('somerandomtopicthatshouldnotexist.channel')
  await new Promise((resolve) => server.close(resolve))
})
