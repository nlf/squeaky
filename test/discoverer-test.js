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

  server.stop = () => new Promise((resolve) => server.close(resolve))

  return new Promise((resolve) => server.listen({ host: '127.0.0.1', port: 41611 }, resolve)).then(() => server)
}

test('can subscribe with a lookup host', (assert) => {
  return getServer().then((server) => {
    const client = new Squeaky({ lookup: 'http://127.0.0.1:41611' })

    assert.equals(client.connections.size, 0, 'client should not have any connections')

    return client.subscribe('test#ephemeral', 'channel#ephemeral').then(() => {
      assert.equals(client.connections.size, 1, 'client should have one connection')
      assert.ok(client.connections.has('test#ephemeral.channel#ephemeral'), 'client should have named connection correctly')
      const connection = client.connections.get('test#ephemeral.channel#ephemeral')
      assert.ok(connection instanceof Discoverer, 'client should be a discoverer')
      assert.equals(connection.connections.size, 1, 'discoverer should have one connection')

      return Promise.all([
        client.close('test#ephemeral.channel#ephemeral'),
        server.stop()
      ])
    })
  })
})

test('can unref connections made with a discoverer', (assert) => {
  return getServer().then((server) => {
    const client = new Squeaky({ lookup: 'http://127.0.0.1:41611' })

    // this is a dummy to keep the test from exiting early
    const timer = setTimeout(() => {}, 1000)
    assert.equals(client.connections.size, 0, 'client should not have any connections')

    return client.subscribe('test#ephemeral', 'channel#ephemeral').then(() => {
      client.unref()

      assert.equals(client.connections.size, 1, 'client should have one connection')
      assert.ok(client.connections.has('test#ephemeral.channel#ephemeral'), 'client should have named connection correctly')
      const connection = client.connections.get('test#ephemeral.channel#ephemeral')
      assert.ok(connection instanceof Discoverer, 'client should be a discoverer')
      assert.equals(connection.connections.size, 1, 'discoverer should have one connection')
      assert.ok(connection.connections.has('127.0.0.1:4150'), 'discoverer should have the correct address')
      assert.notOk(connection.connections.get('127.0.0.1:4150').socket._handle.hasRef(), 'socket should have no ref')

      return Promise.all([
        client.close('test#ephemeral.channel#ephemeral'),
        server.stop()
      ])
    }).then(() => {
      clearTimeout(timer)
    })
  })
})

test('discoverer prepends protocol to lookup hosts and skips hosts that error', (assert) => {
  return getServer().then((server) => {
    const client = new Squeaky({ lookup: ['127.0.0.1:41611', '127.0.0.1:41615'] })

    return client.subscribe('testprepends#ephemeral', 'channel#ephemeral').then(() => {
      assert.equals(client.connections.size, 1, 'client should have one connection')
      assert.ok(client.connections.has('testprepends#ephemeral.channel#ephemeral'), 'client should have named connection correctly')
      const connection = client.connections.get('testprepends#ephemeral.channel#ephemeral')
      assert.ok(connection instanceof Discoverer, 'client should be a discoverer')
      assert.equals(connection.connections.size, 1, 'discoverer should have one connection')

      return Promise.all([
        client.close('testprepends#ephemeral.channel#ephemeral'),
        server.stop()
      ])
    })
  })
})

test('trying to subscribe twice errors', (assert) => {
  return getServer().then((server) => {
    const client = new Squeaky({ lookup: '127.0.0.1:41611' })

    return client.subscribe('test#ephemeral', 'channel#ephemeral').then(() => {
      const conn = client.connections.get('test#ephemeral.channel#ephemeral')

      return assert.rejects(conn.subscribe('test#ephemeral', 'channel#ephemeral'), {
        message: 'This connection is already subscribed to test#ephemeral.channel#ephemeral'
      }, 'should reject')
    }).then(() => {
      return Promise.all([
        client.close('test#ephemeral.channel#ephemeral'),
        server.stop()
      ])
    })
  })
})

test('discoverer actually receives messages', (assert) => {
  return getServer().then((server) => {
    const client = new Squeaky({ lookup: '127.0.0.1:41611' })
    const topic = crypto.randomBytes(16).toString('hex') + '#ephemeral'

    return client.subscribe(topic, 'channel#ephemeral').then(() => {
      const promise = new Promise((resolve) => {
        client.once(`${topic}.channel#ephemeral.message`, (msg) => {
          assert.match(msg, {
            body: { test: 'subscribe' }
          }, 'should receive the correct message')
          msg.finish()
          resolve()
        })
      })

      return Promise.all([
        client.publish(topic, { test: 'subscribe' }),
        promise
      ])
    }).then(() => {
      return Promise.all([
        client.close('writer', `${topic}.channel#ephemeral`),
        server.stop()
      ])
    })
  })
})

test('discoverer refreshes connections on defined interval', (assert) => {
  const topic = crypto.randomBytes(16).toString('hex') + '#ephemeral'
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

  return client.subscribe(topic, 'channel#ephemeral').then(() => {
    assert.equals(client.connections.get(`${topic}.channel#ephemeral`).connections.size, 2, 'discoverer should have 2 connections')

    return Promise.all([
      removedPromise,
      listenerPromise
    ])
  }).then(() => {
    assert.equals(client.connections.get(`${topic}.channel#ephemeral`).connections.size, 1, 'discoverer should have 1 connection')

    return Promise.all([
      client.close('writer', `${topic}.channel#ephemeral`),
      new Promise((resolve) => server.close(resolve))
    ])
  })
})

test('discoverer skips invalid statusCodes when polling nsqlookupd', (assert) => {
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

  return Promise.all([
    client.subscribe('somerandomtopicthatshouldnotexist', 'channel'),
    promise
  ]).then(() => {
    assert.ok(client.connections.has('somerandomtopicthatshouldnotexist.channel'), 'discoverer is created')
    assert.equals(client.connections.get('somerandomtopicthatshouldnotexist.channel').connections.size, 0, 'no connections exist on the discoverer')

    return Promise.all([
      client.close('somerandomtopicthatshouldnotexist.channel'),
      new Promise((resolve) => server.close(resolve))
    ])
  })
})

test('discoverer skips lookupd hosts that return invalid json', (assert) => {
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

  return Promise.all([
    client.subscribe('somerandomtopicthatshouldnotexist', 'channel'),
    promise
  ]).then(() => {
    assert.ok(client.connections.has('somerandomtopicthatshouldnotexist.channel'), 'discoverer is created')
    assert.equals(client.connections.get('somerandomtopicthatshouldnotexist.channel').connections.size, 0, 'no connections exist on the discoverer')

    return Promise.all([
      client.close('somerandomtopicthatshouldnotexist.channel'),
      new Promise((resolve) => server.close(resolve))
    ])
  })
})

test('discoverer propagates errors', (assert) => {
  return getServer().then((server) => {
    const client = new Squeaky({ lookup: '127.0.0.1:41611', timeout: 100 })
    const errored = new Promise((resolve) => client.once('error', (err) => {
      assert.match(err, {
        message: 'E_BAD_BODY IDENTIFY msg timeout (100) is invalid',
        connection: 'testprop#ephemeral.channel#ephemeral',
        address: '127.0.0.1:4150'
      }, 'should emit an error')
      resolve()
    }))

    // don't wait for this, the promise won't resolve
    client.subscribe('testprop#ephemeral', 'channel#ephemeral')
    return errored.then(() => Promise.all([
      client.close('testprop#ephemeral.channel#ephemeral'),
      server.stop()
    ]))
  })
})
