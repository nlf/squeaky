'use strict'

const crypto = require('crypto')
const { test } = require('tap')

const Squeaky = require('../')

test('can subscribe', async (assert) => {
  const client = new Squeaky()

  assert.equals(client.connections.size, 0, 'client should not have any connections')

  await client.subscribe('test#ephemeral', 'channel#ephemeral')

  assert.equals(client.connections.size, 1, 'client should have one connection')
  assert.ok(client.connections.has('test#ephemeral.channel#ephemeral'), 'client should have named connection correctly')

  await client.close('test#ephemeral.channel#ephemeral')
})

test('subscribing twice returns the same connection', async (assert) => {
  const client = new Squeaky()

  assert.equals(client.connections.size, 0, 'client should have no connections')

  const conn = await client.subscribe('test#ephemeral', 'channel#ephemeral')

  assert.equals(client.connections.size, 1, 'client should have one connection')
  assert.ok(client.connections.has('test#ephemeral.channel#ephemeral'), 'client should have named connection correctly')

  const conn2 = await client.subscribe('test#ephemeral', 'channel#ephemeral')

  assert.same(conn, conn2, 'connections should be the same')
  assert.equals(client.connections.size, 1, 'client should still have only one connection')

  await client.close('test#ephemeral.channel#ephemeral')
})

test('errors when a connection tries to subscribe twice', async (assert) => {
  const client = new Squeaky()

  await client.subscribe('test#ephemeral', 'channel#ephemeral')
  const conn = client.connections.get('test#ephemeral.channel#ephemeral')

  try {
    await conn.subscribe('test#ephemeral', 'channel2#ephemeral')
  } catch (err) {
    assert.match(err, {
      message: 'This connection is already subscribed to test#ephemeral.channel#ephemeral'
    }, 'should throw')
  }

  await client.close('test#ephemeral.channel#ephemeral')
})

test('receives the message event', async (assert) => {
  const client = new Squeaky()
  const topic = crypto.randomBytes(16).toString('hex') + '#ephemeral'

  await client.subscribe(topic, 'channel#ephemeral')

  const promise = new Promise((resolve) => {
    client.once(`${topic}.channel#ephemeral.message`, (msg) => {
      assert.match(msg, {
        body: { test: 'subscribe' }
      }, 'should receive the correct message')
      msg.finish()
      resolve()
    })
  })

  await client.publish(topic, { test: 'subscribe' })
  await promise

  await client.close('writer', `${topic}.channel#ephemeral`)
})

test('can receive messages with non-object payloads', async (assert) => {
  const client = new Squeaky()
  const topic = crypto.randomBytes(16).toString('hex') + '#ephemeral'

  await client.subscribe(topic, 'channel#ephemeral')

  const stringPromise = new Promise((resolve) => {
    client.once(`${topic}.channel#ephemeral.message`, (msg) => {
      assert.match(msg, {
        body: Buffer.from('a string') // a raw string does not JSON.parse so we have to wrap it in a buffer
      }, 'should receive strings')
      msg.finish()
      resolve()
    })
  })
  await client.publish(topic, 'a string')
  await stringPromise

  const numberPromise = new Promise((resolve) => {
    client.once(`${topic}.channel#ephemeral.message`, (msg) => {
      assert.match(msg, {
        body: 5 // this one JSON.parses
      }, 'should receive numbers')
      msg.finish()
      resolve()
    })
  })
  await client.publish(topic, 5)
  await numberPromise

  const buffer = Buffer.from([0, 1, 2, 3])
  const bufferPromise = new Promise((resolve) => {
    client.once(`${topic}.channel#ephemeral.message`, (msg) => {
      assert.match(msg, {
        body: buffer
      }, 'should receive buffers')
      msg.finish()
      resolve()
    })
  })
  await client.publish(topic, buffer)
  await bufferPromise

  await client.close('writer', `${topic}.channel#ephemeral`)
})

test('can subscribe with a function', async (assert) => {
  const client = new Squeaky()
  const topic = crypto.randomBytes(16).toString('hex') + '#ephemeral'

  let resolver
  const promise = new Promise((resolve) => {
    resolver = resolve
  })

  await client.subscribe(topic, 'channel#ephemeral', (msg) => {
    assert.match(msg, {
      body: { test: 'subscribe' }
    }, 'should receive the correct message')
    msg.finish()
    resolver()
  })

  await client.publish(topic, { test: 'subscribe' })
  await promise

  await client.close('writer', `${topic}.channel#ephemeral`)
})

test('can touch a received message and extend expiration time', async (assert) => {
  const client = new Squeaky()
  const topic = crypto.randomBytes(16).toString('hex') + '#ephemeral'

  let resolver
  const promise = new Promise((resolve) => {
    resolver = resolve
  })

  await client.subscribe(topic, 'channel#ephemeral', (msg) => {
    assert.match(msg, {
      body: { test: 'subscribe' }
    }, 'should receive the correct message')

    setTimeout(() => {
      const oldExpiration = msg.expiresIn
      msg.touch()

      assert.ok(msg.expiresIn > oldExpiration, 'new expiration should be greater than the old expiration')
      msg.finish()
      resolver()
    }, 50)
  })

  await client.publish(topic, { test: 'subscribe' })
  await promise

  await client.close('writer', `${topic}.channel#ephemeral`)
})

test('can requeue a message', async (assert) => {
  const client = new Squeaky()
  const topic = crypto.randomBytes(16).toString('hex') + '#ephemeral'

  let resolver
  const promise = new Promise((resolve) => {
    resolver = resolve
  })

  let attempt = 0
  await client.subscribe(topic, 'channel#ephemeral', (msg) => {
    assert.match(msg, {
      attempts: ++attempt,
      body: { test: 'subscribe' }
    }, 'should receive the correct message')

    if (attempt === 2) {
      msg.finish()
      return resolver()
    }

    msg.requeue()
  })

  await client.publish(topic, { test: 'subscribe' })
  await promise

  await client.close('writer', `${topic}.channel#ephemeral`)
})

test('skips error events on main client when no listener exists', async (assert) => {
  const client = new Squeaky()

  await client.subscribe('test#ephemeral', 'channel#ephemeral')
  const conn = client.connections.get('test#ephemeral.channel#ephemeral')

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

  await client.close('test#ephemeral.channel#ephemeral')
})

test('fires error events on main client when a listener exists', async (assert) => {
  const client = new Squeaky()

  const promise = new Promise((resolve) => {
    client.once('error', (err) => {
      assert.match(err, {
        message: 'Received error response for "RDY invalid": E_INVALID RDY could not parse count invalid'
      }, 'should receive correct error')
      resolve()
    })
  })

  await client.subscribe('test#ephemeral', 'channel#ephemeral')
  const conn = client.connections.get('test#ephemeral.channel#ephemeral')
  conn.ready('invalid')
  await promise

  await client.close('test#ephemeral.channel#ephemeral')
})

test('waits for inflight messages to timeout before closing', async (assert) => {
  const client = new Squeaky({ timeout: 1000 })

  let resolver
  const promise = new Promise((resolve) => {
    resolver = resolve
  })

  await client.subscribe('test#ephemeral', 'channel#ephemeral', (msg) => {
    assert.same(msg.body, { some: 'data' })
    // intentionally don't finish so we allow the timeout to trigger
    resolver()
  })

  await client.publish('test#ephemeral', { some: 'data' })
  await promise

  let timer
  const timedout = new Promise((resolve, reject) => {
    timer = setTimeout(() => {
      return reject(new Error('Client timed out while closing'))
    }, 1010)
  })

  return Promise.race([
    client.close('writer', 'test#ephemeral.channel#ephemeral').then(() => {
      clearTimeout(timer)
    }),
    timedout
  ])
})

test('calling touch on a message resets inflight timer', async (assert) => {
  const client = new Squeaky({ timeout: 1000 })

  let resolver
  const promise = new Promise((resolve) => {
    resolver = resolve
  })

  await client.subscribe('touchtest#ephemeral', 'channel#ephemeral', (msg) => {
    assert.same(msg.body, { some: 'data' })
    // intentionally don't finish so we allow the timeout to trigger
    setTimeout(() => {
      msg.touch()
      resolver()
    }, 100)
  })

  await client.publish('touchtest#ephemeral', { some: 'data' })
  await promise

  let timer
  const timedout = new Promise((resolve, reject) => {
    timer = setTimeout(() => {
      return reject(new Error('Client timed out while closing'))
    }, 1110)
  })

  return Promise.race([
    client.close('writer', 'touchtest#ephemeral.channel#ephemeral').then(() => {
      clearTimeout(timer)
    }),
    timedout
  ])
})

test('calling functions for messages that arent in flight has no effect', async (assert) => {
  const client = new Squeaky()

  await client.subscribe('test#ephemeral', 'channel#ephemeral')
  const conn = client.connections.get('test#ephemeral.channel#ephemeral')

  assert.doesNotThrow(() => {
    conn.finish('asdf')
    conn.requeue('asdf')
    conn.touch('asdf')
  }, 'should not throw')

  await client.close('test#ephemeral.channel#ephemeral')
})
