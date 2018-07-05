'use strict'

const { test } = require('tap')

const { getTopic, getPubDebugger, getSubDebugger } = require('./utils')
const Squeaky = require('../')

test('can publish', async (assert) => {
  const topic = getTopic()
  const publisher = new Squeaky.Publisher(getPubDebugger())
  const subscriber = new Squeaky.Subscriber({ lookup: undefined, topic, channel: 'test#ephemeral', ...getSubDebugger() })

  let resolver
  const received = new Promise((resolve) => {
    resolver = resolve
  })

  subscriber.on('message', async (msg) => {
    assert.same(msg.body, { some: 'object' }, 'subscriber received the right message')
    await msg.finish()
    resolver()
  })
  const res = await publisher.publish(topic, { some: 'object' })
  assert.equals(res, 'OK')

  await received
  await Promise.all([
    publisher.close(),
    subscriber.close()
  ])
})

test('can requeue a message', async (assert) => {
  const topic = getTopic()
  const publisher = new Squeaky.Publisher(getPubDebugger())
  const subscriber = new Squeaky.Subscriber({ topic, channel: 'test#ephemeral', ...getSubDebugger() })

  let resolver
  const received = new Promise((resolve) => {
    resolver = resolve
  })
  let count = 0

  subscriber.on('message', async (msg) => {
    assert.same(msg.body, { some: 'object' }, 'subscriber received the right message')
    if (++count === 1) {
      await msg.requeue()
    } else {
      await msg.finish()
      resolver()
    }
  })
  await publisher.publish(topic, { some: 'object' })

  await received
  await Promise.all([
    publisher.close(),
    subscriber.close()
  ])
})

test('can touch a message', async (assert) => {
  const topic = getTopic()
  const publisher = new Squeaky.Publisher(getPubDebugger())
  const subscriber = new Squeaky.Subscriber({ topic, channel: 'test#ephemeral', ...getSubDebugger() })

  let resolver
  const received = new Promise((resolve) => {
    resolver = resolve
  })

  subscriber.on('message', (msg) => {
    assert.same(msg.body, { some: 'object' }, 'subscriber received the right message')
    setTimeout(async () => {
      const oldExpiration = msg.expiresIn
      await msg.touch()
      assert.ok(msg.expiresIn > oldExpiration, 'expiresIn should be larger')
      await msg.finish()
      resolver()
    }, 10)
  })
  await publisher.publish(topic, { some: 'object' })

  await received
  await Promise.all([
    publisher.close(),
    subscriber.close()
  ])
})

test('can publish non-objects', async (assert) => {
  const topic = getTopic()
  const publisher = new Squeaky.Publisher(getPubDebugger())
  const subscriber = new Squeaky.Subscriber({ topic, channel: 'test#ephemeral', ...getSubDebugger() })

  let resolver
  const received = new Promise((resolve) => {
    resolver = resolve
  })

  const payloads = [
    'strings',
    5,
    Buffer.from('a buffer')
  ]

  let current = 0
  const handler = async (msg) => {
    const payload = payloads[current++]
    assert.same(msg.body, typeof payload === 'string' ? Buffer.from(payload) : payload, 'subscriber received the right message')
    await msg.finish()
    if (current >= payloads.length) {
      resolver()
    }
  }
  subscriber.on('message', handler)

  for (const payload of payloads) {
    const res = await publisher.publish(topic, payload)
    assert.equals(res, 'OK')
  }

  await received
  return Promise.all([
    publisher.close(),
    subscriber.close()
  ])
})

test('calling publish twice synchronously works correctly', async (assert) => {
  const topic = getTopic()
  const publisher = new Squeaky.Publisher(getPubDebugger())
  const subscriber = new Squeaky.Subscriber({ topic, channel: 'test#ephemeral', ...getSubDebugger() })

  let resolver
  const received = new Promise((resolve) => {
    resolver = resolve
  })
  let counts = 0

  const handler = async (msg) => {
    assert.same(msg.body, { some: 'object' }, 'subscriber received the right message')
    await msg.finish()
    if (++counts === 2) {
      resolver()
    }
  }

  subscriber.on('message', handler)

  await Promise.all([
    publisher.publish(topic, { some: 'object' }),
    publisher.publish(topic, { some: 'object' })
  ])

  await received
  return Promise.all([
    publisher.close(),
    subscriber.close()
  ])
})

test('can mpublish', async (assert) => {
  const topic = getTopic()
  const publisher = new Squeaky.Publisher(getPubDebugger())
  const subscriber = new Squeaky.Subscriber({ topic, channel: 'test#ephemeral', ...getSubDebugger() })

  let resolver
  const received = new Promise((resolve) => {
    resolver = resolve
  })
  let counts = 0

  const handler = async (msg) => {
    assert.same(msg.body, { some: 'object' }, 'subscriber received the right message')
    await msg.finish()
    if (++counts === 2) {
      resolver()
    }
  }

  subscriber.on('message', handler)

  await publisher.publish(topic, [{ some: 'object' }, { some: 'object' }])

  await received
  return Promise.all([
    publisher.close(),
    subscriber.close()
  ])
})

test('can dpublish', async (assert) => {
  const topic = getTopic()
  const publisher = new Squeaky.Publisher(getPubDebugger())
  const subscriber = new Squeaky.Subscriber({ topic, channel: 'test#ephemeral', ...getSubDebugger() })

  let resolver
  const received = new Promise((resolve) => {
    resolver = resolve
  })

  const handler = async (msg) => {
    assert.same(msg.body, { some: 'object' }, 'subscriber received the right message')
    await msg.finish()
    if (Date.now() - 50 > sent) {
      resolver()
    }
  }

  subscriber.on('message', handler)

  const sent = Date.now()
  await publisher.publish(topic, { some: 'object' }, 1)

  await received
  return Promise.all([
    publisher.close(),
    subscriber.close()
  ])
})

test('dpublish returns an error when passed an invalid timeout', async (assert) => {
  const topic = getTopic()
  const publisher = new Squeaky.Publisher(getPubDebugger())

  try {
    await publisher.publish(topic, { some: 'object' }, 'notatimeout')
  } catch (err) {
    assert.match(err, {
      message: 'Received error response: E_INVALID DPUB could not parse timeout notatimeout'
    }, 'should throw')
  }

  await publisher.close()
})

test('errors when trying to delay an mpublish', async (assert) => {
  const topic = getTopic()
  const publisher = new Squeaky.Publisher(getPubDebugger())

  try {
    await publisher.publish(topic, [{ some: 'object' }, { another: 'object' }], 500)
  } catch (err) {
    assert.match(err, {
      message: 'Cannot delay a multi publish'
    }, 'should throw')
  }

  return publisher.close()
})

test('reconnects when disconnected', async (assert) => {
  const topic = 'squeaky_test'
  const publisher = new Squeaky.Publisher(getPubDebugger())
  const subscriber = new Squeaky.Subscriber({ topic, channel: 'test#ephemeral', ...getSubDebugger() })

  await new Promise((resolve) => subscriber.on('ready', resolve))
  await publisher.publish(topic, { some: 'object' })

  subscriber.connections.get('127.0.0.1:4150').socket.destroy()
  await new Promise((resolve) => subscriber.on('disconnect', ({ host, port }) => {
    assert.equals(host, '127.0.0.1')
    assert.equals(port, 4150)
    resolve()
  }))

  await new Promise((resolve) => subscriber.on('ready', resolve))

  publisher.connection.socket.destroy()
  await new Promise((resolve) => publisher.on('disconnect', resolve))
  await new Promise((resolve) => publisher.on('ready', resolve))

  await Promise.all([
    subscriber.close(),
    publisher.close()
  ])
})

test('emits an error and stops when reconnectAttempts is exceeded', async (assert) => {
  const topic = 'squeaky_test'
  const publisher = new Squeaky.Publisher({ maxConnectAttempts: 1, ...getPubDebugger() })
  const subscriber = new Squeaky.Subscriber({ maxConnectAttempts: 1, topic, channel: 'test#ephemeral', ...getSubDebugger() })

  const subscriberErrored = new Promise((resolve) => subscriber.on('error', (err) => {
    assert.equals(err.message, 'Maximum reconnect attempts exceeded')
    resolve()
  }))

  await new Promise((resolve) => subscriber.on('ready', resolve))
  await publisher.publish(topic, { some: 'object' })

  subscriber.connections.get('127.0.0.1:4150').socket.destroy()

  await Promise.all([
    subscriberErrored,
    new Promise((resolve) => subscriber.on('close', resolve))
  ])

  const publisherErrored = new Promise((resolve) => publisher.on('error', (err) => {
    assert.equals(err.message, 'Maximum reconnect attempts exceeded')
    resolve()
  }))

  publisher.connection.socket.destroy()

  await Promise.all([
    publisherErrored,
    new Promise((resolve) => publisher.on('close', resolve))
  ])

  await Promise.all([
    subscriber.close(),
    publisher.close()
  ])
})

test('rejects promises when a connection is in a finished state', async (assert) => {
  const topic = getTopic()
  const publisher = new Squeaky.Publisher({ maxConnectAttempts: 1, ...getPubDebugger() })

  await publisher.publish(topic, { some: 'object' })
  const publisherErrored = new Promise((resolve) => publisher.on('error', (err) => {
    assert.equals(err.message, 'Maximum reconnect attempts exceeded')
    resolve()
  }))

  publisher.connection.socket.destroy()

  await Promise.all([
    publisherErrored,
    new Promise((resolve) => publisher.on('close', resolve))
  ])

  try {
    await publisher.publish(topic, { some: 'object' })
  } catch (err) {
    assert.equals(err.message, 'The connection has been terminated')
  }

  await publisher.close()
})

test('errors that occur during a waited operation reject the promise', async (assert) => {
  const topic = getTopic()
  const publisher = new Squeaky.Publisher({ maxConnectAttempts: 1, ...getPubDebugger() })

  // once to establish the connection
  await publisher.publish(topic, { some: 'data' })

  // don't await here, we want to disconnect before it finishes
  const pubResult = publisher.publish(topic, { some: 'data' })
  publisher.connection.socket.destroy()

  try {
    await pubResult
  } catch (err) {
    assert.equals(err.message, 'Maximum reconnect attempts exceeded')
  }
})

test('errors for non-waited operations emit an event', async (assert) => {
  const topic = getTopic()
  const subscriber = new Squeaky.Subscriber({ topic, channel: 'test#ephemeral', ...getSubDebugger() })

  let resolver
  const errored = new Promise((resolve) => {
    resolver = resolve
  })

  subscriber.once('error', (err) => {
    assert.equals(err.host, '127.0.0.1')
    assert.equals(err.port, 4150)
    assert.equals(err.code, 'E_INVALID')
    resolver()
  })

  subscriber.once('ready', () => {
    subscriber.connections.get('127.0.0.1:4150').finish('notamessageid')
  })

  await errored
  return subscriber.close()
})

test('publisher can delay connections', async (assert) => {
  const publisher = new Squeaky.Publisher({ autoConnect: false, ...getPubDebugger() })
  assert.equals(publisher.connection, undefined)
  await publisher.connect()
  assert.notEquals(publisher.connection, undefined)

  return publisher.close()
})

test('publisher errors when trying to connect twice', async (assert) => {
  const publisher = new Squeaky.Publisher({ autoConnect: false, ...getPubDebugger() })
  assert.equals(publisher.connection, undefined)
  await publisher.connect()
  assert.notEquals(publisher.connection, undefined)

  try {
    await publisher.connect()
  } catch (err) {
    assert.equals(err.message, 'A connection has already been established')
  }

  return publisher.close()
})

test('subscriber can delay connections', async (assert) => {
  const topic = getTopic()
  const subscriber = new Squeaky.Subscriber({ topic, channel: 'test#ephemeral', autoConnect: false, ...getSubDebugger() })

  assert.equals(subscriber.connections.size, 0)
  await subscriber.connect()

  assert.equals(subscriber.connections.size, 1)

  return subscriber.close()
})

test('subscriber errors when trying to connect twice', async (assert) => {
  const topic = getTopic()
  const subscriber = new Squeaky.Subscriber({ topic, channel: 'test#ephemeral', autoConnect: false, ...getSubDebugger() })

  assert.equals(subscriber.connections.size, 0)
  await subscriber.connect()

  assert.equals(subscriber.connections.size, 1)
  try {
    await subscriber.connect()
  } catch (err) {
    assert.equals(err.message, 'A connection has already been established')
  }

  return subscriber.close()
})

test('subscriber can add a listener before connecting', async (assert) => {
  const topic = getTopic()
  const subscriber = new Squeaky.Subscriber({ topic, channel: 'test#ephemeral', autoConnect: false, ...getSubDebugger() })

  subscriber.on('message', (msg) => msg.finish())

  assert.equals(subscriber.connections.size, 0)
  await subscriber.connect()

  assert.equals(subscriber.connections.size, 1)

  return subscriber.close()
})
