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
  await publisher.publish(topic, { some: 'object' })

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

  const handler = async (msg) => {
    const payload = payloads.shift()
    assert.same(msg.body, typeof payload === 'string' ? Buffer.from(payload) : payload, 'subscriber received the right message')
    await msg.finish()
    if (!payloads.length) {
      resolver()
    }
  }
  subscriber.on('message', handler)

  for (const payload of payloads) {
    await publisher.publish(topic, payload)
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

  let resolver
  const received = new Promise((resolve) => {
    resolver = resolve
  })

  publisher.on('error', (err) => {
    assert.match(err, {
      message: 'Received error response: E_INVALID DPUB could not parse timeout notatimeout'
    }, 'should throw')
    resolver()
  })

  await publisher.publish(topic, { some: 'object' }, 'notatimeout')

  await received

  await publisher.close()
})

test('errors when trying to delay an mpublish', async (assert) => {
  const topic = getTopic()
  const publisher = new Squeaky.Publisher(getPubDebugger())

  let resolver
  const received = new Promise((resolve) => {
    resolver = resolve
  })

  publisher.on('error', (err) => {
    assert.match(err, {
      message: 'Cannot delay a multi publish'
    }, 'should throw')
    resolver()
  })

  await publisher.publish(topic, [{ some: 'object' }, { another: 'object' }], 500)

  await received

  await publisher.close()
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
