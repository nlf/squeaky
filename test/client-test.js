'use strict'

const { test } = require('tap')

const Squeaky = require('../')

test('can create a client (no params)', (assert) => {
  let client

  assert.doesNotThrow(() => {
    client = new Squeaky()
  }, 'should not throw')

  assert.same(client.options, {
    host: '127.0.0.1',
    port: 4150,
    lookup: [],
    concurrency: 1,
    timeout: 60000,
    discoverFrequency: 300000,
    maxConnectAttempts: 5,
    reconnectDelayFactor: 1000,
    maxReconnectDelay: 120000
  }, 'should set default options')

  assert.end()
})

test('can create a client (missing some options)', (assert) => {
  let client

  assert.doesNotThrow(() => {
    client = new Squeaky({ host: 'localhost' })
  }, 'should not throw')

  assert.same(client.options, {
    host: 'localhost',
    port: 4150,
    lookup: [],
    concurrency: 1,
    timeout: 60000,
    discoverFrequency: 300000,
    maxConnectAttempts: 5,
    reconnectDelayFactor: 1000,
    maxReconnectDelay: 120000
  }, 'should set other options to defaults')

  assert.end()
})

test('rejects when trying to identify on an already identified connection', (assert) => {
  const client = new Squeaky()

  return client.subscribe('testidentify#ephemeral', 'channel#ephemeral').then(() => {
    const conn = client.connections.get('testidentify#ephemeral.channel#ephemeral')

    return assert.rejects(conn._identify, {
      message: 'Attempted to identify during an invalid state'
    }, 'should reject')
  }).then(() => {
    return client.close('testidentify#ephemeral.channel#ephemeral')
  })
})

test('gives an error when passing an invalid timeout', (assert) => {
  const client = new Squeaky({ timeout: 100 })
  const errored = new Promise((resolve) => client.once('error', (err) => {
    assert.match(err, {
      message: 'Received error response for "IDENTIFY": E_BAD_BODY IDENTIFY msg timeout (100) is invalid'
    }, 'should emit an error')
    resolve()
  }))

  // no need to close ourselves, the error will do it automatically so just wait for the event
  const closed = new Promise((resolve) => client.once('writer.end', resolve))

  // don't await this, the promise won't be resolved
  client.publish('test#ephemeral', { some: 'data' })
  return Promise.all([
    errored,
    closed
  ])
})

test('emits an error when sockets fail to connect', (assert) => {
  const client = new Squeaky({ port: 65530, maxConnectAttempts: 0 })
  const errored = new Promise((resolve) => client.once('error', (err) => {
    assert.match(err, {
      code: 'ECONNREFUSED',
      connection: 'writer'
    }, 'should emit ECONNREFUSED')
    resolve()
  }))

  client.publish('test#ephemeral', { some: 'data' })

  return errored
})

test('ignores invalid params to close', (assert) => {
  const client = new Squeaky()

  return assert.resolves(client.close('some', 'bogus', 'parameters'))
})

test('closes only the connections requested', (assert) => {
  const client = new Squeaky()

  return client.subscribe('one#ephemeral', 'two#ephemeral').then(() => {
    return client.subscribe('three#ephemeral', 'four#ephemeral')
  }).then(() => {
    assert.ok(client.connections.has('one#ephemeral.two#ephemeral'), 'should have connection for one.two')
    assert.ok(client.connections.has('three#ephemeral.four#ephemeral'), 'should have connection for three.four')

    return client.close('one#ephemeral.two#ephemeral')
  }).then(() => {
    assert.notOk(client.connections.has('one#ephemeral.two#ephemeral'), 'should no longer have connection for one.two')
    assert.ok(client.connections.has('three#ephemeral.four#ephemeral'), 'should still have connection for three.four')

    return client.close('three#ephemeral.four#ephemeral')
  })
})

test('reconnects when disconnected', (assert) => {
  const client = new Squeaky()

  return client.publish('testmax#ephemeral', { some: 'object' }).then(() => {
    return client.subscribe('testmax#ephemeral', 'channel#ephemeral')
  }).then(() => {
    assert.ok(client.connections.has('testmax#ephemeral.channel#ephemeral'), 'should have a connection')

    const subscriber = client.connections.get('testmax#ephemeral.channel#ephemeral')
    const subscriberDisconnected = new Promise((resolve) => client.once('testmax#ephemeral.channel#ephemeral.disconnect', resolve))
    const subscriberReady = new Promise((resolve) => client.once('testmax#ephemeral.channel#ephemeral.ready', resolve))

    subscriber.socket.destroy()
    return Promise.all([
      subscriberDisconnected,
      subscriberReady
    ])
  }).then(() => {
    const writer = client.connections.get('writer')
    const writerDisconnected = new Promise((resolve) => client.once('writer.disconnect', resolve))
    const writerReady = new Promise((resolve) => client.once('writer.ready', resolve))

    writer.socket.destroy()
    return Promise.all([
      writerDisconnected,
      writerReady
    ])
  }).then(() => {
    return client.close('writer', 'testmax#ephemeral.channel#ephemeral')
  })
})

test('emits an error when maxConnectAttempts is exceeded', (assert) => {
  const client = new Squeaky({ maxConnectAttempts: 0 })

  return client.publish('test#ephemeral', { some: 'object' }).then(() => {
    return client.subscribe('test#ephemeral', 'channel#ephemeral')
  }).then(() => {
    assert.ok(client.connections.has('test#ephemeral.channel#ephemeral'), 'should have a connection')

    const subscriber = client.connections.get('test#ephemeral.channel#ephemeral')
    const subscriberErrored = new Promise((resolve) => client.once('error', (err) => {
      assert.match(err, {
        message: 'Maximum reconnection attempts exceeded',
        connection: 'test#ephemeral.channel#ephemeral'
      }, 'should return correct error')
      resolve()
    }))
    const subscriberEnded = new Promise((resolve) => client.once('test#ephemeral.channel#ephemeral.end', resolve))

    subscriber.socket.destroy()
    return Promise.all([
      subscriberErrored,
      subscriberEnded
    ])
  }).then(() => {
    const writer = client.connections.get('writer')
    const writerErrored = new Promise((resolve) => client.once('error', (err) => {
      assert.match(err, {
        message: 'Maximum reconnection attempts exceeded',
        connection: 'writer'
      }, 'should return correct error')
      resolve()
    }))
    const writerEnded = new Promise((resolve) => client.once('writer.end', resolve))

    writer.socket.destroy()
    return Promise.all([
      writerErrored,
      writerEnded
    ])
  })
})

test('can unref sockets', (assert) => {
  const client = new Squeaky()

  // this is a dummy to keep the test from exiting early
  const timer = setTimeout(() => {}, 1000)
  return client.subscribe('test#ephemeral', 'channel#ephemeral').then(() => {
    assert.ok(client.connections.get('test#ephemeral.channel#ephemeral').socket._handle.hasRef())
    client.unref()
    assert.notOk(client.connections.get('test#ephemeral.channel#ephemeral').socket._handle.hasRef())
    return client.subscribe('another#ephemeral', 'channel#ephemeral')
  }).then(() => {
    assert.notOk(client.connections.get('another#ephemeral.channel#ephemeral').socket._handle.hasRef())
    return client.publish('something#ephemeral', { some: 'data' })
  }).then(() => {
    assert.notOk(client.connections.get('writer').socket._handle.hasRef())
    return client.close('writer', 'test#ephemeral.channel#ephemeral', 'another#ephemeral.channel#ephemeral')
  }).then(() => {
    clearTimeout(timer)
  })
})
