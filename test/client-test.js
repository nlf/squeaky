'use strict'

const { test } = require('tap')

const Squeaky = require('../')

test('can create a client (no params)', async (assert) => {
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
})

test('can create a client (missing some options)', async (assert) => {
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
})

test('throws when trying to identify on an already identified connection', async (assert) => {
  const client = new Squeaky()

  await client.subscribe('test#ephemeral', 'channel#ephemeral')
  const conn = client.connections.get('test#ephemeral.channel#ephemeral')

  try {
    // throws synchronously
    conn._identify()
  } catch (err) {
    assert.match(err, {
      message: 'Attempted to identify during an invalid state'
    }, 'should throw')
  }

  await client.close('test#ephemeral.channel#ephemeral')
})

test('gives an error when passing an invalid timeout', async (assert) => {
  const client = new Squeaky({ timeout: 100 })
  const errored = new Promise((resolve) => client.once('error', (err) => {
    assert.match(err, {
      message: 'Received error response for "IDENTIFY": E_BAD_BODY IDENTIFY msg timeout (100) is invalid'
    }, 'should emit an error')
    resolve()
  }))

  // no need to close ourselves, the error will do it automatically so just wait for the event
  const closed = new Promise((resolve) => client.once('writer.closed', resolve))

  // don't await this, the promise won't be resolved
  client.publish('test#ephemeral', { some: 'data' })
  await Promise.all([
    errored,
    closed
  ])
})

test('emits an error when sockets fail to connect', async (assert) => {
  const client = new Squeaky({ port: 65530, maxConnectAttempts: 0 })
  const errored = new Promise((resolve) => client.once('error', (err) => {
    assert.match(err, {
      code: 'ECONNREFUSED',
      connection: 'writer'
    }, 'should emit ECONNREFUSED')
    resolve()
  }))

  client.publish('test#ephemeral', { some: 'data' })

  await errored
})

test('ignores invalid params to close', async (assert) => {
  const client = new Squeaky()

  await assert.resolves(client.close('some', 'bogus', 'parameters'))
})

test('closes only the connections requested', async (assert) => {
  const client = new Squeaky()

  await client.subscribe('one#ephemeral', 'two#ephemeral')
  await client.subscribe('three#ephemeral', 'four#ephemeral')

  assert.ok(client.connections.has('one#ephemeral.two#ephemeral'), 'should have connection for one.two')
  assert.ok(client.connections.has('three#ephemeral.four#ephemeral'), 'should have connection for three.four')

  await client.close('one#ephemeral.two#ephemeral')

  assert.notOk(client.connections.has('one#ephemeral.two#ephemeral'), 'should no longer have connection for one.two')
  assert.ok(client.connections.has('three#ephemeral.four#ephemeral'), 'should still have connection for three.four')

  await client.close('three#ephemeral.four#ephemeral')
})

test('reconnects when disconnected', async (assert) => {
  const client = new Squeaky()

  await client.publish('test#ephemeral', { some: 'object' })
  await client.subscribe('test#ephemeral', 'channel#ephemeral')
  assert.ok(client.connections.has('test#ephemeral.channel#ephemeral'), 'should have a connection')

  const subscriber = client.connections.get('test#ephemeral.channel#ephemeral')
  const subscriberEnded = new Promise((resolve) => client.once('test#ephemeral.channel#ephemeral.ready', resolve))

  subscriber.socket.destroy()
  await subscriberEnded

  const writer = client.connections.get('writer')
  const writerEnded = new Promise((resolve) => client.once('writer.ready', resolve))

  writer.socket.destroy()
  await writerEnded

  await client.close('writer', 'test#ephemeral.channel#ephemeral')
})

test('emits an error when maxConnectAttempts is exceeded', async (assert) => {
  const client = new Squeaky({ maxConnectAttempts: 0 })

  await client.publish('test#ephemeral', { some: 'object' })
  await client.subscribe('test#ephemeral', 'channel#ephemeral')
  assert.ok(client.connections.has('test#ephemeral.channel#ephemeral'), 'should have a connection')

  const subscriber = client.connections.get('test#ephemeral.channel#ephemeral')
  const subscriberEnded = new Promise((resolve) => client.once('error', (err) => {
    assert.match(err, {
      message: 'Maximum reconnection attempts exceeded',
      connection: 'test#ephemeral.channel#ephemeral'
    }, 'should return correct error')
    resolve()
  }))

  subscriber.socket.destroy()
  await subscriberEnded

  const writer = client.connections.get('writer')
  const writerEnded = new Promise((resolve) => client.once('error', (err) => {
    assert.match(err, {
      message: 'Maximum reconnection attempts exceeded',
      connection: 'writer'
    }, 'should return correct error')
    resolve()
  }))

  writer.socket.destroy()
  await writerEnded
})

test('can unref sockets', async (assert) => {
  const client = new Squeaky()

  // this is a dummy to keep the test from exiting early
  const timer = setTimeout(() => {}, 1000)
  await client.subscribe('test#ephemeral', 'channel#ephemeral')
  assert.ok(client.connections.get('test#ephemeral.channel#ephemeral').socket._handle.hasRef())
  client.unref()
  assert.notOk(client.connections.get('test#ephemeral.channel#ephemeral').socket._handle.hasRef())
  await client.subscribe('another#ephemeral', 'channel#ephemeral')
  assert.notOk(client.connections.get('another#ephemeral.channel#ephemeral').socket._handle.hasRef())
  await client.publish('something#ephemeral', { some: 'data' })
  assert.notOk(client.connections.get('writer').socket._handle.hasRef())
  await client.close('writer', 'test#ephemeral.channel#ephemeral', 'another#ephemeral.channel#ephemeral')
  clearTimeout(timer)
})
