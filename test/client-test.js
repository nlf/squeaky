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
    discoverFrequency: 300000
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
    discoverFrequency: 300000
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

test('emits the end event when a connection is interrupted', async (assert) => {
  const client = new Squeaky()

  await client.publish('test#ephemeral', { some: 'object' })
  await client.subscribe('test#ephemeral', 'channel#ephemeral')
  assert.ok(client.connections.has('test#ephemeral.channel#ephemeral'), 'should have a connection')

  const subscriber = client.connections.get('test#ephemeral.channel#ephemeral')
  const subscriberEnded = new Promise((resolve) => client.once('test#ephemeral.channel#ephemeral.end', resolve))

  subscriber.socket.emit('end')
  await subscriberEnded

  const writer = client.connections.get('writer')
  const writerEnded = new Promise((resolve) => client.once('writer.end', resolve))

  writer.socket.emit('end')
  await writerEnded

  await client.close('writer', 'test#ephemeral.channel#ephemeral')
})
