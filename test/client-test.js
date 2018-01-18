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
    concurrency: 1
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
    concurrency: 1
  }, 'should set other options to defaults')
})

test('throws when trying to identify on an already identified connection', async (assert) => {
  const client = new Squeaky()

  await client.subscribe('test', 'channel')
  const conn = client.connections.get('test.channel')

  try {
    // throws synchronously
    conn._identify()
  } catch (err) {
    assert.match(err, {
      message: 'Attempted to identify during an invalid state'
    }, 'should throw')
  }

  await client.close('test.channel')
})

test('ignores invalid params to close', async (assert) => {
  const client = new Squeaky()

  await assert.resolves(client.close('some', 'bogus', 'parameters'))
})

test('closes only the connections requested', async (assert) => {
  const client = new Squeaky()

  await client.subscribe('one', 'two')
  await client.subscribe('three', 'four')

  assert.ok(client.connections.has('one.two'), 'should have connection for one.two')
  assert.ok(client.connections.has('three.four'), 'should have connection for three.four')

  await client.close('one.two')

  assert.notOk(client.connections.has('one.two'), 'should no longer have connection for one.two')
  assert.ok(client.connections.has('three.four'), 'should still have connection for three.four')

  await client.close('three.four')
})

test('emits the end event when a connection is interrupted', async (assert) => {
  const client = new Squeaky()

  await client.publish('test', { some: 'object' })
  await client.subscribe('test', 'channel')
  assert.ok(client.connections.has('test.channel'), 'should have a connection')

  const subscriber = client.connections.get('test.channel')
  const subscriberEnded = new Promise((resolve) => {
    client.once('end', (connection) => {
      assert.equals(connection, 'test.channel')
      resolve()
    })
  })

  subscriber.socket.emit('end')
  await subscriberEnded

  const writer = client.connections.get('writer')
  const writerEnded = new Promise((resolve) => {
    client.once('end', (connection) => {
      assert.equals(connection, 'writer')
      resolve()
    })
  })

  writer.socket.emit('end')
  await writerEnded

  await client.close('writer', 'test.channel')
})
