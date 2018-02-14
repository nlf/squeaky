'use strict'

const { test } = require('tap')

const Squeaky = require('../')

test('can publish', async (assert) => {
  const client = new Squeaky()

  assert.notOk(client.connections.has('writer'), 'client should not have a writer before calling publish')

  const res = await client.publish('test#ephemeral', { some: 'object' })
  assert.equals(res, 'OK')

  assert.ok(client.connections.has('writer'), 'client should have a writer after calling publish')
  assert.equals(client.connections.get('writer')._last, 'PUB test#ephemeral', 'publishes to correct topic')
  await client.close('writer')
})

test('can publish non-objects', async (assert) => {
  const client = new Squeaky()

  const stringResponse = await client.publish('test#ephemeral', 'strings')
  assert.equals(stringResponse, 'OK')

  const numberResponse = await client.publish('test#ephemeral', 5)
  assert.equals(numberResponse, 'OK')

  const bufferResponse = await client.publish('test#ephemeral', Buffer.from('a buffer'))
  assert.equals(bufferResponse, 'OK')

  await client.close('writer')
})

test('calling publish twice reuses the same connection', async (assert) => {
  const client = new Squeaky()

  assert.notOk(client.connections.has('writer'), 'client should not have a writer before calling publish')

  const res = await client.publish('test#ephemeral', { some: 'object' })
  assert.equals(res, 'OK')

  assert.ok(client.connections.has('writer'), 'client should have a writer after calling publish')
  assert.equals(client.connections.get('writer')._last, 'PUB test#ephemeral', 'publishes to correct topic')

  const second = await client.publish('test#ephemeral', { another: 'object' })
  assert.equals(second, 'OK')

  await client.close('writer')
})

test('calling publish twice synchronously queues requests correctly', async (assert) => {
  const client = new Squeaky()

  const res = await Promise.all([
    client.publish('test#ephemeral', { object: 'one' }),
    client.publish('test#ephemeral', { object: 'two' })
  ])
  assert.same(res, ['OK', 'OK'])
  assert.equals(client.connections.get('writer')._last, 'PUB test#ephemeral')

  await client.close('writer')
})

test('can mpublish', async (assert) => {
  const client = new Squeaky()

  const res = await client.publish('test#ephemeral', [{ some: 'object' }, { another: 'object' }])
  assert.equals(res, 'OK')

  assert.equals(client.connections.get('writer')._last, 'MPUB test#ephemeral', 'mpublishes to correct topic')
  await client.close('writer')
})

test('can dpublish', async (assert) => {
  const client = new Squeaky()

  const res = await client.publish('test#ephemeral', { some: 'object' }, 500)
  assert.equals(res, 'OK')

  assert.equals(client.connections.get('writer')._last, 'DPUB test#ephemeral 500', 'dpublishes to correct topic')
  await client.close('writer')
})

test('dpublish returns an error when passed an invalid timeout', async (assert) => {
  const client = new Squeaky()

  try {
    await client.publish('test#ephemeral', { some: 'object' }, 'broken')
  } catch (err) {
    assert.match(err, {
      message: 'Received error response for "DPUB test#ephemeral broken": E_INVALID DPUB could not parse timeout broken'
    }, 'should throw')
  }

  assert.equals(client.connections.get('writer')._last, 'DPUB test#ephemeral broken', 'generates the right wrong command')
  await client.close('writer')
})

test('errors when trying to delay an mpublish', async (assert) => {
  const client = new Squeaky()

  try {
    await client.publish('test#ephemeral', [{ some: 'object' }, { another: 'object' }], 500)
  } catch (err) {
    assert.match(err, {
      message: 'Cannot delay a multi publish'
    }, 'should throw')
  }

  assert.ok(!client.connections.get('writer')._last || client.connections.get('writer')._last === 'IDENTIFY', 'should never have generated a command')
  await client.close('writer')
})

test('emits error events when a listener exists', async (assert) => {
  const client = new Squeaky()

  const promise = new Promise((resolve) => {
    client.once('error', (err) => {
      assert.match(err, {
        message: 'Received error response for "DPUB test#ephemeral broken": E_INVALID DPUB could not parse timeout broken'
      }, 'should throw')
      resolve()
    })
  })

  try {
    await client.publish('test#ephemeral', { some: 'object' }, 'broken')
  } catch (err) {
    assert.match(err, {
      message: 'Received error response for "DPUB test#ephemeral broken": E_INVALID DPUB could not parse timeout broken'
    }, 'should throw')
  }

  await promise
  await client.close('writer')
})
