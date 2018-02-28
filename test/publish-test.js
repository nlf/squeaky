'use strict'

const { test } = require('tap')

const Squeaky = require('../')

test('can publish', (assert) => {
  const client = new Squeaky()

  assert.notOk(client.connections.has('writer'), 'client should not have a writer before calling publish')

  return client.publish('test#ephemeral', { some: 'object' }).then((res) => {
    assert.equals(res, 'OK')

    assert.ok(client.connections.has('writer'), 'client should have a writer after calling publish')
    assert.equals(client.connections.get('writer')._last, 'PUB test#ephemeral', 'publishes to correct topic')
    return client.close('writer')
  })
})

test('can publish non-objects', (assert) => {
  const client = new Squeaky()

  return client.publish('test#ephemeral', 'strings').then((stringResponse) => {
    assert.equals(stringResponse, 'OK')

    return client.publish('test#ephemeral', 5)
  }).then((numberResponse) => {
    assert.equals(numberResponse, 'OK')

    return client.publish('test#ephemeral', Buffer.from('a buffer'))
  }).then((bufferResponse) => {
    assert.equals(bufferResponse, 'OK')

    return client.close('writer')
  })
})

test('calling publish twice reuses the same connection', (assert) => {
  const client = new Squeaky()

  assert.notOk(client.connections.has('writer'), 'client should not have a writer before calling publish')

  return client.publish('test#ephemeral', { some: 'object' }).then((res) => {
    assert.equals(res, 'OK')

    assert.ok(client.connections.has('writer'), 'client should have a writer after calling publish')
    assert.equals(client.connections.get('writer')._last, 'PUB test#ephemeral', 'publishes to correct topic')

    return client.publish('test#ephemeral', { another: 'object' })
  }).then((second) => {
    assert.equals(second, 'OK')

    return client.close('writer')
  })
})

test('calling publish twice synchronously queues requests correctly', (assert) => {
  const client = new Squeaky()

  return Promise.all([
    client.publish('test#ephemeral', { object: 'one' }),
    client.publish('test2#ephemeral', { object: 'two' })
  ]).then((res) => {
    assert.same(res, ['OK', 'OK'])
    assert.equals(client.connections.get('writer')._last, 'PUB test2#ephemeral')

    return client.close('writer')
  })
})

test('can mpublish', (assert) => {
  const client = new Squeaky()

  return client.publish('test#ephemeral', [{ some: 'object' }, { another: 'object' }]).then((res) => {
    assert.equals(res, 'OK')

    assert.equals(client.connections.get('writer')._last, 'MPUB test#ephemeral', 'mpublishes to correct topic')
    return client.close('writer')
  })
})

test('can dpublish', (assert) => {
  const client = new Squeaky()

  return client.publish('test#ephemeral', { some: 'object' }, 500).then((res) => {
    assert.equals(res, 'OK')

    assert.equals(client.connections.get('writer')._last, 'DPUB test#ephemeral 500', 'dpublishes to correct topic')
    return client.close('writer')
  })
})

test('dpublish returns an error when passed an invalid timeout', (assert) => {
  const client = new Squeaky()

  return assert.rejects(client.publish('test#ephemeral', { some: 'object' }, 'broken'), {
    message: 'Received error response for "DPUB test#ephemeral broken": E_INVALID DPUB could not parse timeout broken'
  }, 'should throw').then(() => {
    assert.equals(client.connections.get('writer')._last, 'DPUB test#ephemeral broken', 'generates the right wrong command')
    return client.close('writer')
  })
})

test('errors when trying to delay an mpublish', (assert) => {
  const client = new Squeaky()

  return assert.rejects(client.publish('test#ephemeral', [{ some: 'object' }, { another: 'object' }], 500), {
    message: 'Cannot delay a multi publish'
  }, 'should throw').then(() => {
    assert.ok(!client.connections.get('writer')._last || client.connections.get('writer')._last === 'IDENTIFY', 'should never have generated a command')
    return client.close('writer')
  })
})
