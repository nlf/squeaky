'use strict'

const crypto = require('crypto')
const { test } = require('tap')

const Squeaky = require('../')

test('can subscribe', (assert) => {
  const client = new Squeaky()

  assert.equals(client.connections.size, 0, 'client should not have any connections')

  return client.subscribe('testsubscribe#ephemeral', 'channel#ephemeral').then(() => {
    assert.equals(client.connections.size, 1, 'client should have one connection')
    assert.ok(client.connections.has('testsubscribe#ephemeral.channel#ephemeral'), 'client should have named connection correctly')

    return client.close('testsubscribe#ephemeral.channel#ephemeral')
  })
})

test('subscribing twice returns the same connection', (assert) => {
  const client = new Squeaky()

  assert.equals(client.connections.size, 0, 'client should have no connections')

  return client.subscribe('testsubscribe#ephemeral', 'channel#ephemeral').then((conn) => {
    assert.equals(client.connections.size, 1, 'client should have one connection')
    assert.ok(client.connections.has('testsubscribe#ephemeral.channel#ephemeral'), 'client should have named connection correctly')

    return client.subscribe('testsubscribe#ephemeral', 'channel#ephemeral').then((conn2) => {
      assert.same(conn, conn2, 'connections should be the same')
      assert.equals(client.connections.size, 1, 'client should still have only one connection')
    })
  }).then(() => {
    return client.close('testsubscribe#ephemeral.channel#ephemeral')
  })
})

test('errors when a connection tries to subscribe twice', (assert) => {
  const client = new Squeaky()

  return client.subscribe('testsubscribe#ephemeral', 'channel#ephemeral').then(() => {
    const conn = client.connections.get('testsubscribe#ephemeral.channel#ephemeral')

    return assert.rejects(conn.subscribe('testsubscribe#ephemeral', 'channel2#ephemeral'), {
      message: 'This connection is already subscribed to testsubscribe#ephemeral.channel#ephemeral'
    }, 'should throw')
  }).then(() => {
    return client.close('testsubscribe#ephemeral.channel#ephemeral')
  })
})

test('receives the message event', (assert) => {
  const client = new Squeaky()
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
    return client.close('writer', `${topic}.channel#ephemeral`)
  })
})

test('can receive messages with non-object payloads', (assert) => {
  const client = new Squeaky()
  const topic = crypto.randomBytes(16).toString('hex') + '#ephemeral'

  return client.subscribe(topic, 'channel#ephemeral').then(() => {
    const stringPromise = new Promise((resolve) => {
      client.once(`${topic}.channel#ephemeral.message`, (msg) => {
        assert.match(msg, {
          body: Buffer.from('a string') // a raw string does not JSON.parse so we have to wrap it in a buffer
        }, 'should receive strings')
        msg.finish()
        resolve()
      })
    })

    return Promise.all([
      client.publish(topic, 'a string'),
      stringPromise
    ])
  }).then(() => {
    const numberPromise = new Promise((resolve) => {
      client.once(`${topic}.channel#ephemeral.message`, (msg) => {
        assert.match(msg, {
          body: 5 // this one JSON.parses
        }, 'should receive numbers')
        msg.finish()
        resolve()
      })
    })

    return Promise.all([
      client.publish(topic, 5),
      numberPromise
    ])
  }).then(() => {
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

    return Promise.all([
      client.publish(topic, buffer),
      bufferPromise
    ])
  }).then(() => {
    return client.close('writer', `${topic}.channel#ephemeral`)
  })
})

test('can subscribe with a function', (assert) => {
  const client = new Squeaky()
  const topic = crypto.randomBytes(16).toString('hex') + '#ephemeral'

  let resolver
  const promise = new Promise((resolve) => {
    resolver = resolve
  })

  return client.subscribe(topic, 'channel#ephemeral', (msg) => {
    assert.match(msg, {
      body: { test: 'subscribe' }
    }, 'should receive the correct message')
    msg.finish()
    resolver()
  }).then(() => {
    return Promise.all([
      client.publish(topic, { test: 'subscribe' }),
      promise
    ])
  }).then(() => {
    return client.close('writer', `${topic}.channel#ephemeral`)
  })
})

test('can touch a received message and extend expiration time', (assert) => {
  const client = new Squeaky()
  const topic = crypto.randomBytes(16).toString('hex') + '#ephemeral'

  let resolver
  const promise = new Promise((resolve) => {
    resolver = resolve
  })

  return client.subscribe(topic, 'channel#ephemeral', (msg) => {
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
  }).then(() => {
    return Promise.all([
      client.publish(topic, { test: 'subscribe' }),
      promise
    ])
  }).then(() => {
    return client.close('writer', `${topic}.channel#ephemeral`)
  })
})

test('can requeue a message', (assert) => {
  const client = new Squeaky()
  const topic = crypto.randomBytes(16).toString('hex') + '#ephemeral'

  let resolver
  const promise = new Promise((resolve) => {
    resolver = resolve
  })

  let attempt = 0
  return client.subscribe(topic, 'channel#ephemeral', (msg) => {
    assert.match(msg, {
      attempts: ++attempt,
      body: { test: 'subscribe' }
    }, 'should receive the correct message')

    if (attempt === 2) {
      msg.finish()
      return resolver()
    }

    msg.requeue()
  }).then(() => {
    return Promise.all([
      client.publish(topic, { test: 'subscribe' }),
      promise
    ])
  }).then(() => {
    return client.close('writer', `${topic}.channel#ephemeral`)
  })
})

test('waits for inflight messages to timeout before closing', (assert) => {
  const client = new Squeaky({ timeout: 1000 })

  let resolver
  const promise = new Promise((resolve) => {
    resolver = resolve
  })

  return client.subscribe('testtimeout#ephemeral', 'channel#ephemeral', (msg) => {
    assert.same(msg.body, { some: 'data' })
    // intentionally don't finish so we allow the timeout to trigger
    resolver()
  }).then(() => {
    return Promise.all([
      client.publish('testtimeout#ephemeral', { some: 'data' }),
      promise
    ])
  }).then(() => {
    let timer
    const timedout = new Promise((resolve, reject) => {
      timer = setTimeout(() => {
        return reject(new Error('Client timed out while closing'))
      }, 1050)
    })

    return Promise.race([
      client.close('writer', 'testtimeout#ephemeral.channel#ephemeral').then(() => {
        clearTimeout(timer)
      }),
      timedout
    ])
  })
})

test('calling touch on a message resets inflight timer', (assert) => {
  const client = new Squeaky({ timeout: 1000 })

  let resolver
  const promise = new Promise((resolve) => {
    resolver = resolve
  })

  return client.subscribe('touchtest#ephemeral', 'channel#ephemeral', (msg) => {
    assert.same(msg.body, { some: 'data' })
    // intentionally don't finish so we allow the timeout to trigger
    setTimeout(() => {
      msg.touch()
      resolver()
    }, 100)
  }).then(() => {
    return Promise.all([
      client.publish('touchtest#ephemeral', { some: 'data' }),
      promise
    ])
  }).then(() => {
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
})

test('calling functions for messages that arent in flight has no effect', (assert) => {
  const client = new Squeaky()

  return client.subscribe('testinflight#ephemeral', 'channel#ephemeral').then(() => {
    const conn = client.connections.get('testinflight#ephemeral.channel#ephemeral')

    const waitForReconnect = () => {
      return new Promise((resolve) => conn.once('ready', resolve))
    }

    const erroredFin = new Promise((resolve) => conn.once('error-response', (err) => {
      assert.match(err, {
        message: 'Received error response for "FIN asdf": E_INVALID Invalid Message ID'
      })
      resolve()
    }))

    return assert.resolves(conn.finish('asdf')).then(() => erroredFin).then(() => waitForReconnect()).then(() => {
      const erroredReq = new Promise((resolve) => conn.once('error-response', (err) => {
        assert.match(err, {
          message: 'Received error response for "REQ asdf 0": E_INVALID Invalid Message ID'
        })
        resolve()
      }))

      return assert.resolves(conn.requeue('asdf')).then(() => erroredReq).then(() => waitForReconnect())
    }).then(() => {
      const erroredTouch = new Promise((resolve) => conn.once('error-response', (err) => {
        assert.match(err, {
          message: 'Received error response for "TOUCH asdf": E_INVALID Invalid Message ID'
        })
        resolve()
      }))

      return assert.resolves(conn.touch('asdf')).then(() => erroredTouch).then(() => waitForReconnect())
    })
  }).then(() => {
    return client.close('testinflight#ephemeral.channel#ephemeral')
  })
})
