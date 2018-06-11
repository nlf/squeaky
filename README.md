## Squeaky

A minimal (no external dependency) [nsq](http://nsq.io) client for node.js

### Limitations

Squeaky is designed to be as simple to use as possible, automatically creating new connections when needed.
It does, currently, have many limitations however:

- tls is _not_ supported
- snappy compression is _not_ supported
- deflate compression is _not_ supported

### Publishing

To publish to nsq, first create an instance of `Squeaky.Publisher`:

```js
const publisher = new Squeaky.Publisher(/* options */)
```

Available options for the publisher are:
- `host`: the nsqd host to connect to (default: `'127.0.0.1'`)
- `port`: the nsqd port to connect to (default: `4150`)
- `timeout`: message timeout in milliseconds (default: `60000`)
- `maxConnectAttempts`: maximum number of attempts to make to (re)connect a connection (default: `5`)
- `reconnectDelayFactor`: factor to multiply connection attempt count by for exponential backoff in milliseconds (default: `1000`)
- `maxReconnectDelay`: maximum delay between reconnection attempts in milliseconds (default: `120000`)

When creating the publisher, a connection is automatically created to the given `host` and `port`.

Valid methods on the publisher are:

#### `await publisher.publish(topic, data, [delay])`

Publish `data` to `topic`.

`data` may be an object, a Buffer, a string, or an array of any of those types. Any other values will be coerced into a string.

If `data` is an array, the command used will be an `MPUB` which will result in each message in the array being delivered individually.

If `delay` is provided, the command used will be `DPUB` which results in the message being sent after the given delay. Note that you will receive an array if attempting to delay an `MPUB` (i.e. if you pass `data` as an array as well as providing a delay)

#### `await publisher.close()`

Close the connection.

#### `publisher.unref()`

Unref the connection preventing it from keeping your application running.


### Subscribing

To subscribe to an nsq channel, first create an instance of `Squeaky.Subscriber`:

```js
const subscriber = new Squeaky.Subscriber(/* options */)
```

Available options for the subscriber are:
- `topic`: the nsqd topic to subscribe to (REQUIRED)
- `channel`: the channel on the topic to subscribe to (REQUIRED)
- `host`: the nsqd host to connect to (default: `'127.0.0.1'`)
- `port`: the nsqd port to connect to (default: `4150`)
- `lookup`: a string or array of strings representing the requested nsqlookupd instances
- `concurrency`: the maximum number of in-flight messages to allow (default: `1`)
- `timeout`: message timeout in milliseconds (default: `60000`)
- `discoverFrequency`: how often to poll the nsqlookupd instances (when lookup is set)
- `maxConnectAttempts`: maximum number of attempts to make to (re)connect a connection (default: `5`)
- `reconnectDelayFactor`: factor to multiply connection attempt count by for exponential backoff in milliseconds (default: `1000`)
- `maxReconnectDelay`: maximum delay between reconnection attempts in milliseconds (default: `120000`)

If `lookup` is specified, it must be a single nsqlookupd URL or an array of URLs, for example `'http://nsqlookupd:4161'` or `['http://lookup-1:4161', 'http://lookup-2:4161']`

If `lookup` is _not_ specified, a direct connection will be made to `host` on `port`.

Upon creating the subscriber instance, a connection will be automatically created, the given `topic` and `channel` will be subscribed to, and `concurrency` will be distributed amongst the available hosts.

Every `discoverFrequency` hosts that are no longer present in `lookup` hosts will be removed, new hosts will be added, and the `concurrency` will be redistributed.

If `concurrency` is less than the number of hosts available, each host will be assigned a ready state of `1` until `concurrency` is satisfied. The remaining hosts will be assigned a ready state of `0`. On the next `discoverFrequency` interval any hosts with a ready state of `0` will be changed to `1` while hosts that had a ready state of `1` will be changed to `0` based on the host's activity. This helps ensure that `concurrency` is never exceeded while no hosts are ignored completely.

If `concurrency` is greater than or equal to the number of hosts available, each host will receive a ready state of `Math.floor(concurrency / hostCount)` ensuring that `concurrency` is never exceeded.

#### Messages

In order to receive messages, the subscriber must be given a listener on the `message` event:

```js
subscriber.on('message', (msg) => {
  // process message here
  msg.finish()
})
```

The message object contains the following properties and methods:

- `id`: the message id
- `timestamp`: a date object representing the time the message was received
- `attempts`: the number of times this message has been delivered and _not_ completed according to the server
- `body`: the data associated with the message, this could be any JSON.parse-able value or a Buffer
- `expiresIn`: the amount of time before this message will be automatically requeued, represented in milliseconds
- `finish()`: signal successful processing to the server, must be called to receive a new message
- `requeue([delay])`: signal failed processing to the server, the optional delay parameter is represented in milliseconds and represents how long the server will wait before adding the message back to the queue
- `touch()`: inform the server that the message is still being processed, used to prevent an automatic timeout

If none of `finish`, `touch` or `requeue` are called on the message before `expiresIn`, the message will be returned to nsq and retried later.

In addition, the following methods are available on the subscriber

#### `await subscriber.close()`

Close all established connections.

### `squeaky.unref()`

Unref all established connections. New connections established after `unref` has been called will be unreffed upon their creation.
