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
- `autoConnect`: immediately connect to nsqd upon creation (default: `true`)
- `host`: the nsqd host to connect to (default: `'127.0.0.1'`)
- `port`: the nsqd port to connect to (default: `4150`)
- `topic`: the default topic to publish to (default: unset, which requires passing a topic to `publish()`)
- `timeout`: message timeout in milliseconds (default: `60000`)
- `maxConnectAttempts`: maximum number of attempts to make to (re)connect a connection (default: `5`)
- `reconnectDelayFactor`: factor to multiply connection attempt count by for exponential backoff in milliseconds (default: `1000`)
- `maxReconnectDelay`: maximum delay between reconnection attempts in milliseconds (default: `120000`)

You may also specify options as a URI, for example: `nsq://127.0.0.1:4150/mytopic?timeout=1000`

When creating the publisher, a connection is automatically created to the given `host` and `port`.

Valid methods on the publisher are:

#### `await publisher.connect()`

Manually connect to nsqd, this will reject if the connection has already been created (for example, by `autoConnect` being `true`).

#### `await publisher.publish(topic, data, [delay])`

Publish `data` to `topic`.

If a default `topic` was set in your options, you must omit the first parameter. Publishing to multiple topics with one client requires not defining a default.

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
- `autoConnect`: automatically poll nsqlookupd (if applicable) and create a connection to the nsqd(s) (default: `true`)
- `host`: the nsqd host to connect to (default: `'127.0.0.1'`)
- `port`: the nsqd port to connect to (default: `4150`)
- `lookup`: a string or array of strings representing the requested nsqlookupd instances
- `concurrency`: the maximum number of in-flight messages to allow (default: `1`)
- `timeout`: message timeout in milliseconds (default: `60000`)
- `discoverFrequency`: how often to poll the nsqlookupd instances (when lookup is set)
- `maxConnectAttempts`: maximum number of attempts to make to (re)connect a connection (default: `5`)
- `reconnectDelayFactor`: factor to multiply connection attempt count by for exponential backoff in milliseconds (default: `1000`)
- `maxReconnectDelay`: maximum delay between reconnection attempts in milliseconds (default: `120000`)
- `keepaliveOffset`: the delta between the message's expiration and when the keepalive timer will trigger on messages it is enabled for, this is tunable to allow for network latency (default: `500`)

If `lookup` is specified, it must be a single nsqlookupd URL or an array of URLs, for example `'http://nsqlookupd:4161'` or `['http://lookup-1:4161', 'http://lookup-2:4161']`. This may also be specified by passing a list of comma separated URIs using the `nsqlookup` protocol to the constructor such as: `nsqlookup://lookup-1:4161/mytopic?channel=mychannel,nsqlookup://lookup-2:4161/`. Note that options will only be respected on the first entry, additional entries will only respect an `ssl` flag, which is only valid when using URIs, that changes the lookup protocol from `http` to `https`.

If `lookup` is _not_ specified, a direct connection will be made to `host` on `port`. In this case a single URI may be given to the constructor using the `nsq` protocol such as: `nsq://127.0.0.1:4150/mytopic?channel=mychannel`.

Upon creating the subscriber instance, a connection will be automatically created, the given `topic` and `channel` will be subscribed to. Each host will have an initial ready state of `0` until a listener is added to the `message` event.

Every `discoverFrequency` hosts that are no longer present in `lookup` hosts will be removed, new hosts will be added, and the `concurrency` will be redistributed.

If `concurrency` is less than the number of hosts available, each host will be assigned a ready state of `1` until `concurrency` is satisfied. The remaining hosts will be assigned a ready state of `0`. On the next `discoverFrequency` interval any hosts with a ready state of `0` will be changed to `1` while hosts that had a ready state of `1` will be changed to `0` based on the host's activity. This helps ensure that `concurrency` is never exceeded while no hosts are ignored completely.

If `concurrency` is greater than or equal to the number of hosts available, each host will receive a ready state of `Math.floor(concurrency / hostCount)` ensuring that `concurrency` is never exceeded.

#### `await subscriber.connect()`

Manually connect to nsq, this includes polling lookupd if applicable. This will reject if a connection has already been established (for example, if `autoConnect` is `true`).

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
- `expired`: boolean signifying that the message has exceeded its timeout and already been requeued by nsqd
- `expiresIn`: the amount of time before this message will be automatically requeued, represented in milliseconds
- `finish()`: signal successful processing to the server, must be called to receive a new message
- `requeue([delay])`: signal failed processing to the server, the optional delay parameter is represented in milliseconds and represents how long the server will wait before adding the message back to the queue
- `touch()`: inform the server that the message is still being processed, used to prevent an automatic timeout
- `keepalive()`: automatically `touch()` the message on an interval until it fully expires, or either `finish()` or `requeue()` is called

If none of `finish`, `touch` or `requeue` are called on the message before `expiresIn`, the message will be returned to nsq and retried later.

In addition, the following methods are available on the subscriber

#### `await subscriber.close()`

Close all established connections.

#### `await subscriber.pause()`

Pause consumption of new messages.

#### `await subscriber.unpause()`

Resume consumption of new messages.

#### `squeaky.unref()`

Unref all established connections. New connections established after `unref` has been called will be unreffed upon their creation.

### Events

The following events may be emitted:

- `connect`: an initial connection has been established.
- `disconnect`: the connection has been dropped.
- `reconnectAttempt`: a reconnection is about to be attempted
- `reconnect`: a reconnection has finished (the `connect` event will not be fired again after the initial connection).
- `ready`: the instance is connected, identified and ready for use.
- `error`: an error occurred, this could be related to the connection itself, or could be an error response from nsq.
- `close`: this is emitted when the connection is completely closed.
- `drain`: all pending writes have been completed.
- `message`: a message has been received from nsq (SUBSCRIBER ONLY).
- `removed`: a host connection has been removed (SUBSCRIBER ONLY).
- `pollBegin`: the client has begun looking for new hosts (SUBSCRIBER ONLY).
- `pollComplete`: lookup hosts have been polled and distribution state redistributed (SUBSCRIBER ONLY).

On subscribers, the `connect`, `disconnect`, `reconnect`, `ready`, `close`, and `drain` events will be passed a single argument `{ host, port }` to distinguish events for each host. The `error` event will have `host` and `port` properties on the `Error` object if the source of the error is connection specific. Errors reaching lookup hosts will have a `host` property representing the nsqlookupd host that failed to respond correctly, as well as a `code` property set to `ELOOKUPFAILURE`.
