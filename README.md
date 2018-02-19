## Squeaky

A minimal (no external dependency) [nsq](http://nsq.io) client for node.js

### Limitations

Squeaky is designed to be as simple to use as possible, automatically creating new connections when needed and allowing a single client to create multiple subscriptions.
It does, currently, have many limitations however:

- tls is _not_ supported
- snappy compression is _not_ supported
- deflate compression is _not_ supported

### `new Squeaky()`

Create a new client.

The squeaky constructor supports exactly three options:

- `host`: the nsqd host to connect to (default: `'127.0.0.1'`)
- `port`: the nsqd port to connect to (default: `4150`)
- `lookup`: a string or array of strings representing the requested nsqlookupd instances
- `concurrency`: the maximum number of in-flight messages to allow _per subscription_ (default: `1`)
- `timeout`: message timeout in milliseconds (default: `60000`)
- `discoverFrequency`: how often to poll the nsqlookupd instances (when lookup is set)
- `maxConnectAttempts`: maximum number of attempts to make to (re)connect a connection (default: `5`)
- `reconnectDelayFactor`: factor to multiply connection attempt count by for exponential backoff in milliseconds (default: `1000`)
- `maxReconnectDelay`: maximum delay between reconnection attempts in milliseconds (default: `120000`)

Note that `host` and `port` are _always_ used for publishes, while they are only used for subscriptions if `lookup` is not set.

```js
const client = new Squeaky({ host: 'localhost' })
```

Creating a client instance does not inherently create any connections to nsqd, these connections are created only _after_ calling a method that requires a connection.

### `await squeaky.publish(topic, data)`

Publish data to the given topic.

The first time this method is called a new connection named `'writer'` will be created to be used for this and all future publishes.

The data published to the topic may be a Buffer, string or object. Any other type will be attempted to be coerced into a string.

This method will resolve with the string `'OK'` if successful, or will reject with an error if it fails. Note that it is not strictly _necessary_ to wait for completion before calling `publish` again as the connection maintains an internal queue and will process messages in the order received.

```js
const client = new Squeaky()
const response = await client.publish('myTopic', { some: 'data' })
// response === 'OK'
```

### `await squeaky.subscribe(topic, channel, [fn])`

Subscribe to the given topic on the given channel.

If no subscription has been added for the given topic and channel pair, one will be created for you. If only a `host` and `port` is defined when creating your client, this subscriber will be a direct connection to the referenced nsqd instance. If `lookup` is definedthen this subscriber will be a discoverer, which will poll the list of nsqlookupd instances on `discoverFrequency` automatically removing connections that are no longer referenced, and adding new connections where necessary. The discoverer will evenly distribute the `concurrency` value across all connections with any remainder from an even divisibility going to the last connection. That is if `concurrency` is `3` and your lookup host returns `2` connections, the first connection will have a ready count of `2` and the second connection a ready count of `1`. However a `concurrency` of `4` with `2` connections would receive a ready count of `2` on each. If `concurrency` is a number lower than the number of producers your lookup host(s) return, you will have some hosts created with a ready count of `0`. These connections will be kept alive, and if a connection with a ready state above 0 were to disconnect, the concurrency would be redistributed amongst the remaining connections.

If a function is passed as a third parameter it will be used as a callback for received messages, if no function is passed then you should add a listener for the `${topic}.${channel}.message` event, which will be fired whether a callback is passed or not.

The callback (and the event handler) will receive a single parameter, the message that is received.

```js
const client = new Squeaky()
await client.subscribe('myTopic', 'myChannel', (msg) => {
  // got a message here
  msg.finish()
})

// alternatively
client.on('myTopic.myChannel.message', (msg) => {
  // also got the message here
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

### `squeaky.close([...connections])`

Close the given connection(s) (or all connections if none are specified). The publisher connection is always named `writer`, while subscriber connections are named `${topic}.${channel}`. As in:

```js
await client.subscribe('mytopic', 'mychannel')
await client.close('mytopic.mychannel')
```

Note that squeaky tracks current in flight messages and will wait until each has had `finish()` or `requeue()` called on them, or the message timeout has been reached before the connection will close.

### `squeaky.unref()`

Unref all current _and_ future connections on this client, this will allow your program to terminate if the only thing keeping it running is squeaky.

### Client events

The squeaky client is an EventEmitter. These are the events which it emits and the conditions which trigger each.

- `error`: emitted on max (re)connect attempts or unexpected error
- `reader.disconnect`, `writer.disconnect`: emitted when the connection to nsqd is severed (timeout/drop/close)
- `reader.end`, `writer.end`: emitted when the client is disconnected and will not attempt further (re)connects
- `reader.ready`, `writer.ready`: emitted when a connection has been established to nsqd
- `<topic>.<channel>.message`: emitted by each time a message is received from <topic> and <channel>
