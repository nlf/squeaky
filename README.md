## Squeaky

A minimal (only one external dependency) [nsq](http://nsq.io) client for node.js

### Limitations

Squeaky is designed to be as simple to use as possible, automatically creating new connections when needed and allowing a single client to create multiple subscriptions.
It does, currently, have many limitations however:

- nsqlookupd is _not_ supported
- tls is _not_ supported
- snappy compression is _not_ supported
- deflate compression is _not_ supported

### `new Squeaky()`

Create a new client.

The squeaky constructor supports exactly three options:

- `host`: the nsqd host to connect to (default: `'127.0.0.1'`)
- `port`: the nsqd port to connect to (default: `4150`)
- `concurrency`: the maximum number of in-flight messages to allow _per subscription_ (default: `1`)

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

If no subscription has been added for the given topic and channel pair, one will be created for you.

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
