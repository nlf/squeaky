'use strict'

function encodeToBuffer (value) {
  if (Buffer.isBuffer(value)) {
    return value
  } else if (typeof value === 'string') {
    return Buffer.from(value)
  } else if (typeof value === 'object') {
    return Buffer.from(JSON.stringify(value))
  } else {
    return Buffer.from(`${value}`)
  }
}

function createPayload (command, ...args) {
  let bits = [Buffer.from(`${command}\n`)]
  if (args.length === 0) {
    return bits[0]
  }

  const payloads = args.reduce((acc, chunk) => {
    const header = Buffer.alloc(4)
    const payload = encodeToBuffer(chunk)
    header.writeInt32BE(payload.byteLength, 0)
    return acc.concat([header, payload])
  }, [])

  if (payloads.length > 2) {
    const header = Buffer.alloc(8)
    header.writeInt32BE(payloads.reduce((acc, chunk, idx) => {
      if (idx % 2 > 0) {
        return acc
      }

      return acc + chunk.readInt32BE(0)
    }, 0), 0)
    header.writeInt32BE(payloads.length / 2, 4)
    bits = [bits[0], header, ...payloads]
  } else {
    bits = [bits[0], ...payloads]
  }

  return Buffer.concat(bits)
}

// coverage disabled so tests don't have to wait for a heartbeat
/* istanbul ignore next */
function nop () {
  return createPayload('NOP')
}

function identify (body) {
  return createPayload('IDENTIFY', body)
}

function pub (topic, body) {
  return createPayload(`PUB ${topic}`, body)
}

function dpub (topic, delay, body) {
  return createPayload(`DPUB ${topic} ${delay}`, body)
}

function mpub (topic, body) {
  return createPayload(`MPUB ${topic}`, ...body)
}

function sub (topic, channel) {
  return createPayload(`SUB ${topic} ${channel}`)
}

function rdy (count) {
  return createPayload(`RDY ${count}`)
}

function fin (id) {
  return createPayload(`FIN ${id}`)
}

function req (id, delay) {
  return createPayload(`REQ ${id} ${delay}`)
}

function touch (id) {
  return createPayload(`TOUCH ${id}`)
}

function decodeFrames (buf) {
  const result = {
    frames: []
  }

  while (buf.byteLength > 4) {
    const size = 4 + buf.readInt32BE(0)
    // coverage disabled here due to complications of forcing nsq to split a payload
    /* istanbul ignore if */
    if (buf.byteLength < size) {
      break
    }

    const type = buf.readInt32BE(4)
    const data = buf.slice(8, size)
    buf = buf.slice(size)
    result.frames.push({ size, type, data })
  }

  // coverage disabled here for same reason as above
  /* istanbul ignore next */
  result.remainder = buf.byteLength ? buf : Buffer.alloc(0)

  return result
}

module.exports = {
  decodeFrames,
  nop,
  identify,
  pub,
  dpub,
  mpub,
  sub,
  rdy,
  fin,
  req,
  touch
}
