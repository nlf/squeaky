'use strict'

// coverage is disabled in this module in 3 places,
// all 3 of which are only guards to make sure the
// wrapping promise is only resolved or rejected _once_

const http = require('http')

function get (url) {
  return new Promise((resolve, reject) => {
    let finished = false
    const client = http.get(url, (res) => {
      if (res.statusCode !== 200) {
        /* istanbul ignore else */
        if (!finished) {
          finished = true
          return reject(new Error(`Got unexpected statusCode: ${res.statusCode}`))
        }
      }

      const chunks = []
      res.on('data', (chunk) => chunks.push(chunk))
      res.on('end', () => {
        /* istanbul ignore else */
        if (!finished) {
          finished = true
          try {
            const payload = JSON.parse(Buffer.concat(chunks))
            return resolve(payload)
          } catch (err) {
            return reject(err)
          }
        }
      })
    })

    client.on('error', (err) => {
      /* istanbul ignore else */
      if (!finished) {
        finished = true
        return reject(err)
      }
    })
  })
}

module.exports = {
  get
}
