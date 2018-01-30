'use strict'

module.exports = (iterable, method) => {
  let result = Promise.resolve()

  for (const item of iterable) {
    result = result.then(() => {
      return method(item)
    })
  }

  return result
}
