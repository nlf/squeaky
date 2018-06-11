'use strict'

const crypto = require('crypto')
const debug = require('debug')

let pubDebugger = 0
let subDebugger = 0

exports.getTopic = () => crypto.randomBytes(16).toString('hex') + '#ephemeral'
exports.getPubDebugger = () => ({ debug: debug(`publisher${++pubDebugger}`) })
exports.getSubDebugger = () => ({ debug: debug(`subscriber${++subDebugger}`) })
