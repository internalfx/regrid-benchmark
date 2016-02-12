'use strict'

// var Promise = require('bluebird')
require('bluebird-co')

var ReGrid = require('rethinkdb-regrid')
var bucket = ReGrid({db: 'test'})

var stream = bucket.listRegex('^0-p.', {sort: 'DESC', limit: 1, showAll: false})
// var stream = bucket.listRegex('.', {sort: 'ASC'})

stream.on('data', function (file) {
  console.log(file)
})

stream.on('end', function () {
  console.log('Done!')
})
