'use strict'

var Promise = require('bluebird')
require('bluebird-co')

var ReGrid = require('rethinkdb-regrid')
var fs = Promise.promisifyAll(require('fs'))
var path = require('path')
var co = require('co')
var _ = require('lodash')
var r = require('rethinkdbdash')({db: 'test', silent: true})
var moment = require('moment')
var colors = require('colors')
var numeral = require('numeral')
var meter = require('stream-meter')

var taskQueue = []
var bucket = ReGrid({db: 'test'})

var maxQ = 10

var errCatch = function (err) {
  console.log('ERR ==========================')
  console.log(err.stack)
}

var delay = function (ms) {
  var deferred = Promise.pending()
  setTimeout(function () {
    deferred.resolve()
  }, ms)
  return deferred.promise
}

var taskMessage = function (msg, duration, size) {
  return `[${moment().format('LTS')}] ${colors.green(msg)} - ${moment.duration(duration).asSeconds()} Seconds - ${numeral(size).format('0.0b')}`
}

var randomItem = function (list) {
  return list[Math.floor(Math.random() * list.length)]
}

var streamPromise = function (stream) {
  return new Promise(function (resolve, reject) {
    stream.on('end', resolve)
    stream.on('finish', resolve)
    stream.on('error', reject)
  })
}

var taskUpload = Promise.coroutine(function *(filename) {
  var startTime = Date.now()
  var filepath = path.join(__dirname, 'input_files', filename)
  var writeStream = bucket.upload(filename)
  var promise = streamPromise(writeStream)
  var m = meter()
  fs.createReadStream(filepath).pipe(m).pipe(writeStream)
  yield promise
  var duration = Date.now() - startTime
  return taskMessage(`bucket.upload('${filename}')`, duration, m.bytes)
})

var taskDownloadFilename = Promise.coroutine(function *(filename) {
  var startTime = Date.now()
  var foundFile = true
  var savePath
  var tryCount = 1
  while (foundFile) {
    savePath = path.join(__dirname, 'output_files', `${path.basename(filename, path.extname(filename))}${tryCount}${path.extname(filename)}`)
    try {
      yield fs.statAsync(savePath)
      tryCount += 1
    } catch (err) {
      foundFile = false
    }
  }
  var readStream = bucket.downloadFilename(filename)
  var writeStream = fs.createWriteStream(savePath)
  var promise = streamPromise(writeStream)
  var m = meter()
  readStream.pipe(m).pipe(writeStream)
  yield promise
  var duration = Date.now() - startTime
  return taskMessage(`bucket.downloadFilename('${filename}')`, duration, m.bytes)
})

var onInit = Promise.coroutine(function *() {
  console.time('Tables dropped')
  var tables = yield r.tableList()
  var queries = tables.map(function (table) {
    return r.tableDrop(table).run()
  })
  yield Promise.all(queries)
  console.timeEnd('Tables dropped')

  console.time('Bucket initialized')
  yield bucket.initBucket()
  console.timeEnd('Bucket initialized')
})

onInit().then(function () {
  // Queue Processor
  co(function *() {
    while (true) {
      process.stdout.write(`  Task Queue Size: ${taskQueue.length}              \r`)
      if (taskQueue.length > 0) {
        let completed = taskQueue.filter((promise) => { return promise.isFulfilled() })
        Promise.each(completed, function (result) {
          console.log(result)
        })
        taskQueue = taskQueue.filter((promise) => { return promise.isPending() })
      }
      yield delay(100)
    }
  }).catch(errCatch)

  // Write task creator
  co(function *() {
    var testFiles = yield fs.readdirAsync(path.join(__dirname, 'input_files'))
    testFiles = testFiles.filter((testFile) => { return !testFile.match(/^\./) })
    while (true) {
      let testFile = randomItem(testFiles)
      if (taskQueue.length < maxQ) {
        taskQueue.push(taskUpload(testFile))
      }
      yield delay(_.random(500, 2500))
    }
  }).catch(errCatch)

  // Read task creator
  co(function *() {
    while (true) {
      let fileList = yield bucket.listRegex('.').toArray()
      if (fileList.length > 0) {
        let testFile = randomItem(fileList)
        if (taskQueue.length < maxQ) {
          let task = taskDownloadFilename(testFile.filename)
          taskQueue.push(task)
        }
      }
      yield delay(_.random(500, 2500))
    }
  }).catch(errCatch)
}).catch(errCatch)
