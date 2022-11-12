const url = require('url')
const exitHook = require('async-exit-hook')

const protoHelpers = require('./proto/helpers')
const req = require('./requests')

/**
 * A batching transport layer for Grafana Loki
 *
 * @class Batcher
 */
class Batcher {
  /**
   * Creates an instance of Batcher.
   * Starts the batching loop if enabled.
   * @param {*} options
   * @memberof Batcher
   */
  constructor (options) {
    // Load given options to the object
    this.options = options

    // Construct Grafana Loki push API url
    this.url = new url.URL(this.options.host + '/loki/api/v1/push')

    // Parse basic auth parameters if given
    if (options.basicAuth) {
      const btoa = require('btoa')
      const basicAuth = 'Basic ' + btoa(options.basicAuth)
      this.options.headers = Object.assign(this.options.headers, { Authorization: basicAuth })
    }

    // Define the batching intervals
    this.interval = this.options.interval
      ? Number(this.options.interval) * 1000
      : 5000
    this.circuitBreakerInterval = 60000

    // Initialize the log batch
    this.batch = {
      streams: []
    }

    // Define the content type headers for the POST request based on the data type
    this.contentType = 'application/json'

    // If batching is enabled, run the loop
    this.options.batching && this.run()

    if (this.options.gracefulShutdown) {
      exitHook(callback => {
        this.close(() => callback())
      })
    }
  }

  /**
   * Returns a promise that resolves after the given duration.
   *
   * @param {*} duration
   * @returns {Promise}
   */
  wait (duration) {
    return new Promise(resolve => {
      setTimeout(resolve, duration)
    })
  }

  /**
   * Pushes logs into the batch.
   * If logEntry is given, pushes it straight to this.sendBatchToLoki()
   *
   * @param {*} logEntry
   */
  async pushLogEntry (logEntry) {
    const noTimestamp =
      logEntry && logEntry.entries && logEntry.entries[0].ts === undefined
    // If user has decided to replace the given timestamps with a generated one, generate it
    if (this.options.replaceTimestamp || noTimestamp) {
      logEntry.entries[0].ts = Date.now()
    }

    // If batching is not enabled, push the log immediately to Loki API
    if (this.options.batching !== undefined && !this.options.batching) {
      await this.sendBatchToLoki(logEntry)
    } else {
      const { streams } = this.batch

      // Find if there's already a log with identical labels in the batch
      const match = streams.findIndex(
        stream => JSON.stringify(stream.labels) === JSON.stringify(logEntry.labels)
      )

      if (match > -1) {
        // If there's a match, push the log under the same label
        logEntry.entries.forEach(entry => {
          streams[match].entries.push(entry)
        })
      } else {
        // Otherwise, create a new label under streams
        streams.push(logEntry)
      }
    }
  }

  /**
   * Clears the batch.
   */
  clearBatch () {
    this.batch.streams = []
  }

  /**
   * Sends a batch to Grafana Loki push endpoint.
   * If a single logEntry is given, creates a batch first around it.
   *
   * @param {*} logEntry
   * @returns {Promise}
   */
  sendBatchToLoki (logEntry) {
    return new Promise((resolve, reject) => {
      // If the batch is empty, do nothing
      if (this.batch.streams.length === 0 && !logEntry) {
        resolve()
      } else {
        // If the data format is JSON, there's no need to construct a buffer
        let preparedJSONBatch
        if (logEntry !== undefined) {
          // If a single logEntry is given, wrap it according to the batch format
          preparedJSONBatch = protoHelpers.prepareJSONBatch({ streams: [logEntry] })
        } else {
          // Stringify the JSON ready for transport
          preparedJSONBatch = protoHelpers.prepareJSONBatch(this.batch)
        }
        const reqBody = JSON.stringify(preparedJSONBatch)

        // Send the data to Grafana Loki
        req.post(this.url, this.contentType, this.options.headers, reqBody, this.options.timeout)
          .then(() => {
            // No need to clear the batch if batching is disabled
            logEntry === undefined && this.clearBatch()
            resolve()
          })
          .catch(err => {
            // Clear the batch on error if enabled
            this.options.clearOnError && this.clearBatch()

            this.options.onConnectionError !== undefined && this.options.onConnectionError(err)

            reject(err)
          })
      }
    })
  }

  /**
   * Runs the batch push loop.
   *
   * Sends the batch to Loki and waits for
   * the amount of this.interval between requests.
   */
  async run () {
    this.runLoop = true
    while (this.runLoop) {
      try {
        await this.sendBatchToLoki()
        if (this.interval === this.circuitBreakerInterval) {
          if (this.options.interval !== undefined) {
            this.interval = Number(this.options.interval) * 1000
          } else {
            this.interval = 5000
          }
        }
      } catch (e) {
        this.interval = this.circuitBreakerInterval
      }
      await this.wait(this.interval)
    }
  }

  /**
   * Stops the batch push loop
   *
   * @param {() => void} [callback]
   */
  close (callback) {
    this.runLoop = false
    this.sendBatchToLoki()
      .then(() => { if (callback) { callback() } }) // maybe should emit something here
      .catch(() => { if (callback) { callback() } }) // maybe should emit something here
  }
}

module.exports = Batcher
