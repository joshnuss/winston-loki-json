const { createLogger, format } = require('winston')
const LokiTransport = require('../index')

describe('Integration tests', function () {
  it('Winston should accept LokiTransport', function () {
    jest.useFakeTimers()
    const lokiTransport = new LokiTransport({
      host: 'http://localhost',
      level: 'debug',
      interval: 10,
      labels: {
        module: 'name',
        app: 'appname'
      },
      format: format.combine(
        format.label({ label: 'name' }),
        format.printf(({ message, label }) => {
          return `[${label}] ${message}`
        })
      )
    })

    const options = {
      transports: [lokiTransport]
    }
    const logger = createLogger(options)

    const testMessage = 'testMessage'
    const testLabel = 'testLabel'
    logger.debug({ message: testMessage, labels: { customLabel: testLabel } })
    expect(lokiTransport.batcher.batch.streams.length).toBe(1)
    expect(
      lokiTransport.batcher.batch.streams[0]
    ).toEqual({
      labels: { level: 'debug', module: 'name', app: 'appname', customLabel: testLabel },
      entries: [{
        line: `[name] ${testMessage}`,
        ts: Date.now()
      }]
    })
  })
})
