/**
 * The application entry point
 */

global.Promise = require('bluebird')
const config = require('config')
const logger = require('./common/logger')
const Kafka = require('no-kafka')
const co = require('co')
const ProcessorService = require('./services/ProcessorService')
const healthcheck = require('topcoder-healthcheck-dropin')
const _ = require('lodash')
const tracer = require('./common/tracer')

// Initialize tracing if configured.
// Even if tracer is not initialized, calls to tracer module will not raise any errors
if (config.has('tracing')) {
  tracer.initTracing(config.get('tracing'))
}

// create consumer
const options = { connectionString: config.KAFKA_URL }
if (config.KAFKA_CLIENT_CERT && config.KAFKA_CLIENT_CERT_KEY) {
  options.ssl = { cert: config.KAFKA_CLIENT_CERT, key: config.KAFKA_CLIENT_CERT_KEY }
}
const consumer = new Kafka.SimpleConsumer(options)

// data handler
const dataHandler = (messageSet, topic, partition) => Promise.each(messageSet, (m) => {
  const span = tracer.startSpans('dataHandler')
  span.setTag('kafka.topic', topic)
  span.setTag('message_bus.destination', topic)
  span.setTag('kafka.partition', partition)
  span.setTag('kafka.offset', m.offset)

  const message = m.message.value.toString('utf8')
  logger.info(`Handle Kafka event message; Topic: ${topic}; Partition: ${partition}; Offset: ${
    m.offset}; Message: ${message}.`)

  const parserSpan = tracer.startChildSpans('parseMessage', span)
  let messageJSON

  try {
    messageJSON = JSON.parse(message)
  } catch (e) {
    logger.error('Invalid message JSON.')
    logger.error(e)

    parserSpan.setTag('error', true)
    parserSpan.log({
      event: 'error',
      message: e.message,
      stack: e.stack,
      'error.object': e
    })
    parserSpan.finish()
    span.finish()

    return
  }

  parserSpan.finish()

  if (messageJSON.topic !== topic) {
    logger.error(`The message topic ${messageJSON.topic} doesn't match the Kafka topic ${topic}.`)

    span.setTag('error', true)
    span.log({
      event: 'error',
      message: `The message topic ${messageJSON.topic} doesn't match the Kafka topic ${topic}`
    })
    span.finish()

    return
  }

  // Only process messages with scanned status
  if (messageJSON.topic === config.AVSCAN_TOPIC && messageJSON.payload.status !== 'scanned') {
    logger.debug(`Ignoring message in topic ${messageJSON.topic} with status ${messageJSON.payload.status}`)
    // Not an error scenario so just log and finish
    span.log({
      message: `Ignoring message in topic ${messageJSON.topic} with status ${messageJSON.payload.status}`
    })
    span.finish()
    return
  }

  if (topic === config.SUBMISSION_CREATE_TOPIC && messageJSON.payload.fileType === 'url') {
    logger.debug(`Ignoring message in topic ${messageJSON.topic} with file type as url`)
    // Not an error scenario so just log and finish
    span.log({
      message: `Ignoring message in topic ${messageJSON.topic} with file type as url`
    })
    span.finish()
    return
  }

  let serviceSpan

  return co(function * () {
    switch (topic) {
      case config.SUBMISSION_CREATE_TOPIC:
        serviceSpan = tracer.startChildSpans('ProcessorService.processCreate', span)
        yield ProcessorService.processCreate(messageJSON, serviceSpan)
        break
      case config.AVSCAN_TOPIC:
        serviceSpan = tracer.startChildSpans('ProcessorService.processScan', span)
        yield ProcessorService.processScan(messageJSON, serviceSpan)
        break
      default:
        throw new Error(`Invalid topic: ${topic}`)
    }
  })
    // commit offset
    .then(() => {
      consumer.commitOffset({ topic, partition, offset: m.offset })
      serviceSpan.finish()
      span.finish()
    })
    .catch((err) => {
      logger.error(err)

      if (serviceSpan) {
        serviceSpan.log({
          event: 'error',
          message: err.message,
          stack: err.stack,
          'error.object': err
        })
        serviceSpan.setTag('error', true)
        serviceSpan.finish()
      }

      span.setTag('error', true)
      span.finish()
    })
})

// check if there is kafka connection alive
function check () {
  if (!consumer.client.initialBrokers && !consumer.client.initialBrokers.length) {
    return false
  }
  let connected = true
  consumer.client.initialBrokers.forEach(conn => {
    logger.debug(`url ${conn.server()} - connected=${conn.connected}`)
    connected = conn.connected & connected
  })
  return connected
}

consumer
  .init()
  // consume configured topic
  .then(() => {
    healthcheck.init([check])
    const topics = [config.SUBMISSION_CREATE_TOPIC, config.AVSCAN_TOPIC]
    _.each(topics, (tp) => consumer.subscribe(tp, { time: Kafka.LATEST_OFFSET }, dataHandler))
  })
  .catch((err) => logger.error(err))
