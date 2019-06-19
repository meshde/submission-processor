/**
 * Contains generic helper methods
 */

const _ = require('lodash')
const axios = require('axios')
const bluebird = require('bluebird')
const config = require('config')
const logger = require('./logger')
const tracer = require('./tracer')
const AWS = require('aws-sdk')
const AmazonS3URI = require('amazon-s3-uri')

AWS.config.region = config.get('aws.REGION')
const s3 = new AWS.S3()
const s3p = bluebird.promisifyAll(s3)
const m2mAuth = require('tc-core-library-js').auth.m2m
const m2m = m2mAuth(_.pick(config, ['AUTH0_URL', 'AUTH0_AUDIENCE', 'TOKEN_CACHE_TIME', 'AUTH0_PROXY_SERVER_URL']))

// Variable to cache reviewTypes from Submission API
const reviewTypes = {}

/* Function to get M2M token
 * @param {Object} parentSpan Parent span object
 * @returns {Promise}
 */
function * getM2Mtoken (parentSpan) {
  const span = tracer.startChildSpans('getM2Mtoken', parentSpan)
  const token = yield m2m.getMachineToken(config.AUTH0_CLIENT_ID, config.AUTH0_CLIENT_SECRET)
  span.finish()
  return token
}

/**
 * Function to send request to Submission API
 * @param {String} reqType Type of the request POST / PATCH
 * @param {String} path Complete path of the Submission API URL
 * @param {Object} reqBody Body of the request
 * @param {Object} parentSpan Parent span object
 * @returns {Promise}
 */
function * reqToSubmissionAPI (reqType, path, reqBody, parentSpan) {
  const span = tracer.startChildSpans('reqToSubmissionAPI', parentSpan)
  // Set tags according to conventions at https://github.com/opentracing/specification/blob/master/semantic_conventions.md#span-tags-table
  span.setTag('http.method', reqType)
  span.setTag('http.url', path)

  // Token necessary to send request to Submission API
  const token = yield getM2Mtoken(span)
  if (reqType === 'POST') {
    yield axios.post(path, reqBody, { headers: { 'Authorization': `Bearer ${token}` } })
  } else if (reqType === 'PATCH') {
    yield axios.patch(path, reqBody, { headers: { 'Authorization': `Bearer ${token}` } })
  } else if (reqType === 'GET') {
    const response = yield axios.get(path, { headers: { 'Authorization': `Bearer ${token}` } })
    span.finish()
    return response
  }
  span.finish()
}

/*
 * Function to get reviewTypeId from Name
 * @param {String} reviewTypeName Name of the reviewType
 * @param {Object} parentSpan Parent span object
 * @returns {String} reviewTypeId
 */
function * getreviewTypeId (reviewTypeName, parentSpan) {
  const span = tracer.startChildSpans('getreviewTypeId', parentSpan)
  span.setTag('reviewTypeName', reviewTypeName)

  if (reviewTypes[reviewTypeName]) {
    span.finish()
    return reviewTypes[reviewTypeName]
  } else {
    const response = yield reqToSubmissionAPI('GET',
      `${config.SUBMISSION_API_URL}/reviewTypes?name=${reviewTypeName}`, {}, span)
    if (response.data.length !== 0) {
      reviewTypes[reviewTypeName] = response.data[0].id
      span.finish()
      return reviewTypes[reviewTypeName]
    }
    span.finish()
    return null
  }
}

/**
 * Function to download file from given URL
 * @param {String} fileURL URL of the file to be downloaded
 * @param {Object} parentSpan Parent span object
 * @returns {Buffer} Buffer of downloaded file
 */
function * downloadFile (fileURL, parentSpan) {
  const span = tracer.startChildSpans('downloadFile', parentSpan)
  span.setTag('http.url', fileURL)

  let downloadedFile
  if (/.*amazonaws.*/.test(fileURL)) {
    const { bucket, key } = AmazonS3URI(fileURL)
    logger.info(`downloadFile(): file is on S3 ${bucket} / ${key}`)
    downloadedFile = yield s3p.getObjectAsync({ Bucket: bucket, Key: key })
    span.finish()
    return downloadedFile.Body
  } else {
    logger.info(`downloadFile(): file is (hopefully) a public URL at ${fileURL}`)
    downloadedFile = yield axios.get(fileURL, { responseType: 'arraybuffer' })
    span.finish()
    return downloadedFile.data
  }
}

/**
 * Move file from one AWS S3 bucket to another bucket.
 * @param {String} sourceBucket the source bucket
 * @param {String} sourceKey the source key
 * @param {String} targetBucket the target bucket
 * @param {String} targetKey the target key
 * @param {Object} parentSpan Parent span object
 */
function * moveFile (sourceBucket, sourceKey, targetBucket, targetKey, parentSpan) {
  const span = tracer.startChildSpans('moveFile', parentSpan)
  span.setTag('sourceBucket', sourceBucket)
  span.setTag('sourceKey', sourceKey)
  span.setTag('targetBucket', targetBucket)
  span.setTag('targetKey', targetKey)

  yield s3p.copyObjectAsync({ Bucket: targetBucket, CopySource: `/${sourceBucket}/${sourceKey}`, Key: targetKey })
  yield s3p.deleteObjectAsync({ Bucket: sourceBucket, Key: sourceKey })

  span.finish()
}

module.exports = {
  reqToSubmissionAPI,
  getreviewTypeId,
  downloadFile,
  moveFile
}
