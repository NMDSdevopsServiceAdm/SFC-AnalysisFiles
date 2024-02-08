const AWS = require('aws-sdk');
const config = require('../../../config');

async function getBenchmarksFiles() {

  const s3 = new AWS.S3({
    accessKeyId: config.get('reports.accessKey'),
    secretAccessKey: config.get('reports.secretKey'),
  });
  const params = { Bucket: config.get('s3.benchmarksBucket') };



  const files = await s3.listObjects(params).promise();
  return files.Contents;
}

module.exports = { getBenchmarksFiles };
