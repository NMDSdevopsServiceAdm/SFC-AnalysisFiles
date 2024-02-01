const AWS = require('aws-sdk');
const config = require('../../../config');

const uploadFile = async (fileName, body) => {
  console.log('Uploading to S3');

  const s3 = new AWS.S3({
    accessKeyId: config.get('reports.accessKey'),
    secretAccessKey: config.get('reports.secretKey'),
  });
  const params = {
    Bucket: config.get('s3.bucket'),
    Key: `${config.get('environment')}/${fileName}`,
    Body: body,
  };

  return s3
    .upload(params, (err) => {
      if (err) {
        throw err;
      }
    })
    .promise();
};

const uploadFileToDataEngineering = async (fileKey, body) => {
  console.log('Uploading to data engineering S3');

  const s3 = new AWS.S3({
    accessKeyId: config.get('dataEngineering.accessKey'),
    secretAccessKey: config.get('dataEngineering.secretKey'),
  });

  const params = {
    Bucket: config.get('s3.dataEngineeringBucket'),
    Key: fileKey,
    Body: body,
  };

  return s3
    .upload(params, (err) => {
      if (err) {
        throw err;
      }
    })
    .promise();
};

module.exports = {
  uploadFile,
  uploadFileToDataEngineering,
};
