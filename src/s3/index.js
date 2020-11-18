const AWS = require('aws-sdk');
const config = require('../../config');

const uploadFile = async (fileName, body) => {
  console.log('Uploading to S3');

  const s3 = new AWS.S3();
  const params = {
    Bucket: config.get('s3.bucket'),
    Key: fileName,
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
};
