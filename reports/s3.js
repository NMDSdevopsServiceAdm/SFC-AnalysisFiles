const AWS = require('aws-sdk');

const s3 = new AWS.S3({
  accessKeyId: process.env.AWS_ACCESS_KEY_ID,
  secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
});

const uploadFile = async (bucket, fileName, body) => {
  const params = {
    Bucket: bucket,
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
