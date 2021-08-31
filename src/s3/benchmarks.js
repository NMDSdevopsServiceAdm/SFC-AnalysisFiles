const AWS = require('aws-sdk');
AWS.config.update({ region: 'eu-west-2' });

const s3 = new AWS.S3();

s3.listBuckets(function(err, data) {
  if (err) {
    console.log("Error", err);
  } else {
    console.log('Success', data.Buckets);
  }
});

s3.listObjects({ Bucket: 'sfc-benchmark-upload' }, function(err, data) {
  if (err) {
    console.log("Error", err);
  } else {
    console.log("Success", data);
  }
});
