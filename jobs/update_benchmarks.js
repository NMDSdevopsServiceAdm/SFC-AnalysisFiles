const AWS = require('aws-sdk');
const dayjs = require('dayjs');
const exec = require('await-exec');
const fs = require('fs');
const config = require('../config');
const { getBenchmarksFiles } = require('../src/s3/benchmarks');
const { updateBenchmarksDbTables } = require('../src/benchmarks/update_benchmarks_db_tables');
const slack = require('../../slack/slack-logger');

const reportDir = './benchmarkFiles';
const s3 = new AWS.S3();

const setup = async () => {
  console.log(`Refreshing ${reportDir} directory`);
  
  await exec(`rm -rf ${reportDir}`);
  await exec(`mkdir ${reportDir}`);
};

const downloadAllFilesFromS3 = async (benchmarksFiles) => {
  console.log('downloading from s3')
  const bucket = config.get('s3.benchmarksBucket');

  const reportsObj = {}
  await Promise.all(benchmarksFiles.map(async (file) => {
    const params = {
      Bucket: bucket,
      Key: file.Key
    };

    const downloadedReport = await s3.getObject(params).promise();
    reportsObj[file.Key] = downloadedReport.Body.toString()
  }));
  return reportsObj;
}

const updateBenchmarks = async () => {
  const benchmarksFiles = await getBenchmarksFiles();
  
  if (updatedSinceYesterday(benchmarksFiles)) {
    await setup();
    const reports = await downloadAllFilesFromS3(benchmarksFiles);
    await updateBenchmarksDbTables(reports);

    console.log(`${dayjs()}: Files successfully updated`);
    
    return;
  } else {
    console.log(`${dayjs()}: Files not updated since ${dayjs().subtract(1, 'day')}`)
    return;
  }
};

const updatedSinceYesterday = (files) => {
  return files.some(file => {
    return dayjs(file.LastModified) >= dayjs().subtract(1, 'day');
  });
}

const sendSlackBenchmarksUpdatedMessage = () => {
  slack.info();
} 
const sendSlackBenchmarksErrorMessage = () => {
  slack.error();
} 

// Reference function for slack notifications
// const run = async () => {
//   try {
//     feedbackMsg = ''
//     slack.info('Feedback', JSON.stringify(feedbackMsg, null, 2));
//   } catch (error) {
//     slack.error()
//   }
// }

(async () => {
  updateBenchmarks()
    .then(() => {
      sendSlackBenchmarksUpdatedMessage();
      process.exit(0);
    })
    .catch(err => {
      sendSlackBenchmarksErrorMessage();
      console.error(err);
      process.exit(1);
    });
})();
