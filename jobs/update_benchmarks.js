const AWS = require('aws-sdk');
const dayjs = require('dayjs');
const exec = require('await-exec');
const config = require('../config');
const { getBenchmarksFiles } = require('../src/utils/s3/benchmarks');
// const { updateBenchmarksDbTables } = require('../src/benchmarks/update_benchmarks_db_tables');
const { updateNewBenchmarksDbTables } = require('../src/benchmarks/update_new_benchmarks_db_tables');

const slack = require('../src/utils/slack/slack-logger');

const reportDir = './benchmarkFiles';
const s3 = new AWS.S3();

const setup = async () => {
  console.log(`Refreshing ${reportDir} directory`);

  await exec(`rm -rf ${reportDir}`);
  await exec(`mkdir ${reportDir}`);
};

const downloadAllFilesFromS3 = async (benchmarksFiles) => {
  console.log('downloading from s3');
  const bucket = config.get('s3.benchmarksBucket');

  const reportsObj = {};
  await Promise.all(
    benchmarksFiles.map(async (file) => {
      const params = {
        Bucket: bucket,
        Key: file.Key,
      };

      const downloadedReport = await s3.getObject(params).promise();
      reportsObj[file.Key] = downloadedReport.Body.toString();
    }),
  );

  return reportsObj;
};

const updateBenchmarks = async () => {
  const benchmarksFiles = await getBenchmarksFiles();
  const benchmarksToBeUpdated = updatedSinceYesterday(benchmarksFiles);

  if (benchmarksToBeUpdated) {
    await setup();
    const reports = await downloadAllFilesFromS3(benchmarksFiles);
    // await updateBenchmarksDbTables(reports);
    await updateNewBenchmarksDbTables(reports);

    console.log(`${dayjs()}: Files successfully updated`);
    await sendSlackBenchmarksUpdatedMessage(`${dayjs()}: Benchmarks successfully updated`);
    return;
  } else {
    console.log(`${dayjs()}: Files not updated since ${dayjs().subtract(1, 'day')}`);
    await sendSlackBenchmarksUpdatedMessage(`${dayjs()}: Benchmarks not updated since ${dayjs().subtract(1, 'day')}`);
    return;
  }
};

const updatedSinceYesterday = (files) => {
  return files.some((file) => {
    return dayjs(file.LastModified) >= dayjs().subtract(1, 'day');
  });
};

const sendSlackBenchmarksUpdatedMessage = async (message) => {
  await slack.info(`${config.get('db.name')} - Update Benchmarks`, message, 'slack.benchmarksUrl');
};

const sendSlackBenchmarksErrorMessage = async (errorMessage) => {
  await slack.error(
    `${config.get('db.name')} - Update Benchmarks`,
    `There was an error updating Benchmarks \n ${errorMessage}`,
    'slack.benchmarksUrl',
  );
};

(async () => {
  updateBenchmarks()
    .then(() => {
      process.exit(0);
    })
    .catch(async (err) => {
      await sendSlackBenchmarksErrorMessage(err);
      console.error(err);
      process.exit(1);
    });
})();
