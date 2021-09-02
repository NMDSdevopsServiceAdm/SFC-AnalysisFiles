const AWS = require('aws-sdk');
const dayjs = require('dayjs');
const exec = require('await-exec');
const fs = require('fs');
const config = require('../config');
const { getBenchmarksFiles } = require('../src/s3/benchmarks');
const { updateBenchmarksDbTables } = require('../src/benchmarks/update_benchmarks_db_tables');

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

  await Promise.all(benchmarksFiles.map(async (file) => {
    const params = {
      Bucket: bucket,
      Key: file.Key
    };

    const downloadedReport = await s3.getObject(params).promise();
    const path = `${reportDir}/${file.Key}`;

    fs.writeFileSync(path, downloadedReport.Body.toString());
  }));
}

const teardown = async () => {
  console.log('Removing directory');
  await exec(`rm -rf ${reportDir}`);
}

const run = async () => {
  const benchmarksFiles = await getBenchmarksFiles();
  
  if (updatedSinceYesterday(benchmarksFiles)) {
    await setup();
    await downloadAllFilesFromS3(benchmarksFiles);
    // await updateBenchmarksDbTables();
    console.log(`${dayjs()}: Files successfully updated`);
    await teardown();
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

(async () => {
  run()
    .then(() => process.exit(0))
    .catch(err => {
      console.error(err);
      process.exit(1);
    });
})();
