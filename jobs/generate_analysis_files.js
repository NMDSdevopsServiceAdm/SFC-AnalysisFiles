const exec = require('await-exec');
const fs = require('fs');

const dayjs = require('dayjs');
const relativeTime = require('dayjs/plugin/relativeTime');
const humanizeDuration = require('humanize-duration');
dayjs.extend(relativeTime);

const generateWorkplaceReport = require('../src/reports/workplace');
const generateWorkersReport = require('../src/reports/workers');
const generateLeaversReport = require('../src/reports/leavers');
const { refreshViews } = require('../src/reports/views');
const { uploadFile } = require('../src/reports/s3');

const reportDir = './reports';

const setup = async () => {
  console.log(`Refreshing ${reportDir} directory`);

  await exec(`rm -rf ${reportDir}`);
  await exec(`mkdir ${reportDir}`);
};

const zipAndUploadReports = async () => {
  const now = dayjs();
  const zipName = `${now.format('YYYY-MM-DD-HH-mm-ss')}_analysis_files.zip`;
  const bucketName = 'sfcreports';

  await exec(`cd ${reportDir} && zip -r ${zipName} *.csv`);

  return uploadFile(bucketName, zipName, fs.readFileSync(`${reportDir}/${zipName}`));
};

const run = async () => {
  const startTime = dayjs();
  console.log(`Start: ${startTime.format('DD-MM-YYYY HH:mm:ss')}`);

  await setup();
  await refreshViews();

  const runDate = dayjs().format('DD-MM-YYYY');
  await generateWorkplaceReport(runDate, reportDir);
  await generateWorkersReport(runDate, reportDir);
  await generateLeaversReport(runDate, reportDir);

  await zipAndUploadReports();

  const finishTime = dayjs();
  console.log(`Finish: ${finishTime.format('DD-MM-YYYY HH:mm:ss')}`);

  const duration = startTime.diff(finishTime);
  console.log(`Duration: ${humanizeDuration(duration)}`);
};

(async () => {
  run()
    .then(() => {
      process.exit(0);
    })
    .catch((err) => {
      console.error(err);
    });
})();
