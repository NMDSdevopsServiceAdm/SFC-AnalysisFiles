const exec = require('await-exec');
const fs = require('fs');

const dayjs = require('dayjs');
const relativeTime = require('dayjs/plugin/relativeTime');
const humanizeDuration = require('humanize-duration');
dayjs.extend(relativeTime);

const generateWorkplaceReportForNHS = require('../src/nhs_bsa-report/generate_report');
const { uploadFile } = require('../src/utils/s3/nhs_bsa');
const version = require('../package.json').version;
const config = require('../config');

const reportDir = './output';

const run = async () => {
  const startTime = dayjs();
  console.log(`Start: ${startTime.format('DD-MM-YYYY HH:mm:ss')}`);

  await setup();

  const runDate = dayjs().format('DD-MM-YYYY');
  const workplaceFilePath = await generateWorkplaceReportForNHS(runDate, reportDir);

  await zipAndUploadReports();


  
  logCompletionTimes(startTime);
};

const setup = async () => {
  console.log(`Refreshing ${reportDir} directory`);

  await exec(`rm -rf ${reportDir}`);
  await exec(`mkdir ${reportDir}`);
};

const zipAndUploadReports = async () => {
  const now = dayjs();
  const zipName = `${now.format('YYYY-MM-DD-HH-mm-ss')}_nhs_bsa_files.zip`;

  await exec(`cd ${reportDir} && zip -r ${zipName} *.csv`);

  return uploadFile(zipName, fs.readFileSync(`${reportDir}/${zipName}`));
};



const logCompletionTimes = (startTime) => {
  const finishTime = dayjs();
  console.log(`Finish: ${finishTime.format('DD-MM-YYYY HH:mm:ss')}`);

  const duration = startTime.diff(finishTime);
  console.log(`Duration: ${humanizeDuration(duration)}`);
}

(async () => {
  run()
    .then(() => {
      process.exit(0);
    })
    .catch((err) => {
      console.error(err);
      process.exit(1);
    });
})();
