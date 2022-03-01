const exec = require('await-exec');
const fs = require('fs');

const dayjs = require('dayjs');
const relativeTime = require('dayjs/plugin/relativeTime');
const humanizeDuration = require('humanize-duration');
dayjs.extend(relativeTime);

const generateWorkplaceReport = require('../src/generate_analysis_files/reports/workplace');
const generateWorkersReport = require('../src/generate_analysis_files/reports/workers');
const generateLeaversReport = require('../src/generate_analysis_files/reports/leavers');
const { refreshViews } = require('../src/generate_analysis_files/reports/views');
const { uploadFile, uploadFileToDataEngineering } = require('../src/utils/s3');
const version = require('../package.json').version;
const config = require('../config');

const reportDir = './output';

const setup = async () => {
  console.log(`Refreshing ${reportDir} directory`);

  await exec(`rm -rf ${reportDir}`);
  await exec(`mkdir ${reportDir}`);
};

const zipAndUploadReports = async () => {
  const now = dayjs();
  const zipName = `${now.format('YYYY-MM-DD-HH-mm-ss')}_analysis_files.zip`;

  await exec(`cd ${reportDir} && zip -r ${zipName} *.csv`);

  return uploadFile(zipName, fs.readFileSync(`${reportDir}/${zipName}`));
};

const run = async () => {
  const startTime = dayjs();
  console.log(`Start: ${startTime.format('DD-MM-YYYY HH:mm:ss')}`);

  await setup();
  await refreshViews();

  const runDate = dayjs().format('DD-MM-YYYY');
  const workplaceFilePath = await generateWorkplaceReport(runDate, reportDir);
  const workerFilePath = await generateWorkersReport(runDate, reportDir);
  const leaverFilePath = await generateLeaversReport(runDate, reportDir);

  await zipAndUploadReports();

  if (inProductionEnvironment()) {
    await uploadReportsToDataEngineering(workplaceFilePath, workerFilePath, leaverFilePath);
  }
  
  logCompletionTimes(startTime);
};

const uploadReportsToDataEngineering = async (workplaceFilePath, workerFilePath, leaverFilePath) => {
  await uploadFileToDataEngineering(getFileKey('workplace'), fs.readFileSync(workplaceFilePath));
  await uploadFileToDataEngineering(getFileKey('worker'), fs.readFileSync(workerFilePath));
  await uploadFileToDataEngineering(getFileKey('leaver'), fs.readFileSync(leaverFilePath));
};

const getFileKey = (fileType) => {
  const now = dayjs();
  return `domain=ASCWDS/dataset=${fileType}/version=${version}/year=${now.format('YYYY')}/month=${now.format('MM')}/day=${now.format('DD')}/import_date=${now.format('YYYYMMDD')}/${fileType}.csv`;
}

const inProductionEnvironment = () => config.get('environment') === 'production';

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
