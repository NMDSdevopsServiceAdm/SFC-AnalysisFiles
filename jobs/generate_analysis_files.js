const exec = require('await-exec');
const fs = require('fs');

const dayjs = require('dayjs');
const relativeTime = require('dayjs/plugin/relativeTime');
const humanizeDuration = require('humanize-duration');
dayjs.extend(relativeTime);

const slack = require('../src/utils/slack/slack-logger');

const generateWorkplaceReport = require('../src/generate_analysis_files/reports/workplace');
const generateWorkersReport = require('../src/generate_analysis_files/reports/workers');
const generateLeaversReport = require('../src/generate_analysis_files/reports/leavers');
const { refreshViews } = require('../src/generate_analysis_files/reports/views');
const { uploadFile, uploadFileToDataEngineering } = require('../src/utils/s3');
const version = require('../package.json').version;
const config = require('../config');

const reportDir = '/tmp/generate_analysis_files/output';

const run = async () => {
  const startTime = dayjs();
  console.log(`Start: ${startTime.format('DD-MM-YYYY HH:mm:ss')}`);

  if (runInLocal()) { 
    console.log('Running the job locally. Will not upload the files or send slack messages.')
  }

  await setup();
  await refreshViews();

  const runDate = dayjs().format('DD-MM-YYYY');
  // const workplaceFilePath = await generateWorkplaceReport(runDate, reportDir);
  // const workerFilePath = await generateWorkersReport(runDate, reportDir);
  // const leaverFilePath = await generateLeaversReport(runDate, reportDir);

  if (runInLocal()) { 
    console.log(`Job finished. The files are generated at ${reportDir}.`)
    process.exit(0)
  }

  await zipAndUploadReports();

  // if (config.get('dataEngineering.uploadToDataEngineering')) {
  //   await uploadReportsToDataEngineering(workplaceFilePath, workerFilePath, leaverFilePath);
  // }

  logCompletionTimes(startTime);
  await sendSlackAnalysisFilesSuccessMessage();
};

const setup = async () => {
  console.log(`Refreshing ${reportDir} directory`);

  // await exec(`rm -rf ${reportDir}`);
  // await exec(`mkdir -p ${reportDir}`);
};

const zipAndUploadReports = async () => {
  // const now = dayjs();
  // const zipName = `${now.format('YYYY-MM-DD-HH-mm-ss')}_analysis_files.zip`;
  const zipName = '2025-03-26-14-38-04_analysis_files.zip'

  // await exec(`cd ${reportDir} && zip -r ${zipName} *.csv`);

  return uploadFile(zipName, fs.createReadStream(`${reportDir}/${zipName}`));
};

const uploadReportsToDataEngineering = async (workplaceFilePath, workerFilePath, leaverFilePath) => {
  await uploadFileToDataEngineering(getFileKey('workplace'), fs.createReadStream(workplaceFilePath));
  await uploadFileToDataEngineering(getFileKey('worker'), fs.createReadStream(workerFilePath));
  await uploadFileToDataEngineering(getFileKey('leaver'), fs.createReadStream(leaverFilePath));
};

const getFileKey = (fileType) => {
  const now = dayjs();
  return `domain=ASCWDS/dataset=${fileType}/version=${version}/year=${now.format('YYYY')}/month=${now.format('MM')}/day=${now.format('DD')}/import_date=${now.format('YYYYMMDD')}/${fileType}.csv`;
}

const logCompletionTimes = (startTime) => {
  const finishTime = dayjs();
  console.log(`Finish: ${finishTime.format('DD-MM-YYYY HH:mm:ss')}`);

  const duration = startTime.diff(finishTime);
  console.log(`Duration: ${humanizeDuration(duration)}`);
}

const sendSlackAnalysisFilesSuccessMessage = async () => {
  console.log(`${dayjs()}: The analysis files were successfully uploaded`) 
  await slack.info(`${config.get('db.name')} - Run analysis files`, 
  `${dayjs()}: The analysis files were successfully uploaded`, 
  'slack.analysisFileUrl');
}

const sendSlackAnalysisFilesErrorMessage = async (errorMessage) => {
  if (runInLocal()) { 
    return;
  }

  await slack.error(
    `${config.get('db.name')} - Run analysis files`,
    `There was an error uploading analysis files \n ${errorMessage}`,
    'slack.analysisFileUrl',
  );
}

const runInLocal = () => { 
  return process.argv.includes('--runLocal');
}

(async () => {
  run()
    .then(() => {
      process.exit(0);
    })
    .catch(async (err) => {
      await sendSlackAnalysisFilesErrorMessage(err)
      console.error(err);
      process.exit(1);
    });
})();
