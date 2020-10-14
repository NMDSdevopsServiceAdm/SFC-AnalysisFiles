const exec = require('await-exec');
const dayjs = require('dayjs');
const fs = require('fs');
const generateWorkplaceReport = require('../src/reports/workplace');
const generateWorkersReport = require('../src/reports/workers');
const generateLeaversReport = require('../src/reports/leavers');
const { refreshViews } = require('../src/reports/views');
const { uploadFile } = require('../src/reports/s3');

const finish = async (dest) => {
  const now = dayjs();
  const zipName = `analysis_files_${now.format('YYYY_MM_DD_HH_mm_ss')}.zip`;

  await exec(`cd ${dest} && zip -r ${zipName} *.csv`);

  await uploadFile('sfcreports', zipName, fs.readFileSync(`${dest}/${zipName}`));

  console.log(`Cleaning up ${dest}`);
  await exec(`rm -rf ${dest}`);
}

const run = async () => {
  console.log(dayjs().format('DD-MM-YYYY HH:mm:ss'));

  const dest = './output';
  await exec(`mkdir -p ${dest}`);

  await refreshViews();

  const runDate = dayjs().format('DD-MM-YYYY');
  
  await generateWorkplaceReport(runDate, dest);
  await generateWorkersReport(runDate, dest);
  await generateLeaversReport(runDate, dest);

  await finish();

  console.log(dayjs().format('DD-MM-YYYY HH:mm:ss'));
}

(async () => {
  run()
    .then(() => {
      process.exit(0);
    })
    .catch((err) => {
      console.error(err);
    });
})();
