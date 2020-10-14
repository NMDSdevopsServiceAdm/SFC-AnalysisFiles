const exec = require('await-exec');
const dayjs = require('dayjs');
const fs = require('fs');
const generateWorkplaceReport = require('../reports/workplace');
const generateWorkersReport = require('../reports/workers');
const generateLeaversReport = require('../reports/leavers');
const { refreshViews } = require('../reports/views');
const { uploadFile } = require('../reports/s3');

(async () => {
  async function run() {
    const now = dayjs();
    const runDate = now.format('DD-MM-YYYY');

    const dest = './output';
    const zipName = `analysis_files_${now.format('YYYY_MM_DD_HH_mm_ss')}.zip`;

    await exec(`mkdir -p ${dest}`);

    console.log(dayjs().format('DD-MM-YYYY HH:mm:ss'));

    await refreshViews();

    await generateWorkplaceReport(runDate, dest);
    await generateWorkersReport(runDate, dest);
    await generateLeaversReport(runDate, dest);

    await exec(`cd ${dest} && zip -r ${zipName} *.csv`);

    await uploadFile('sfcreports', zipName, fs.readFileSync(`${dest}/${zipName}`));

    console.log(dayjs().format('DD-MM-YYYY HH:mm:ss'));
  }

  run()
    .then(() => {
      process.exit(0);
    })
    .catch((err) => {
      console.error(err);
    });
})();
