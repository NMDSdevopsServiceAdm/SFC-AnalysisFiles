const Promise = require('bluebird');
const { createBatches, dropBatch, getBatches, findWorkplacesByBatch } = require('./batch');
const { concatFiles } = require('../../csv/concat');
const { streamToCsv } = require('../../csv/stream');

const before = async (runDate) => {
  await createBatches(runDate);
};

const after = async () => {
  dropBatch();
};

async function processBatch(batchNo, fileName) {
  await streamToCsv(fileName, findWorkplacesByBatch(batchNo));
}

module.exports = async (runDate, reportDir) => {
  await before(runDate);

  const files = [];

  await Promise.map(
    getBatches(),
    (batch) => {
      console.log(`Processing Workplace Batch #${batch.BatchNo}`);

      const csvName = `${reportDir}/${runDate}_workplaces_report_${batch.BatchNo.toString().padStart(2, '0')}.csv`;
      files.push(csvName);

      return processBatch(batch.BatchNo, csvName);
    },
    { concurrency: 10 },
  );

  await after();

  await concatFiles(files, `${reportDir}/${runDate}_workplaces_report.csv`);
};
