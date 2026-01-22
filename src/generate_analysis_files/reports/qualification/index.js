const Promise = require('bluebird');
const { createBatchesForQualification, dropBatch, getBatches, findQualificationsByBatch } = require('./batch');
const { concatFiles } = require('../../csv/concat');
const { streamToCsv } = require('../../csv/stream');

const before = async (runDate) => {
  await createBatchesForQualification(runDate);
};

const after = async () => {
  dropBatch();
};

async function processBatch(batchNo, fileName) {
  await streamToCsv(fileName, findQualificationsByBatch(batchNo));
}

module.exports = async (runDate, reportDir) => {
  await before(runDate);

  const files = [];

  await Promise.map(
    getBatches(),
    (batch) => {
      console.log(`Processing Qualification Batch #${batch.BatchNo}`);

      const csvName = `${reportDir}/${runDate}_qualification_report_${batch.BatchNo.toString().padStart(2, '0')}.csv`;
      files.push(csvName);

      return processBatch(batch.BatchNo, csvName);
    },
    { concurrency: 10 },
  );

  await after();

  const filePath = `${reportDir}/${runDate}_qualification_report.csv`;
  await concatFiles(files, filePath);

  return filePath;
};


