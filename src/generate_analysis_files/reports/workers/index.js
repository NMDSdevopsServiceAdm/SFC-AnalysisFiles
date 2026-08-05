const Promise = require('bluebird');
const { createBatches, dropBatch, getBatches, findWorkersByBatch } = require('./batch');
const { concatFiles } = require('../../csv/concat');
const { streamToCsv } = require('../../csv/stream');
const { ColumnNamesUtil } = require('../../../utils/sql/column-names');
const db = require('../../db');

const ColumnNames = new ColumnNamesUtil(db);

const before = async (runDate) => {
  await createBatches(runDate);

  await ColumnNames.reloadColumnNamesFromDb(db);
};

const after = async () => {
  dropBatch();
};

async function processBatch(batchNo, fileName) {
  await streamToCsv(fileName, findWorkersByBatch(batchNo, ColumnNames));
}

module.exports = async (runDate, reportDir) => {
  await before(runDate);

  const files = [];

  await Promise.map(
    getBatches(),
    (batch) => {
      console.log(`Processing Workers Batch #${batch.BatchNo}`);

      const csvName = `${reportDir}/${runDate}_workers_report_${batch.BatchNo.toString().padStart(2, '0')}.csv`;
      files.push(csvName);

      return processBatch(batch.BatchNo, csvName);
    },
    { concurrency: 10 },
  );

  await after();

  const filePath = `${reportDir}/${runDate}_workers_report.csv`;
  await concatFiles(files, filePath);

  return filePath;
};
