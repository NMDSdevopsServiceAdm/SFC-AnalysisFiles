const Promise = require('bluebird');
const { createBatches, dropBatch, getBatches, findWorkersByBatch } = require('./batch');
const { streamToCsv } = require('../csv');

const before = async (runDate) => {
  await createBatches(runDate);
};

const after = async () => {
  dropBatch();
};

async function processBatch(batchNo, fileName) {
  await streamToCsv(fileName, findWorkersByBatch(batchNo));
}

module.exports = async (runDate, folder) => {
  await before(runDate);

  await Promise.map(
    getBatches(),
    (batch) => {
      console.log(`Processing Workers Batch #${batch.BatchNo}`);

      const csvName = `${folder}/${runDate}_workers_report_${batch.BatchNo.toString().padStart(2, '0')}.csv`;

      return processBatch(batch.BatchNo, csvName);
    },
    { concurrency: 10 },
  );

  await after();
};
