const Promise = require('bluebird');
const { createBatches, dropBatch, getBatches, findWorkplacesByBatch } = require('./batch');
const { streamToCsv } = require('../csv');

const before = async (runDate) => {
  await createBatches(runDate);
};

const after = async () => {
  dropBatch();
};

async function processBatch(batchNo, fileName) {
  await streamToCsv(fileName, findWorkplacesByBatch(batchNo));
}

module.exports = async (runDate, folder) => {
  await before(runDate);

  await Promise.map(
    getBatches(),
    (batch) => {
      console.log(`Processing Workplace Batch #${batch.BatchNo}`);

      const csvName = `${folder}/workplaces_${batch.BatchNo}_${runDate}.csv`;

      return processBatch(batch.BatchNo, csvName);
    },
    { concurrency: 10 },
  );

  await after();
};
