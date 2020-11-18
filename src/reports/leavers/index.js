const Promise = require('bluebird');
const { createBatches, dropBatch, getBatches, findLeaversByBatch } = require('./batch');
const { streamToCsv } = require('../../csv/stream');

const before = async (runDate) => {
  await createBatches(runDate);
};

const after = async () => {
  dropBatch();
};

async function processBatch(batchNo, fileName) {
  await streamToCsv(fileName, findLeaversByBatch(batchNo));
}

module.exports = async (runDate, reportDir) => {
  await before(runDate);

  await Promise.map(
    getBatches(),
    (batch) => {
      console.log(`Processing Leavers Batch #${batch.BatchNo}`);

      const csvName = `${reportDir}/${runDate}_leavers_report_${batch.BatchNo.toString().padStart(2, '0')}.csv`;

      return processBatch(batch.BatchNo, csvName);
    },
    { concurrency: 10 },
  );

  await after();
};
