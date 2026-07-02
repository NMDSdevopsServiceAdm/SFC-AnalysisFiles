const Promise = require('bluebird');
const { createBatchesForTraining, dropBatch, getBatches, findTrainingsByBatch } = require('./batch');
const { concatFiles } = require('../../csv/concat');
const { streamToCsv } = require('../../csv/stream');
const {decodeTrainingNames}= require('../../../utils/sql/decode-url-encoded-text')

const before = async (runDate) => {
  await createBatchesForTraining(runDate);
};

const after = async () => {
  dropBatch();
};

async function processBatch(batchNo, fileName) {
    const stream = findTrainingsByBatch(batchNo);

  await streamToCsv(
    fileName,
    stream.pipe(decodeTrainingNames)
  );
}

module.exports = async (runDate, reportDir) => {
  await before(runDate);

  const files = [];

  await Promise.map(
    getBatches(),
    (batch) => {
      console.log(`Processing Training Batch #${batch.BatchNo}`);

      const csvName = `${reportDir}/${runDate}_training_report_${batch.BatchNo.toString().padStart(2, '0')}.csv`;
      files.push(csvName);

      return processBatch(batch.BatchNo, csvName);
    },
    { concurrency: 10 },
  );

  await after();

  const filePath = `${reportDir}/${runDate}_training_report.csv`;
  await concatFiles(files, filePath);

  return filePath;
};


