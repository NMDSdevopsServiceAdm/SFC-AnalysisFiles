const fs = require('fs');
const fastcsv = require('fast-csv');
const pipeline = require('util').promisify(require('stream').pipeline);

async function streamToCsv(fileName, stream) {
  const csvStream = fastcsv.format({ headers: true, delimiter: '|' });
  const writeStream = fs.createWriteStream(fileName);

  await pipeline(stream, csvStream, writeStream);
}

module.exports = {
  streamToCsv,
};
