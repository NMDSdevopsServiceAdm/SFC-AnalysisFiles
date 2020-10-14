const fs = require('fs');
const fastcsv = require('fast-csv');

async function streamToCsv(fileName, stream) {
  const csvStream = fastcsv.format({ headers: true, delimiter: '|' });
  const writeStream = fs.createWriteStream(fileName);

  csvStream.pipe(writeStream);

  for await (const row of stream) {
    csvStream.write(row);
  }
}

module.exports = {
  streamToCsv,
};
