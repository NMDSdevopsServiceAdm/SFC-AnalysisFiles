const fs = require('fs');
const fastcsv = require('fast-csv');

async function streamToCsv(fileName, stream) {
  const csvStream = fastcsv.format({ headers: true, delimiter: '|' });
  const writeStream = fs.createWriteStream(fileName);

  csvStream.pipe(writeStream);

  stream.on('data', function (row) {
    csvStream.write(row);
  });

  await new Promise(function (resolve, reject) {
    stream.on('finish', function () {
      resolve();
    });

    stream.on('error', function () {
      reject();
    });
  });
}

module.exports = {
  streamToCsv,
};
