const fs = require('fs');
const fastcsv = require('fast-csv');
const Promise = require('bluebird');
const pipeline = require('util').promisify(require('stream').pipeline);

async function concatFiles(files, into) {
  await Promise.each(files, async (file) => {
    const csvWriteStream = fastcsv.format({
      headers: true,
      writeHeaders: fs.existsSync(into) ? false : true,
      delimiter: '|',
      includeEndRowDelimiter: true,
    });

    const readStream = fs.createReadStream(file);
    const writeStream = fs.createWriteStream(into, { flags: 'a' });

    let csvStream = fastcsv.parse({
      delimiter: '|',
      objectMode: true,
      headers: true,
    });

    await pipeline(readStream, csvStream, csvWriteStream, writeStream);
    fs.unlinkSync(file);
  });
}

module.exports = {
  concatFiles,
};
