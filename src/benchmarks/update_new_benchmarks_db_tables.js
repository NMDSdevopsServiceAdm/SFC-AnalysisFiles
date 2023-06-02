const db = require('../generate_analysis_files/db');
const neatCsv = require('neat-csv');
const { reportToDbColumnMap, fileNameToDbTableMap, fileNames } = require('./columnAndTableMaps');

const setNullValues = (benchmark) =>
  Object.keys(benchmark).map((key) => (benchmark[key] = benchmark[key] === '' ? null : benchmark[key]));

const updateNewBenchmarksDbTables = async (reports) => {
  console.log('Begin update Benchmarks Db Tables');
  const transaction = await db.transaction();

  const filesArr = await Promise.all(
    fileNames.map(async (fileName) => {
      const file = await neatCsv(reports[fileName]);
      return { fileName, file };
    }),
  );

  const files = filesArr.map((files) => {
    return files.file;
  });

  if (files.some((file) => file.length === 0)) {
    throw new Error('One or more of the files has no rows');
  }

  await Promise.all(
    filesArr.map(async (fileObj) => {
      const { file, fileName } = fileObj;
      await updateTable(transaction, file, fileNameToDbTableMap.get(fileName));
    }),
  );

  await transaction('DataImports').withSchema('cqc').insert({
    Type: 'Benchmarks',
    Date: new Date(),
  });

  await transaction.commit();

  console.log('Completed Benchmarks tables update');
};

const updateTable = async (transaction, benchmarksFile, tableName) => {
  await transaction(tableName).truncate();

  const headings = Object.keys(benchmarksFile[0]);
  await Promise.all(
    benchmarksFile.map(async (benchmark) => {
      setNullValues(benchmark);

      const dataRow = {};
      headings.map((heading) => (dataRow[reportToDbColumnMap.get(heading.trim())] = benchmark[heading]));

      await transaction(tableName).withSchema('cqc').insert(dataRow);
    }),
  );
};

module.exports = { updateNewBenchmarksDbTables };
