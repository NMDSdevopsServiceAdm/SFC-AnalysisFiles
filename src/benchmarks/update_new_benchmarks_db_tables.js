const db = require('../generate_analysis_files/db');
const neatCsv = require('neat-csv');
const { reportToDbColumnMap } = require('./columnMap');

const setNullValues = (benchmark) =>
  Object.keys(benchmark).map((key) => (benchmark[key] = benchmark[key] === '' ? null : benchmark[key]));

const updateNewBenchmarksDbTables = async (reports) => {
  console.log('Begin update Benchmarks Db Tables');
  const transaction = await db.transaction();

  const payByEstablishmentId = await neatCsv(reports['Pay_by_establishmentid.csv']);
  const payByEstablishmentIdGoodOutstanding = await neatCsv(reports['Pay_by_establishmentid_Good_Outstanding.csv']);
  const payByLAAndService = await neatCsv(reports['Pay_by_LA_and_Service.csv']);
  const payByLAAndServiceGoodOutstanding = await neatCsv(reports['Pay_by_LA_and_Service_Good_Outstanding.csv']);

  const qualsByEstablishmentId = await neatCsv(reports['Qualifications_by_establishmentid.csv']);
  const qualsByEstablishmentIdGoodOutstanding = await neatCsv(
    reports['Qualifications_by_establishmentid_Good_Oustanding.csv'],
  );
  const qualsByLAAndService = await neatCsv(reports['Qualifications_by_LA_and_service.csv']);
  const qualsByLAAndServiceGoodOutstanding = await neatCsv(
    reports['Qualifications_by_LA_and_service_Good_Outstanding.csv'],
  );

  const sicknessByEstablishmentId = await neatCsv(reports['Sickness_by_establishmentid.csv']);
  const sicknessByEstablishmentIdGoodOutstanding = await neatCsv(
    reports['Sickness_by_establishmentid_Good_Outstanding.csv'],
  );
  const sicknessByLAAndService = await neatCsv(reports['Sickness_by_LA_and_service.csv']);
  const sicknessByLAAndServiceGoodOutstanding = await neatCsv(
    reports['Sickness_by_LA_and_service_Good_Outstanding.csv'],
  );

  const turnoverByEstablishmentId = await neatCsv(reports['Turnover_by_establishmentid.csv']);
  const turnoverByEstablishmentIdGoodOutstanding = await neatCsv(
    reports['Turnover_by_establishmentid_Good_Outstanding.csv'],
  );
  const turnoverByLAAndService = await neatCsv(reports['Turnover_by_LA_and_service.csv']);
  const turnoverByLAAndServiceGoodOutstanding = await neatCsv(
    reports['Turnover_by_LA_and_service_Good_Outstanding.csv'],
  );

  const vacanciesByEstablishmentId = await neatCsv(reports['Vacancies_by_establishmentid.csv']);
  const vacanciesByEstablishmentIdGoodOutstanding = await neatCsv(
    reports['Vacancies_by_establishmentid_Good_Outstanding.csv'],
  );
  const vacanciesByLAAndService = await neatCsv(reports['Vacancies_by_LA_and_service.csv']);
  const vacanciesByLAAndServiceGoodOutstanding = await neatCsv(
    reports['Vacancies_by_LA_and_service_Good_Outstanding.csv'],
  );

  const establishmentsAndWorkers = await neatCsv(reports['Establishments_and_workers.csv']);

  const filesArr = [
    payByEstablishmentId,
    payByEstablishmentIdGoodOutstanding,
    payByLAAndService,
    payByLAAndServiceGoodOutstanding,
    qualsByEstablishmentId,
    qualsByEstablishmentIdGoodOutstanding,
    qualsByLAAndService,
    qualsByLAAndServiceGoodOutstanding,
    sicknessByEstablishmentId,
    sicknessByEstablishmentIdGoodOutstanding,
    sicknessByLAAndService,
    sicknessByLAAndServiceGoodOutstanding,
    turnoverByEstablishmentId,
    turnoverByEstablishmentIdGoodOutstanding,
    turnoverByLAAndService,
    turnoverByLAAndServiceGoodOutstanding,
    vacanciesByEstablishmentId,
    vacanciesByEstablishmentIdGoodOutstanding,
    vacanciesByLAAndService,
    vacanciesByLAAndServiceGoodOutstanding,
    establishmentsAndWorkers,
  ];

  if (filesArr.some((file) => file.length == 0)) {
    throw new Error('One or more of the files has no rows');
  }

  await updateTable(transaction, payByEstablishmentId, 'BenchmarksPayByEstId', reportToDbColumnMap);
  await updateTable(
    transaction,
    payByEstablishmentIdGoodOutstanding,
    'BenchmarksPayByEstIdGoodOutstanding',
    reportToDbColumnMap,
  );
  await updateTable(transaction, payByLAAndService, 'BenchmarksPayByLAAndService', reportToDbColumnMap);
  await updateTable(
    transaction,
    payByLAAndServiceGoodOutstanding,
    'BenchmarksPayByLAAndServiceGoodOutstanding',
    reportToDbColumnMap,
  );

  await updateTable(transaction, qualsByEstablishmentId, 'BenchmarksQualificationsByEstId', reportToDbColumnMap);
  await updateTable(
    transaction,
    qualsByEstablishmentIdGoodOutstanding,
    'BenchmarksQualificationsByEstIdGoodOutstanding',
    reportToDbColumnMap,
  );
  await updateTable(transaction, qualsByLAAndService, 'BenchmarksQualificationsByLAAndService', reportToDbColumnMap);
  await updateTable(
    transaction,
    qualsByLAAndServiceGoodOutstanding,
    'BenchmarksQualificationsByLAAndServiceGoodOutstanding',
    reportToDbColumnMap,
  );

  await updateTable(transaction, sicknessByEstablishmentId, 'BenchmarksSicknessByEstId', reportToDbColumnMap);
  await updateTable(
    transaction,
    sicknessByEstablishmentIdGoodOutstanding,
    'BenchmarksSicknessByEstIdGoodOutstanding',
    reportToDbColumnMap,
  );
  await updateTable(transaction, sicknessByLAAndService, 'BenchmarksSicknessByLAAndService', reportToDbColumnMap);
  await updateTable(
    transaction,
    sicknessByLAAndServiceGoodOutstanding,
    'BenchmarksSicknessByLAAndServiceGoodOutstanding',
    reportToDbColumnMap,
  );

  await updateTable(transaction, turnoverByEstablishmentId, 'BenchmarksTurnoverByEstId', reportToDbColumnMap);
  await updateTable(
    transaction,
    turnoverByEstablishmentIdGoodOutstanding,
    'BenchmarksTurnoverByEstIdGoodOutstanding',
    reportToDbColumnMap,
  );
  await updateTable(transaction, turnoverByLAAndService, 'BenchmarksTurnoverByLAAndService', reportToDbColumnMap);
  await updateTable(
    transaction,
    turnoverByLAAndServiceGoodOutstanding,
    'BenchmarksTurnoverByLAAndServiceGoodOutstanding',
    reportToDbColumnMap,
  );

  await updateTable(transaction, vacanciesByEstablishmentId, 'BenchmarksVacanciesByEstId', reportToDbColumnMap);
  await updateTable(
    transaction,
    vacanciesByEstablishmentIdGoodOutstanding,
    'BenchmarksVacanciesByEstIdGoodOutstanding',
    reportToDbColumnMap,
  );
  await updateTable(transaction, vacanciesByLAAndService, 'BenchmarksVacanciesByLAAndService', reportToDbColumnMap);
  await updateTable(
    transaction,
    vacanciesByLAAndServiceGoodOutstanding,
    'BenchmarksVacanciesByLAAndServiceGoodOutstanding',
    reportToDbColumnMap,
  );

  await updateTable(transaction, establishmentsAndWorkers, 'BenchmarksEstablishmentAndWorkers', reportToDbColumnMap);

  await transaction('DataImports').withSchema('cqc').insert({
    Type: 'Benchmarks',
    Date: new Date(),
  });

  await transaction.commit();

  console.log('Completed Benchmarks tables update');
};

const updateTable = async (transaction, benchmarksFile, tableName, columnMap) => {
  await transaction(tableName).truncate();

  const headings = Object.keys(benchmarksFile[0]);
  await Promise.all(
    benchmarksFile.map(async (benchmark) => {
      setNullValues(benchmark);

      const dataRow = {};
      headings.map((heading) => (dataRow[columnMap.get(heading.trim())] = benchmark[heading]));

      await transaction(tableName).withSchema('cqc').insert(dataRow);
    }),
  );
};

module.exports = { updateNewBenchmarksDbTables };
