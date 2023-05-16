const db = require('../generate_analysis_files/db');
const neatCsv = require('neat-csv');

const setNullValues = (benchmark) =>
  Object.keys(benchmark).map((key) => (benchmark[key] = benchmark[key] === '' ? null : benchmark[key]));

const updateNewBenchmarksDbTables = async (reports) => {
  console.log('Begin update Benchmarks Db Tables');
  const transaction = await db.transaction();

  const payByEstablishmentId = await neatCsv(reports['Pay_by_establishmentid.csv']);
  const payByEstablishmentIdGoodOutstanding = await neatCsv(reports['Pay_by_establishmentid_Good_Outstanding.csv']);

  const filesArr = [payByEstablishmentId, payByEstablishmentIdGoodOutstanding];

  if (filesArr.some((file) => file.length == 0)) {
    throw new Error('One or more of the files has no rows');
  }

  await transaction('PayByEstablishmentId').truncate();

  const PayByEstablishmentIdHeadings = Object.keys(payByEstablishmentId[0]);

  await Promise.all(
    payByEstablishmentId.map(async (benchmark) => {
      setNullValues(benchmark);

      await transaction('PayByEstablishmentId').withSchema('cqc').insert({
        EstablishmentFK: benchmark[PayByEstablishmentIdHeadings[0]],
        MainJobRole: benchmark[PayByEstablishmentIdHeadings[1]],
        LocalAuthorityArea: benchmark[PayByEstablishmentIdHeadings[2]],
        MainServiceFK: benchmark[PayByEstablishmentIdHeadings[3]],
        BaseEstablishments: benchmark[PayByEstablishmentIdHeadings[4]],
        BaseWorkers: benchmark[PayByEstablishmentIdHeadings[5]],
        AverageHourlyRate: benchmark[PayByEstablishmentIdHeadings[6]],
        AverageAnnualFTE: benchmark[PayByEstablishmentIdHeadings[7]],
      });
    }),
  );

  await transaction('PayByEstablishmentIdGoodOutstanding').truncate();

  const PayByEstablishmentIdGoodOutstandingHeadings = Object.keys(payByEstablishmentIdGoodOutstanding[0]);

  await Promise.all(
    payByEstablishmentIdGoodOutstanding.map(async (benchmark) => {
      setNullValues(benchmark);

      await transaction('PayByEstablishmentIdGoodOutstanding').withSchema('cqc').insert({
        EstablishmentFK: benchmark[PayByEstablishmentIdGoodOutstandingHeadings[0]],
        MainJobRole: benchmark[PayByEstablishmentIdGoodOutstandingHeadings[1]],
        LocalAuthorityArea: benchmark[PayByEstablishmentIdGoodOutstandingHeadings[2]],
        CQCGoodOutstandingRating: benchmark[PayByEstablishmentIdGoodOutstandingHeadings[3]],
        MainServiceFK: benchmark[PayByEstablishmentIdGoodOutstandingHeadings[4]],
        BaseWorkers: benchmark[PayByEstablishmentIdGoodOutstandingHeadings[5]],
        AverageHourlyRate: benchmark[PayByEstablishmentIdGoodOutstandingHeadings[6]],
        AverageAnnualFTE: benchmark[PayByEstablishmentIdGoodOutstandingHeadings[7]],
        BaseEstablishments: benchmark[PayByEstablishmentIdGoodOutstandingHeadings[8]],
      });
    }),
  );

  // await transaction('DataImports').withSchema('cqc').insert({
  //   "Type": 'Benchmarks',
  //   "Date": new Date(),
  // });

  await transaction.commit();

  console.log('Completed Benchmarks tables update');
};

module.exports = { updateNewBenchmarksDbTables };
