const db = require('../generate_analysis_files/db');
const neatCsv = require('neat-csv');

const setNullValues = (benchmark) => (
  Object.keys(benchmark).map(key => (
    benchmark[key] = benchmark[key] === '' ? null : benchmark[key])
  )
);

const updateBenchmarksDbTables = async (reports) => {
  console.log('Begin update Benchmarks Db Tables');
  const transaction = await db.transaction();
  
  const benchmarks = await neatCsv(reports['Benchmarks.csv']);
  const benchmarksPay = await neatCsv(reports['BenchmarksPay.csv']);
  const benchmarksTurnover = await neatCsv(reports['BenchmarksTurnover.csv']);
  const benchmarksSickness = await neatCsv(reports['BenchmarksSickness.csv']);
  const benchmarksQualifications = await neatCsv(reports['BenchmarksQualifications.csv']);

  const filesArr = [benchmarks, benchmarksPay, benchmarksTurnover, benchmarksSickness, benchmarksQualifications];
  
  if (filesArr.some((file) => file.length == 0)) {
    throw new Error('One or more of the files has no rows');
  }

  await transaction('Benchmarks').truncate();
  
  const benchmarksHeadings = Object.keys(benchmarks[0]);

  await Promise.all(benchmarks.map(async benchmark => {
    setNullValues(benchmark);

    await transaction('Benchmarks').withSchema('cqc').insert({
      "CssrID": benchmark[benchmarksHeadings[0]],
      "MainServiceFK": benchmark[benchmarksHeadings[1]],
      "Pay": benchmark[benchmarksHeadings[2]],
      "Sickness": benchmark[benchmarksHeadings[3]],
      "Turnover": benchmark[benchmarksHeadings[4]],
      "Qualifications": benchmark[benchmarksHeadings[5]],
      "Workplaces": benchmark[benchmarksHeadings[6]],
      "Staff": benchmark[benchmarksHeadings[7]],
      "PayWorkplaces": benchmark[benchmarksHeadings[8]],
      "PayStaff": benchmark[benchmarksHeadings[9]],
      "SicknessWorkplaces": benchmark[benchmarksHeadings[10]],
      "SicknessStaff": benchmark[benchmarksHeadings[11]],
      "QualificationsWorkplaces": benchmark[benchmarksHeadings[12]],
      "QualificationsStaff": benchmark[benchmarksHeadings[13]],
      "TurnoverWorkplaces": benchmark[benchmarksHeadings[14]],
      "TurnoverStaff": benchmark[benchmarksHeadings[15]],
      "PayGoodCQC": benchmark[benchmarksHeadings[16]],
      "PayLowTurnover": benchmark[benchmarksHeadings[17]],
      "SicknessGoodCQC": benchmark[benchmarksHeadings[18]],
      "SicknessLowTurnover": benchmark[benchmarksHeadings[19]],
      "QualificationsGoodCQC": benchmark[benchmarksHeadings[20]],
      "QualificationsLowTurnover": benchmark[benchmarksHeadings[21]],
      "TurnoverGoodCQC": benchmark[benchmarksHeadings[22]],
      "TurnoverLowTurnover": benchmark[benchmarksHeadings[23]]
    })
  }));
  
  await transaction('BenchmarksPay').truncate();

  const benchmarksPayHeadings = Object.keys(benchmarksPay[0]);

  await Promise.all(benchmarksPay.map(async benchmark => {
    setNullValues(benchmark);

    await transaction('BenchmarksPay').withSchema('cqc').insert({
      "CssrID": benchmark[benchmarksPayHeadings[0]],
      "MainServiceFK": benchmark[benchmarksPayHeadings[1]],
      "EstablishmentFK": benchmark[benchmarksPayHeadings[2]],
      "Pay": benchmark[benchmarksPayHeadings[3]],
    })
  }));



  await transaction('BenchmarksTurnover').truncate();
    
  const benchmarksTurnoverHeadings = Object.keys(benchmarksTurnover[0]);

  await Promise.all(benchmarksTurnover.map(async benchmark => {
    setNullValues(benchmark);

    await transaction('BenchmarksTurnover').withSchema('cqc').insert({
      "CssrID": benchmark[benchmarksTurnoverHeadings[0]],
      "MainServiceFK": benchmark[benchmarksTurnoverHeadings[1]],
      "EstablishmentFK": benchmark[benchmarksTurnoverHeadings[2]],
      "Turnover": benchmark[benchmarksTurnoverHeadings[3]],
    })
  }));

  await transaction('BenchmarksSickness').truncate();

    
  const benchmarksSicknessHeadings = Object.keys(benchmarksSickness[0]);

  await Promise.all(benchmarksSickness.map(async benchmark => {
    setNullValues(benchmark);

    await transaction('BenchmarksSickness').withSchema('cqc').insert({
      "CssrID": benchmark[benchmarksSicknessHeadings[0]],
      "MainServiceFK": benchmark[benchmarksSicknessHeadings[1]],
      "EstablishmentFK": benchmark[benchmarksSicknessHeadings[2]],
      "Sickness": benchmark[benchmarksSicknessHeadings[3]],
    })
  }));

  await transaction('BenchmarksQualifications').truncate();

  const benchmarksQualificationsHeadings = Object.keys(benchmarksQualifications[0]);

  await Promise.all(benchmarksQualifications.map(async benchmark => {
    setNullValues(benchmark);

    await transaction('BenchmarksQualifications').withSchema('cqc').insert({
      "CssrID": benchmark[benchmarksQualificationsHeadings[0]],
      "MainServiceFK": benchmark[benchmarksQualificationsHeadings[1]],
      "EstablishmentFK": benchmark[benchmarksQualificationsHeadings[2]],
      "Qualifications": benchmark[benchmarksQualificationsHeadings[3]],
    })
  }));

  await transaction('DataImports').withSchema('cqc').insert({
    "Type": 'Benchmarks',
    "Date": new Date(), 
  });

  await transaction.commit();

  console.log('Completed Benchmarks tables update');
};

module.exports = { updateBenchmarksDbTables };
