const db = require('../db');
const neatCsv = require('neat-csv');

const setNullValues = (benchmark) => (
  Object.keys(benchmark).map(key => (
    benchmark[key] = benchmark[key] === '' ? null : benchmark[key])
  )
);

const updateBenchmarksDbTables = async (reports) => {
  const transaction = await db.transaction();
  
  const benchmarks = await neatCsv(reports['Benchmarks.csv']);
  const benchmarksPay = await neatCsv(reports['BenchmarksPay.csv']);
  const benchmarksTurnover = await neatCsv(reports['BenchmarksTurnover.csv']);
  const benchmarksSickness = await neatCsv(reports['BenchmarksSickness.csv']);
  const benchmarksQualifications = await neatCsv(reports['BenchmarksQuals.csv']);

  const filesArr = [benchmarks, benchmarksPay, benchmarksTurnover, benchmarksSickness, benchmarksQualifications];
  
  if (filesArr.some((file) => file.length == 0)) {
    throw new Error('One or more of the files has no rows');
  }

  await transaction('Benchmarks').truncate();
  
  await Promise.all(benchmarks.map(async benchmark => {
    setNullValues(benchmark);
    
    await transaction('Benchmarks').withSchema('cqc').insert({
      "CssrID": benchmark['LA'],
      "MainServiceFK": benchmark['Main service'],
      "Pay": benchmark['Pay'],
      "Sickness": benchmark['Sickness'],
      "Turnover": benchmark['Turnover'],
      "Qualifications": benchmark['Qualifications'],
      "Workplaces": benchmark['Workplaces'],
      "Staff": benchmark['Staff'],
      "PayWorkplaces": benchmark['PayWorkplaces'],
      "PayStaff": benchmark['PayStaff'],
      "SicknessWorkplaces": benchmark['SicknessWorkplaces'],
      "SicknessStaff": benchmark['SicknessStaff'],
      "QualificationsWorkplaces": benchmark['QualsWorkplaces'],
      "QualificationsStaff": benchmark['QualsStaff'],
      "TurnoverWorkplaces": benchmark['TurnoverWorkplaces'],
      "TurnoverStaff": benchmark['TurnoverStaff'],
      "PayGoodCQC": benchmark['PayGoodCQC'],
      "PayLowTurnover": benchmark['PayLowTurnover'],
      "SicknessGoodCQC": benchmark['SicknessGoodCQC'],
      "SicknessLowTurnover": benchmark['SicknessLowTurnover'],
      "QualificationsGoodCQC": benchmark['QualificationsGoodCQC'],
      "QualificationsLowTurnover": benchmark['QualificationsLowTurnover'],
      "TurnoverGoodCQC": benchmark['TurnoverGoodCQC'],
      "TurnoverLowTurnover": benchmark['TurnoverLowTurnover']
    })
  }));

  
  await transaction('BenchmarksPay').truncate();

  await Promise.all(benchmarksPay.map(async benchmark => {
    setNullValues(benchmark);

    await transaction('BenchmarksPay').withSchema('cqc').insert({
      "CssrID": benchmark['Local authority'],
      "EstablishmentFK": benchmark['Establishment ID'],
      "MainServiceFK": benchmark['Main service'],
      "Pay": benchmark['Pay'],
    });
  }));

  await transaction('BenchmarksTurnover').truncate();

  await Promise.all(benchmarksTurnover.map(async benchmark => {
    setNullValues(benchmark);

    await transaction('BenchmarksTurnover').withSchema('cqc').insert({
      "CssrID": benchmark['Local authority'],
      "MainServiceFK": benchmark['Main service'],
      "EstablishmentFK": benchmark['Establishment ID'],
      "Turnover": benchmark['Turnover'],
    })
  }));

  await transaction('BenchmarksSickness').truncate();

  await Promise.all(benchmarksSickness.map(async benchmark => {
    setNullValues(benchmark);

    await transaction('BenchmarksSickness').withSchema('cqc').insert({
      "CssrID": benchmark['Local authority'],
      "MainServiceFK": benchmark['Main service'],
      "EstablishmentFK": benchmark['Establishment ID'],
      "Sickness": benchmark['Sickness'],
    })
  }));

  await transaction('BenchmarksQualifications').truncate();

  await Promise.all(benchmarksQualifications.map(async benchmark => {
    setNullValues(benchmark);

    await transaction('BenchmarksQualifications').withSchema('cqc').insert({
      "CssrID": benchmark['Local authority'],
      "MainServiceFK": benchmark['Main service'],
      "EstablishmentFK": benchmark['Establishment ID'],
      "Qualifications": benchmark['Quals'],
    })
  }));
};

module.exports = { updateBenchmarksDbTables };
