const db = require('../db');
const config = require('../../config');
const { promisify } = require('util');
const fs = require('fs');
const stream = require('stream');
const pipeline = promisify(stream.pipeline);
const copyFrom = require('pg-copy-streams').from;
const csv = require('csv-parser');
const neatCsv = require('neat-csv');

const updateBenchmarksDbTables = async (reports) => {
  const transaction = await db.transaction();
  const benchmarks = await neatCsv(reports['Benchmarks.csv']);

  const benchmarksSickness = await neatCsv(reports['BenchmarksSickness.csv']);
  const benchmarksQualifications = await neatCsv(reports['BenchmarksQuals.csv']);
  
  const setNullValues = (benchmark) => Object.keys(benchmark).map(key => benchmark[key] = benchmark[key] === '' ? null : benchmark[key]);
  
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

  const benchmarksPay = await neatCsv(reports['BenchmarksPay.csv']);

  await Promise.all(benchmarksPay.map(async benchmark => {
    setNullValues(benchmark);

    await transaction('BenchmarksPay').withSchema('cqc').insert({
      "CssrID": benchmark['Local authority'],
      "EstablishmentFK": benchmark['Establishment ID'],
      "MainServiceFK": benchmark['Main service'],
      "Pay": benchmark['Pay'],
    });
  }));

  // const benchmarksTurnover = await neatCsv(reports['BenchmarksTurnover.csv']);

  // await Promise.all(benchmarksTurnover.map(async benchmark => {
  //   Object.keys(benchmark).map(key => {
  //     return benchmark[key] = benchmark[key] === '' ? null : benchmark[key];
  //   });

  //   await transaction('Benchmarks').withSchema('cqc').insert({
  //     "CssrID": benchmark['LA'],
  //     "MainServiceFK": benchmark['Main service'],
  //     "EstablishmentFK": benchmark['Pay'],
  //     "Pay": benchmark['Pay'],
      
  //   })
  // }));

  // const benchmarksFileStream = fs.createReadStream('./benchmarkFiles/Benchmarks.csv');
  // const benchmarksPayFileStream = fs.createReadStream('./benchmarkFiles/BenchmarksPay.csv');
  // const benchmarksTurnoverFileStream = fs.createReadStream('./benchmarkFiles/BenchmarksTurnover.csv');
  // const benchmarksQualsFileStream = fs.createReadStream('./benchmarkFiles/BenchmarksQuals.csv');
  // const benchmarksSicknessFileStream = fs.createReadStream('./benchmarkFiles/BenchmarksSickness.csv');

  // await db.transaction(async (trx) => {
  //   const client = trx.trxClient
  //   await trx.raw(
  //     `TRUNCATE cqc."Benchmarks", cqc."BenchmarksPay", cqc."BenchmarksTurnover", cqc."BenchmarksQualifications", cqc."BenchmarksSickness";`
  //   );
 
  //   await pipeline(
  //     benchmarksFileStream,
  //     db.query(copyFrom(`COPY cqc."Benchmarks" ("CssrID", "MainServiceFK", "Pay", "Sickness", "Turnover", "Qualifications", "Workplaces", "Staff", "PayWorkplaces", "PayStaff", "SicknessWorkplaces", "SicknessStaff", "QualificationsWorkplaces", "QualificationsStaff", "TurnoverWorkplaces", "TurnoverStaff", "PayGoodCQC", "PayLowTurnover", "SicknessGoodCQC", "SicknessLowTurnover", "QualificationsGoodCQC", "QualificationsLowTurnover", "TurnoverGoodCQC", "TurnoverLowTurnover") FROM STDIN WITH (FORMAT csv)`)).transacting(trx)
  //   )
    
  //   // await pipeline(
    //   benchmarksPayFileStream,
    //   db.query(copyFrom(`COPY cqc."BenchmarksPay.csv" ("CssrID", "MainServiceFK", "EstablishmentFK", "Pay") FROM STDIN WITH (FORMAT csv)`)).transacting(trx)
    // )

    // await pipeline(
    //   benchmarksTurnoverFileStream,
    //   db.query(copyFrom(`COPY cqc."BenchmarksTurnover" ("CssrID", "MainServiceFK", "EstablishmentFK", "Turnover") FROM STDIN WITH (FORMAT csv)`)).transacting(trx)
    // )
    // await pipeline(
    //   benchmarksQualsFileStream,
    //   db.query(copyFrom(`COPY cqc."BenchmarksQuals.csv" ("CssrID", "MainServiceFK", "EstablishmentFK", "Qualifications") FROM STDIN WITH (FORMAT csv)`)).transacting(trx)
    // )
    // await pipeline(
    //   benchmarksSicknessFileStream,
    //   db.query(copyFrom(`COPY cqc."BenchmarksSickness.csv" ("CssrID", "MainServiceFK", "EstablishmentFK", "Sickness") FROM STDIN WITH (FORMAT csv)`)).transacting(trx)
    // )
    
    // await trx.raw(
    //   `
    //     INSERT INTO cqc."DataImports" ("Type", "Date") VALUES ('Benchmarks', current_timestamp);
        
    //     SELECT
    //       COUNT(0)
    //     FROM
    //       cqc."Benchmarks";
        
    //       SELECT
    //       COUNT(0)
    //     FROM
    //       cqc."BenchmarksPay";
        
    //       SELECT
    //       COUNT(0)
    //     FROM
    //       cqc."BenchmarksTurnover";
        
    //       SELECT
    //       COUNT(0)
    //     FROM
    //       cqc."BenchmarksQualifications";
        
    //       SELECT
    //       COUNT(0)
    //     FROM
    //       cqc."BenchmarksSickness";
        
    //     DO
    //     $$
    //     BEGIN
    //       IF (SELECT COUNT(0) FROM cqc."Benchmarks") = 0 THEN
    //         RAISE EXCEPTION 'CANT HAVE NO ROWS IN BENCHMARKS';
    //       END IF;
    //       IF (SELECT COUNT(0) FROM cqc."BenchmarksPay") = 0 THEN
    //         RAISE EXCEPTION 'CANT HAVE NO ROWS IN PAY';
    //       END IF;
    //       IF (SELECT COUNT(0) FROM cqc."BenchmarksTurnover") = 0 THEN
    //         RAISE EXCEPTION 'CANT HAVE NO ROWS IN TURNOVER';
    //       END IF;
    //       IF (SELECT COUNT(0) FROM cqc."BenchmarksQualifications") = 0 THEN
    //         RAISE EXCEPTION 'CANT HAVE NO ROWS IN QUALS';
    //       END IF;
    //       IF (SELECT COUNT(0) FROM cqc."BenchmarksSickness") = 0 THEN
    //         RAISE EXCEPTION 'CANT HAVE NO ROWS IN SICKNESS';
    //       END IF;
    //     END;
    //     $$ LANGUAGE plpgsql;
    //   `
    // )
    // const stringStream = stream.Readable.from(stringContainingCsvData);
    // await copyToTable(tx, 'table_name', stringStream);
  // });
  // await db.raw(
  //   `
  //     BEGIN TRANSACTION;

  //     TRUNCATE cqc."Benchmarks", cqc."BenchmarksPay", cqc."BenchmarksTurnover", cqc."BenchmarksQualifications", cqc."BenchmarksSickness";
      
  //     copy cqc."Benchmarks" ("CssrID", "MainServiceFK", "Pay", "Sickness", "Turnover", "Qualifications", "Workplaces", "Staff", "PayWorkplaces", "PayStaff", "SicknessWorkplaces", "SicknessStaff", "QualificationsWorkplaces", "QualificationsStaff", "TurnoverWorkplaces", "TurnoverStaff", "PayGoodCQC", "PayLowTurnover", "SicknessGoodCQC", "SicknessLowTurnover", "QualificationsGoodCQC", "QualificationsLowTurnover", "TurnoverGoodCQC", "TurnoverLowTurnover") FROM (${reports['Benchmarks.csv']}) WITH (FORMAT csv, ENCODING 'UTF8', HEADER);
  //     copy cqc."BenchmarksPay" ("CssrID", "MainServiceFK", "EstablishmentFK", "Pay") FROM (${reports['BenchmarksPay.csv']}) WITH (FORMAT csv, ENCODING 'UTF8', HEADER);
  //     copy cqc."BenchmarksTurnover" ("CssrID", "MainServiceFK", "EstablishmentFK", "Turnover") FROM (${reports['BenchmarksTurnover.csv']}) WITH (FORMAT csv, ENCODING 'UTF8', HEADER);
  //     copy cqc."BenchmarksQualifications" ("CssrID", "MainServiceFK", "EstablishmentFK", "Qualifications") FROM (${reports['BenchmarksQuals.csv']}) WITH (FORMAT csv, ENCODING 'UTF8', HEADER);
  //     copy cqc."BenchmarksSickness" ("CssrID", "MainServiceFK", "EstablishmentFK", "Sickness") FROM (${reports['BenchmarksSickness.csv']}) WITH (FORMAT csv, ENCODING 'UTF8', HEADER);
      
  //     INSERT INTO cqc."DataImports" ("Type", "Date") VALUES ('Benchmarks', current_timestamp);
      
  //     SELECT
  //       COUNT(0)
  //     FROM
  //       cqc."Benchmarks";
      
  //       SELECT
  //       COUNT(0)
  //     FROM
  //       cqc."BenchmarksPay";
      
  //       SELECT
  //       COUNT(0)
  //     FROM
  //       cqc."BenchmarksTurnover";
      
  //       SELECT
  //       COUNT(0)
  //     FROM
  //       cqc."BenchmarksQualifications";
      
  //       SELECT
  //       COUNT(0)
  //     FROM
  //       cqc."BenchmarksSickness";
      
  //     DO
  //     $$
  //     BEGIN
  //       IF (SELECT COUNT(0) FROM cqc."Benchmarks") = 0 THEN
  //         RAISE EXCEPTION 'CANT HAVE NO ROWS IN BENCHMARKS';
  //       END IF;
  //       IF (SELECT COUNT(0) FROM cqc."BenchmarksPay") = 0 THEN
  //         RAISE EXCEPTION 'CANT HAVE NO ROWS IN PAY';
  //       END IF;
  //       IF (SELECT COUNT(0) FROM cqc."BenchmarksTurnover") = 0 THEN
  //         RAISE EXCEPTION 'CANT HAVE NO ROWS IN TURNOVER';
  //       END IF;
  //       IF (SELECT COUNT(0) FROM cqc."BenchmarksQualifications") = 0 THEN
  //         RAISE EXCEPTION 'CANT HAVE NO ROWS IN QUALS';
  //       END IF;
  //       IF (SELECT COUNT(0) FROM cqc."BenchmarksSickness") = 0 THEN
  //         RAISE EXCEPTION 'CANT HAVE NO ROWS IN SICKNESS';
  //       END IF;
  //     END;
  //     $$ LANGUAGE plpgsql;
  
  //     END TRANSACTION;
  //   `,
  // );
};

module.exports = { updateBenchmarksDbTables };
