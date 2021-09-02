const db = require('../db');
// const config = require('../config/index');

const updateBenchmarksDbTables = async () => {
  await db.raw(
    `
      BEGIN TRANSACTION;

      TRUNCATE cqc."Benchmarks", cqc."BenchmarksPay", cqc."BenchmarksTurnover", cqc."BenchmarksQualifications", cqc."BenchmarksSickness";
      
      \\copy cqc."Benchmarks" ("CssrID", "MainServiceFK", "Pay", "Sickness", "Turnover", "Qualifications", "Workplaces", "Staff", "PayWorkplaces", "PayStaff", "SicknessWorkplaces", "SicknessStaff", "QualificationsWorkplaces", "QualificationsStaff", "TurnoverWorkplaces", "TurnoverStaff", "PayGoodCQC", "PayLowTurnover", "SicknessGoodCQC", "SicknessLowTurnover", "QualificationsGoodCQC", "QualificationsLowTurnover", "TurnoverGoodCQC", "TurnoverLowTurnover") FROM '../../benchmarkFiles/Benchmarks.csv' WITH (FORMAT csv, ENCODING 'UTF8', HEADER);
      \\copy cqc."BenchmarksPay" ("CssrID", "MainServiceFK", "EstablishmentFK", "Pay") FROM '../../benchmarkFiles/BenchmarksPay.csv' WITH (FORMAT csv, ENCODING 'UTF8', HEADER);
      \\copy cqc."BenchmarksTurnover" ("CssrID", "MainServiceFK", "EstablishmentFK", "Turnover") FROM '../../benchmarkFiles/BenchmarksTurnover.csv' WITH (FORMAT csv, ENCODING 'UTF8', HEADER);
      \\copy cqc."BenchmarksQualifications" ("CssrID", "MainServiceFK", "EstablishmentFK", "Qualifications") FROM '../../benchmarkFiles/BenchmarksQualifications.csv' WITH (FORMAT csv, ENCODING 'UTF8', HEADER);
      \\copy cqc."BenchmarksSickness" ("CssrID", "MainServiceFK", "EstablishmentFK", "Sickness") FROM '../../benchmarkFiles/BenchmarksSickness.csv' WITH (FORMAT csv, ENCODING 'UTF8', HEADER);
      
      INSERT INTO cqc."DataImports" ("Type", "Date") VALUES ('Benchmarks', current_timestamp);
      
      SELECT
        COUNT(0)
      FROM
        cqc."Benchmarks";
      
        SELECT
        COUNT(0)
      FROM
        cqc."BenchmarksPay";
      
        SELECT
        COUNT(0)
      FROM
        cqc."BenchmarksTurnover";
      
        SELECT
        COUNT(0)
      FROM
        cqc."BenchmarksQualifications";
      
        SELECT
        COUNT(0)
      FROM
        cqc."BenchmarksSickness";
      
      DO
      $$
      BEGIN
        IF (SELECT COUNT(0) FROM cqc."Benchmarks") = 0 THEN
          RAISE EXCEPTION 'CANT HAVE NO ROWS IN BENCHMARKS';
        END IF;
        IF (SELECT COUNT(0) FROM cqc."BenchmarksPay") = 0 THEN
          RAISE EXCEPTION 'CANT HAVE NO ROWS IN PAY';
        END IF;
        IF (SELECT COUNT(0) FROM cqc."BenchmarksTurnover") = 0 THEN
          RAISE EXCEPTION 'CANT HAVE NO ROWS IN TURNOVER';
        END IF;
        IF (SELECT COUNT(0) FROM cqc."BenchmarksQualifications") = 0 THEN
          RAISE EXCEPTION 'CANT HAVE NO ROWS IN QUALS';
        END IF;
        IF (SELECT COUNT(0) FROM cqc."BenchmarksSickness") = 0 THEN
          RAISE EXCEPTION 'CANT HAVE NO ROWS IN SICKNESS';
        END IF;
      END
      $$ LANGUAGE plpgsql;
  
      END TRANSACTION;
    `
  );
};

module.exports = { updateBenchmarksDbTables };
