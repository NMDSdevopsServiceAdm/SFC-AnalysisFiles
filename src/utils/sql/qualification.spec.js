const { describe, it } = require('mocha');
const expect = require('chai').expect;
const { qualificationColumn, qualificationYearColumn } = require('./qualification');

describe('src/utils/sql/qualification.js', () => {
  describe('qualificationColumn()', () => {
    it('should return a string that represent the sql query for adding a qualification column with the id and qualification code number provided', () => {
      const id = '139';
      const qualificationCode = 'ql145achq3';

      const expected = 'CASE WHEN EXISTS (SELECT 1 FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 139 LIMIT 1) THEN 1 ELSE 0 END ql145achq3,';
      const actual = qualificationColumn(id, qualificationCode);

      expect(actual).to.equal(expected);
    });

    it('should return a string that represent the sql query for adding a qualification column with the id and qualification code number provided', () => {
      const id = '140';
      const qualificationCode = 'ql146achq3';

      const expected = 'CASE WHEN EXISTS (SELECT 1 FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 140 LIMIT 1) THEN 1 ELSE 0 END ql146achq3,';
      const actual = qualificationColumn(id, qualificationCode);

      expect(actual).to.equal(expected);
    });
  });

  describe('qualificationYearColumn', () => {
    it('should return a string that represent the sql query for adding a qualification year column, with the id and year code number provided', () => { 
      const id = '139';
      const qualificationYearCode = 'ql145year3';

      const expected = '(SELECT "Year" FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 139 LIMIT 1) ql145year3,';
      const actual = qualificationYearColumn(id, qualificationYearCode);

      expect(actual).to.equal(expected);
    })

    it('should return a string that represent the sql query for adding a qualification year column, with the id and year code number provided', () => { 
      const id = '140';
      const qualificationYearCode = 'ql146year3';

      const expected = '(SELECT "Year" FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 140 LIMIT 1) ql146year3,';
      const actual = qualificationYearColumn(id, qualificationYearCode);

      expect(actual).to.equal(expected);
    })
  })
});
