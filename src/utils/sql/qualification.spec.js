const { describe, it } = require('mocha');
const expect = require('chai').expect;
const { qualificationColumn } = require('./qualification');

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
});
