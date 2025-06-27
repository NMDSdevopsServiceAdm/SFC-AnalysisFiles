const { describe, it } = require('mocha');
const expect = require('chai').expect;
const { cwpAwarnessReasonColumn, generateSqlQueriesForCwpAwarnessReanosColumns } = require('./cwp-awarness-reasons');

describe('src/utils/sql/cwp-awarness-reasons.js', () => {


  describe('cwpAwarnessReasonColumn', () => {
    it('should return a number that represent the sql query for adding a cwp reasons with the id and reason column provided', () => {
      const id = '1';
      const cwpAwarnessReasonCode = 'cwpreason1';

      const expected =
        '(SELECT "AnalysisFileCode"  FROM   "CareWorkforcePathwayReasons" c JOIN "EstablishmentCWPReasons" ec on  c."ID" = ec."CareWorkforcePathwayReasonID"  WHERE e."EstablishmentID" = ec."EstablishmentID"  AND "CareWorkforcePathwayReasonID"= 1 LIMIT 1) cwpreason1,';
      const actual = cwpAwarnessReasonColumn(id, cwpAwarnessReasonCode);

      expect(actual).to.equal(expected);
    });

    it('should return a number that represent the sql query for adding a cwp reasons with the id and reason column provided', () => {
      const id = '2';
      const cwpAwarnessReasonCode = 'cwpreason2';

      const expected =
        '(SELECT "AnalysisFileCode"  FROM   "CareWorkforcePathwayReasons" c JOIN "EstablishmentCWPReasons" ec on  c."ID" = ec."CareWorkforcePathwayReasonID"  WHERE e."EstablishmentID" = ec."EstablishmentID"  AND "CareWorkforcePathwayReasonID"= 2 LIMIT 1) cwpreason2,';
       const actual = cwpAwarnessReasonColumn(id, cwpAwarnessReasonCode);

      expect(actual).to.equal(expected);
    });
  });

  describe('generateSqlQueriesForCwpAwarnessReanosColumns', () => {
    it('should generate sql queries for the cwp reasons columns', () => {
      const mockMappings = [
        { id: 1, cwpAwarnessReasonCode: 'cwpreason1'},
        { id: 2, cwpAwarnessReasonCode: 'cwpreason2' },
      ];

      const expectedSqlQueries = (
      
        '(SELECT "AnalysisFileCode"  FROM   "CareWorkforcePathwayReasons" c JOIN "EstablishmentCWPReasons" ec on  c."ID" = ec."CareWorkforcePathwayReasonID"  WHERE e."EstablishmentID" = ec."EstablishmentID"  AND "CareWorkforcePathwayReasonID"= 1 LIMIT 1) cwpreason1,'+
        '\n' +
        '(SELECT "AnalysisFileCode"  FROM   "CareWorkforcePathwayReasons" c JOIN "EstablishmentCWPReasons" ec on  c."ID" = ec."CareWorkforcePathwayReasonID"  WHERE e."EstablishmentID" = ec."EstablishmentID"  AND "CareWorkforcePathwayReasonID"= 2 LIMIT 1) cwpreason2,')
      const actual = generateSqlQueriesForCwpAwarnessReanosColumns(mockMappings)
      
      expect(actual).to.equal(expectedSqlQueries)
    });
  });
});
