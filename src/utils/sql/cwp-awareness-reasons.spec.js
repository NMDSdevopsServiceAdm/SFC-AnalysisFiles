const { describe, it } = require('mocha');
const expect = require('chai').expect;
const { cwpAwarenessReasonColumn, generateSqlQueriesForCwpAwarenessReasonsColumns } = require('./cwp-awareness-reasons');
const { removeIndentation } = require('../test-utils.js')

describe('src/utils/sql/cwp-Awareness-reasons.js', () => {
  describe('cwpAwarenessReasonColumn', () => {
    it('should return a number that represent the sql query for adding a cwp reasons with the id and reason column provided', () => {
      const id = '1';
      const cwpAwarenessReasonCode = 'cwpreason1';

      const expected =
        '(SELECT "AnalysisFileCode" FROM "CareWorkforcePathwayReasons" c JOIN "EstablishmentCWPReasons" ec on  c."ID" = ec."CareWorkforcePathwayReasonID"  WHERE e."EstablishmentID" = ec."EstablishmentID"  AND "CareWorkforcePathwayReasonID"= 1 LIMIT 1) cwpreason1,';
      const actual = cwpAwarenessReasonColumn(id, cwpAwarenessReasonCode);

      expect(removeIndentation(actual)).to.equal(removeIndentation(expected));
    });

    it('should return a number that represent the sql query for adding a cwp reasons with the id and reason column provided', () => {
      const id = '2';
      const cwpAwarenessReasonCode = 'cwpreason2';

      const expected =
        '(SELECT "AnalysisFileCode" FROM "CareWorkforcePathwayReasons" c JOIN "EstablishmentCWPReasons" ec on  c."ID" = ec."CareWorkforcePathwayReasonID"  WHERE e."EstablishmentID" = ec."EstablishmentID" AND "CareWorkforcePathwayReasonID"= 2 LIMIT 1) cwpreason2,';
       const actual = cwpAwarenessReasonColumn(id, cwpAwarenessReasonCode);

      expect(removeIndentation(actual)).to.equal(removeIndentation(expected));
    });
  });

  describe('generateSqlQueriesForCwpAwarenessReasonsColumns', () => {
    it('should generate sql queries for the cwp reasons columns', () => {
      const mockMappings = [
        { id: 1, cwpAwarenessReasonCode: 'cwpreason1'},
        { id: 2, cwpAwarenessReasonCode: 'cwpreason2' },
      ];

      const expectedSqlQueries = (
      
        '(SELECT "AnalysisFileCode" FROM "CareWorkforcePathwayReasons" c JOIN "EstablishmentCWPReasons" ec on c."ID" = ec."CareWorkforcePathwayReasonID" WHERE e."EstablishmentID" = ec."EstablishmentID" AND "CareWorkforcePathwayReasonID"= 1 LIMIT 1) cwpreason1,'+
        '\n' +
        '(SELECT "AnalysisFileCode" FROM "CareWorkforcePathwayReasons" c JOIN "EstablishmentCWPReasons" ec on c."ID" = ec."CareWorkforcePathwayReasonID" WHERE e."EstablishmentID" = ec."EstablishmentID" AND "CareWorkforcePathwayReasonID"= 2 LIMIT 1) cwpreason2,')
      const actual = generateSqlQueriesForCwpAwarenessReasonsColumns(mockMappings)
      
      expect(removeIndentation(actual)).to.equal(removeIndentation(expectedSqlQueries))
    });
  });
});
