const { describe, it } = require('mocha');
const expect = require('chai').expect;
const { generateColumnsForYesNoDontKnowQuestion } = require('./generate-yes-no-dont-know-columns.js');
const { removeIndentation } = require('../../test-utils.js')

describe('src/utils/sql/generate-yes-no-don-know-columns.js', () => {
  describe('generateColumnsForYesNoDontKnowQuestion', () => {
    it('should return SQL statement retrieving from Value/ChangedAt columns set to standard variable format', () => {
        
        const expected =
        `(
        SELECT CASE
            WHEN w."CarryOutDelegatedHealthcareActivitiesValue" = 'No' THEN 2
            WHEN w."CarryOutDelegatedHealthcareActivitiesValue" = 'Yes' THEN 1
            WHEN w."CarryOutDelegatedHealthcareActivitiesValue" IS NULL THEN -1
            WHEN w."CarryOutDelegatedHealthcareActivitiesValue" = 'Don''t know' THEN -2
        END
        ) delegatedhealthcareactivities,
        TO_CHAR(w."CarryOutDelegatedHealthcareActivitiesChangedAt",'DD/MM/YYYY') delegatedhealthcareactivities_changedate,
        TO_CHAR(w."CarryOutDelegatedHealthcareActivitiesSavedAt",'DD/MM/YYYY')  delegatedhealthcareactivities_savedate`;
        
      const actual = generateColumnsForYesNoDontKnowQuestion('CarryOutDelegatedHealthcareActivities', 'delegatedhealthcareactivities');

      expect(removeIndentation(actual)).to.equal(removeIndentation(expected));
    });
  });
});
