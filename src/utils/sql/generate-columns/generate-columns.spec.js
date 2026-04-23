const { describe, it } = require('mocha');
const expect = require('chai').expect;

const { generateDateColumns, generateCaseColumn } = require('./generate-columns');
const { removeIndentation } = require('../../test-utils');

describe('src/utils/sql/generate-columns/generate-columns.js', () => {
  describe('generateCaseColumn', () => {
    it('should buildSQL query that convert enum value to analysis file code', () => {
      const databaseColumnName = 'PensionContribution';
      const analysisFileAlias = 'CWEnhancedPension';
      const mapping = {
        Yes: 1,
        No: 2,
        "Don''t know": -1,
      };
      const expectedOutput = `(
        CASE  
          WHEN  "PensionContribution" = 'Yes'
            THEN 1
          WHEN  "PensionContribution" = 'No'
            THEN 2
          WHEN  "PensionContribution" = 'Don''t know'
            THEN -1
          WHEN  "PensionContribution" IS NULL
            THEN NULL
          ELSE NULL
        END 
        ) AS CWEnhancedPension,
      `;

      const actual = generateCaseColumn(databaseColumnName, analysisFileAlias, mapping, 'NULL');
      expect(removeIndentation(actual)).to.equal(removeIndentation(expectedOutput));
    });
  });

  describe('generateDateColumns', () => {
    it('should generate SQL queries for extracting savedAt and changedAt dates', () => {
      const databaseColumnName = 'PensionContributionPercentage';
      const analysisFileAliasName = 'cwenhancedpensioncontribution';

      const expectedOutput = `
        TO_CHAR(e."PensionContributionPercentageSavedAt",'DD/MM/YYYY') cwenhancedpensioncontribution_savedate,
        TO_CHAR(e."PensionContributionPercentageChangedAt",'DD/MM/YYYY') cwenhancedpensioncontribution_changedate,
      `;

      const actual = generateDateColumns(databaseColumnName, analysisFileAliasName);

      expect(removeIndentation(actual)).to.equal(removeIndentation(expectedOutput));
    });
  });
});
