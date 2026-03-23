const { describe, it } = require('mocha');
const expect = require('chai').expect;

const { generateDateColumns } = require('./generate-columns');
const { removeIndentation } = require('../../test-utils');

describe('src/utils/sql/generate-columns/generate-columns.js', () => {
  describe('generateDateColumns', () => {
    it('should generate sql queries for extracting savedAt and changedAt dates', () => {
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
