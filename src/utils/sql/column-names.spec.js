const { describe, it, beforeEach, afterEach } = require('mocha');
const expect = require('chai').expect;
const sinon = require('sinon');

const { ColumnNamesUtil } = require('./column-names.js');

describe('src/utils/sql/get-column-names.js', () => {
  const mockDb = {
    raw: async () => {},
  };

  beforeEach(() => {
    sinon.stub(mockDb, 'raw');

    mockDb.raw.callsFake((_rawQuery, [tableName, pattern]) => {
      const mockColumns = {
        Establishment: {
          '%ChangedAt': ['LocationIdChangedAt', 'IsRegulatedChangedAt', 'MainServiceFKChangedAt'],
          '%SavedAt': ['LocationIdSavedAt', 'IsRegulatedSavedAt', 'MainServiceFKSavedAt'],
        },
        Worker: {
          '%ChangedAt': ['NameOrIdChangedAt', 'MainJobFkChangedAt', 'DateOfBirthChangedAt'],
          '%SavedAt': ['NameOrIdSavedAt', 'MainJobFkSavedAt', 'DateOfBirthSavedAt'],
        },
      };

      const columns = mockColumns[tableName][pattern];
      return {
        rows: [{ column_name: columns[0] }, { column_name: columns[1] }, { column_name: columns[2] }],
      };
    });
  });

  afterEach(() => {
    sinon.restore();
  });

  describe('getColumnNamesAsString', () => {
    let columnNames = new ColumnNamesUtil(mockDb);
    beforeEach(async () => {
      await columnNames.reloadColumnNamesFromDb(mockDb);
    });

    it('should return a string that contains all Establishment ChangedAt columns and the updated column, joined with quotes and comma', async () => {
      const actual = await columnNames.getColumnNamesAsString('Establishment', 'ChangedAt');
      const expectedString = '"LocationIdChangedAt", "IsRegulatedChangedAt", "MainServiceFKChangedAt", "updated"';

      expect(actual).to.equal(expectedString);
    });

    it('should return a string that contains all Establishment SavedAt columns and updated column', async () => {
      const actual = await columnNames.getColumnNamesAsString('Establishment', 'SavedAt');
      const expectedString = '"LocationIdSavedAt", "IsRegulatedSavedAt", "MainServiceFKSavedAt", "updated"';

      expect(actual).to.equal(expectedString);
    });

    it('should add table short hand before each column if given', async () => {
      const actual = await columnNames.getColumnNamesAsString('Establishment', 'SavedAt', 'e');
      const expectedString = 'e."LocationIdSavedAt", e."IsRegulatedSavedAt", e."MainServiceFKSavedAt", e."updated"';

      expect(actual).to.equal(expectedString);
    });

    it('should return a string that contains all Worker ChangedAt columns and updated column', async () => {
      const actual = await columnNames.getColumnNamesAsString('Worker', 'ChangedAt');
      const expectedString = '"NameOrIdChangedAt", "MainJobFkChangedAt", "DateOfBirthChangedAt", "updated"';

      expect(actual).to.equal(expectedString);
    });

    it('should return a string that contains all Worker SavedAt columns and updated column', async () => {
      const actual = await columnNames.getColumnNamesAsString('Worker', 'SavedAt');
      const expectedString = '"NameOrIdSavedAt", "MainJobFkSavedAt", "DateOfBirthSavedAt", "updated"';

      expect(actual).to.equal(expectedString);
    });
  });
});
