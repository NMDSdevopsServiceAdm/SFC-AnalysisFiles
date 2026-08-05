class ColumnNamesUtil {
  constructor(db) {
    this._db = db;
    this.resetCache();

    this._establishmentTable = 'Establishment';
    this._workerTable = 'Worker';
  }

  resetCache() {
    this._cache = {
      Establishment: { ChangedAt: [], SavedAt: [] },
      Worker: { ChangedAt: [], SavedAt: [] },
    };
  }

  async reloadColumnNamesFromDb(db) {
    this._db = db;
    this.resetCache();

    this._cache.Establishment.ChangedAt = await this.getColumnNamesWithPattern(this._establishmentTable, '%ChangedAt');
    this._cache.Establishment.SavedAt = await this.getColumnNamesWithPattern(this._establishmentTable, '%SavedAt');
    this._cache.Worker.ChangedAt = await this.getColumnNamesWithPattern(this._workerTable, '%ChangedAt');
    this._cache.Worker.SavedAt = await this.getColumnNamesWithPattern(this._workerTable, '%SavedAt');
  }

  async getColumnNamesWithPattern(tableName, pattern) {
    const rawQuery = `
    SELECT column_name
    FROM information_schema.columns
    WHERE table_schema = 'cqc'
      AND table_name = ?
      AND (column_name LIKE ?);
    `;
    const result = await this._db.raw(rawQuery, [tableName, pattern]);

    if (result?.rows?.length) {
      const columnNames = result.rows.map((res) => res.column_name);
      return columnNames;
    }

    throw new Error('Failed to load column names from database');
  }

  joinColumnNames(columnNames, tableNameShortHand) {
    if (tableNameShortHand) {
      return columnNames.map((col) => `${tableNameShortHand}."${col}"`).join(', ');
    }

    return columnNames.map((col) => `"${col}"`).join(', ');
  }

  getColumnNamesAsString(tableName, changedAtOrSavedAt, tableNameShortHand = null) {
    const columnNames = this._cache?.[tableName]?.[changedAtOrSavedAt];
    if (!columnNames) {
      throw new Error('column names could not be found in cache');
    }
    if (!['ChangedAt', 'SavedAt'].includes(changedAtOrSavedAt)) {
      throw new Error('changedAtOrSavedAt is not valid');
    }

    const columnNamesWithUpdateColumn = [...columnNames, 'updated'];

    if (tableNameShortHand) {
      return columnNamesWithUpdateColumn.map((col) => `${tableNameShortHand}."${col}"`).join(', ');
    }

    return columnNamesWithUpdateColumn.map((col) => `"${col}"`).join(', ');
  }
}

module.exports = {
  ColumnNamesUtil,
};
