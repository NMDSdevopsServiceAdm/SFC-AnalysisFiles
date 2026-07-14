const generateCaseColumn = (columnName, alias, mapping, nullValue = -1) => {
  const columnRef =
    columnName.includes('.') || columnName.includes('"')
      ? columnName
      : `"${columnName}"`;

  const whenStatements = Object.entries(mapping)
    .map(([key, value]) => `WHEN ${columnRef} = '${key}' THEN ${value}`)
    .join('\n    ');

  return `
(
  CASE
    ${whenStatements}
    WHEN ${columnRef} IS NULL THEN ${nullValue}
    ELSE ${nullValue}
  END
) AS ${alias},
`;
};

const generateDateColumns = (databaseColumnName, analysisFileAliasName) => {
  return `
    TO_CHAR(e."${databaseColumnName}SavedAt",'DD/MM/YYYY') ${analysisFileAliasName}_savedate,
    TO_CHAR(e."${databaseColumnName}ChangedAt",'DD/MM/YYYY') ${analysisFileAliasName}_changedate,
  `;
};

module.exports = {
  generateCaseColumn,
  generateDateColumns,
};
