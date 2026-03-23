const generateCaseColumn = (columnName, alias, mapping, nullValue = -1) => {
  const whenStatements = Object.entries(mapping)
    .map(([key, value]) => `WHEN "${columnName}" = '${key}' THEN ${value}`)
    .join('\n    ');

  return `
(
  CASE
    ${whenStatements}
    WHEN "${columnName}" IS NULL THEN ${nullValue}
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
