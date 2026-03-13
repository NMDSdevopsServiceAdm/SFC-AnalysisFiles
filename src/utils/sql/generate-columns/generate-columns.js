
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



module.exports = {
generateCaseColumn
};
