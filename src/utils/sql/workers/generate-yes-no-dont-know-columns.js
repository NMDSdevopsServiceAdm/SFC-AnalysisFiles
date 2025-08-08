const generateColumnsForYesNoDontKnowQuestion = (columnName, analysisFileVariable) => {
  return (
    `(
        SELECT CASE
            WHEN w."${columnName}Value" = 'No' THEN 2
            WHEN w."${columnName}Value" = 'Yes' THEN 1
            WHEN w."${columnName}Value" IS NULL THEN -1
            WHEN w."${columnName}Value" = 'Don''t know' THEN -2
        END
      ) ${analysisFileVariable},
      TO_CHAR(w."${columnName}ChangedAt",'DD/MM/YYYY') ${analysisFileVariable}_changedate,
      TO_CHAR(w."${columnName}SavedAt",'DD/MM/YYYY')  ${analysisFileVariable}_savedate`
  );
};

module.exports = {
    generateColumnsForYesNoDontKnowQuestion,
};