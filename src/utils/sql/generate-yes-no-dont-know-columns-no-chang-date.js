const generateColumnsForYesNoDontKnowQuestionNoChangeDate = (columnName, analysisFileVariable) => {
  return (
    `(
        SELECT CASE
            WHEN "${columnName}" = 'No' THEN 0
            WHEN "${columnName}" = 'Yes' THEN 1
            WHEN "${columnName}" IS NULL THEN -1
            WHEN "${columnName}" = 'Don''t know' THEN -2
        END
      ) ${analysisFileVariable},`
  );
};

module.exports = {
generateColumnsForYesNoDontKnowQuestionNoChangeDate};