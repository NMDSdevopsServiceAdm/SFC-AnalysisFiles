

const nursesQuestionAnswersColumn = (id, nursesQuestionAnswersCode) => {
  return (
    `CASE ` +
    `WHEN EXISTS (` +
      `SELECT 1 ` +
      `FROM "WorkerNurseFieldsOfPractice" wn ` +
      `WHERE wn."WorkerID" = w."ID" ` +
      `AND wn."NurseFieldOfPracticeID" = ${id}` +
    `) THEN 1 ` +
    `WHEN w."MainJobFKValue" = 23 ` +
      `AND NOT EXISTS (` +
        `SELECT 1 ` +
        `FROM "WorkerNurseFieldsOfPractice" wn ` +
        `WHERE wn."WorkerID" = w."ID"` +
      `) THEN -1 ` +
    `ELSE 0 ` +
    `END ${nursesQuestionAnswersCode},`
  );
};

const generateSqlQueriesForNursesQuestionAnswersColumns = (nursesQuestionAnswersMappings) => {
  const sqlQueriesForEachNursesQuestionAnswers = nursesQuestionAnswersMappings.map(({ id, nursesQuestionAnswersCode }) => 
   nursesQuestionAnswersColumn(id, nursesQuestionAnswersCode)
  );
  return sqlQueriesForEachNursesQuestionAnswers.join('\n')
};

module.exports = {
  nursesQuestionAnswersColumn,
  generateSqlQueriesForNursesQuestionAnswersColumns,
};
