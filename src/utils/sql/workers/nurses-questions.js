

const nursesQuestionAnswersColumn = (id, nursesQuestionAnswersCode) => {
  return (
    '(SELECT "AnalysisFileCode" FROM "NurseFieldOfPractice" n ' +
    'JOIN "WorkerNurseFieldsOfPractice" wn on n."ID" = wn."NurseFieldOfPracticeID" ' +
    `WHERE w."ID" = wn."WorkerID" AND "NurseFieldOfPracticeID"= ${id} LIMIT 1) ${nursesQuestionAnswersCode},`

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
