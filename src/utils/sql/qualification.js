const qualificationColumn = (id, qualificationCode) => {
  return (
    'CASE WHEN EXISTS (SELECT 1 FROM "WorkerQualificationStats"' +
    ` WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = ${id} LIMIT 1)` +
    ` THEN 1 ELSE 0 END ${qualificationCode},`
  );
};

const qualificationYearColumn = (id, qualificationYearCode) => {
  return (
    '(SELECT "Year" FROM "WorkerQualificationStats" ' +
    `WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = ${id} LIMIT 1) ${qualificationYearCode},`
  );
};

const generateSqlQueriesForQualificationColumns = (qualificationMappings) => {
  const sqlQueriesForEachQualification = qualificationMappings.map(({ id, qualificationCode, qualificationYearCode }) => 
    qualificationColumn(id, qualificationCode) + '\n' + qualificationYearColumn(id, qualificationYearCode)
  );
  return sqlQueriesForEachQualification.join('\n')
};

module.exports = {
  qualificationColumn,
  qualificationYearColumn,
  generateSqlQueriesForQualificationColumns,
};
