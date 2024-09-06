const qualificationColumn = (id, qualificationCode) => {
  return (
    'CASE WHEN EXISTS (SELECT 1 FROM "WorkerQualificationStats"' +
    ` WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = ${id} LIMIT 1)` +
    ` THEN 1 ELSE 0 END ${qualificationCode},`
  );
};

const qualificationYearColumn = (id, qualificationYearCode) => { 
  return '(SELECT "Year" FROM "WorkerQualificationStats" ' +
  `WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = ${id} LIMIT 1) ${qualificationYearCode},`;
}

module.exports = {
  qualificationColumn, 
  qualificationYearColumn
}