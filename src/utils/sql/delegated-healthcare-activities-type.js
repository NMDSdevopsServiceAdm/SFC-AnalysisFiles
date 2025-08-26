

const dhaActivityTypeColumn = (id, dhaActivitiesType) => {
  return (
    '( SELECT "AnalysisFileCode"  FROM   "DelegatedHealthcareActivities" d  ' +
    ' JOIN "EstablishmentDHActivities" ed on  d."ID" = ed."DelegatedHealthcareActivitiesID" ' +
    `WHERE e."EstablishmentID" = ed."EstablishmentID"  AND ed."DelegatedHealthcareActivitiesID"= ${id} LIMIT 1) ${dhaActivitiesType},`

  );
};

const generateSqlQueriesForDhaActivitiesTypeColumns = (dhaActivitiesTypeMappings) => {
  const sqlQueriesForEachDhaActivityType = dhaActivitiesTypeMappings.map(({ id, dhaActivitiesTypeCode }) => 
   dhaActivityTypeColumn(id, dhaActivitiesTypeCode)
  );
  return sqlQueriesForEachDhaActivityType.join('\n')
};

module.exports = {
  dhaActivityTypeColumn,
  generateSqlQueriesForDhaActivitiesTypeColumns,
};
