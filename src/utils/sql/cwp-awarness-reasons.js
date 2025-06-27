

const cwpAwarnessReasonColumn = (id, cwpAwarnessReasonCode) => {
  return (
    '( SELECT "AnalysisFileCode"  FROM   "CareWorkforcePathwayReasons" c  ' +
    ' JOIN "EstablishmentCWPReasons" ec on  c."ID" = ec."CareWorkforcePathwayReasonID" ' +
    `WHERE e."EstablishmentID" = ec."EstablishmentID"  AND "CareWorkforcePathwayReasonID"= ${id} LIMIT 1) ${cwpAwarnessReasonCode},`

  );
};

const generateSqlQueriesForCwpAwarnessReanosColumns = (cwpAwarnessReasonMappings) => {
  const sqlQueriesForEachCwpAwarnessReason = cwpAwarnessReasonMappings.map(({ id, cwpAwarnessReasonCode }) => 
   cwpAwarnessReasonColumn(id, cwpAwarnessReasonCode)
  );
  return sqlQueriesForEachCwpAwarnessReason.join('\n')
};

module.exports = {
  cwpAwarnessReasonColumn,
  generateSqlQueriesForCwpAwarnessReanosColumns,
};
