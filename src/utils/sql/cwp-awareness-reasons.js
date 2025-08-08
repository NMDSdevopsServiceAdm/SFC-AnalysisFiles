

const cwpAwarenessReasonColumn = (id, cwpAwarenessReasonCode) => {
  return (
    '(SELECT "AnalysisFileCode" FROM "CareWorkforcePathwayReasons" c ' +
    'JOIN "EstablishmentCWPReasons" ec on c."ID" = ec."CareWorkforcePathwayReasonID" ' +
    `WHERE e."EstablishmentID" = ec."EstablishmentID" AND "CareWorkforcePathwayReasonID"= ${id} LIMIT 1) ${cwpAwarenessReasonCode},`

  );
};

const generateSqlQueriesForCwpAwarenessReasonsColumns = (cwpAwarenessReasonMappings) => {
  const sqlQueriesForEachCwpAwarenessReason = cwpAwarenessReasonMappings.map(({ id, cwpAwarenessReasonCode }) => 
   cwpAwarenessReasonColumn(id, cwpAwarenessReasonCode)
  );
  return sqlQueriesForEachCwpAwarenessReason.join('\n')
};

module.exports = {
  cwpAwarenessReasonColumn,
  generateSqlQueriesForCwpAwarenessReasonsColumns,
};
