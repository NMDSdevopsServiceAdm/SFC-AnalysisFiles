const db = require('../db');

const refreshViews = async () => {
  console.log('Refreshing Materialized Views');

  await db.raw('REFRESH MATERIALIZED VIEW "WorkerTrainingStats"');
  await db.raw('REFRESH MATERIALIZED VIEW "WorkerQualificationStats"');
  await db.raw('REFRESH MATERIALIZED VIEW "WorkerContractStats"');
  await db.raw('REFRESH MATERIALIZED VIEW "WorkerJobStats"');
};

module.exports = {
  refreshViews,
};
