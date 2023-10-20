const db = require('../../generate_analysis_files/db');

const getUnassignedBatchCount = async () => {
  const { count } = (await db('Afr1BatchiSkAi0mo').whereNull('BatchNo').count().first()) || {};

  return count;
};

const populateBatch = async (numInBatch) => {
  let batchNum = 1;


  while ((await getUnassignedBatchCount()) > 0) {
    await db.raw(
      `
          UPDATE "Afr1BatchiSkAi0mo" 
              SET "BatchNo" = ? 
              WHERE "EstablishmentID" IN (
                  SELECT "EstablishmentID" FROM "Afr1BatchiSkAi0mo" WHERE "BatchNo" IS NULL LIMIT ?
              );
        `,
      [batchNum, numInBatch],
    );

    batchNum++;
  }
};

const createBatches = async (runDate, numInBatch = 2000) => {
  await db.schema.dropTableIfExists('Afr1BatchiSkAi0mo');

  await db.raw(
    `
    CREATE TABLE "Afr1BatchiSkAi0mo" AS
        SELECT "EstablishmentID",
            ROW_NUMBER() OVER (ORDER BY "EstablishmentID") "SerialNo",
            NULL::INT "BatchNo",
            TO_DATE(?,'DD-MM-YYYY')::DATE AS "RunDate"
        FROM "Establishment"
        WHERE 
            "Archived" = false
            AND 
            "Status" IS NULL;
      `,
    [runDate],
  );

  await db.raw('CREATE INDEX "Afr1BatchiSkAi0mo_idx" ON "Afr1BatchiSkAi0mo"("BatchNo")');

  await populateBatch(numInBatch);
};

const dropBatch = async () => {
  await db.schema.dropTableIfExists('Afr1BatchiSkAi0mo');
};

const getBatches = async () => db.select('BatchNo').from('Afr1BatchiSkAi0mo').groupBy(1).orderBy(1);

const findWorkplacesByBatch = (batchNum) =>
  db
    .raw(
      `
      SELECT e."NmdsID" AS WorkplaceID,
       e."NameValue" As WorkplaceName,
      CASE e."IsRegulated" 
      WHEN true 
           THEN 'Yes'
      WHEN false 
           THEN 'No'
      END IsRegulatedWithCQC,
     e."NumberOfStaffValue" AS Totalstaff, 
    CASE e."CurrentWdfEligibility" 
      WHEN true 
           THEN 'Yes'
      WHEN false 
           THEN 'No'
      END CurrentWdfEligibility  
    FROM "Establishment" e
    JOIN "Afr1BatchiSkAi0mo" b ON e."EstablishmentID" = b."EstablishmentID"
      AND b."BatchNo" = ?`,
      [batchNum],
    )
    .stream();

module.exports = {
  createBatches,
  dropBatch,
  getBatches,
  findWorkplacesByBatch,
};

