const db = require('../../db');


const populateBatch = async (numInBatch) => {
  await db.raw(
    `
    WITH numbered AS (
        SELECT
            ctid,
            ROW_NUMBER() OVER (ORDER BY "EstablishmentID", "WorkerID", "TrainingID") AS rn
        FROM "Afr4BatchiSkAi0mo"
    )
    UPDATE "Afr4BatchiSkAi0mo" b
    SET "BatchNo" = ((n.rn - 1) / ?) + 1
    FROM numbered n
    WHERE b.ctid = n.ctid;
    `,
    [numInBatch]
  );
};




const createBatchesForTraining = async (runDate, numInBatch =20000) => {
  await db.schema.dropTableIfExists('Afr4BatchiSkAi0mo');

  await db.raw(
    `
     CREATE TABLE "Afr4BatchiSkAi0mo" AS
        SELECT
            t."ID" AS "TrainingID",
            w."ID" As "WorkerID",
            e."EstablishmentID",
            NULL::INT AS "BatchNo",
            TO_DATE(?, 'DD-MM-YYYY')::DATE AS "RunDate"
        FROM "Establishment" e
        JOIN "Worker" w
        ON e."EstablishmentID" = w."EstablishmentFK" AND
        e."Archived" = w."Archived" 
        AND e."Status" IS NULL
        AND w."Archived" = false 
        JOIN "WorkerTraining" t
        ON w."ID" = t."WorkerFK" ;
    `,
    [runDate],
  );

  await db.raw('CREATE INDEX "Afr3BatchiSkAi0mo_idx" ON "Afr4BatchiSkAi0mo"("BatchNo");');
  await db.raw('CREATE INDEX idx_batch_training ON "Afr4BatchiSkAi0mo"("TrainingID");');
  await db.raw('CREATE INDEX idx_batch_worker ON "Afr4BatchiSkAi0mo"("WorkerID");');


  await populateBatch(numInBatch); // can reuse your batch population function
};

const dropBatch = async () => {
  await db.schema.dropTableIfExists('Afr4BatchiSkAi0mo');
};

const getBatches = async () => db.select('BatchNo').from('Afr4BatchiSkAi0mo').groupBy(1).orderBy(1);

const findTrainingsByBatch = (batchNum) => {
  return db
    .raw(
      `
      SELECT
        b."BatchNo",
        b."EstablishmentID",
        b."WorkerID",
        b."TrainingID"
      FROM "Afr4BatchiSkAi0mo" b
      WHERE b."BatchNo" = ?
      ORDER BY b."EstablishmentID", b."WorkerID";
      `,
      [batchNum]
    )
    .stream();
};


module.exports = {
  createBatchesForTraining,
  dropBatch,
  getBatches,
  findTrainingsByBatch,
};
