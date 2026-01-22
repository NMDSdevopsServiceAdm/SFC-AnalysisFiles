const db = require('../../db');


const populateBatch = async (numInBatch) => {
  await db.raw(
    `
    WITH numbered AS (
        SELECT
            ctid,
            ROW_NUMBER() OVER (ORDER BY "EstablishmentID", "WorkerID", "QualificationID") AS rn
        FROM "Afr3BatchiSkAi0mo"
    )
    UPDATE "Afr3BatchiSkAi0mo" b
    SET "BatchNo" = ((n.rn - 1) / ?) + 1
    FROM numbered n
    WHERE b.ctid = n.ctid;
    `,
    [numInBatch]
  );
};




const createBatchesForQualification = async (runDate, numInBatch =20000) => {
  await db.schema.dropTableIfExists('Afr3BatchiSkAi0mo');

  await db.raw(
    `
     CREATE TABLE "Afr3BatchiSkAi0mo" AS
        SELECT
            q."ID" AS "QualificationID",
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
        JOIN "WorkerQualifications" q
        ON w."ID" = q."WorkerFK" ;
    `,
    [runDate],
  );

  await db.raw('CREATE INDEX "Afr3BatchiSkAi0mo_idx" ON "Afr3BatchiSkAi0mo"("BatchNo");');
  await db.raw('CREATE INDEX idx_batch_training ON "Afr3BatchiSkAi0mo"("QualificationID");');
  await db.raw('CREATE INDEX idx_batch_worker ON "Afr3BatchiSkAi0mo"("WorkerID");');


  await populateBatch(numInBatch); // can reuse your batch population function
};

const dropBatch = async () => {
  await db.schema.dropTableIfExists('Afr3BatchiSkAi0mo');
};

const getBatches = async () => db.select('BatchNo').from('Afr3BatchiSkAi0mo').groupBy(1).orderBy(1);

const findQualificationsByBatch = (batchNum) => {
  return db
    .raw(
      `
      SELECT
        b."BatchNo",
        b."EstablishmentID",
        b."WorkerID",
        b."QualificationID"
      FROM "Afr3BatchiSkAi0mo" b
      WHERE b."BatchNo" = ?
      ORDER BY b."EstablishmentID", b."WorkerID";
      `,
      [batchNum]
    )
    .stream();
};


module.exports = {
  createBatchesForQualification,
  dropBatch,
  getBatches,
  findQualificationsByBatch,
};
