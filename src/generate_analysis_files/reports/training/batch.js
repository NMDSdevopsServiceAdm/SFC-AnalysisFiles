const db = require('../../db');
const { YESNO_TYPE_MAPPING,TRAINING_DELIVERY_MAPPING ,TRAINING_DELIVERY_TYPE_MAPPING,ESTABLISHMENT_TYPE_MAPPING} = require('../../../utils/sql/generate-columns/generate-mapping');
const { generateCaseColumn, generateDateColumns } = require('../../../utils/sql/generate-columns/generate-columns');


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
        ON e."EstablishmentID" = w."EstablishmentFK"
        JOIN "WorkerTraining" t
        ON w."ID" = t."WorkerFK" 
        AND e."Status" IS NULL
        AND e."Archived" = false AND w."Archived" = false;
    `,
    [runDate],
  );

await db.raw(`
  CREATE INDEX idx_batch_training_lookup
  ON "Afr4BatchiSkAi0mo" ("BatchNo", "TrainingID");
`);

await db.raw(`
  CREATE INDEX idx_batch_worker_lookup
  ON "Afr4BatchiSkAi0mo" ("BatchNo", "WorkerID");
`);

  await populateBatch(numInBatch); // can reuse your batch population function
};

const dropBatch = async () => {
  await db.schema.dropTableIfExists('Afr4BatchiSkAi0mo');
};

const getBatches = async () => db.select('BatchNo').from('Afr4BatchiSkAi0mo').groupBy(1).orderBy(1);

const buildFindTrainingsByBatchQuery = () => `
      SELECT
       'M' || DATE_PART('year',(b."RunDate" - INTERVAL '1 day')) || LPAD(DATE_PART('month',(b."RunDate" - INTERVAL '1 day'))::TEXT,2,'0') period,
       e."EstablishmentID" establishmentid,
       CASE WHEN e."IsParent" THEN e."EstablishmentID" ELSE CASE WHEN e."ParentID" IS NOT NULL THEN e."ParentID" ELSE e."EstablishmentID" END END orgid,
       e."NmdsID" nmdsid,
       w."ID" workerid,
       TO_CHAR(w."created",'DD/MM/YYYY') createddate,
       TO_CHAR(GREATEST(w."updated"),'DD/MM/YYYY') updateddate,

       COALESCE((
          SELECT "AnalysisFileCode"
          FROM "Job"
          WHERE "JobID" = w."MainJobFKValue"
       ), -1) mainjrid,

       COALESCE((
          SELECT "AnalysisFileCode"
          FROM services
          WHERE "id" = e."MainServiceFKValue"
       ), -1) mainstid,

       ${generateCaseColumn('e."EmployerTypeValue"', 'esttype', ESTABLISHMENT_TYPE_MAPPING)}

       COALESCE((
          SELECT "CssrID"
          FROM "Cssr"
          WHERE "NmdsIDLetter" = SUBSTRING(e."NmdsID", 1, 1)
            AND "LocalCustodianCode" IN (
              SELECT local_custodian_code
              FROM cqcref.pcodedata
              WHERE postcode = e."PostCode"
            )
          LIMIT 1
       ), -1) cssr,

      CASE 
          WHEN t."DataSource" = 'Bulk'
              THEN 1
          ELSE 0
      END derivedfrom_hasbulkuploaded,

       COALESCE((
          SELECT "AnalysisFileCode"
          FROM "CareWorkforcePathwayRoleCategories"
          WHERE "ID" = w."CareWorkforcePathwayRoleCategoryFK"
       ), -1) careworkforcepathway,
   
       t."Title" Training_name,
       t."ID" Training_id,
       TO_CHAR(t."created",'DD/MM/YYYY') training_savedate,
       TO_CHAR(GREATEST(t."updated"),'DD/MM/YYYY') training_changedate,

       COALESCE(tc."AnalysisFileCode", -1) AS Training_category,

       ${generateCaseColumn('t."Accredited"', 'Accredited_training', YESNO_TYPE_MAPPING)}

       ${generateCaseColumn('t."DeliveredBy"', 'Training_delivery', TRAINING_DELIVERY_MAPPING)}

       CASE
         WHEN tp."IsOther"
         THEN t."OtherTrainingProviderName"
         ELSE tp."Name"
       END AS Training_provider_name,

       ${generateCaseColumn('t."HowWasItDelivered"', 'Training_type', TRAINING_DELIVERY_TYPE_MAPPING)}

       t."ValidityPeriodInMonth" Training_valid_months,
       TO_CHAR(t."Completed",'DD/MM/YYYY') Training_completion_date,
       TO_CHAR(t."Expires",'DD/MM/YYYY') Training_expiry_date,

       CASE
         WHEN EXISTS (
           SELECT 1
           FROM "TrainingCertificates" cert
           WHERE cert."WorkerTrainingFK" = t."ID"
         )
         THEN 1
         ELSE 0
       END AS Train_cert_upload,

       CASE
         WHEN t."TrainingCourseFK" IS NOT NULL THEN 1
         ELSE 0
       END AS Training_linked

      FROM "Establishment" e
      JOIN "Worker" w ON e."EstablishmentID" = w."EstablishmentFK"
      JOIN "WorkerTraining" t ON w."ID" = t."WorkerFK"

      LEFT JOIN "TrainingCategories" tc
        ON tc."ID" = t."CategoryFK"

      LEFT JOIN "TrainingProvider" tp
        ON tp."ID" = t."TrainingProviderFK"

      JOIN "Afr4BatchiSkAi0mo" b
        ON b."TrainingID" = t."ID"

      WHERE
        b."BatchNo" = ?
        AND e."Status" IS NULL
        AND e."Archived" = false
        AND w."Archived" = false

      ORDER BY
        b."EstablishmentID",
        b."WorkerID"
`;

const findTrainingsByBatch = (batchNum) => {
  return db
    .raw(
      buildFindTrainingsByBatchQuery(),
      [batchNum]
    )
    .stream();
};


module.exports = {
  createBatchesForTraining,
  dropBatch,
  getBatches,
  findTrainingsByBatch,
  buildFindTrainingsByBatchQuery
};
