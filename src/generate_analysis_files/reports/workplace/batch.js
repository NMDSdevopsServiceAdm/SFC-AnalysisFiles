const db = require('../../db');
const { jobRoleGroups } = require('./jobRoleGroups');

const getUnassignedBatchCount = async () => {
  const { count } = (await db('Afr1BatchiSkAi0mo').whereNull('BatchNo').count().first()) || {};

  return count;
};

const populateBatch = async (numInBatch) => {
  let batchNum = 1;

  // TODO - functionise this

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
      SELECT 'M' || DATE_PART('year', (b."RunDate" - INTERVAL '1 day')) || LPAD(DATE_PART('month', (b."RunDate" - INTERVAL '1 day'))::TEXT, 2, '0') period,
      e."EstablishmentID" establishmentid,
      "TribalID" tribalid,
      "ParentID" parentid,
      CASE 
          WHEN e."IsParent"
              THEN e."EstablishmentID"
          ELSE CASE 
                  WHEN e."ParentID" IS NOT NULL
                      THEN e."ParentID"
                  ELSE e."EstablishmentID"
                  END
          END orgid,
      "NmdsID" nmdsid,
      1 wkplacestat,
      TO_CHAR("created", 'DD/MM/YYYY') estabcreateddate,
      (
          SELECT COUNT(1)
          FROM "User" u
          JOIN "UserAudit" a ON u."RegistrationID" = a."UserFK"
              AND a."When" >= b."RunDate" - INTERVAL '1 month'
              AND a."EventType" = 'loginSuccess'
          WHERE u."EstablishmentID" = e."EstablishmentID"
              AND u."Archived" = false
          ) logincount_month,
      (
          SELECT COUNT(1)
          FROM "User" u
          JOIN "UserAudit" a ON u."RegistrationID" = a."UserFK"
              AND a."When" >= b."RunDate" - INTERVAL '1 year'
              AND a."EventType" = 'loginSuccess'
          WHERE u."EstablishmentID" = e."EstablishmentID"
              AND u."Archived" = false
          ) logincount_year,
      (
          SELECT TO_CHAR(MAX(a."When"), 'DD/MM/YYYY')
          FROM "User" u
          JOIN "UserAudit" a ON u."RegistrationID" = a."UserFK"
              AND a."EventType" = 'loginSuccess'
          WHERE u."EstablishmentID" = e."EstablishmentID"
              AND u."Archived" = false
          ) lastloggedin,
      TO_CHAR((
              SELECT "When"
              FROM (
                  SELECT a."When"
                  FROM "User" u
                  JOIN "UserAudit" a ON u."RegistrationID" = a."UserFK"
                      AND a."EventType" = 'loginSuccess'
                  WHERE u."EstablishmentID" = e."EstablishmentID"
                      AND u."Archived" = false
                  ORDER BY 1 DESC LIMIT 2
                  ) x
              ORDER BY 1 LIMIT 1
              ), 'DD/MM/YYYY') previous_logindate,
        (SELECT COUNT(DISTINCT changedate) FROM (
            SELECT to_char("When", 'yyyy-mm-dd') changeDate
            FROM "EstablishmentAudit"
            WHERE "EstablishmentFK" = e."EstablishmentID"
            AND "EventType" = 'changed'
            AND "When" >= b."RunDate" - INTERVAL '1 month'
            GROUP BY changeDate
            union
            SELECT DISTINCT changedate FROM (
                SELECT DISTINCT a."WorkerFK", to_char(a."When", 'yyyy-mm-dd') changeDate
                FROM cqc."WorkerAudit" a
                JOIN cqc."Worker" w ON a."WorkerFK" = w."ID"
                AND a."EventType" = 'changed'
                AND a."When" >= b."RunDate" - INTERVAL '1 month'
                WHERE w."EstablishmentFK" = e."EstablishmentID"
                AND w."Archived" = false
            ) as wrkAudit
        ) as chngDate) updatecount_month,
        (SELECT COUNT(DISTINCT changedate) FROM (
            SELECT to_char("When", 'yyyy-mm-dd') changeDate
            FROM "EstablishmentAudit"
            WHERE "EstablishmentFK" = e."EstablishmentID"
            AND "EventType" = 'changed'
            AND "When" >= b."RunDate" - INTERVAL '1 year'
            GROUP BY changeDate
            union
            SELECT DISTINCT changedate FROM (
                SELECT DISTINCT a."WorkerFK", to_char(a."When", 'yyyy-mm-dd') changeDate
                FROM cqc."WorkerAudit" a
                JOIN cqc."Worker" w ON a."WorkerFK" = w."ID"
                AND a."EventType" = 'changed'
                AND a."When" >= b."RunDate" - INTERVAL '1 year'
                WHERE w."EstablishmentFK" = e."EstablishmentID"
                AND w."Archived" = false
            ) as wrkAudit
        ) as chngDate) updatecount_year,
      -- TO_CHAR(GREATEST(created,updated),'DD/MM/YYYY') estabupdateddate,
      TO_CHAR(GREATEST(e."EmployerTypeChangedAt", e."NumberOfStaffChangedAt", e."OtherServicesChangedAt", e."CapacityServicesChangedAt", e."ShareDataChangedAt", e."VacanciesChangedAt", e."StartersChangedAt", e."LeaversChangedAt", e."ServiceUsersChangedAt", e."NameChangedAt", e."MainServiceFKChangedAt", e."LocalIdentifierChangedAt", e."LocationIdChangedAt", e."Address1ChangedAt", e."Address2ChangedAt", e."Address3ChangedAt", e."TownChangedAt", e."CountyChangedAt", e."PostcodeChangedAt"), 'DD/MM/YYYY') estabupdateddate,
      TO_CHAR(GREATEST(e."EmployerTypeSavedAt", e."NumberOfStaffSavedAt", e."OtherServicesSavedAt", e."CapacityServicesSavedAt", e."ShareDataSavedAt", e."VacanciesSavedAt", e."StartersSavedAt", e."LeaversSavedAt", e."ServiceUsersSavedAt", e."NameSavedAt", e."MainServiceFKSavedAt", e."LocalIdentifierSavedAt", e."LocationIdSavedAt", e."Address1SavedAt", e."Address2SavedAt", e."Address3SavedAt", e."TownSavedAt", e."CountySavedAt", e."PostcodeSavedAt"), 'DD/MM/YYYY') estabsavedate,
      (
          SELECT TO_CHAR(MAX(GREATEST("NameOrIdChangedAt", "ContractChangedAt", "MainJobFKChangedAt", "ApprovedMentalHealthWorkerChangedAt", "MainJobStartDateChangedAt", "OtherJobsChangedAt", "NationalInsuranceNumberChangedAt", "DateOfBirthChangedAt", "PostcodeChangedAt", "DisabilityChangedAt", "GenderChangedAt", "EthnicityFKChangedAt", "NationalityChangedAt", "CountryOfBirthChangedAt", "RecruitedFromChangedAt", "BritishCitizenshipChangedAt", "YearArrivedChangedAt", "SocialCareStartDateChangedAt", "DaysSickChangedAt", "ZeroHoursContractChangedAt", "WeeklyHoursAverageChangedAt", "WeeklyHoursContractedChangedAt", "AnnualHourlyPayChangedAt", "CareCertificateChangedAt", "ApprenticeshipTrainingChangedAt", "QualificationInSocialCareChangedAt", "SocialCareQualificationFKChangedAt", "OtherQualificationsChangedAt", "HighestQualificationFKChangedAt", "CompletedChangedAt", "RegisteredNurseChangedAt", "NurseSpecialismFKChangedAt", "LocalIdentifierChangedAt", "EstablishmentFkChangedAt", "FluJabChangedAt")), 'DD/MM/YYYY')
          FROM "Worker"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "Archived" = false
      ) workerupdate,
     TO_CHAR(GREATEST(e.updated, (
                  SELECT MAX(updated)
                  FROM "Worker"
                  WHERE "EstablishmentFK" = e."EstablishmentID"
                      AND "Archived" = false
                  )), 'DD/MM/YYYY') mupddate,
      TO_CHAR(GREATEST((
        CASE 
            WHEN e.updated < GREATEST(e.updated, (
                        SELECT MAX(updated)
                        FROM "Worker"
                        WHERE "EstablishmentFK" = e."EstablishmentID"
                            AND "Archived" = false
                        ))
                THEN e.updated
            ELSE NULL
            END
        ), (
        SELECT MAX(updated)
        FROM "Worker"
        WHERE "EstablishmentFK" = e."EstablishmentID"
            AND "Archived" = false
            AND updated < GREATEST(e.updated, (
                    SELECT MAX(updated)
                    FROM "Worker"
                    WHERE "EstablishmentFK" = e."EstablishmentID"
                        AND "Archived" = false
                    ))
        )), 'DD/MM/YYYY') previous_mupddate,
      CASE 
          WHEN "DataSource" = 'Bulk'
              THEN 1
          ELSE 0
      END derivedfrom_hasbulkuploaded,
      CASE 
          WHEN "LastBulkUploaded" IS NULL
              THEN 0
          ELSE 1
          END isbulkuploader,
      TO_CHAR("LastBulkUploaded", 'DD/MM/YYYY') lastbulkuploaddate,
      CASE "IsParent"
          WHEN true
              THEN 1
          ELSE 0
          END isparent,
      CASE "DataOwner"
          WHEN 'Parent'
              THEN 1
          WHEN 'Workplace'
              THEN 2
          ELSE 3
          END parentpermission,
      (
          SELECT TO_CHAR(MAX(GREATEST("NameOrIdSavedAt", "ContractSavedAt", "MainJobFKSavedAt", "ApprovedMentalHealthWorkerSavedAt", "MainJobStartDateSavedAt", "OtherJobsSavedAt", "NationalInsuranceNumberSavedAt", "DateOfBirthSavedAt", "PostcodeSavedAt", "DisabilitySavedAt", "GenderSavedAt", "EthnicityFKSavedAt", "NationalitySavedAt", "CountryOfBirthSavedAt", "RecruitedFromSavedAt", "BritishCitizenshipSavedAt", "YearArrivedSavedAt", "SocialCareStartDateSavedAt", "DaysSickSavedAt", "ZeroHoursContractSavedAt", "WeeklyHoursAverageSavedAt", "WeeklyHoursContractedSavedAt", "AnnualHourlyPaySavedAt", "CareCertificateSavedAt", "ApprenticeshipTrainingSavedAt", "QualificationInSocialCareSavedAt", "SocialCareQualificationFKSavedAt", "OtherQualificationsSavedAt", "HighestQualificationFKSavedAt", "CompletedSavedAt", "RegisteredNurseSavedAt", "NurseSpecialismFKSavedAt", "LocalIdentifierSavedAt", "EstablishmentFkSavedAt", "FluJabSavedAt")), 'DD/MM/YYYY')
          FROM "Worker"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "Archived" = false
          ) workersavedate,
      CASE "ShareDataWithCQC"
          WHEN true
              THEN 1
          WHEN false
              THEN 0
          ELSE -1
          END cqcpermission,
      CASE "ShareDataWithLA"
          WHEN true
              THEN 1
          WHEN false
              THEN 0
          ELSE -1
          END lapermission,
      CASE 
          WHEN "IsRegulated" IS true
              THEN 2
          ELSE 0
          END regtype,
      "ProvID" providerid,
      "LocationID" locationid,
      CASE "EmployerTypeValue"
          WHEN 'Local Authority (adult services)'
              THEN 1
          WHEN 'Local Authority (generic/other)'
              THEN 3
          WHEN 'Private Sector'
              THEN 6
          WHEN 'Voluntary / Charity'
              THEN 7
          WHEN 'Other'
              THEN 8
          END esttype,
      TO_CHAR("EmployerTypeChangedAt", 'DD/MM/YYYY') esttype_changedate,
      TO_CHAR("EmployerTypeSavedAt", 'DD/MM/YYYY') esttype_savedate,
      RTRIM(regexp_replace("NameValue", '(\\\s{2,})|([\\n\\r])|(\\\s*,\\\s*)', ' ', 'g' )) establishmentname,
      RTRIM(regexp_replace("Address1", '(\\\s{2,})|([\\n\\r])|(\\\s*,\\\s*)', ' ', 'g' )) address,
      "PostCode" postcode,
      COALESCE((
              SELECT "RegionID"
              FROM "Cssr"
              WHERE "NmdsIDLetter" = SUBSTRING(e."NmdsID", 1, 1) LIMIT 1
              ), NULL, - 1) regionid,
      COALESCE((
              SELECT "CssrID"
              FROM "Cssr"
              WHERE "NmdsIDLetter" = SUBSTRING(e."NmdsID", 1, 1)
                  AND "LocalCustodianCode" IN (
                      SELECT local_custodian_code
                      FROM cqcref.pcodedata
                      WHERE postcode = e."PostCode"
                      ) LIMIT 1
              ), NULL, - 1) cssr,
      (
          SELECT CASE "LocalAuthority"
                  WHEN 'Mid Bedfordshire'
                      THEN 1 -- Does not exists in database.
                  WHEN 'Bedford'
                      THEN 2
                  WHEN 'South Bedfordshire'
                      THEN 3 -- Does not exists in database.
                  WHEN 'Cambridge'
                      THEN 4
                  WHEN 'East Cambridgeshire'
                      THEN 5
                  WHEN 'Fenland'
                      THEN 6
                  WHEN 'Huntingdonshire'
                      THEN 7
                  WHEN 'South Cambridgeshire'
                      THEN 8
                  WHEN 'Basildon'
                      THEN 9
                  WHEN 'Braintree'
                      THEN 10
                  WHEN 'Brentwood'
                      THEN 11
                  WHEN 'Castle Point'
                      THEN 12
                  WHEN 'Chelmsford'
                      THEN 13
                  WHEN 'Colchester'
                      THEN 14
                  WHEN 'Epping Forest'
                      THEN 15
                  WHEN 'Harlow'
                      THEN 16
                  WHEN 'Maldon'
                      THEN 17
                  WHEN 'Rochford'
                      THEN 18
                  WHEN 'Tendring'
                      THEN 19
                  WHEN 'Uttlesford'
                      THEN 20
                  WHEN 'Broxbourne'
                      THEN 21
                  WHEN 'Dacorum'
                      THEN 22
                  WHEN 'East Hertfordshire'
                      THEN 23
                  WHEN 'Hertsmere'
                      THEN 24
                  WHEN 'North Hertfordshire'
                      THEN 25
                  WHEN 'St Albans'
                      THEN 26
                  WHEN 'Stevenage'
                      THEN 27
                  WHEN 'Three Rivers'
                      THEN 28
                  WHEN 'Watford'
                      THEN 29
                  WHEN 'Welwyn Hatfield'
                      THEN 30
                  WHEN 'Luton'
                      THEN 31
                  WHEN 'Breckland'
                      THEN 32
                  WHEN 'Broadland'
                      THEN 33
                  WHEN 'Great Yarmouth'
                      THEN 34
                  WHEN 'King\`s Lynn and West Norfolk'
                      THEN 35
                  WHEN 'North Norfolk'
                      THEN 36
                  WHEN 'Norwich'
                      THEN 37
                  WHEN 'South Norfolk'
                      THEN 38
                  WHEN 'Peterborough'
                      THEN 39
                  WHEN 'Southend-on-Sea'
                      THEN 40
                  WHEN 'Babergh'
                      THEN 41
                  WHEN 'Forest Heath'
                      THEN 42
                  WHEN 'Ipswich'
                      THEN 43
                  WHEN 'Mid Suffolk'
                      THEN 44
                  WHEN 'St. Edmundsbury'
                      THEN 45
                  WHEN 'Suffolk Coastal'
                      THEN 46
                  WHEN 'Waveney'
                      THEN 47
                  WHEN 'Thurrock'
                      THEN 48
                  WHEN 'Derby'
                      THEN 49
                  WHEN 'Amber Valley'
                      THEN 50
                  WHEN 'Bolsover'
                      THEN 51
                  WHEN 'Chesterfield'
                      THEN 52
                  WHEN 'Derbyshire Dales'
                      THEN 53
                  WHEN 'Erewash'
                      THEN 54
                  WHEN 'High Peak'
                      THEN 55
                  WHEN 'North East Derbyshire'
                      THEN 56
                  WHEN 'South Derbyshire'
                      THEN 57
                  WHEN 'Leicester'
                      THEN 58
                  WHEN 'Blaby'
                      THEN 59
                  WHEN 'Charnwood'
                      THEN 60
                  WHEN 'Harborough'
                      THEN 61
                  WHEN 'Hinckley and Bosworth'
                      THEN 62
                  WHEN 'Melton'
                      THEN 63
                  WHEN 'North West Leicestershire'
                      THEN 64
                  WHEN 'Oadby and Wigston'
                      THEN 65
                  WHEN 'Boston'
                      THEN 66
                  WHEN 'East Lindsey'
                      THEN 67
                  WHEN 'Lincoln'
                      THEN 68
                  WHEN 'North Kesteven'
                      THEN 69
                  WHEN 'South Holland'
                      THEN 70
                  WHEN 'South Kesteven'
                      THEN 71
                  WHEN 'West Lindsey'
                      THEN 72
                  WHEN 'Corby'
                      THEN 73
                  WHEN 'Daventry'
                      THEN 74
                  WHEN 'East Northamptonshire'
                      THEN 75
                  WHEN 'Kettering'
                      THEN 76
                  WHEN 'Northampton'
                      THEN 77
                  WHEN 'South Northamptonshire'
                      THEN 78
                  WHEN 'Wellingborough'
                      THEN 79
                  WHEN 'Nottingham'
                      THEN 80
                  WHEN 'Ashfield'
                      THEN 81
                  WHEN 'Bassetlaw'
                      THEN 82
                  WHEN 'Broxtowe'
                      THEN 83
                  WHEN 'Gedling'
                      THEN 84
                  WHEN 'Mansfield'
                      THEN 85
                  WHEN 'Newark and Sherwood'
                      THEN 86
                  WHEN 'Rushcliffe'
                      THEN 87
                  WHEN 'Rutland'
                      THEN 88
                  WHEN 'Barking and Dagenham'
                      THEN 89
                  WHEN 'Barnet'
                      THEN 90
                  WHEN 'Bexley'
                      THEN 91
                  WHEN 'Brent'
                      THEN 92
                  WHEN 'Bromley'
                      THEN 93
                  WHEN 'Camden'
                      THEN 94
                  WHEN 'City of London'
                      THEN 95
                  WHEN 'Croydon'
                      THEN 96
                  WHEN 'Ealing'
                      THEN 97
                  WHEN 'Enfield'
                      THEN 98
                  WHEN 'Greenwich'
                      THEN 99
                  WHEN 'Hackney'
                      THEN 100
                  WHEN 'Hammersmith and Fulham'
                      THEN 101
                  WHEN 'Haringey'
                      THEN 102
                  WHEN 'Harrow'
                      THEN 103
                  WHEN 'Havering'
                      THEN 104
                  WHEN 'Hillingdon'
                      THEN 105
                  WHEN 'Hounslow'
                      THEN 106
                  WHEN 'Islington'
                      THEN 107
                  WHEN 'Kensington and Chelsea'
                      THEN 108
                  WHEN 'Kingston upon Thames'
                      THEN 109
                  WHEN 'Lambeth'
                      THEN 110
                  WHEN 'Lewisham'
                      THEN 111
                  WHEN 'Merton'
                      THEN 112
                  WHEN 'Newham'
                      THEN 113
                  WHEN 'Redbridge'
                      THEN 114
                  WHEN 'Richmond upon Thames'
                      THEN 115
                  WHEN 'Southwark'
                      THEN 116
                  WHEN 'Sutton'
                      THEN 117
                  WHEN 'Tower Hamlets'
                      THEN 118
                  WHEN 'Waltham Forest'
                      THEN 119
                  WHEN 'Wandsworth'
                      THEN 120
                  WHEN 'Westminster'
                      THEN 121
                  WHEN 'Darlington'
                      THEN 122
                  WHEN 'Chester-le-Street'
                      THEN 123 -- Does not exists in database.
                  WHEN 'Derwentside'
                      THEN 124 -- Does not exists in database.
                  WHEN 'Durham'
                      THEN 125 -- Does not exists in database.
                  WHEN 'Easington'
                      THEN 126 -- Does not exists in database.
                  WHEN 'Sedgefield'
                      THEN 127 -- Does not exists in database.
                  WHEN 'Teesdale'
                      THEN 128 -- Does not exists in database.
                  WHEN 'Wear Valley'
                      THEN 129 -- Does not exists in database.
                  WHEN 'Gateshead'
                      THEN 130
                  WHEN 'Hartlepool'
                      THEN 131
                  WHEN 'Middlesbrough'
                      THEN 132
                  WHEN 'Newcastle upon Tyne'
                      THEN 133
                  WHEN 'North Tyneside'
                      THEN 134
                  WHEN 'Alnwick'
                      THEN 135 -- Does not exists in database.
                  WHEN 'Berwick-upon-Tweed'
                      THEN 136 -- Does not exists in database.
                  WHEN 'Blyth Valley'
                      THEN 137 -- Does not exists in database.
                  WHEN 'Castle Morpeth'
                      THEN 138 -- Does not exists in database.
                  WHEN 'Tynedale'
                      THEN 139 -- Does not exists in database.
                  WHEN 'Wansbeck'
                      THEN 140 -- Does not exists in database.
                  WHEN 'Redcar and Cleveland'
                      THEN 141
                  WHEN 'South Tyneside'
                      THEN 142
                  WHEN 'Stockton-on-Tees'
                      THEN 143
                  WHEN 'Sunderland'
                      THEN 144
                  WHEN 'Blackburn with Darwen'
                      THEN 145
                  WHEN 'Blackpool'
                      THEN 146
                  WHEN 'Bolton'
                      THEN 147
                  WHEN 'Bury'
                      THEN 148
                  WHEN 'Chester'
                      THEN 149 -- Does not exists in database.
                  WHEN 'Congleton'
                      THEN 150 -- Does not exists in database.
                  WHEN 'Crewe and Nantwich'
                      THEN 151 -- Does not exists in database.
                  WHEN 'Ellesmere Port & Neston'
                      THEN 152 -- Does not exists in database.
                  WHEN 'Macclesfield'
                      THEN 153 -- Does not exists in database.
                  WHEN 'Vale Royal'
                      THEN 154 -- Does not exists in database.
                  WHEN 'Allerdale'
                      THEN 155
                  WHEN 'Barrow-in-Furness'
                      THEN 156
                  WHEN 'Carlisle'
                      THEN 157
                  WHEN 'Copeland'
                      THEN 158
                  WHEN 'Eden'
                      THEN 159
                  WHEN 'South Lakeland'
                      THEN 160
                  WHEN 'Halton'
                      THEN 161
                  WHEN 'Knowsley'
                      THEN 162
                  WHEN 'Burnley'
                      THEN 163
                  WHEN 'Chorley'
                      THEN 164
                  WHEN 'Fylde'
                      THEN 165
                  WHEN 'Hyndburn'
                      THEN 166
                  WHEN 'Lancaster'
                      THEN 167
                  WHEN 'Pendle'
                      THEN 168
                  WHEN 'Preston'
                      THEN 169
                  WHEN 'Ribble Valley'
                      THEN 170
                  WHEN 'Rossendale'
                      THEN 171
                  WHEN 'South Ribble'
                      THEN 172
                  WHEN 'West Lancashire'
                      THEN 173
                  WHEN 'Wyre'
                      THEN 174
                  WHEN 'Liverpool'
                      THEN 175
                  WHEN 'Manchester'
                      THEN 176
                  WHEN 'Oldham'
                      THEN 177
                  WHEN 'Rochdale'
                      THEN 178
                  WHEN 'Salford'
                      THEN 179
                  WHEN 'Sefton'
                      THEN 180
                  WHEN 'St. Helens'
                      THEN 181
                  WHEN 'Stockport'
                      THEN 182
                  WHEN 'Tameside'
                      THEN 183
                  WHEN 'Trafford'
                      THEN 184
                  WHEN 'Warrington'
                      THEN 185
                  WHEN 'Wigan'
                      THEN 186
                  WHEN 'Wirral'
                      THEN 187
                  WHEN 'Bracknell Forest'
                      THEN 188
                  WHEN 'Brighton and Hove'
                      THEN 189
                  WHEN 'Aylesbury Vale'
                      THEN 190
                  WHEN 'Chiltern'
                      THEN 191
                  WHEN 'South Bucks'
                      THEN 192
                  WHEN 'Wycombe'
                      THEN 193
                  WHEN 'Eastbourne'
                      THEN 194
                  WHEN 'Hastings'
                      THEN 195
                  WHEN 'Lewes'
                      THEN 196
                  WHEN 'Rother'
                      THEN 197
                  WHEN 'Wealden'
                      THEN 198
                  WHEN 'Basingstoke and Deane'
                      THEN 199
                  WHEN 'East Hampshire'
                      THEN 200
                  WHEN 'Eastleigh'
                      THEN 201
                  WHEN 'Fareham'
                      THEN 202
                  WHEN 'Gosport'
                      THEN 203
                  WHEN 'Hart'
                      THEN 204
                  WHEN 'Havant'
                      THEN 205
                  WHEN 'New Forest'
                      THEN 206
                  WHEN 'Rushmoor'
                      THEN 207
                  WHEN 'Test Valley'
                      THEN 208
                  WHEN 'Winchester'
                      THEN 209
                  WHEN 'Isle of Wight'
                      THEN 210
                  WHEN 'Ashford'
                      THEN 211
                  WHEN 'Canterbury'
                      THEN 212
                  WHEN 'Dartford'
                      THEN 213
                  WHEN 'Dover'
                      THEN 214
                  WHEN 'Gravesham'
                      THEN 215
                  WHEN 'Maidstone'
                      THEN 216
                  WHEN 'Sevenoaks'
                      THEN 217
                  WHEN 'Shepway'
                      THEN 218
                  WHEN 'Swale'
                      THEN 219
                  WHEN 'Thanet'
                      THEN 220
                  WHEN 'Tonbridge and Malling'
                      THEN 221
                  WHEN 'Tunbridge Wells'
                      THEN 222
                  WHEN 'Medway'
                      THEN 223
                  WHEN 'Milton Keynes'
                      THEN 224
                  WHEN 'Cherwell'
                      THEN 225
                  WHEN 'Oxford'
                      THEN 226
                  WHEN 'South Oxfordshire'
                      THEN 227
                  WHEN 'Vale of White Horse'
                      THEN 228
                  WHEN 'West Oxfordshire'
                      THEN 229
                  WHEN 'Portsmouth'
                      THEN 230
                  WHEN 'Reading'
                      THEN 231
                  WHEN 'Slough'
                      THEN 232
                  WHEN 'Southampton'
                      THEN 233
                  WHEN 'Elmbridge'
                      THEN 234
                  WHEN 'Epsom and Ewell'
                      THEN 235
                  WHEN 'Guildford'
                      THEN 236
                  WHEN 'Mole Valley'
                      THEN 237
                  WHEN 'Reigate and Banstead'
                      THEN 238
                  WHEN 'Runnymede'
                      THEN 239
                  WHEN 'Spelthorne'
                      THEN 240
                  WHEN 'Surrey Heath'
                      THEN 241
                  WHEN 'Tandridge'
                      THEN 242
                  WHEN 'Waverley'
                      THEN 243
                  WHEN 'Woking'
                      THEN 244
                  WHEN 'West Berkshire'
                      THEN 245
                  WHEN 'Adur'
                      THEN 246
                  WHEN 'Arun'
                      THEN 247
                  WHEN 'Chichester'
                      THEN 248
                  WHEN 'Crawley'
                      THEN 249
                  WHEN 'Horsham'
                      THEN 250
                  WHEN 'Mid Sussex'
                      THEN 251
                  WHEN 'Worthing'
                      THEN 252
                  WHEN 'Windsor and Maidenhead'
                      THEN 253
                  WHEN 'Wokingham'
                      THEN 254
                  WHEN 'Bath and North East Somerset'
                      THEN 255
                  WHEN 'Bournemouth'
                      THEN 256
                  WHEN 'City of Bristol'
                      THEN 257
                  WHEN 'Caradon'
                      THEN 258 -- Does not exists in database.
                  WHEN 'Carrick'
                      THEN 259 -- Does not exists in database.
                  WHEN 'Kerrier'
                      THEN 260 -- Does not exists in database.
                  WHEN 'North Cornwall'
                      THEN 261 -- Does not exists in database.
                  WHEN 'Penwith'
                      THEN 262 -- Does not exists in database.
                  WHEN 'Restormel'
                      THEN 263 -- Does not exists in database.
                  WHEN 'East Devon'
                      THEN 264
                  WHEN 'Exeter'
                      THEN 265
                  WHEN 'Mid Devon'
                      THEN 266
                  WHEN 'North Devon'
                      THEN 267
                  WHEN 'South Hams'
                      THEN 268
                  WHEN 'Teignbridge'
                      THEN 269
                  WHEN 'Torridge'
                      THEN 270
                  WHEN 'West Devon'
                      THEN 271
                  WHEN 'Christchurch'
                      THEN 272
                  WHEN 'East Dorset'
                      THEN 273
                  WHEN 'North Dorset'
                      THEN 274
                  WHEN 'Purbeck'
                      THEN 275
                  WHEN 'West Dorset'
                      THEN 276
                  WHEN 'Weymouth and Portland'
                      THEN 277
                  WHEN 'Cheltenham'
                      THEN 278
                  WHEN 'Cotswold'
                      THEN 279
                  WHEN 'Forest of Dean'
                      THEN 280
                  WHEN 'Gloucester'
                      THEN 281
                  WHEN 'Stroud'
                      THEN 282
                  WHEN 'Tewkesbury'
                      THEN 283
                  WHEN 'Isles of Scilly'
                      THEN 284
                  WHEN 'North Somerset'
                      THEN 285
                  WHEN 'Plymouth'
                      THEN 286
                  WHEN 'Poole'
                      THEN 287
                  WHEN 'Mendip'
                      THEN 288
                  WHEN 'Sedgemoor'
                      THEN 289
                  WHEN 'South Somerset'
                      THEN 290
                  WHEN 'Taunton Deane'
                      THEN 291
                  WHEN 'West Somerset'
                      THEN 292
                  WHEN 'South Gloucestershire'
                      THEN 293
                  WHEN 'Swindon'
                      THEN 294
                  WHEN 'Torbay'
                      THEN 295
                  WHEN 'Kennet'
                      THEN 296 -- Does not exists in database.
                  WHEN 'North Wiltshire'
                      THEN 297 -- Does not exists in database.
                  WHEN 'Salisbury'
                      THEN 298 -- Does not exists in database.
                  WHEN 'West Wiltshire'
                      THEN 299 -- Does not exists in database.
                  WHEN 'Birmingham'
                      THEN 300
                  WHEN 'Coventry'
                      THEN 301
                  WHEN 'Dudley'
                      THEN 302
                  WHEN 'Herefordshire'
                      THEN 303
                  WHEN 'Sandwell'
                      THEN 304
                  WHEN 'Bridgnorth'
                      THEN 305 -- Does not exists in database.
                  WHEN 'North Shropshire'
                      THEN 306 -- Does not exists in database.
                  WHEN 'Oswestry'
                      THEN 307 -- Does not exists in database.
                  WHEN 'Shrewsbury and Atcham'
                      THEN 308 -- Does not exists in database.
                  WHEN 'South Shropshire'
                      THEN 309 -- Does not exists in database.
                  WHEN 'Solihull'
                      THEN 310
                  WHEN 'Cannock Chase'
                      THEN 311
                  WHEN 'East Staffordshire'
                      THEN 312
                  WHEN 'Lichfield'
                      THEN 313
                  WHEN 'Newcastle-under-Lyme'
                      THEN 314
                  WHEN 'South Staffordshire'
                      THEN 315
                  WHEN 'Stafford'
                      THEN 316
                  WHEN 'Staffordshire Moorlands'
                      THEN 317
                  WHEN 'Tamworth'
                      THEN 318
                  WHEN 'Stoke-on-Trent'
                      THEN 319
                  WHEN 'Telford and Wrekin'
                      THEN 320
                  WHEN 'Walsall'
                      THEN 321
                  WHEN 'North Warwickshire'
                      THEN 322
                  WHEN 'Nuneaton and Bedworth'
                      THEN 323
                  WHEN 'Rugby'
                      THEN 324
                  WHEN 'Stratford-on-Avon'
                      THEN 325
                  WHEN 'Warwick'
                      THEN 326
                  WHEN 'Wolverhampton'
                      THEN 327
                  WHEN 'Bromsgrove'
                      THEN 328
                  WHEN 'Malvern Hills'
                      THEN 329
                  WHEN 'Redditch'
                      THEN 330
                  WHEN 'Worcester'
                      THEN 331
                  WHEN 'Wychavon'
                      THEN 332
                  WHEN 'Wyre Forest'
                      THEN 333
                  WHEN 'Barnsley'
                      THEN 334
                  WHEN 'Bradford'
                      THEN 335
                  WHEN 'Calderdale'
                      THEN 336
                  WHEN 'Doncaster'
                      THEN 337
                  WHEN 'East Riding of Yorkshire'
                      THEN 338
                  WHEN 'Kingston upon Hull'
                      THEN 339
                  WHEN 'Kirklees'
                      THEN 340
                  WHEN 'Leeds'
                      THEN 341
                  WHEN 'North East Lincolnshire'
                      THEN 342
                  WHEN 'North Lincolnshire'
                      THEN 343
                  WHEN 'Craven'
                      THEN 344
                  WHEN 'Hambleton'
                      THEN 345
                  WHEN 'Harrogate'
                      THEN 346
                  WHEN 'Richmondshire'
                      THEN 347
                  WHEN 'Ryedale'
                      THEN 348
                  WHEN 'Scarborough'
                      THEN 349
                  WHEN 'Selby'
                      THEN 350
                  WHEN 'Rotherham'
                      THEN 351
                  WHEN 'Sheffield'
                      THEN 352
                  WHEN 'Wakefield'
                      THEN 353
                  WHEN 'York'
                      THEN 354
                  WHEN 'Bedford'
                      THEN 400
                  WHEN 'Central Bedfordshire'
                      THEN 401
                  WHEN 'Cheshire East'
                      THEN 402
                  WHEN 'Cheshire West and Chester'
                      THEN 403
                  WHEN 'Cornwall'
                      THEN 404
                  WHEN 'Isles of Scilly'
                      THEN 405
                  WHEN 'County Durham'
                      THEN 406
                  WHEN 'Northumberland'
                      THEN 407
                  WHEN 'Shropshire'
                      THEN 408
                  WHEN 'Wiltshire'
                      THEN 409
                  ELSE - 1
                  END
          FROM "Cssr"
          WHERE "NmdsIDLetter" = SUBSTRING(e."NmdsID", 1, 1)
              AND "LocalCustodianCode" IN (
                  SELECT local_custodian_code
                  FROM cqcref.pcodedata
                  WHERE postcode = e."PostCode"
                  ) LIMIT 1
          ) lauthid,
      -- 'na' parliamentaryconstituency,
      COALESCE("NumberOfStaffValue", - 1) totalstaff, -- 038
      TO_CHAR("NumberOfStaffChangedAt", 'DD/MM/YYYY') totalstaff_changedate,
      TO_CHAR("NumberOfStaffSavedAt", 'DD/MM/YYYY') totalstaff_savedate,
      (
          SELECT COUNT(1)
          FROM "Worker"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "Archived" = false
          ) wkrrecs,
      (
          SELECT TO_CHAR(MAX("When"), 'DD/MM/YYYY')
          FROM "WorkerAudit"
          WHERE "WorkerFK" IN (
                  SELECT "ID"
                  FROM "Worker"
                  WHERE "EstablishmentFK" = e."EstablishmentID"
                  )
              AND "EventType" IN (
                  'created',
                  'deleted',
                  'updated'
                  )
          ) wkrrecs_changedate,
      TO_CHAR("NumberOfStaffSavedAt", 'DD/MM/YYYY') wkrrecs_WDFsavedate,
      CASE 
          WHEN "StartersValue" = 'None'
              THEN 0
          WHEN "StartersValue" = 'Don''t know'
              THEN - 2
          WHEN "StartersValue" IS NULL
              THEN - 1
          ELSE (
                  SELECT total_starters
                  FROM "WorkerJobStats"
                  WHERE "EstablishmentID" = e."EstablishmentID"
                      AND "JobID" IS NULL
                  )
          END totalstarters,
      TO_CHAR("StartersChangedAt", 'DD/MM/YYYY') totalstarters_changedate,
      TO_CHAR("StartersSavedAt", 'DD/MM/YYYY') totalstarters_savedate,
      CASE 
          WHEN "LeaversValue" = 'None'
              THEN 0
          WHEN "LeaversValue" = 'Don''t know'
              THEN - 2
          WHEN "LeaversValue" IS NULL
              THEN - 1
          ELSE (
                  SELECT total_leavers
                  FROM "WorkerJobStats"
                  WHERE "EstablishmentID" = e."EstablishmentID"
                      AND "JobID" IS NULL
                  )
          END totalleavers,
      TO_CHAR("LeaversChangedAt", 'DD/MM/YYYY') totalleavers_changedate,
      TO_CHAR("LeaversSavedAt", 'DD/MM/YYYY') totalleavers_savedate,
      CASE 
          WHEN "VacanciesValue" = 'None'
              THEN 0
          WHEN "VacanciesValue" = 'Don''t know'
              THEN - 2
          WHEN "VacanciesValue" IS NULL
              THEN - 1
          ELSE (
                  SELECT total_vacancies
                  FROM "WorkerJobStats"
                  WHERE "EstablishmentID" = e."EstablishmentID"
                      AND "JobID" IS NULL
                  )
          END totalvacancies,
      TO_CHAR("VacanciesChangedAt", 'DD/MM/YYYY') totalvacancies_changedate,
      TO_CHAR("VacanciesSavedAt", 'DD/MM/YYYY') totalvacancies_savedate,
      (
          SELECT CASE name
                  WHEN 'Care home services with nursing'
                      THEN 1
                  WHEN 'Care home services without nursing'
                      THEN 2
                  WHEN 'Other adult residential care service'
                      THEN 5
                  WHEN 'Day care and day services'
                      THEN 6
                  WHEN 'Other adult day care service'
                      THEN 7
                  WHEN 'Domiciliary care services'
                      THEN 8
                  WHEN 'Domestic services and home help'
                      THEN 10
                  WHEN 'Other adult domiciliary care service'
                      THEN 12
                  WHEN 'Carers support'
                      THEN 13
                  WHEN 'Short breaks, respite care'
                      THEN 14
                  WHEN 'Community support and outreach'
                      THEN 15
                  WHEN 'Social work and care management'
                      THEN 16
                  WHEN 'Shared lives'
                      THEN 17
                  WHEN 'Disability adaptations, assistive technology services'
                      THEN 18
                  WHEN 'Occupational, employment-related services'
                      THEN 19
                  WHEN 'Information and advice services'
                      THEN 20
                  WHEN 'Other adult community care service'
                      THEN 21
                  WHEN 'Other service (not healthcare)'
                      THEN 52
                  WHEN 'Sheltered housing'
                      THEN 53
                  WHEN 'Extra care housing services'
                      THEN 54
                  WHEN 'Supported living services'
                      THEN 55
                  WHEN 'Specialist college services'
                      THEN 60
                  WHEN 'Community based services for people with a learning disability'
                      THEN 61
                  WHEN 'Community based services for people with mental health needs'
                      THEN 62
                  WHEN 'Community based services for people who misuse substances'
                      THEN 63
                  WHEN 'Community healthcare services'
                      THEN 64
                  WHEN 'Hospice services'
                      THEN 66
                  WHEN 'Long-term conditions services'
                      THEN 67
                  WHEN 'Hospital services for people with mental health needs, learning disabilities, problems with substance misuse'
                      THEN 68
                  WHEN 'Rehabilitation services'
                      THEN 69
                  WHEN 'Residential substance misuse treatment, rehabilitation services'
                      THEN 70
                  WHEN 'Other service (healthcare)'
                      THEN 71
                  WHEN 'Head office services'
                      THEN 72
                  WHEN 'Nurses agency'
                      THEN 74
                  WHEN 'Any children''s, young people''s service'
                      THEN 75
                  END
          FROM services
          WHERE id = e."MainServiceFKValue"
          ) mainstid,
      TO_CHAR("MainServiceFKChangedAt", 'DD/MM/YYYY') mainstid_changedate,
      TO_CHAR("MainServiceFKSavedAt", 'DD/MM/YYYY') mainstid_savedate,
      CASE 
        WHEN ((SELECT GREATEST(e.updated, (
            SELECT MAX(updated) FROM "Worker"
                WHERE "EstablishmentFK" = e."EstablishmentID" AND "Archived" = false
                )) < current_date - interval '2 years')
            AND ((
            SELECT MAX(l."LastLoggedIn") 
                FROM "User" u 
                LEFT JOIN "Login" l
                ON u."RegistrationID" = l."RegistrationID"
                WHERE e."EstablishmentID" = u."EstablishmentID"
                ) > current_date - interval '2 years')
        )
        THEN 0
        ELSE 1
      END login_date_purge, 
      
      CASE
         WHEN ("MainServiceFKValue" IN (20, 24, 25)
          AND "IsRegulated" = true )
           THEN (
                    SELECT TO_CHAR(MAX("ViewedTime"),'DD/MM/YYYY' )   FROM "cqc"."BenchmarksViewed" "lvB"
                     WHERE "lvB"."EstablishmentID" =   e."EstablishmentID"
                )
          Else '0'
        END lastviewedbenchmarks,

      CASE
        WHEN ("MainServiceFKValue" IN (20, 24, 25)
            AND "IsRegulated" = true )
             THEN (
                       SELECT count("ID") FROM "cqc"."BenchmarksViewed"  "lvB"
                         WHERE "lvB"."ViewedTime" > current_date - interval '1 month'  and  "lvB"."EstablishmentID" =   e."EstablishmentID"
                  ) 
            ELSE 0
        END benchmarkscount_month,

      CASE
	    WHEN ("MainServiceFKValue" IN (20, 24, 25)
		  AND "IsRegulated" = true )
		   THEN (
                   SELECT count("ID") FROM "cqc"."BenchmarksViewed"  "lvB"
                       WHERE "lvB"."ViewedTime" > current_date - interval '12 months'    and  "lvB"."EstablishmentID" =  e."EstablishmentID"
              ) 
		  ELSE 0
		END benchmarkscount_year,

      CASE  
        WHEN  "MoneySpentOnAdvertisingInTheLastFourWeeks" ='Don''t know'
          THEN '-1' 
        WHEN  "MoneySpentOnAdvertisingInTheLastFourWeeks"= 'None'
          THEN '0'
         ELSE(
              SELECT  "MoneySpentOnAdvertisingInTheLastFourWeeks" FROM cqc."Establishment"  WHERE     "EstablishmentID" = e."EstablishmentID"           
             )
        END Advertising_costs,

     CASE  
       WHEN  "PeopleInterviewedInTheLastFourWeeks" ='Don''t know'
          THEN '-1'
        WHEN  "PeopleInterviewedInTheLastFourWeeks"= 'None'
          THEN '0'
      
         ELSE(
              SELECT "PeopleInterviewedInTheLastFourWeeks" FROM cqc."Establishment" WHERE "EstablishmentID" =  e."EstablishmentID"       
             )
        END Number_interviewed,

      CASE  
        WHEN  "DoNewStartersRepeatMandatoryTrainingFromPreviousEmployment" = 'Yes, always'
           THEN 1
        WHEN  "DoNewStartersRepeatMandatoryTrainingFromPreviousEmployment"= 'Yes, very often'
          THEN 2
        WHEN  "DoNewStartersRepeatMandatoryTrainingFromPreviousEmployment"= 'Yes, but not very often'
          THEN 3
        WHEN  "DoNewStartersRepeatMandatoryTrainingFromPreviousEmployment"= 'No, never'
          THEN 4
        END Repeat_training_accepted,

      CASE  
        WHEN  "WouldYouAcceptCareCertificatesFromPreviousEmployment" = 'Yes, always'
           THEN 1
        WHEN  "WouldYouAcceptCareCertificatesFromPreviousEmployment"= 'Yes, very often'
           THEN 2
        WHEN  "WouldYouAcceptCareCertificatesFromPreviousEmployment"= 'Yes, but not very often'
           THEN 3 
        WHEN  "WouldYouAcceptCareCertificatesFromPreviousEmployment"= 'No, never'
           THEN 4
        END Care_Cert_accepted,

     CASE
        WHEN  "CareWorkersCashLoyaltyForFirstTwoYears" IS NULL  
           THEN NULL
         WHEN  "CareWorkersCashLoyaltyForFirstTwoYears" = 'Don''t know'
           THEN -1
        WHEN  "CareWorkersCashLoyaltyForFirstTwoYears" = 'Yes'
           THEN 1 
        WHEN  "CareWorkersCashLoyaltyForFirstTwoYears" = 'No'
           THEN 2
        END HAS_CWLoyaltyBonus,

     CASE
        WHEN "CareWorkersCashLoyaltyForFirstTwoYears" ~ '^[0-9\.]+$'
          THEN ( SELECT  "CareWorkersCashLoyaltyForFirstTwoYears" FROM cqc."Establishment" WHERE "EstablishmentID" =  e."EstablishmentID" )
		
        END CWLoyaltyBonusAMOUNT,
		
     CASE  
        WHEN  "SickPay" = 'Yes'
           THEN 1
        WHEN  "SickPay"= 'No'
          THEN 2
        WHEN  "SickPay"= 'Don''t know'
          THEN -1
        WHEN  "SickPay" IS NULL
          THEN NULL
        END CWEnhancedSickPay,

      CASE  
        WHEN  "PensionContribution" = 'Yes'
           THEN 1
        WHEN  "PensionContribution"= 'No'
          THEN 2
        WHEN  "PensionContribution"= 'Don''t know'
          THEN -1
        WHEN  "PensionContribution" IS NULL
          THEN NULL
        END CWEnhancedPension,

        CASE  
        WHEN  "CareWorkersLeaveDaysPerYear" IS NULL
          THEN NULL
        ELSE(
            SELECT "CareWorkersLeaveDaysPerYear" FROM cqc."Establishment" WHERE "EstablishmentID" =  e."EstablishmentID"       
            )
        END CWAnnual_leave,

      -- jr28
      CASE 
          WHEN EXISTS (
                  SELECT 1
                  FROM "WorkerContractStats"
                  WHERE "EstablishmentFK" = e."EstablishmentID"
                      AND "MainJobFKValue" IS NULL
                  )
              THEN 1
          ELSE 0
          END jr28flag,
    COALESCE((
          SELECT total_perm_staff
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" IS NULL
          ), 0) jr28perm,
    COALESCE((
          SELECT total_temp_staff
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" IS NULL
          ), 0) jr28temp,
    COALESCE((
          SELECT total_pool_bank
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" IS NULL
          ), 0) jr28pool,
    COALESCE((
          SELECT total_agency
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" IS NULL
          ), 0) jr28agcy,
    COALESCE((
          SELECT total_other
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" IS NULL
          ), 0) jr28oth,
    COALESCE((
          SELECT total_employed
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" IS NULL
          ), 0) jr28emp,
    COALESCE((
          SELECT total_staff
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" IS NULL
          ), 0) jr28work,
      CASE 
          WHEN "StartersValue" = 'None'
              THEN 0
          ELSE COALESCE((
                      SELECT total_starters
                      FROM "WorkerJobStats"
                      WHERE "EstablishmentID" = e."EstablishmentID"
                          AND "JobID" IS NULL
                      ), - 1)
          END jr28strt,
      CASE 
          WHEN "LeaversValue" = 'None'
              THEN 0
          ELSE COALESCE((
                      SELECT total_leavers
                      FROM "WorkerJobStats"
                      WHERE "EstablishmentID" = e."EstablishmentID"
                          AND "JobID" IS NULL
                      ), - 1)
          END jr28stop,
      CASE 
          WHEN "VacanciesValue" = 'None'
              THEN 0
          ELSE COALESCE((
                      SELECT total_vacancies
                      FROM "WorkerJobStats"
                      WHERE "EstablishmentID" = e."EstablishmentID"
                          AND "JobID" IS NULL
                      ), - 1)
          END jr28vacy,
      -- jr29
      CASE 
          WHEN EXISTS (
                  SELECT 1
                  FROM "WorkerContractStats"
                  WHERE "EstablishmentFK" = e."EstablishmentID"
                  AND "MainJobFKValue" IN (${jobRoleGroups.directCare})
                  )
              THEN 1
          ELSE 0
          END jr29flag,
      COALESCE((
          SELECT SUM(total_perm_staff)
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" IN (${jobRoleGroups.directCare})
          ), 0) jr29perm,
      COALESCE((
          SELECT SUM(total_temp_staff)
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" IN (${jobRoleGroups.directCare})
          ), 0) jr29temp,
      COALESCE((
          SELECT SUM(total_pool_bank)
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" IN (${jobRoleGroups.directCare})
          ), 0) jr29pool,
      COALESCE((
          SELECT SUM(total_agency)
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" IN (${jobRoleGroups.directCare})
          ), 0) jr29agcy,
      COALESCE((
          SELECT SUM(total_other)
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" IN (${jobRoleGroups.directCare})
          ), 0) jr29oth,
      COALESCE((
          SELECT SUM(total_employed)
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" IN (${jobRoleGroups.directCare})
          ), 0) jr29emp,
      COALESCE((
          SELECT SUM(total_staff)
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" IN (${jobRoleGroups.directCare})
          ), 0) jr29work,
      CASE 
          WHEN "StartersValue" = 'None'
              THEN 0
          ELSE COALESCE((
                      SELECT SUM(total_starters)
                      FROM "WorkerJobStats"
                      WHERE "EstablishmentID" = e."EstablishmentID"
                          AND "JobID" IN (${jobRoleGroups.directCare})
                      ), - 1)
          END jr29strt,
      CASE 
          WHEN "LeaversValue" = 'None'
              THEN 0
          ELSE COALESCE((
                      SELECT SUM(total_leavers)
                      FROM "WorkerJobStats"
                      WHERE "EstablishmentID" = e."EstablishmentID"
                          AND "JobID" IN (${jobRoleGroups.directCare})
                      ), - 1)
          END jr29stop,
      CASE 
          WHEN "VacanciesValue" = 'None'
              THEN 0
          ELSE COALESCE((
                      SELECT SUM(total_vacancies)
                      FROM "WorkerJobStats"
                      WHERE "EstablishmentID" = e."EstablishmentID"
                          AND "JobID" IN (${jobRoleGroups.directCare})
                      ), - 1)
          END jr29vacy,
      -- jr30
      CASE 
          WHEN EXISTS (
                  SELECT 1
                  FROM "WorkerContractStats"
                  WHERE "EstablishmentFK" = e."EstablishmentID"
                  AND "MainJobFKValue" IN (${jobRoleGroups.manager})
                  )
              THEN 1
          ELSE 0
          END jr30flag,
      COALESCE((
          SELECT SUM(total_perm_staff)
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
          AND "MainJobFKValue" IN (${jobRoleGroups.manager})
          ), 0) jr30perm,
      COALESCE((
          SELECT SUM(total_temp_staff)
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
          AND "MainJobFKValue" IN (${jobRoleGroups.manager})
          ), 0) jr30temp,
      COALESCE((
          SELECT SUM(total_pool_bank)
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
          AND "MainJobFKValue" IN (${jobRoleGroups.manager})
          ), 0) jr30pool,
      COALESCE((
          SELECT SUM(total_agency)
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
          AND "MainJobFKValue" IN (${jobRoleGroups.manager})
          ), 0) jr30agcy,
      COALESCE((
          SELECT SUM(total_other)
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
         AND "MainJobFKValue" IN (${jobRoleGroups.manager})
          ), 0) jr30oth,
      COALESCE((
          SELECT SUM(total_employed)
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
          AND "MainJobFKValue" IN (${jobRoleGroups.manager})
          ), 0) jr30emp,
      COALESCE((
          SELECT SUM(total_staff)
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
          AND "MainJobFKValue" IN (${jobRoleGroups.manager})
          ), 0) jr30work,
      CASE 
          WHEN "StartersValue" = 'None'
              THEN 0
          ELSE COALESCE((
                      SELECT SUM(total_starters)
                      FROM "WorkerJobStats"
                      WHERE "EstablishmentID" = e."EstablishmentID"
                          AND "JobID" IN (${jobRoleGroups.manager})
                      ), - 1)
          END jr30strt,
      CASE 
          WHEN "LeaversValue" = 'None'
              THEN 0
          ELSE COALESCE((
                      SELECT SUM(total_leavers)
                      FROM "WorkerJobStats"
                      WHERE "EstablishmentID" = e."EstablishmentID"
                          AND "JobID" IN (${jobRoleGroups.manager})
                      ), - 1)
          END jr30stop,
      CASE 
          WHEN "VacanciesValue" = 'None'
              THEN 0
          ELSE COALESCE((
                      SELECT SUM(total_vacancies)
                      FROM "WorkerJobStats"
                      WHERE "EstablishmentID" = e."EstablishmentID"
                          AND "JobID" IN (${jobRoleGroups.manager})
                      ), - 1)
          END jr30vacy,
      -- jr31
      CASE 
          WHEN EXISTS (
                  SELECT 1
                  FROM "WorkerContractStats"
                  WHERE "EstablishmentFK" = e."EstablishmentID"
                      AND "MainJobFKValue" IN (${jobRoleGroups.professional})
                  )
              THEN 1
          ELSE 0
          END jr31flag,
      COALESCE((
          SELECT SUM(total_perm_staff)
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" IN (${jobRoleGroups.professional})
          ), 0) jr31perm,
      COALESCE((
          SELECT SUM(total_temp_staff)
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" IN (${jobRoleGroups.professional})
          ), 0) jr31temp,
      COALESCE((
          SELECT SUM(total_pool_bank)
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" IN (${jobRoleGroups.professional})
          ), 0) jr31pool,
      COALESCE((
          SELECT SUM(total_agency)
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" IN (${jobRoleGroups.professional})
          ), 0) jr31agcy,
      COALESCE((
          SELECT SUM(total_other)
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" IN (${jobRoleGroups.professional})
          ), 0) jr31oth,
      COALESCE((
          SELECT SUM(total_employed)
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" IN (${jobRoleGroups.professional})
          ), 0) jr31emp,
      COALESCE((
          SELECT SUM(total_staff)
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" IN (${jobRoleGroups.professional})
          ), 0) jr31work,
      CASE 
          WHEN "StartersValue" = 'None'
              THEN 0
          ELSE COALESCE((
                      SELECT SUM(total_starters)
                      FROM "WorkerJobStats"
                      WHERE "EstablishmentID" = e."EstablishmentID"
                          AND "JobID" IN (${jobRoleGroups.professional})
                      ), - 1)
          END jr31strt,
      CASE 
          WHEN "LeaversValue" = 'None'
              THEN 0
          ELSE COALESCE((
                      SELECT SUM(total_leavers)
                      FROM "WorkerJobStats"
                      WHERE "EstablishmentID" = e."EstablishmentID"
                          AND "JobID" IN (${jobRoleGroups.professional})
                      ), - 1)
          END jr31stop,
      CASE 
          WHEN "VacanciesValue" = 'None'
              THEN 0
          ELSE COALESCE((
                      SELECT SUM(total_vacancies)
                      FROM "WorkerJobStats"
                      WHERE "EstablishmentID" = e."EstablishmentID"
                          AND "JobID" IN (${jobRoleGroups.professional})
                      ), - 1)
          END jr31vacy,
      -- jr32
      CASE 
          WHEN EXISTS (
                  SELECT 1
                  FROM "WorkerContractStats"
                  WHERE "EstablishmentFK" = e."EstablishmentID"
                  AND "MainJobFKValue" IN (${jobRoleGroups.other})
                  )
              THEN 1
          ELSE 0
          END jr32flag,
      COALESCE((
          SELECT SUM(total_perm_staff)
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" IN (${jobRoleGroups.other})
          ), 0) jr32perm,
    COALESCE((
          SELECT SUM(total_temp_staff)
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" IN (${jobRoleGroups.other})
          ), 0) jr32temp,
    COALESCE((
          SELECT SUM(total_pool_bank)
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" IN (${jobRoleGroups.other})
          ), 0) jr32pool,
    COALESCE((
          SELECT SUM(total_agency)
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" IN (${jobRoleGroups.other})
          ), 0) jr32agcy,
    COALESCE((
          SELECT SUM(total_other)
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" IN (${jobRoleGroups.other})
          ), 0) jr32oth,
    COALESCE((
          SELECT SUM(total_employed)
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" IN (${jobRoleGroups.other})
          ), 0) jr32emp,
    COALESCE((
          SELECT SUM(total_staff)
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" IN (${jobRoleGroups.other})
          ), 0) jr32work,
      CASE 
          WHEN "StartersValue" = 'None'
              THEN 0
          ELSE COALESCE((
                      SELECT SUM(total_starters)
                      FROM "WorkerJobStats"
                      WHERE "EstablishmentID" = e."EstablishmentID"
                          AND "JobID" IN (${jobRoleGroups.other})
                      ), - 1)
          END jr32strt,
      CASE 
          WHEN "LeaversValue" = 'None'
              THEN 0
          ELSE COALESCE((
                      SELECT SUM(total_leavers)
                      FROM "WorkerJobStats"
                      WHERE "EstablishmentID" = e."EstablishmentID"
                          AND "JobID" IN (${jobRoleGroups.other})
                      ), - 1)
          END jr32stop,
      CASE 
          WHEN "VacanciesValue" = 'None'
              THEN 0
          ELSE COALESCE((
                      SELECT SUM(total_vacancies)
                      FROM "WorkerJobStats"
                      WHERE "EstablishmentID" = e."EstablishmentID"
                          AND "JobID" IN (${jobRoleGroups.other})
                      ), - 1)
          END jr32vacy,
      -- jr01
      CASE 
          WHEN EXISTS (
                  SELECT 1
                  FROM "WorkerContractStats"
                  WHERE "EstablishmentFK" = e."EstablishmentID"
                      AND "MainJobFKValue" = 26
                  )
              THEN 1
          ELSE 0
          END jr01flag,
      COALESCE((
          SELECT total_perm_staff
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 26
          ), 0) jr01perm,
      COALESCE((
          SELECT total_temp_staff
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 26
          ), 0) jr01temp,
      COALESCE((
          SELECT total_pool_bank
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 26
          ), 0) jr01pool,
      COALESCE((
          SELECT total_agency
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 26
          ), 0) jr01agcy,
      COALESCE((
          SELECT total_other
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 26
          ), 0) jr01oth,
      COALESCE((
          SELECT total_employed
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 26
          ), 0) jr01emp,
      COALESCE((
          SELECT total_staff
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 26
          ), 0) jr01work,
      CASE 
          WHEN "StartersValue" = 'None'
              THEN 0
          ELSE COALESCE((
                      SELECT total_starters
                      FROM "WorkerJobStats"
                      WHERE "EstablishmentID" = e."EstablishmentID"
                          AND "JobID" = 26
                      ), - 1)
          END jr01strt,
      CASE 
          WHEN "LeaversValue" = 'None'
              THEN 0
          ELSE COALESCE((
                      SELECT total_leavers
                      FROM "WorkerJobStats"
                      WHERE "EstablishmentID" = e."EstablishmentID"
                          AND "JobID" = 26
                      ), - 1)
          END jr01stop,
      CASE 
          WHEN "VacanciesValue" = 'None'
              THEN 0
          ELSE COALESCE((
                      SELECT total_vacancies
                      FROM "WorkerJobStats"
                      WHERE "EstablishmentID" = e."EstablishmentID"
                          AND "JobID" = 26
                      ), - 1)
          END jr01vacy,
      -- jr02
      CASE 
          WHEN EXISTS (
                  SELECT 1
                  FROM "WorkerContractStats"
                  WHERE "EstablishmentFK" = e."EstablishmentID"
                      AND "MainJobFKValue" = 15
                  )
              THEN 1
          ELSE 0
          END jr02flag,
      COALESCE((
          SELECT total_perm_staff
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 15
          ), 0) jr02perm,
      COALESCE((
          SELECT total_temp_staff
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 15
          ), 0) jr02temp,
      COALESCE((
          SELECT total_pool_bank
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 15
          ), 0) jr02pool,
      COALESCE((
          SELECT total_agency
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 15
          ), 0) jr02agcy,
      COALESCE((
          SELECT total_other
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 15
          ), 0) jr02oth,
      COALESCE((
          SELECT total_employed
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 15
          ), 0) jr02emp,
      COALESCE((
          SELECT total_staff
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 15
          ), 0) jr02work,
      CASE 
          WHEN "StartersValue" = 'None'
              THEN 0
          ELSE COALESCE((
                      SELECT total_starters
                      FROM "WorkerJobStats"
                      WHERE "EstablishmentID" = e."EstablishmentID"
                          AND "JobID" = 15
                      ), - 1)
          END jr02strt,
      CASE 
          WHEN "LeaversValue" = 'None'
              THEN 0
          ELSE COALESCE((
                      SELECT total_leavers
                      FROM "WorkerJobStats"
                      WHERE "EstablishmentID" = e."EstablishmentID"
                          AND "JobID" = 15
                      ), - 1)
          END jr02stop,
      CASE 
          WHEN "VacanciesValue" = 'None'
              THEN 0
          ELSE COALESCE((
                      SELECT total_vacancies
                      FROM "WorkerJobStats"
                      WHERE "EstablishmentID" = e."EstablishmentID"
                          AND "JobID" = 15
                      ), - 1)
          END jr02vacy,
      -- jr03
      CASE 
          WHEN EXISTS (
                  SELECT 1
                  FROM "WorkerContractStats"
                  WHERE "EstablishmentFK" = e."EstablishmentID"
                      AND "MainJobFKValue" = 13
                  )
              THEN 1
          ELSE 0
          END jr03flag,
      COALESCE((
          SELECT total_perm_staff
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 13
          ), 0) jr03perm,
      COALESCE((
          SELECT total_temp_staff
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 13
          ), 0) jr03temp,
      COALESCE((
          SELECT total_pool_bank
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 13
          ), 0) jr03pool,
      COALESCE((
          SELECT total_agency
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 13
          ), 0) jr03agcy,
      COALESCE((
          SELECT total_other
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 13
          ), 0) jr03oth,
      COALESCE((
          SELECT total_employed
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 13
          ), 0) jr03emp,
      COALESCE((
          SELECT total_staff
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 13
          ), 0) jr03work,
      CASE 
          WHEN "StartersValue" = 'None'
              THEN 0
          ELSE COALESCE((
                      SELECT total_starters
                      FROM "WorkerJobStats"
                      WHERE "EstablishmentID" = e."EstablishmentID"
                          AND "JobID" = 13
                      ), - 1)
          END jr03strt,
      CASE 
          WHEN "LeaversValue" = 'None'
              THEN 0
          ELSE COALESCE((
                      SELECT total_leavers
                      FROM "WorkerJobStats"
                      WHERE "EstablishmentID" = e."EstablishmentID"
                          AND "JobID" = 13
                      ), - 1)
          END jr03stop,
      CASE 
          WHEN "VacanciesValue" = 'None'
              THEN 0
          ELSE COALESCE((
                      SELECT total_vacancies
                      FROM "WorkerJobStats"
                      WHERE "EstablishmentID" = e."EstablishmentID"
                          AND "JobID" = 13
                      ), - 1)
          END jr03vacy,
      -- jr04
      CASE 
          WHEN EXISTS (
                  SELECT 1
                  FROM "WorkerContractStats"
                  WHERE "EstablishmentFK" = e."EstablishmentID"
                      AND "MainJobFKValue" = 22
                  )
              THEN 1
          ELSE 0
          END jr04flag,
      COALESCE((
          SELECT total_perm_staff
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 22
          ), 0) jr04perm,
      COALESCE((
          SELECT total_temp_staff
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 22
          ), 0) jr04temp,
      COALESCE((
          SELECT total_pool_bank
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 22
          ), 0) jr04pool,
      COALESCE((
          SELECT total_agency
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 22
          ), 0) jr04agcy,
      COALESCE((
          SELECT total_other
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 22
          ), 0) jr04oth,
      COALESCE((
          SELECT total_employed
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 22
          ), 0) jr04emp,
      COALESCE((
          SELECT total_staff
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 22
          ), 0) jr04work,
      CASE 
          WHEN "StartersValue" = 'None'
              THEN 0
          ELSE COALESCE((
                      SELECT total_starters
                      FROM "WorkerJobStats"
                      WHERE "EstablishmentID" = e."EstablishmentID"
                          AND "JobID" = 22
                      ), - 1)
          END jr04strt,
      CASE 
          WHEN "LeaversValue" = 'None'
              THEN 0
          ELSE COALESCE((
                      SELECT total_leavers
                      FROM "WorkerJobStats"
                      WHERE "EstablishmentID" = e."EstablishmentID"
                          AND "JobID" = 22
                      ), - 1)
          END jr04stop,
      CASE 
          WHEN "VacanciesValue" = 'None'
              THEN 0
          ELSE COALESCE((
                      SELECT total_vacancies
                      FROM "WorkerJobStats"
                      WHERE "EstablishmentID" = e."EstablishmentID"
                          AND "JobID" = 22
                      ), - 1)
          END jr04vacy,
      -- jr05
      CASE 
          WHEN EXISTS (
                  SELECT 1
                  FROM "WorkerContractStats"
                  WHERE "EstablishmentFK" = e."EstablishmentID"
                      AND "MainJobFKValue" = 28
                  )
              THEN 1
          ELSE 0
          END jr05flag,
      COALESCE((
          SELECT total_perm_staff
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 28
          ), 0) jr05perm,
      COALESCE((
          SELECT total_temp_staff
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 28
          ), 0) jr05temp,
      COALESCE((
          SELECT total_pool_bank
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 28
          ), 0) jr05pool,
      COALESCE((
          SELECT total_agency
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 28
          ), 0) jr05agcy,
      COALESCE((
          SELECT total_other
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 28
          ), 0) jr05oth,
      COALESCE((
          SELECT total_employed
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 28
          ), 0) jr05emp,
      COALESCE((
          SELECT total_staff
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 28
          ), 0) jr05work,
      CASE 
          WHEN "StartersValue" = 'None'
              THEN 0
          ELSE COALESCE((
                      SELECT total_starters
                      FROM "WorkerJobStats"
                      WHERE "EstablishmentID" = e."EstablishmentID"
                          AND "JobID" = 28
                      ), - 1)
          END jr05strt,
      CASE 
          WHEN "LeaversValue" = 'None'
              THEN 0
          ELSE COALESCE((
                      SELECT total_leavers
                      FROM "WorkerJobStats"
                      WHERE "EstablishmentID" = e."EstablishmentID"
                          AND "JobID" = 28
                      ), - 1)
          END jr05stop,
      CASE 
          WHEN "VacanciesValue" = 'None'
              THEN 0
          ELSE COALESCE((
                      SELECT total_vacancies
                      FROM "WorkerJobStats"
                      WHERE "EstablishmentID" = e."EstablishmentID"
                          AND "JobID" = 28
                      ), - 1)
          END jr05vacy,
      -- jr06
      CASE 
          WHEN EXISTS (
                  SELECT 1
                  FROM "WorkerContractStats"
                  WHERE "EstablishmentFK" = e."EstablishmentID"
                      AND "MainJobFKValue" = 27
                  )
              THEN 1
          ELSE 0
          END jr06flag,
      COALESCE((
          SELECT total_perm_staff
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 27
          ), 0) jr06perm,
      COALESCE((
          SELECT total_temp_staff
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 27
          ), 0) jr06temp,
      COALESCE((
          SELECT total_pool_bank
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 27
          ), 0) jr06pool,
      COALESCE((
          SELECT total_agency
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 27
          ), 0) jr06agcy,
      COALESCE((
          SELECT total_other
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 27
          ), 0) jr06oth,
      COALESCE((
          SELECT total_employed
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 27
          ), 0) jr06emp,
      COALESCE((
          SELECT total_staff
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 27
          ), 0) jr06work,
      CASE 
          WHEN "StartersValue" = 'None'
              THEN 0
          ELSE COALESCE((
                      SELECT total_starters
                      FROM "WorkerJobStats"
                      WHERE "EstablishmentID" = e."EstablishmentID"
                          AND "JobID" = 27
                      ), - 1)
          END jr06strt,
      CASE 
          WHEN "LeaversValue" = 'None'
              THEN 0
          ELSE COALESCE((
                      SELECT total_leavers
                      FROM "WorkerJobStats"
                      WHERE "EstablishmentID" = e."EstablishmentID"
                          AND "JobID" = 27
                      ), - 1)
          END jr06stop,
      CASE 
          WHEN "VacanciesValue" = 'None'
              THEN 0
          ELSE COALESCE((
                      SELECT total_vacancies
                      FROM "WorkerJobStats"
                      WHERE "EstablishmentID" = e."EstablishmentID"
                          AND "JobID" = 27
                      ), - 1)
          END jr06vacy,
      -- jr07
      CASE 
          WHEN EXISTS (
                  SELECT 1
                  FROM "WorkerContractStats"
                  WHERE "EstablishmentFK" = e."EstablishmentID"
                      AND "MainJobFKValue" = 25
                  )
              THEN 1
          ELSE 0
          END jr07flag,
      COALESCE((
          SELECT total_perm_staff
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 25
          ), 0) jr07perm,
      COALESCE((
          SELECT total_temp_staff
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 25
          ), 0) jr07temp,
      COALESCE((
          SELECT total_pool_bank
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 25
          ), 0) jr07pool,
      COALESCE((
          SELECT total_agency
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 25
          ), 0) jr07agcy,
      COALESCE((
          SELECT total_other
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 25
          ), 0) jr07oth,
      COALESCE((
          SELECT total_employed
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 25
          ), 0) jr07emp,
      COALESCE((
          SELECT total_staff
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 25
          ), 0) jr07work,
      CASE 
          WHEN "StartersValue" = 'None'
              THEN 0
          ELSE COALESCE((
                      SELECT total_starters
                      FROM "WorkerJobStats"
                      WHERE "EstablishmentID" = e."EstablishmentID"
                          AND "JobID" = 25
                      ), - 1)
          END jr07strt,
      CASE 
          WHEN "LeaversValue" = 'None'
              THEN 0
          ELSE COALESCE((
                      SELECT total_leavers
                      FROM "WorkerJobStats"
                      WHERE "EstablishmentID" = e."EstablishmentID"
                          AND "JobID" = 25
                      ), - 1)
          END jr07stop,
      CASE 
          WHEN "VacanciesValue" = 'None'
              THEN 0
          ELSE COALESCE((
                      SELECT total_vacancies
                      FROM "WorkerJobStats"
                      WHERE "EstablishmentID" = e."EstablishmentID"
                          AND "JobID" = 25
                      ), - 1)
          END jr07vacy,
      -- jr 08
      CASE 
          WHEN EXISTS (
                  SELECT 1
                  FROM "WorkerContractStats"
                  WHERE "EstablishmentFK" = e."EstablishmentID"
                      AND "MainJobFKValue" = 10
                  )
              THEN 1
          ELSE 0
          END jr08flag,
      COALESCE((
          SELECT total_perm_staff
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 10
          ), 0) jr08perm,
      COALESCE((
          SELECT total_temp_staff
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 10
          ), 0) jr08temp,
      COALESCE((
          SELECT total_pool_bank
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 10
          ), 0) jr08pool,
      COALESCE((
          SELECT total_agency
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 10
          ), 0) jr08agcy,
      COALESCE((
          SELECT total_other
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 10
          ), 0) jr08oth,
      COALESCE((
          SELECT total_employed
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 10
          ), 0) jr08emp,
      COALESCE((
          SELECT total_staff
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 10
          ), 0) jr08work,
      CASE 
          WHEN "StartersValue" = 'None'
              THEN 0
          ELSE COALESCE((
                      SELECT total_starters
                      FROM "WorkerJobStats"
                      WHERE "EstablishmentID" = e."EstablishmentID"
                          AND "JobID" = 10
                      ), - 1)
          END jr08strt,
      CASE 
          WHEN "LeaversValue" = 'None'
              THEN 0
          ELSE COALESCE((
                      SELECT total_leavers
                      FROM "WorkerJobStats"
                      WHERE "EstablishmentID" = e."EstablishmentID"
                          AND "JobID" = 10
                      ), - 1)
          END jr08stop,
      CASE 
          WHEN "VacanciesValue" = 'None'
              THEN 0
          ELSE COALESCE((
                      SELECT total_vacancies
                      FROM "WorkerJobStats"
                      WHERE "EstablishmentID" = e."EstablishmentID"
                          AND "JobID" = 10
                      ), - 1)
          END jr08vacy,
      -- jr09
      CASE 
          WHEN EXISTS (
                  SELECT 1
                  FROM "WorkerContractStats"
                  WHERE "EstablishmentFK" = e."EstablishmentID"
                      AND "MainJobFKValue" = 11
                  )
              THEN 1
          ELSE 0
          END jr09flag,
      COALESCE((
          SELECT total_perm_staff
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 11
          ), 0) jr09perm,
      COALESCE((
          SELECT total_temp_staff
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 11
          ), 0) jr09temp,
      COALESCE((
          SELECT total_pool_bank
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 11
          ), 0) jr09pool,
      COALESCE((
          SELECT total_agency
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 11
          ), 0) jr09agcy,
      COALESCE((
          SELECT total_other
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 11
          ), 0) jr09oth,
      COALESCE((
          SELECT total_employed
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 11
          ), 0) jr09emp,
      COALESCE((
          SELECT total_staff
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 11
          ), 0) jr09work,
      CASE 
          WHEN "StartersValue" = 'None'
              THEN 0
          ELSE COALESCE((
                      SELECT total_starters
                      FROM "WorkerJobStats"
                      WHERE "EstablishmentID" = e."EstablishmentID"
                          AND "JobID" = 11
                      ), - 1)
          END jr09strt,
      CASE 
          WHEN "LeaversValue" = 'None'
              THEN 0
          ELSE COALESCE((
                      SELECT total_leavers
                      FROM "WorkerJobStats"
                      WHERE "EstablishmentID" = e."EstablishmentID"
                          AND "JobID" = 11
                      ), - 1)
          END jr09stop,
      CASE 
          WHEN "VacanciesValue" = 'None'
              THEN 0
          ELSE COALESCE((
                      SELECT total_vacancies
                      FROM "WorkerJobStats"
                      WHERE "EstablishmentID" = e."EstablishmentID"
                          AND "JobID" = 11
                      ), - 1)
          END jr09vacy,
      -- jr10
      CASE 
          WHEN EXISTS (
                  SELECT 1
                  FROM "WorkerContractStats"
                  WHERE "EstablishmentFK" = e."EstablishmentID"
                      AND "MainJobFKValue" = 12
                  )
              THEN 1
          ELSE 0
          END jr10flag,
      COALESCE((
          SELECT total_perm_staff
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 12
          ), 0) jr10perm,
      COALESCE((
          SELECT total_temp_staff
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 12
          ), 0) jr10temp,
      COALESCE((
          SELECT total_pool_bank
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 12
          ), 0) jr10pool,
      COALESCE((
          SELECT total_agency
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 12
          ), 0) jr10agcy,
      COALESCE((
          SELECT total_other
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 12
          ), 0) jr10oth,
      COALESCE((
          SELECT total_employed
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 12
          ), 0) jr10emp,
      COALESCE((
          SELECT total_staff
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 12
          ), 0) jr10work,
      CASE 
          WHEN "StartersValue" = 'None'
              THEN 0
          ELSE COALESCE((
                      SELECT total_starters
                      FROM "WorkerJobStats"
                      WHERE "EstablishmentID" = e."EstablishmentID"
                          AND "JobID" = 12
                      ), - 1)
          END jr10strt,
      CASE 
          WHEN "LeaversValue" = 'None'
              THEN 0
          ELSE COALESCE((
                      SELECT total_leavers
                      FROM "WorkerJobStats"
                      WHERE "EstablishmentID" = e."EstablishmentID"
                          AND "JobID" = 12
                      ), - 1)
          END jr10stop,
      CASE 
          WHEN "VacanciesValue" = 'None'
              THEN 0
          ELSE COALESCE((
                      SELECT total_vacancies
                      FROM "WorkerJobStats"
                      WHERE "EstablishmentID" = e."EstablishmentID"
                          AND "JobID" = 12
                      ), - 1)
          END jr10vacy,
      -- jr11
      CASE 
          WHEN EXISTS (
                  SELECT 1
                  FROM "WorkerContractStats"
                  WHERE "EstablishmentFK" = e."EstablishmentID"
                      AND "MainJobFKValue" = 3
                  )
              THEN 1
          ELSE 0
          END jr11flag,
     COALESCE((
          SELECT total_perm_staff
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 3
          ), 0) jr11perm,
     COALESCE((
          SELECT total_temp_staff
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
          AND "MainJobFKValue" = 3
          ), 0) jr11temp,
     COALESCE((
          SELECT total_pool_bank
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 3
          ), 0) jr11pool,
      COALESCE((
          SELECT total_agency
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 3
          ), 0) jr11agcy,
     COALESCE((
          SELECT total_other
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 3
          ), 0) jr11oth,
     COALESCE((
          SELECT total_employed
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 3
          ), 0) jr11emp,
     COALESCE((
          SELECT total_staff
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 3
          ), 0) jr11work,
      CASE 
          WHEN "StartersValue" = 'None'
              THEN 0
          ELSE COALESCE((
                      SELECT total_starters
                      FROM "WorkerJobStats"
                      WHERE "EstablishmentID" = e."EstablishmentID"
                          AND "JobID" = 3
                      ), - 1)
          END jr11strt,
      CASE 
          WHEN "LeaversValue" = 'None'
              THEN 0
          ELSE COALESCE((
                      SELECT total_leavers
                      FROM "WorkerJobStats"
                      WHERE "EstablishmentID" = e."EstablishmentID"
                          AND "JobID" = 3
                      ), - 1)
          END jr11stop,
      CASE 
          WHEN "VacanciesValue" = 'None'
              THEN 0
          ELSE COALESCE((
                      SELECT total_vacancies
                      FROM "WorkerJobStats"
                      WHERE "EstablishmentID" = e."EstablishmentID"
                          AND "JobID" = 3
                      ), - 1)
          END jr11vacy,
      -- jr15
      CASE 
          WHEN EXISTS (
                  SELECT 1
                  FROM "WorkerContractStats"
                  WHERE "EstablishmentFK" = e."EstablishmentID"
                      AND "MainJobFKValue" = 18
                  )
              THEN 1
          ELSE 0
          END jr15flag,
      COALESCE((
          SELECT total_perm_staff
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 18
          ), 0) jr15perm,
      COALESCE((
          SELECT total_temp_staff
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 18
          ), 0) jr15temp,
      COALESCE((
          SELECT total_pool_bank
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 18
          ), 0) jr15pool,
      COALESCE((
          SELECT total_agency
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 18
          ), 0) jr15agcy,
      COALESCE((
          SELECT total_other
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 18
          ), 0) jr15oth,
      COALESCE((
          SELECT total_employed
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 18
          ), 0) jr15emp,
      COALESCE((
          SELECT total_staff
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 18
          ), 0) jr15work,
      CASE 
          WHEN "StartersValue" = 'None'
              THEN 0
          ELSE COALESCE((
                      SELECT total_starters
                      FROM "WorkerJobStats"
                      WHERE "EstablishmentID" = e."EstablishmentID"
                          AND "JobID" = 18
                      ), - 1)
          END jr15strt,
      CASE 
          WHEN "LeaversValue" = 'None'
              THEN 0
          ELSE COALESCE((
                      SELECT total_leavers
                      FROM "WorkerJobStats"
                      WHERE "EstablishmentID" = e."EstablishmentID"
                          AND "JobID" = 18
                      ), - 1)
          END jr15stop,
      CASE 
          WHEN "VacanciesValue" = 'None'
              THEN 0
          ELSE COALESCE((
                      SELECT total_vacancies
                      FROM "WorkerJobStats"
                      WHERE "EstablishmentID" = e."EstablishmentID"
                          AND "JobID" = 18
                      ), - 1)
          END jr15vacy,
      -- jr16
      CASE 
          WHEN EXISTS (
                  SELECT 1
                  FROM "WorkerContractStats"
                  WHERE "EstablishmentFK" = e."EstablishmentID"
                      AND "MainJobFKValue" = 23
                  )
              THEN 1
          ELSE 0
          END jr16flag,
      COALESCE((
          SELECT total_perm_staff
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 23
          ), 0) jr16perm,
      COALESCE((
          SELECT total_temp_staff
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 23
          ), 0) jr16temp,
      COALESCE((
          SELECT total_pool_bank
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 23
          ), 0) jr16pool,
      COALESCE((
          SELECT total_agency
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 23
          ), 0) jr16agcy,
      COALESCE((
          SELECT total_other
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 23
          ), 0) jr16oth,
      COALESCE((
          SELECT total_employed
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 23
          ), 0) jr16emp,
      COALESCE((
          SELECT total_staff
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 23
          ), 0) jr16work,
      CASE 
          WHEN "StartersValue" = 'None'
              THEN 0
          ELSE COALESCE((
                      SELECT total_starters
                      FROM "WorkerJobStats"
                      WHERE "EstablishmentID" = e."EstablishmentID"
                          AND "JobID" = 23
                      ), - 1)
          END jr16strt,
      CASE 
          WHEN "LeaversValue" = 'None'
              THEN 0
          ELSE COALESCE((
                      SELECT total_leavers
                      FROM "WorkerJobStats"
                      WHERE "EstablishmentID" = e."EstablishmentID"
                          AND "JobID" = 23
                      ), - 1)
          END jr16stop,
      CASE 
          WHEN "VacanciesValue" = 'None'
              THEN 0
          ELSE COALESCE((
                      SELECT total_vacancies
                      FROM "WorkerJobStats"
                      WHERE "EstablishmentID" = e."EstablishmentID"
                          AND "JobID" = 23
                      ), - 1)
          END jr16vacy,
      -- jr17
      CASE 
          WHEN EXISTS (
                  SELECT 1
                  FROM "WorkerContractStats"
                  WHERE "EstablishmentFK" = e."EstablishmentID"
                      AND "MainJobFKValue" = 4
                  )
              THEN 1
          ELSE 0
          END jr17flag,
     COALESCE((
          SELECT total_perm_staff
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 4
          ), 0) jr17perm,
     COALESCE((
          SELECT total_temp_staff
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 4
          ), 0) jr17temp,
     COALESCE((
          SELECT total_pool_bank
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 4
          ), 0) jr17pool,
     COALESCE((
          SELECT total_agency
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 4
          ), 0) jr17agcy,
     COALESCE((
          SELECT total_other
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 4
          ), 0) jr17oth,
     COALESCE((
          SELECT total_employed
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 4
          ), 0) jr17emp,
      COALESCE((
          SELECT total_staff
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 4
          ), 0) jr17work,
      CASE 
          WHEN "StartersValue" = 'None'
              THEN 0
          ELSE COALESCE((
                      SELECT total_starters
                      FROM "WorkerJobStats"
                      WHERE "EstablishmentID" = e."EstablishmentID"
                          AND "JobID" = 4
                      ), - 1)
          END jr17strt,
      CASE 
          WHEN "LeaversValue" = 'None'
              THEN 0
          ELSE COALESCE((
                      SELECT total_leavers
                      FROM "WorkerJobStats"
                      WHERE "EstablishmentID" = e."EstablishmentID"
                          AND "JobID" = 4
                      ), - 1)
          END jr17stop,
      CASE 
          WHEN "VacanciesValue" = 'None'
              THEN 0
          ELSE COALESCE((
                      SELECT total_vacancies
                      FROM "WorkerJobStats"
                      WHERE "EstablishmentID" = e."EstablishmentID"
                          AND "JobID" = 4
                      ), - 1)
          END jr17vacy,
      -- jr22
      CASE 
          WHEN EXISTS (
                  SELECT 1
                  FROM "WorkerContractStats"
                  WHERE "EstablishmentFK" = e."EstablishmentID"
                      AND "MainJobFKValue" = 29
                  )
              THEN 1
          ELSE 0
          END jr22flag,
      COALESCE((
          SELECT total_perm_staff
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 29
          ), 0) jr22perm,
      COALESCE((
          SELECT total_temp_staff
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 29
          ), 0) jr22temp,
      COALESCE((
          SELECT total_pool_bank
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 29
          ), 0) jr22pool,
      COALESCE((
          SELECT total_agency
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 29
          ), 0) jr22agcy,
     COALESCE((
          SELECT total_other
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 29
          ), 0) jr22oth,
      COALESCE((
          SELECT total_employed
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 29
          ), 0) jr22emp,
      COALESCE((
          SELECT total_staff
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 29
          ), 0) jr22work,
      CASE 
          WHEN "StartersValue" = 'None'
              THEN 0
          ELSE COALESCE((
                      SELECT total_starters
                      FROM "WorkerJobStats"
                      WHERE "EstablishmentID" = e."EstablishmentID"
                          AND "JobID" = 29
                      ), - 1)
          END jr22strt,
      CASE 
          WHEN "LeaversValue" = 'None'
              THEN 0
          ELSE COALESCE((
                      SELECT total_leavers
                      FROM "WorkerJobStats"
                      WHERE "EstablishmentID" = e."EstablishmentID"
                          AND "JobID" = 29
                      ), - 1)
          END jr22stop,
      CASE 
          WHEN "VacanciesValue" = 'None'
              THEN 0
          ELSE COALESCE((
                      SELECT total_vacancies
                      FROM "WorkerJobStats"
                      WHERE "EstablishmentID" = e."EstablishmentID"
                          AND "JobID" = 29
                      ), - 1)
          END jr22vacy,
      -- jr23
      CASE 
          WHEN EXISTS (
                  SELECT 1
                  FROM "WorkerContractStats"
                  WHERE "EstablishmentFK" = e."EstablishmentID"
                      AND "MainJobFKValue" = 20
                  )
              THEN 1
          ELSE 0
          END jr23flag,
      COALESCE((
          SELECT total_perm_staff
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 20
          ), 0) jr23perm,
      COALESCE((
          SELECT total_temp_staff
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 20
          ), 0) jr23temp,
      COALESCE((
          SELECT total_pool_bank
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 20
          ), 0) jr23pool,
      COALESCE((
          SELECT total_agency
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 20
          ), 0) jr23agcy,
      COALESCE((
          SELECT total_other
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 20
          ), 0) jr23oth,
      COALESCE((
          SELECT total_employed
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 20
          ), 0) jr23emp,
      COALESCE((
          SELECT total_staff
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 20
          ), 0) jr23work,
      CASE 
          WHEN "StartersValue" = 'None'
              THEN 0
          ELSE COALESCE((
                      SELECT total_starters
                      FROM "WorkerJobStats"
                      WHERE "EstablishmentID" = e."EstablishmentID"
                          AND "JobID" = 20
                      ), - 1)
          END jr23strt,
      CASE 
          WHEN "LeaversValue" = 'None'
              THEN 0
          ELSE COALESCE((
                      SELECT total_leavers
                      FROM "WorkerJobStats"
                      WHERE "EstablishmentID" = e."EstablishmentID"
                          AND "JobID" = 20
                      ), - 1)
          END jr23stop,
      CASE 
          WHEN "VacanciesValue" = 'None'
              THEN 0
          ELSE COALESCE((
                      SELECT total_vacancies
                      FROM "WorkerJobStats"
                      WHERE "EstablishmentID" = e."EstablishmentID"
                          AND "JobID" = 20
                      ), - 1)
          END jr23vacy,
      -- jr24
      CASE 
          WHEN EXISTS (
                  SELECT 1
                  FROM "WorkerContractStats"
                  WHERE "EstablishmentFK" = e."EstablishmentID"
                      AND "MainJobFKValue" = 14
                  )
              THEN 1
          ELSE 0
          END jr24flag,
      COALESCE((
          SELECT total_perm_staff
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 14
          ), 0) jr24perm,
      COALESCE((
          SELECT total_temp_staff
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 14
          ), 0) jr24temp,
      COALESCE((
          SELECT total_pool_bank
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 14
          ), 0) jr24pool,
      COALESCE((
          SELECT total_agency
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 14
          ), 0) jr24agcy,
      COALESCE((
          SELECT total_other
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 14
          ), 0) jr24oth,
      COALESCE((
          SELECT total_employed
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 14
          ), 0) jr24emp,
      COALESCE((
          SELECT total_staff
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 14
          ), 0) jr24work,
      CASE 
          WHEN "StartersValue" = 'None'
              THEN 0
          ELSE COALESCE((
                      SELECT total_starters
                      FROM "WorkerJobStats"
                      WHERE "EstablishmentID" = e."EstablishmentID"
                          AND "JobID" = 14
                      ), - 1)
          END jr24strt,
      CASE 
          WHEN "LeaversValue" = 'None'
              THEN 0
          ELSE COALESCE((
                      SELECT total_leavers
                      FROM "WorkerJobStats"
                      WHERE "EstablishmentID" = e."EstablishmentID"
                          AND "JobID" = 14
                      ), - 1)
          END jr24stop,
      CASE 
          WHEN "VacanciesValue" = 'None'
              THEN 0
          ELSE COALESCE((
                      SELECT total_vacancies
                      FROM "WorkerJobStats"
                      WHERE "EstablishmentID" = e."EstablishmentID"
                          AND "JobID" = 14
                      ), - 1)
          END jr24vacy,
      -- jr25
      CASE 
          WHEN EXISTS (
                  SELECT 1
                  FROM "WorkerContractStats"
                  WHERE "EstablishmentFK" = e."EstablishmentID"
                      AND "MainJobFKValue" = 2
                  )
              THEN 1
          ELSE 0
          END jr25flag,
      COALESCE((
          SELECT total_perm_staff
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 2
          ), 0) jr25perm,
      COALESCE((
          SELECT total_temp_staff
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 2
          ), 0) jr25temp,
      COALESCE((
          SELECT total_pool_bank
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 2
          ), 0) jr25pool,
      COALESCE((
          SELECT total_agency
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 2
          ), 0) jr25agcy,
      COALESCE((
          SELECT total_other
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 2
          ), 0) jr25oth,
      COALESCE((
          SELECT total_employed
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 2
          ), 0) jr25emp,
      COALESCE((
          SELECT total_staff
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 2
          ), 0) jr25work,
      CASE 
          WHEN "StartersValue" = 'None'
              THEN 0
          ELSE COALESCE((
                      SELECT total_starters
                      FROM "WorkerJobStats"
                      WHERE "EstablishmentID" = e."EstablishmentID"
                          AND "JobID" = 2
                      ), - 1)
          END jr25strt,
      CASE 
          WHEN "LeaversValue" = 'None'
              THEN 0
          ELSE COALESCE((
                      SELECT total_leavers
                      FROM "WorkerJobStats"
                      WHERE "EstablishmentID" = e."EstablishmentID"
                          AND "JobID" = 2
                      ), - 1)
          END jr25stop,
      CASE 
          WHEN "VacanciesValue" = 'None'
              THEN 0
          ELSE COALESCE((
                      SELECT total_vacancies
                      FROM "WorkerJobStats"
                      WHERE "EstablishmentID" = e."EstablishmentID"
                          AND "JobID" = 2
                      ), - 1)
          END jr25vacy,
      -- jr26
      CASE 
          WHEN EXISTS (
                  SELECT 1
                  FROM "WorkerContractStats"
                  WHERE "EstablishmentFK" = e."EstablishmentID"
                      AND "MainJobFKValue" = 5
                  )
              THEN 1
          ELSE 0
          END jr26flag,
      COALESCE((
          SELECT total_perm_staff
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 5
          ), 0) jr26perm,
      COALESCE((
          SELECT total_temp_staff
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 5
          ), 0) jr26temp,
      COALESCE((
          SELECT total_pool_bank
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 5
          ), 0) jr26pool,
      COALESCE((
          SELECT total_agency
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 5
          ), 0) jr26agcy,
      COALESCE((
          SELECT total_other
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 5
          ), 0) jr26oth,
      COALESCE((
          SELECT total_employed
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 5
          ), 0) jr26emp,
      COALESCE((
          SELECT total_staff
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 5
          ), 0) jr26work,
      CASE 
          WHEN "StartersValue" = 'None'
              THEN 0
          ELSE COALESCE((
                      SELECT total_starters
                      FROM "WorkerJobStats"
                      WHERE "EstablishmentID" = e."EstablishmentID"
                          AND "JobID" = 5
                      ), - 1)
          END jr26strt,
      CASE 
          WHEN "LeaversValue" = 'None'
              THEN 0
          ELSE COALESCE((
                      SELECT total_leavers
                      FROM "WorkerJobStats"
                      WHERE "EstablishmentID" = e."EstablishmentID"
                          AND "JobID" = 5
                      ), - 1)
          END jr26stop,
      CASE 
          WHEN "VacanciesValue" = 'None'
              THEN 0
          ELSE COALESCE((
                      SELECT total_vacancies
                      FROM "WorkerJobStats"
                      WHERE "EstablishmentID" = e."EstablishmentID"
                          AND "JobID" = 5
                      ), - 1)
          END jr26vacy,
      -- jr27
      CASE 
          WHEN EXISTS (
                  SELECT 1
                  FROM "WorkerContractStats"
                  WHERE "EstablishmentFK" = e."EstablishmentID"
                      AND "MainJobFKValue" = 21
                  )
              THEN 1
          ELSE 0
          END jr27flag,
      COALESCE((
          SELECT total_perm_staff
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 21
          ), 0) jr27perm,
      COALESCE((
          SELECT total_temp_staff
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 21
          ), 0) jr27temp,
      COALESCE((
          SELECT total_pool_bank
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 21
          ), 0) jr27pool,
      COALESCE((
          SELECT total_agency
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 21
          ), 0) jr27agcy,
      COALESCE((
          SELECT total_other
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 21
          ), 0) jr27oth,
      COALESCE((
          SELECT total_employed
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 21
          ), 0) jr27emp,
      COALESCE((
          SELECT total_staff
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 21
          ), 0) jr27work,
      CASE 
          WHEN "StartersValue" = 'None'
              THEN 0
          ELSE COALESCE((
                      SELECT total_starters
                      FROM "WorkerJobStats"
                      WHERE "EstablishmentID" = e."EstablishmentID"
                          AND "JobID" = 21
                      ), - 1)
          END jr27strt,
      CASE 
          WHEN "LeaversValue" = 'None'
              THEN 0
          ELSE COALESCE((
                      SELECT total_leavers
                      FROM "WorkerJobStats"
                      WHERE "EstablishmentID" = e."EstablishmentID"
                          AND "JobID" = 21
                      ), - 1)
          END jr27stop,
      CASE 
          WHEN "VacanciesValue" = 'None'
              THEN 0
          ELSE COALESCE((
                      SELECT total_vacancies
                      FROM "WorkerJobStats"
                      WHERE "EstablishmentID" = e."EstablishmentID"
                          AND "JobID" = 21
                      ), - 1)
          END jr27vacy,
      -- jr34
      CASE 
          WHEN EXISTS (
                  SELECT 1
                  FROM "WorkerContractStats"
                  WHERE "EstablishmentFK" = e."EstablishmentID"
                      AND "MainJobFKValue" = 1
                  )
              THEN 1
          ELSE 0
          END jr34flag,
      COALESCE((
          SELECT total_perm_staff
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 1
          ), 0) jr34perm,
      COALESCE((
          SELECT total_temp_staff
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 1
          ), 0) jr34temp,
      COALESCE((
          SELECT total_pool_bank
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 1
          ), 0) jr34pool,
      COALESCE((
          SELECT total_agency
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 1
          ), 0) jr34agcy,
      COALESCE((
          SELECT total_other
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 1
          ), 0) jr34oth,
      COALESCE((
          SELECT total_employed
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 1
          ), 0) jr34emp,
      COALESCE((
          SELECT total_staff
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 1
          ), 0) jr34work,
      CASE 
          WHEN "StartersValue" = 'None'
              THEN 0
          ELSE COALESCE((
                      SELECT total_starters
                      FROM "WorkerJobStats"
                      WHERE "EstablishmentID" = e."EstablishmentID"
                          AND "JobID" = 1
                      ), - 1)
          END jr34strt,
      CASE 
          WHEN "LeaversValue" = 'None'
              THEN 0
          ELSE COALESCE((
                      SELECT total_leavers
                      FROM "WorkerJobStats"
                      WHERE "EstablishmentID" = e."EstablishmentID"
                          AND "JobID" = 1
                      ), - 1)
          END jr34stop,
      CASE 
          WHEN "VacanciesValue" = 'None'
              THEN 0
          ELSE COALESCE((
                      SELECT total_vacancies
                      FROM "WorkerJobStats"
                      WHERE "EstablishmentID" = e."EstablishmentID"
                          AND "JobID" = 1
                      ), - 1)
          END jr34vacy,
      -- jr35
      CASE 
          WHEN EXISTS (
                  SELECT 1
                  FROM "WorkerContractStats"
                  WHERE "EstablishmentFK" = e."EstablishmentID"
                      AND "MainJobFKValue" = 24
                  )
              THEN 1
          ELSE 0
          END jr35flag,
      COALESCE((
          SELECT total_perm_staff
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 24
          ), 0) jr35perm,
      COALESCE((
          SELECT total_temp_staff
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 24
          ), 0) jr35temp,
      COALESCE((
          SELECT total_pool_bank
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 24
          ), 0) jr35pool,
      COALESCE((
          SELECT total_agency
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 24
          ), 0) jr35agcy,
      COALESCE((
          SELECT total_other
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 24
          ), 0) jr35oth,
      COALESCE((
          SELECT total_employed
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 24
          ), 0) jr35emp,
      COALESCE((
          SELECT total_staff
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 24
          ), 0) jr35work,
      CASE 
          WHEN "StartersValue" = 'None'
              THEN 0
          ELSE COALESCE((
                      SELECT total_starters
                      FROM "WorkerJobStats"
                      WHERE "EstablishmentID" = e."EstablishmentID"
                          AND "JobID" = 24
                      ), - 1)
          END jr35strt,
      CASE 
          WHEN "LeaversValue" = 'None'
              THEN 0
          ELSE COALESCE((
                      SELECT total_leavers
                      FROM "WorkerJobStats"
                      WHERE "EstablishmentID" = e."EstablishmentID"
                          AND "JobID" = 24
                      ), - 1)
          END jr35stop,
      CASE 
          WHEN "VacanciesValue" = 'None'
              THEN 0
          ELSE COALESCE((
                      SELECT total_vacancies
                      FROM "WorkerJobStats"
                      WHERE "EstablishmentID" = e."EstablishmentID"
                          AND "JobID" = 24
                      ), - 1)
          END jr35vacy,
      -- jr36
      CASE 
          WHEN EXISTS (
                  SELECT 1
                  FROM "WorkerContractStats"
                  WHERE "EstablishmentFK" = e."EstablishmentID"
                      AND "MainJobFKValue" = 19
                  )
              THEN 1
          ELSE 0
          END jr36flag,
      COALESCE((
          SELECT total_perm_staff
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 19
          ), 0) jr36perm,
      COALESCE((
          SELECT total_temp_staff
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 19
          ), 0) jr36temp,
      COALESCE((
          SELECT total_pool_bank
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 19
          ), 0) jr36pool,
      COALESCE((
          SELECT total_agency
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 19
          ), 0) jr36agcy,
      COALESCE((
          SELECT total_other
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 19
          ), 0) jr36oth,
      COALESCE((
          SELECT total_employed
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 19
          ), 0) jr36emp,
      COALESCE((
          SELECT total_staff
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 19
          ), 0) jr36work,
      CASE 
          WHEN "StartersValue" = 'None'
              THEN 0
          ELSE COALESCE((
                      SELECT total_starters
                      FROM "WorkerJobStats"
                      WHERE "EstablishmentID" = e."EstablishmentID"
                          AND "JobID" = 19
                      ), - 1)
          END jr36strt,
      CASE 
          WHEN "LeaversValue" = 'None'
              THEN 0
          ELSE COALESCE((
                      SELECT total_leavers
                      FROM "WorkerJobStats"
                      WHERE "EstablishmentID" = e."EstablishmentID"
                          AND "JobID" = 19
                      ), - 1)
          END jr36stop,
      CASE 
          WHEN "VacanciesValue" = 'None'
              THEN 0
          ELSE COALESCE((
                      SELECT total_vacancies
                      FROM "WorkerJobStats"
                      WHERE "EstablishmentID" = e."EstablishmentID"
                          AND "JobID" = 19
                      ), - 1)
          END jr36vacy,
      -- jr37
      CASE 
          WHEN EXISTS (
                  SELECT 1
                  FROM "WorkerContractStats"
                  WHERE "EstablishmentFK" = e."EstablishmentID"
                      AND "MainJobFKValue" = 17
                  )
              THEN 1
          ELSE 0
          END jr37flag,
      COALESCE((
          SELECT total_perm_staff
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 17
          ), 0) jr37perm,
      COALESCE((
          SELECT total_temp_staff
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 17
          ), 0) jr37temp,
      COALESCE((
          SELECT total_pool_bank
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 17
          ), 0) jr37pool,
      COALESCE((
          SELECT total_agency
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 17
          ), 0) jr37agcy,
      COALESCE((
          SELECT total_other
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 17
          ), 0) jr37oth,
      COALESCE((
          SELECT total_employed
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 17
          ), 0) jr37emp,
      COALESCE((
          SELECT total_staff
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 17
          ), 0) jr37work,
      CASE 
          WHEN "StartersValue" = 'None'
              THEN 0
          ELSE COALESCE((
                      SELECT total_starters
                      FROM "WorkerJobStats"
                      WHERE "EstablishmentID" = e."EstablishmentID"
                          AND "JobID" = 17
                      ), - 1)
          END jr37strt,
      CASE 
          WHEN "LeaversValue" = 'None'
              THEN 0
          ELSE COALESCE((
                      SELECT total_leavers
                      FROM "WorkerJobStats"
                      WHERE "EstablishmentID" = e."EstablishmentID"
                          AND "JobID" = 17
                      ), - 1)
          END jr37stop,
      CASE 
          WHEN "VacanciesValue" = 'None'
              THEN 0
          ELSE COALESCE((
                      SELECT total_vacancies
                      FROM "WorkerJobStats"
                      WHERE "EstablishmentID" = e."EstablishmentID"
                          AND "JobID" = 17
                      ), - 1)
          END jr37vacy,
      -- jr38
      CASE 
          WHEN EXISTS (
                  SELECT 1
                  FROM "WorkerContractStats"
                  WHERE "EstablishmentFK" = e."EstablishmentID"
                      AND "MainJobFKValue" = 16
                  )
              THEN 1
          ELSE 0
          END jr38flag,
      COALESCE((
          SELECT total_perm_staff
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 16
          ), 0) jr38perm,
      COALESCE((
          SELECT total_temp_staff
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 16
          ), 0) jr38temp,
      COALESCE((
          SELECT total_pool_bank
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 16
          ), 0) jr38pool,
      COALESCE((
          SELECT total_agency
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 16
          ), 0) jr38agcy,
      COALESCE((
          SELECT total_other
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 16
          ), 0) jr38oth,
      COALESCE((
          SELECT total_employed
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 16
          ), 0) jr38emp,
      COALESCE((
          SELECT total_staff
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 16
          ), 0) jr38work,
      CASE 
          WHEN "StartersValue" = 'None'
              THEN 0
          ELSE COALESCE((
                      SELECT total_starters
                      FROM "WorkerJobStats"
                      WHERE "EstablishmentID" = e."EstablishmentID"
                          AND "JobID" = 16
                      ), - 1)
          END jr38strt,
      CASE 
          WHEN "LeaversValue" = 'None'
              THEN 0
          ELSE COALESCE((
                      SELECT total_leavers
                      FROM "WorkerJobStats"
                      WHERE "EstablishmentID" = e."EstablishmentID"
                          AND "JobID" = 16
                      ), - 1)
          END jr38stop,
      CASE 
          WHEN "VacanciesValue" = 'None'
              THEN 0
          ELSE COALESCE((
                      SELECT total_vacancies
                      FROM "WorkerJobStats"
                      WHERE "EstablishmentID" = e."EstablishmentID"
                          AND "JobID" = 16
                      ), - 1)
          END jr38vacy,
      -- jr39
      CASE 
          WHEN EXISTS (
                  SELECT 1
                  FROM "WorkerContractStats"
                  WHERE "EstablishmentFK" = e."EstablishmentID"
                      AND "MainJobFKValue" = 7
                  )
              THEN 1
          ELSE 0
          END jr39flag,
      COALESCE((
          SELECT total_perm_staff
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 7
          ), 0) jr39perm,
      COALESCE((
          SELECT total_temp_staff
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 7
          ), 0) jr39temp,
      COALESCE((
          SELECT total_pool_bank
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 7
          ), 0) jr39pool,
      COALESCE((
          SELECT total_agency
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 7
          ), 0) jr39agcy,
      COALESCE((
          SELECT total_other
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 7
          ), 0) jr39oth,
      COALESCE((
          SELECT total_employed
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 7
          ), 0) jr39emp,
      COALESCE((
          SELECT total_staff
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 7
          ), 0) jr39work,
      CASE 
          WHEN "StartersValue" = 'None'
              THEN 0
          ELSE COALESCE((
                      SELECT total_starters
                      FROM "WorkerJobStats"
                      WHERE "EstablishmentID" = e."EstablishmentID"
                          AND "JobID" = 7
                      ), - 1)
          END jr39strt,
      CASE 
          WHEN "LeaversValue" = 'None'
              THEN 0
          ELSE COALESCE((
                      SELECT total_leavers
                      FROM "WorkerJobStats"
                      WHERE "EstablishmentID" = e."EstablishmentID"
                          AND "JobID" = 7
                      ), - 1)
          END jr39stop,
      CASE 
          WHEN "VacanciesValue" = 'None'
              THEN 0
          ELSE COALESCE((
                      SELECT total_vacancies
                      FROM "WorkerJobStats"
                      WHERE "EstablishmentID" = e."EstablishmentID"
                          AND "JobID" = 7
                      ), - 1)
          END jr39vacy,
      -- jr40
      CASE 
          WHEN EXISTS (
                  SELECT 1
                  FROM "WorkerContractStats"
                  WHERE "EstablishmentFK" = e."EstablishmentID"
                      AND "MainJobFKValue" = 8
                  )
              THEN 1
          ELSE 0
          END jr40flag,
      COALESCE((
          SELECT total_perm_staff
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 8
          ), 0) jr40perm,
      COALESCE((
          SELECT total_temp_staff
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 8
          ), 0) jr40temp,
      COALESCE((
          SELECT total_pool_bank
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 8
          ), 0) jr40pool,
      COALESCE((
          SELECT total_agency
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 8
          ), 0) jr40agcy,
      COALESCE((
          SELECT total_other
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 8
          ), 0) jr40oth,
      COALESCE((
          SELECT total_employed
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 8
          ), 0) jr40emp,
      COALESCE((
          SELECT total_staff
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 8
          ), 0) jr40work,
      CASE 
          WHEN "StartersValue" = 'None'
              THEN 0
          ELSE COALESCE((
                      SELECT total_starters
                      FROM "WorkerJobStats"
                      WHERE "EstablishmentID" = e."EstablishmentID"
                          AND "JobID" = 8
                      ), - 1)
          END jr40strt,
      CASE 
          WHEN "LeaversValue" = 'None'
              THEN 0
          ELSE COALESCE((
                      SELECT total_leavers
                      FROM "WorkerJobStats"
                      WHERE "EstablishmentID" = e."EstablishmentID"
                          AND "JobID" = 8
                      ), - 1)
          END jr40stop,
      CASE 
          WHEN "VacanciesValue" = 'None'
              THEN 0
          ELSE COALESCE((
                      SELECT total_vacancies
                      FROM "WorkerJobStats"
                      WHERE "EstablishmentID" = e."EstablishmentID"
                          AND "JobID" = 8
                      ), - 1)
          END jr40vacy,
      -- jr41
      CASE 
          WHEN EXISTS (
                  SELECT 1
                  FROM "WorkerContractStats"
                  WHERE "EstablishmentFK" = e."EstablishmentID"
                      AND "MainJobFKValue" = 9
                  )
              THEN 1
          ELSE 0
          END jr41flag,
      COALESCE((
          SELECT total_perm_staff
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 9
          ), 0) jr41perm,
      COALESCE((
          SELECT total_temp_staff
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 9
          ), 0) jr41temp,
      COALESCE((
          SELECT total_pool_bank
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 9
          ), 0) jr41pool,
      COALESCE((
          SELECT total_agency
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 9
          ), 0) jr41agcy,
      COALESCE((
          SELECT total_other
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 9
          ), 0) jr41oth,
      COALESCE((
          SELECT total_employed
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 9
          ), 0) jr41emp,
      COALESCE((
          SELECT total_staff
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 9
          ), 0) jr41work,
      CASE 
          WHEN "StartersValue" = 'None'
              THEN 0
          ELSE COALESCE((
                      SELECT total_starters
                      FROM "WorkerJobStats"
                      WHERE "EstablishmentID" = e."EstablishmentID"
                          AND "JobID" = 9
                      ), - 1)
          END jr41strt,
      CASE 
          WHEN "LeaversValue" = 'None'
              THEN 0
          ELSE COALESCE((
                      SELECT total_leavers
                      FROM "WorkerJobStats"
                      WHERE "EstablishmentID" = e."EstablishmentID"
                          AND "JobID" = 9
                      ), - 1)
          END jr41stop,
      CASE 
          WHEN "VacanciesValue" = 'None'
              THEN 0
          ELSE COALESCE((
                      SELECT total_vacancies
                      FROM "WorkerJobStats"
                      WHERE "EstablishmentID" = e."EstablishmentID"
                          AND "JobID" = 9
                      ), - 1)
          END jr41vacy,
      -- jr42
      CASE 
          WHEN EXISTS (
                  SELECT 1
                  FROM "WorkerContractStats"
                  WHERE "EstablishmentFK" = e."EstablishmentID"
                      AND "MainJobFKValue" = 6
                  )
              THEN 1
          ELSE 0
          END jr42flag,
      COALESCE((
          SELECT total_perm_staff
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 6
          ), 0) jr42perm,
      COALESCE((
          SELECT total_temp_staff
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 6
          ), 0) jr42temp,
      COALESCE((
          SELECT total_pool_bank
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 6
          ), 0) jr42pool,
      COALESCE((
          SELECT total_agency
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 6
          ), 0) jr42agcy,
      COALESCE((
          SELECT total_other
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 6
          ), 0) jr42oth,
      COALESCE((
          SELECT total_employed
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 6
          ), 0) jr42emp,
      COALESCE((
          SELECT total_staff
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 6
          ), 0) jr42work,
      CASE 
          WHEN "StartersValue" = 'None'
              THEN 0
          ELSE COALESCE((
                      SELECT total_starters
                      FROM "WorkerJobStats"
                      WHERE "EstablishmentID" = e."EstablishmentID"
                          AND "JobID" = 6
                      ), - 1)
          END jr42strt,
      CASE 
          WHEN "LeaversValue" = 'None'
              THEN 0
          ELSE COALESCE((
                      SELECT total_leavers
                      FROM "WorkerJobStats"
                      WHERE "EstablishmentID" = e."EstablishmentID"
                          AND "JobID" = 6
                      ), - 1)
          END jr42stop,
      CASE 
          WHEN "VacanciesValue" = 'None'
              THEN 0
          ELSE COALESCE((
                      SELECT total_vacancies
                      FROM "WorkerJobStats"
                      WHERE "EstablishmentID" = e."EstablishmentID"
                          AND "JobID" = 6
                      ), - 1)
          END jr42vacy,
      -- jr43
      CASE 
          WHEN EXISTS (
                  SELECT 1
                  FROM "WorkerContractStats"
                  WHERE "EstablishmentFK" = e."EstablishmentID"
                      AND "MainJobFKValue" = 30
                  )
              THEN 1
          ELSE 0
          END jr43flag,
      COALESCE((
          SELECT total_perm_staff
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 30
          ), 0) jr43perm,
      COALESCE((
          SELECT total_temp_staff
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 30
          ), 0) jr43temp,
      COALESCE((
          SELECT total_pool_bank
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 30
          ), 0) jr43pool,
      COALESCE((
          SELECT total_agency
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 30
          ), 0) jr43agcy,
      COALESCE((
          SELECT total_other
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 30
          ), 0) jr43oth,
      COALESCE((
          SELECT total_employed
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 30
          ), 0) jr43emp,
      COALESCE((
          SELECT total_staff
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 30
          ), 0) jr43work,
      CASE 
          WHEN "StartersValue" = 'None'
              THEN 0
          ELSE COALESCE((
                      SELECT total_starters
                      FROM "WorkerJobStats"
                      WHERE "EstablishmentID" = e."EstablishmentID"
                          AND "JobID" = 30
                      ), - 1)
          END jr43strt,
      CASE 
          WHEN "LeaversValue" = 'None'
              THEN 0
          ELSE COALESCE((
                      SELECT total_leavers
                      FROM "WorkerJobStats"
                      WHERE "EstablishmentID" = e."EstablishmentID"
                          AND "JobID" = 30
                      ), - 1)
          END jr43stop,
      CASE 
          WHEN "VacanciesValue" = 'None'
              THEN 0
          ELSE COALESCE((
                      SELECT total_vacancies
                      FROM "WorkerJobStats"
                      WHERE "EstablishmentID" = e."EstablishmentID"
                          AND "JobID" = 30
                      ), - 1)
          END jr43vacy,
      -- jr44
      CASE 
          WHEN EXISTS (
                  SELECT 1
                  FROM "WorkerContractStats"
                  WHERE "EstablishmentFK" = e."EstablishmentID"
                      AND "MainJobFKValue" = 31
                  )
              THEN 1
          ELSE 0
          END jr44flag,
      COALESCE((
          SELECT total_perm_staff
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 31
          ), 0) jr44perm,
      COALESCE((
          SELECT total_temp_staff
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 31
          ), 0) jr44temp,
      COALESCE((
          SELECT total_pool_bank
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 31
          ), 0) jr44pool,
      COALESCE((
          SELECT total_agency
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 31
          ), 0) jr44agcy,
      COALESCE((
          SELECT total_other
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 31
          ), 0) jr44oth,
      COALESCE((
          SELECT total_employed
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 31
          ), 0) jr44emp,
      COALESCE((
          SELECT total_staff
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 31
          ), 0) jr44work,
      CASE 
          WHEN "StartersValue" = 'None'
              THEN 0
          ELSE COALESCE((
                      SELECT total_starters
                      FROM "WorkerJobStats"
                      WHERE "EstablishmentID" = e."EstablishmentID"
                          AND "JobID" = 31
                      ), - 1)
          END jr44strt,
      CASE 
          WHEN "LeaversValue" = 'None'
              THEN 0
          ELSE COALESCE((
                      SELECT total_leavers
                      FROM "WorkerJobStats"
                      WHERE "EstablishmentID" = e."EstablishmentID"
                          AND "JobID" = 31
                      ), - 1)
          END jr44stop,
      CASE 
          WHEN "VacanciesValue" = 'None'
              THEN 0
          ELSE COALESCE((
                      SELECT total_vacancies
                      FROM "WorkerJobStats"
                      WHERE "EstablishmentID" = e."EstablishmentID"
                          AND "JobID" = 31
                      ), - 1)
          END jr44vacy,
      -- jr45
      CASE 
          WHEN EXISTS (
                  SELECT 1
                  FROM "WorkerContractStats"
                  WHERE "EstablishmentFK" = e."EstablishmentID"
                      AND "MainJobFKValue" = 32
                  )
              THEN 1
          ELSE 0
          END jr45flag,
      COALESCE((
          SELECT total_perm_staff
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 32
          ), 0) jr45perm,
      COALESCE((
          SELECT total_temp_staff
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 32
          ), 0) jr45temp,
      COALESCE((
          SELECT total_pool_bank
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 32
          ), 0) jr45pool,
      COALESCE((
          SELECT total_agency
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 32
          ), 0) jr45agcy,
      COALESCE((
          SELECT total_other
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 32
          ), 0) jr45oth,
      COALESCE((
          SELECT total_employed
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 32
          ), 0) jr45emp,
      COALESCE((
          SELECT total_staff
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 32
          ), 0) jr45work,
      CASE 
          WHEN "StartersValue" = 'None'
              THEN 0
          ELSE COALESCE((
                      SELECT total_starters
                      FROM "WorkerJobStats"
                      WHERE "EstablishmentID" = e."EstablishmentID"
                          AND "JobID" = 32
                      ), - 1)
          END jr45strt,
      CASE 
          WHEN "LeaversValue" = 'None'
              THEN 0
          ELSE COALESCE((
                      SELECT total_leavers
                      FROM "WorkerJobStats"
                      WHERE "EstablishmentID" = e."EstablishmentID"
                          AND "JobID" = 32
                      ), - 1)
          END jr45stop,
      CASE 
          WHEN "VacanciesValue" = 'None'
              THEN 0
          ELSE COALESCE((
                      SELECT total_vacancies
                      FROM "WorkerJobStats"
                      WHERE "EstablishmentID" = e."EstablishmentID"
                          AND "JobID" = 32
                      ), - 1)
          END jr45vacy, 
      -- jr46
      CASE 
          WHEN EXISTS (
                  SELECT 1
                  FROM "WorkerContractStats"
                  WHERE "EstablishmentFK" = e."EstablishmentID"
                      AND "MainJobFKValue" = 33
                  )
              THEN 1
          ELSE 0
          END jr46flag,
      COALESCE((
          SELECT total_perm_staff
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 33
          ), 0) jr46perm,
      COALESCE((
          SELECT total_temp_staff
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 33
          ), 0) jr46temp,
      COALESCE((
          SELECT total_pool_bank
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 33
          ), 0) jr46pool,
      COALESCE((
          SELECT total_agency
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 33
          ), 0) jr46agcy,
      COALESCE((
          SELECT total_other
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 33
          ), 0) jr46oth,
      COALESCE((
          SELECT total_employed
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 33
          ), 0) jr46emp,
      COALESCE((
          SELECT total_staff
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 33
          ), 0) jr46work,
      CASE 
          WHEN "StartersValue" = 'None'
              THEN 0
          ELSE COALESCE((
                      SELECT total_starters
                      FROM "WorkerJobStats"
                      WHERE "EstablishmentID" = e."EstablishmentID"
                          AND "JobID" = 33
                      ), - 1)
          END jr46strt,
      CASE 
          WHEN "LeaversValue" = 'None'
              THEN 0
          ELSE COALESCE((
                      SELECT total_leavers
                      FROM "WorkerJobStats"
                      WHERE "EstablishmentID" = e."EstablishmentID"
                          AND "JobID" = 33
                      ), - 1)
          END jr46stop,
      CASE 
          WHEN "VacanciesValue" = 'None'
              THEN 0
          ELSE COALESCE((
                      SELECT total_vacancies
                      FROM "WorkerJobStats"
                      WHERE "EstablishmentID" = e."EstablishmentID"
                          AND "JobID" = 33
                      ), - 1)
          END jr46vacy,
      -- jr47
      CASE 
          WHEN EXISTS (
                  SELECT 1
                  FROM "WorkerContractStats"
                  WHERE "EstablishmentFK" = e."EstablishmentID"
                      AND "MainJobFKValue" = 34
                  )
              THEN 1
          ELSE 0
          END jr47flag,
      COALESCE((
          SELECT total_perm_staff
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 34
          ), 0) jr47perm,
      COALESCE((
          SELECT total_temp_staff
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 34
          ), 0) jr47temp,
      COALESCE((
          SELECT total_pool_bank
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 34
          ), 0) jr47pool,
      COALESCE((
          SELECT total_agency
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 34
          ), 0) jr47agcy,
      COALESCE((
          SELECT total_other
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 34
          ), 0) jr47oth,
      COALESCE((
          SELECT total_employed
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 34
          ), 0) jr47emp,
      COALESCE((
          SELECT total_staff
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 34
          ), 0) jr47work,
      CASE 
          WHEN "StartersValue" = 'None'
              THEN 0
          ELSE COALESCE((
                      SELECT total_starters
                      FROM "WorkerJobStats"
                      WHERE "EstablishmentID" = e."EstablishmentID"
                          AND "JobID" = 34
                      ), - 1)
          END jr47strt,
      CASE 
          WHEN "LeaversValue" = 'None'
              THEN 0
          ELSE COALESCE((
                      SELECT total_leavers
                      FROM "WorkerJobStats"
                      WHERE "EstablishmentID" = e."EstablishmentID"
                          AND "JobID" = 34
                      ), - 1)
          END jr47stop,
      CASE 
          WHEN "VacanciesValue" = 'None'
              THEN 0
          ELSE COALESCE((
                      SELECT total_vacancies
                      FROM "WorkerJobStats"
                      WHERE "EstablishmentID" = e."EstablishmentID"
                          AND "JobID" = 34
                      ), - 1)
          END jr47vacy,
      -- jr48
      CASE 
          WHEN EXISTS (
                  SELECT 1
                  FROM "WorkerContractStats"
                  WHERE "EstablishmentFK" = e."EstablishmentID"
                      AND "MainJobFKValue" = 35
                  )
              THEN 1
          ELSE 0
          END jr48flag,
      COALESCE((
          SELECT total_perm_staff
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 35
          ), 0) jr48perm,
      COALESCE((
          SELECT total_temp_staff
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 35
          ), 0) jr48temp,
      COALESCE((
          SELECT total_pool_bank
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 35
          ), 0) jr48pool,
      COALESCE((
          SELECT total_agency
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 35
          ), 0) jr48agcy,
      COALESCE((
          SELECT total_other
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 35
          ), 0) jr48oth,
      COALESCE((
          SELECT total_employed
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 35
          ), 0) jr48emp,
      COALESCE((
          SELECT total_staff
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 35
          ), 0) jr48work,
      CASE 
          WHEN "StartersValue" = 'None'
              THEN 0
          ELSE COALESCE((
                      SELECT total_starters
                      FROM "WorkerJobStats"
                      WHERE "EstablishmentID" = e."EstablishmentID"
                          AND "JobID" = 35
                      ), - 1)
          END jr48strt,
      CASE 
          WHEN "LeaversValue" = 'None'
              THEN 0
          ELSE COALESCE((
                      SELECT total_leavers
                      FROM "WorkerJobStats"
                      WHERE "EstablishmentID" = e."EstablishmentID"
                          AND "JobID" = 35
                      ), - 1)
          END jr48stop,
      CASE 
          WHEN "VacanciesValue" = 'None'
              THEN 0
          ELSE COALESCE((
                      SELECT total_vacancies
                      FROM "WorkerJobStats"
                      WHERE "EstablishmentID" = e."EstablishmentID"
                          AND "JobID" = 35
                      ), - 1)
          END jr48vacy,
      -- jr49
      CASE 
          WHEN EXISTS (
                  SELECT 1
                  FROM "WorkerContractStats"
                  WHERE "EstablishmentFK" = e."EstablishmentID"
                      AND "MainJobFKValue" = 36
                  )
              THEN 1
          ELSE 0
          END jr49flag,
      COALESCE((
          SELECT total_perm_staff
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 36
          ), 0) jr49perm,
      COALESCE((
          SELECT total_temp_staff
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 36
          ), 0) jr49temp,
      COALESCE((
          SELECT total_pool_bank
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 36
          ), 0) jr49pool,
      COALESCE((
          SELECT total_agency
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 36
          ), 0) jr49agcy,
      COALESCE((
          SELECT total_other
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 36
          ), 0) jr49oth,
      COALESCE((
          SELECT total_employed
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 36
          ), 0) jr49emp,
      COALESCE((
          SELECT total_staff
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 36
          ), 0) jr49work,
      CASE 
          WHEN "StartersValue" = 'None'
              THEN 0
          ELSE COALESCE((
                      SELECT total_starters
                      FROM "WorkerJobStats"
                      WHERE "EstablishmentID" = e."EstablishmentID"
                          AND "JobID" = 36
                      ), - 1)
          END jr49strt,
      CASE 
          WHEN "LeaversValue" = 'None'
              THEN 0
          ELSE COALESCE((
                      SELECT total_leavers
                      FROM "WorkerJobStats"
                      WHERE "EstablishmentID" = e."EstablishmentID"
                          AND "JobID" = 36
                      ), - 1)
          END jr49stop,
      CASE 
          WHEN "VacanciesValue" = 'None'
              THEN 0
          ELSE COALESCE((
                      SELECT total_vacancies
                      FROM "WorkerJobStats"
                      WHERE "EstablishmentID" = e."EstablishmentID"
                          AND "JobID" = 36
                      ), - 1)
          END jr49vacy,
      -- jr50
      CASE 
          WHEN EXISTS (
                  SELECT 1
                  FROM "WorkerContractStats"
                  WHERE "EstablishmentFK" = e."EstablishmentID"
                      AND "MainJobFKValue" = 37
                  )
              THEN 1
          ELSE 0
          END jr50flag,
      COALESCE((
          SELECT total_perm_staff
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 37
          ), 0) jr50perm,
      COALESCE((
          SELECT total_temp_staff
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 37
          ), 0) jr50temp,
      COALESCE((
          SELECT total_pool_bank
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 37
          ), 0) jr50pool,
      COALESCE((
          SELECT total_agency
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 37
          ), 0) jr50agcy,
      COALESCE((
          SELECT total_other
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 37
          ), 0) jr50oth,
      COALESCE((
          SELECT total_employed
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 37
          ), 0) jr50emp,
      COALESCE((
          SELECT total_staff
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 37
          ), 0) jr50work,
      CASE 
          WHEN "StartersValue" = 'None'
              THEN 0
          ELSE COALESCE((
                      SELECT total_starters
                      FROM "WorkerJobStats"
                      WHERE "EstablishmentID" = e."EstablishmentID"
                          AND "JobID" = 37
                      ), - 1)
          END jr50strt,
      CASE 
          WHEN "LeaversValue" = 'None'
              THEN 0
          ELSE COALESCE((
                      SELECT total_leavers
                      FROM "WorkerJobStats"
                      WHERE "EstablishmentID" = e."EstablishmentID"
                          AND "JobID" = 37
                      ), - 1)
          END jr50stop,
      CASE 
          WHEN "VacanciesValue" = 'None'
              THEN 0
          ELSE COALESCE((
                      SELECT total_vacancies
                      FROM "WorkerJobStats"
                      WHERE "EstablishmentID" = e."EstablishmentID"
                          AND "JobID" = 37
                      ), - 1)
          END jr50vacy,
      -- jr51
      CASE 
          WHEN EXISTS (
                  SELECT 1
                  FROM "WorkerContractStats"
                  WHERE "EstablishmentFK" = e."EstablishmentID"
                      AND "MainJobFKValue" = 38
                  )
              THEN 1
          ELSE 0
          END jr51flag,
      COALESCE((
          SELECT total_perm_staff
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 38
          ), 0) jr51perm,
      COALESCE((
          SELECT total_temp_staff
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 38
          ), 0) jr51temp,
      COALESCE((
          SELECT total_pool_bank
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 38
          ), 0) jr51pool,
      COALESCE((
          SELECT total_agency
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 38
          ), 0) jr51agcy,
      COALESCE((
          SELECT total_other
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 38
          ), 0) jr51oth,
      COALESCE((
          SELECT total_employed
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 38
          ), 0) jr51emp,
      COALESCE((
          SELECT total_staff
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 38
          ), 0) jr51work,
      CASE 
          WHEN "StartersValue" = 'None'
              THEN 0
          ELSE COALESCE((
                      SELECT total_starters
                      FROM "WorkerJobStats"
                      WHERE "EstablishmentID" = e."EstablishmentID"
                          AND "JobID" = 38
                      ), - 1)
          END jr51strt,
      CASE 
          WHEN "LeaversValue" = 'None'
              THEN 0
          ELSE COALESCE((
                      SELECT total_leavers
                      FROM "WorkerJobStats"
                      WHERE "EstablishmentID" = e."EstablishmentID"
                          AND "JobID" = 38
                      ), - 1)
          END jr51stop,
      CASE 
          WHEN "VacanciesValue" = 'None'
              THEN 0
          ELSE COALESCE((
                      SELECT total_vacancies
                      FROM "WorkerJobStats"
                      WHERE "EstablishmentID" = e."EstablishmentID"
                          AND "JobID" = 38
                      ), - 1)
          END jr51vacy,
      -- jr52
      CASE 
          WHEN EXISTS (
                  SELECT 1
                  FROM "WorkerContractStats"
                  WHERE "EstablishmentFK" = e."EstablishmentID"
                      AND "MainJobFKValue" = 39
                  )
              THEN 1
          ELSE 0
          END jr52flag,
      COALESCE((
          SELECT total_perm_staff
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 39
          ), 0) jr52perm,
      COALESCE((
          SELECT total_temp_staff
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 39
          ), 0) jr52temp,
      COALESCE((
          SELECT total_pool_bank
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 39
          ), 0) jr52pool,
      COALESCE((
          SELECT total_agency
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 39
          ), 0) jr52agcy,
      COALESCE((
          SELECT total_other
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 39
          ), 0) jr52oth,
      COALESCE((
          SELECT total_employed
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 39
          ), 0) jr52emp,
      COALESCE((
          SELECT total_staff
          FROM "WorkerContractStats"
          WHERE "EstablishmentFK" = e."EstablishmentID"
              AND "MainJobFKValue" = 39
          ), 0) jr52work,
      CASE 
          WHEN "StartersValue" = 'None'
              THEN 0
          ELSE COALESCE((
                      SELECT total_starters
                      FROM "WorkerJobStats"
                      WHERE "EstablishmentID" = e."EstablishmentID"
                          AND "JobID" = 39
                      ), - 1)
          END jr52strt,
      CASE 
          WHEN "LeaversValue" = 'None'
              THEN 0
          ELSE COALESCE((
                      SELECT total_leavers
                      FROM "WorkerJobStats"
                      WHERE "EstablishmentID" = e."EstablishmentID"
                          AND "JobID" = 39
                      ), - 1)
          END jr52stop,
      CASE 
          WHEN "VacanciesValue" = 'None'
              THEN 0
          ELSE COALESCE((
                      SELECT total_vacancies
                      FROM "WorkerJobStats"
                      WHERE "EstablishmentID" = e."EstablishmentID"
                          AND "JobID" = 39
                      ), - 1)
          END jr52vacy,
      TO_CHAR("ServiceUsersChangedAt", 'DD/MM/YYYY') ut_changedate,
      TO_CHAR("ServiceUsersSavedAt", 'DD/MM/YYYY') ut_savedate,
      COALESCE((
              SELECT 1
              FROM "EstablishmentServiceUsers"
              WHERE "EstablishmentID" = e."EstablishmentID"
                  AND "ServiceUserID" = 1 LIMIT 1
              ), 0) ut01flag,
      COALESCE((
              SELECT 1
              FROM "EstablishmentServiceUsers"
              WHERE "EstablishmentID" = e."EstablishmentID"
                  AND "ServiceUserID" = 2 LIMIT 1
              ), 0) ut02flag,
      COALESCE((
              SELECT 1
              FROM "EstablishmentServiceUsers"
              WHERE "EstablishmentID" = e."EstablishmentID"
                  AND "ServiceUserID" = 3 LIMIT 1
              ), 0) ut22flag,
      COALESCE((
              SELECT 1
              FROM "EstablishmentServiceUsers"
              WHERE "EstablishmentID" = e."EstablishmentID"
                  AND "ServiceUserID" = 4 LIMIT 1
              ), 0) ut23flag,
      COALESCE((
              SELECT 1
              FROM "EstablishmentServiceUsers"
              WHERE "EstablishmentID" = e."EstablishmentID"
                  AND "ServiceUserID" = 5 LIMIT 1
              ), 0) ut25flag,
      COALESCE((
              SELECT 1
              FROM "EstablishmentServiceUsers"
              WHERE "EstablishmentID" = e."EstablishmentID"
                  AND "ServiceUserID" = 6 LIMIT 1
              ), 0) ut26flag,
      COALESCE((
              SELECT 1
              FROM "EstablishmentServiceUsers"
              WHERE "EstablishmentID" = e."EstablishmentID"
                  AND "ServiceUserID" = 7 LIMIT 1
              ), 0) ut27flag,
      COALESCE((
              SELECT 1
              FROM "EstablishmentServiceUsers"
              WHERE "EstablishmentID" = e."EstablishmentID"
                  AND "ServiceUserID" = 8 LIMIT 1
              ), 0) ut46flag,
      COALESCE((
              SELECT 1
              FROM "EstablishmentServiceUsers"
              WHERE "EstablishmentID" = e."EstablishmentID"
                  AND "ServiceUserID" = 9 LIMIT 1
              ), 0) ut03flag,
      COALESCE((
              SELECT 1
              FROM "EstablishmentServiceUsers"
              WHERE "EstablishmentID" = e."EstablishmentID"
                  AND "ServiceUserID" = 10 LIMIT 1
              ), 0) ut28flag,
      COALESCE((
              SELECT 1
              FROM "EstablishmentServiceUsers"
              WHERE "EstablishmentID" = e."EstablishmentID"
                  AND "ServiceUserID" = 11 LIMIT 1
              ), 0) ut06flag,
      COALESCE((
              SELECT 1
              FROM "EstablishmentServiceUsers"
              WHERE "EstablishmentID" = e."EstablishmentID"
                  AND "ServiceUserID" = 12 LIMIT 1
              ), 0) ut29flag,
      COALESCE((
              SELECT 1
              FROM "EstablishmentServiceUsers"
              WHERE "EstablishmentID" = e."EstablishmentID"
                  AND "ServiceUserID" = 13 LIMIT 1
              ), 0) ut05flag,
      COALESCE((
              SELECT 1
              FROM "EstablishmentServiceUsers"
              WHERE "EstablishmentID" = e."EstablishmentID"
                  AND "ServiceUserID" = 14 LIMIT 1
              ), 0) ut04flag,
      COALESCE((
              SELECT 1
              FROM "EstablishmentServiceUsers"
              WHERE "EstablishmentID" = e."EstablishmentID"
                  AND "ServiceUserID" = 15 LIMIT 1
              ), 0) ut07flag,
      COALESCE((
              SELECT 1
              FROM "EstablishmentServiceUsers"
              WHERE "EstablishmentID" = e."EstablishmentID"
                  AND "ServiceUserID" = 16 LIMIT 1
              ), 0) ut08flag,
      COALESCE((
              SELECT 1
              FROM "EstablishmentServiceUsers"
              WHERE "EstablishmentID" = e."EstablishmentID"
                  AND "ServiceUserID" = 17 LIMIT 1
              ), 0) ut31flag,
      COALESCE((
              SELECT 1
              FROM "EstablishmentServiceUsers"
              WHERE "EstablishmentID" = e."EstablishmentID"
                  AND "ServiceUserID" = 18 LIMIT 1
              ), 0) ut09flag,
      COALESCE((
              SELECT 1
              FROM "EstablishmentServiceUsers"
              WHERE "EstablishmentID" = e."EstablishmentID"
                  AND "ServiceUserID" = 19 LIMIT 1
              ), 0) ut45flag,
      COALESCE((
              SELECT 1
              FROM "EstablishmentServiceUsers"
              WHERE "EstablishmentID" = e."EstablishmentID"
                  AND "ServiceUserID" = 20 LIMIT 1
              ), 0) ut18flag,
      COALESCE((
              SELECT 1
              FROM "EstablishmentServiceUsers"
              WHERE "EstablishmentID" = e."EstablishmentID"
                  AND "ServiceUserID" = 21 LIMIT 1
              ), 0) ut19flag,
      COALESCE((
              SELECT 1
              FROM "EstablishmentServiceUsers"
              WHERE "EstablishmentID" = e."EstablishmentID"
                  AND "ServiceUserID" = 22 LIMIT 1
              ), 0) ut20flag,
      COALESCE((
              SELECT 1
              FROM "EstablishmentServiceUsers"
              WHERE "EstablishmentID" = e."EstablishmentID"
                  AND "ServiceUserID" = 23 LIMIT 1
              ), 0) ut21flag,
      TO_CHAR(GREATEST("MainServiceFKChangedAt", "OtherServicesChangedAt"), 'DD/MM/YYYY') st_changedate,
      TO_CHAR(GREATEST("MainServiceFKSavedAt", "OtherServicesSavedAt"), 'DD/MM/YYYY') st_savedate,
      CASE 
          WHEN "MainServiceFKValue" = 24
              OR (
                  SELECT COUNT(1)
                  FROM "EstablishmentServices"
                  WHERE "EstablishmentID" = e."EstablishmentID"
                      AND "ServiceID" = 24
                  ) = 1
              THEN 1
          ELSE 0
          END st01flag,
      COALESCE((
              SELECT "Answer"
              FROM "EstablishmentCapacity" ec
              JOIN "ServicesCapacity" sc ON ec."ServiceCapacityID" = sc."ServiceCapacityID"
                  AND sc."ServiceID" = 24
                  AND sc."Type" = 'Capacity'
              WHERE ec."EstablishmentID" = e."EstablishmentID"
              ), - 1) st01cap,
      CASE 
          WHEN "MainServiceFKValue" = 24
              OR (
                  SELECT COUNT(1)
                  FROM "EstablishmentServices"
                  WHERE "EstablishmentID" = e."EstablishmentID"
                      AND "ServiceID" = 24
                  ) = 1
              THEN TO_CHAR("CapacityServicesChangedAt", 'DD/MM/YYYY')
          ELSE NULL
          END st01cap_changedate,
      CASE 
          WHEN "MainServiceFKValue" = 24
              OR (
                  SELECT COUNT(1)
                  FROM "EstablishmentServices"
                  WHERE "EstablishmentID" = e."EstablishmentID"
                      AND "ServiceID" = 24
                  ) = 1
              THEN TO_CHAR("CapacityServicesSavedAt", 'DD/MM/YYYY')
          ELSE NULL
          END st01cap_savedate,
      COALESCE((
              SELECT "Answer"
              FROM "EstablishmentCapacity" ec
              JOIN "ServicesCapacity" sc ON ec."ServiceCapacityID" = sc."ServiceCapacityID"
                  AND sc."ServiceID" = 24
                  AND sc."Type" = 'Utilisation'
              WHERE ec."EstablishmentID" = e."EstablishmentID"
              ), - 1) st01util,
      CASE 
          WHEN "MainServiceFKValue" = 24
              OR (
                  SELECT COUNT(1)
                  FROM "EstablishmentServices"
                  WHERE "EstablishmentID" = e."EstablishmentID"
                      AND "ServiceID" = 24
                  ) = 1
              THEN TO_CHAR("CapacityServicesChangedAt", 'DD/MM/YYYY')
          ELSE NULL
          END st01util_changedate,
      CASE 
          WHEN "MainServiceFKValue" = 24
              OR (
                  SELECT COUNT(1)
                  FROM "EstablishmentServices"
                  WHERE "EstablishmentID" = e."EstablishmentID"
                      AND "ServiceID" = 24
                  ) = 1
              THEN TO_CHAR("CapacityServicesSavedAt", 'DD/MM/YYYY')
          ELSE NULL
          END st01util_savedate,
      CASE 
          WHEN "MainServiceFKValue" = 25
              OR (
                  SELECT COUNT(1)
                  FROM "EstablishmentServices"
                  WHERE "EstablishmentID" = e."EstablishmentID"
                      AND "ServiceID" = 25
                  ) = 1
              THEN 1
          ELSE 0
          END st02flag,
      COALESCE((
              SELECT "Answer"
              FROM "EstablishmentCapacity" ec
              JOIN "ServicesCapacity" sc ON ec."ServiceCapacityID" = sc."ServiceCapacityID"
                  AND sc."ServiceID" = 25
                  AND sc."Type" = 'Capacity'
              WHERE ec."EstablishmentID" = e."EstablishmentID"
              ), - 1) st02cap,
      CASE 
          WHEN "MainServiceFKValue" = 25
              OR (
                  SELECT COUNT(1)
                  FROM "EstablishmentServices"
                  WHERE "EstablishmentID" = e."EstablishmentID"
                      AND "ServiceID" = 25
                  ) = 1
              THEN TO_CHAR("CapacityServicesChangedAt", 'DD/MM/YYYY')
          ELSE NULL
          END st02cap_changedate,
      CASE 
          WHEN "MainServiceFKValue" = 25
              OR (
                  SELECT COUNT(1)
                  FROM "EstablishmentServices"
                  WHERE "EstablishmentID" = e."EstablishmentID"
                      AND "ServiceID" = 25
                  ) = 1
              THEN TO_CHAR("CapacityServicesSavedAt", 'DD/MM/YYYY')
          ELSE NULL
          END st02cap_savedate,
      COALESCE((
              SELECT "Answer"
              FROM "EstablishmentCapacity" ec
              JOIN "ServicesCapacity" sc ON ec."ServiceCapacityID" = sc."ServiceCapacityID"
                  AND sc."ServiceID" = 25
                  AND sc."Type" = 'Utilisation'
              WHERE ec."EstablishmentID" = e."EstablishmentID"
              ), - 1) st02util,
      CASE 
          WHEN "MainServiceFKValue" = 25
              OR (
                  SELECT COUNT(1)
                  FROM "EstablishmentServices"
                  WHERE "EstablishmentID" = e."EstablishmentID"
                      AND "ServiceID" = 25
                  ) = 1
              THEN TO_CHAR("CapacityServicesChangedAt", 'DD/MM/YYYY')
          ELSE NULL
          END st02util_changedate,
      CASE 
          WHEN "MainServiceFKValue" = 25
              OR (
                  SELECT COUNT(1)
                  FROM "EstablishmentServices"
                  WHERE "EstablishmentID" = e."EstablishmentID"
                      AND "ServiceID" = 25
                  ) = 1
              THEN TO_CHAR("CapacityServicesSavedAt", 'DD/MM/YYYY')
          ELSE NULL
          END st02util_savedate,
      CASE 
          WHEN "MainServiceFKValue" = 13
              OR (
                  SELECT COUNT(1)
                  FROM "EstablishmentServices"
                  WHERE "EstablishmentID" = e."EstablishmentID"
                      AND "ServiceID" = 13
                  ) = 1
              THEN 1
          ELSE 0
          END st53flag,
      COALESCE((
              SELECT "Answer"
              FROM "EstablishmentCapacity" ec
              JOIN "ServicesCapacity" sc ON ec."ServiceCapacityID" = sc."ServiceCapacityID"
                  AND sc."ServiceID" = 13
                  AND sc."Type" = 'Utilisation'
              WHERE ec."EstablishmentID" = e."EstablishmentID"
              ), - 1) st53util,
      CASE 
          WHEN "MainServiceFKValue" = 13
              OR (
                  SELECT COUNT(1)
                  FROM "EstablishmentServices"
                  WHERE "EstablishmentID" = e."EstablishmentID"
                      AND "ServiceID" = 13
                  ) = 1
              THEN TO_CHAR("CapacityServicesChangedAt", 'DD/MM/YYYY')
          ELSE NULL
          END st53util_changedate,
      CASE 
          WHEN "MainServiceFKValue" = 13
              OR (
                  SELECT COUNT(1)
                  FROM "EstablishmentServices"
                  WHERE "EstablishmentID" = e."EstablishmentID"
                      AND "ServiceID" = 13
                  ) = 1
              THEN TO_CHAR("CapacityServicesSavedAt", 'DD/MM/YYYY')
          ELSE NULL
          END st53util_savedate,
      CASE 
          WHEN "MainServiceFKValue" = 12
              OR (
                  SELECT COUNT(1)
                  FROM "EstablishmentServices"
                  WHERE "EstablishmentID" = e."EstablishmentID"
                      AND "ServiceID" = 12
                  ) = 1
              THEN 1
          ELSE 0
          END st05flag,
      COALESCE((
              SELECT "Answer"
              FROM "EstablishmentCapacity" ec
              JOIN "ServicesCapacity" sc ON ec."ServiceCapacityID" = sc."ServiceCapacityID"
                  AND sc."ServiceID" = 12
                  AND sc."Type" = 'Capacity'
              WHERE ec."EstablishmentID" = e."EstablishmentID"
              ), - 1) st05cap,
      CASE 
          WHEN "MainServiceFKValue" = 12
              OR (
                  SELECT COUNT(1)
                  FROM "EstablishmentServices"
                  WHERE "EstablishmentID" = e."EstablishmentID"
                      AND "ServiceID" = 12
                  ) = 1
              THEN TO_CHAR("CapacityServicesChangedAt", 'DD/MM/YYYY')
          ELSE NULL
          END st05cap_changedate,
      CASE 
          WHEN "MainServiceFKValue" = 12
              OR (
                  SELECT COUNT(1)
                  FROM "EstablishmentServices"
                  WHERE "EstablishmentID" = e."EstablishmentID"
                      AND "ServiceID" = 12
                  ) = 1
              THEN TO_CHAR("CapacityServicesSavedAt", 'DD/MM/YYYY')
          ELSE NULL
          END st05cap_savedate,
      COALESCE((
              SELECT "Answer"
              FROM "EstablishmentCapacity" ec
              JOIN "ServicesCapacity" sc ON ec."ServiceCapacityID" = sc."ServiceCapacityID"
                  AND sc."ServiceID" = 12
                  AND sc."Type" = 'Utilisation'
              WHERE ec."EstablishmentID" = e."EstablishmentID"
              ), - 1) st05util,
      CASE 
          WHEN "MainServiceFKValue" = 12
              OR (
                  SELECT COUNT(1)
                  FROM "EstablishmentServices"
                  WHERE "EstablishmentID" = e."EstablishmentID"
                      AND "ServiceID" = 12
                  ) = 1
              THEN TO_CHAR("CapacityServicesChangedAt", 'DD/MM/YYYY')
          ELSE NULL
          END st05util_changedate,
      CASE 
          WHEN "MainServiceFKValue" = 12
              OR (
                  SELECT COUNT(1)
                  FROM "EstablishmentServices"
                  WHERE "EstablishmentID" = e."EstablishmentID"
                      AND "ServiceID" = 12
                  ) = 1
              THEN TO_CHAR("CapacityServicesSavedAt", 'DD/MM/YYYY')
          ELSE NULL
          END st05util_savedate,
      CASE 
          WHEN "MainServiceFKValue" = 9
              OR (
                  SELECT COUNT(1)
                  FROM "EstablishmentServices"
                  WHERE "EstablishmentID" = e."EstablishmentID"
                      AND "ServiceID" = 9
                  ) = 1
              THEN 1
          ELSE 0
          END st06flag,
      COALESCE((
              SELECT "Answer"
              FROM "EstablishmentCapacity" ec
              JOIN "ServicesCapacity" sc ON ec."ServiceCapacityID" = sc."ServiceCapacityID"
                  AND sc."ServiceID" = 9
                  AND sc."Type" = 'Capacity'
              WHERE ec."EstablishmentID" = e."EstablishmentID"
              ), - 1) st06cap,
      CASE 
          WHEN "MainServiceFKValue" = 9
              OR (
                  SELECT COUNT(1)
                  FROM "EstablishmentServices"
                  WHERE "EstablishmentID" = e."EstablishmentID"
                      AND "ServiceID" = 9
                  ) = 1
              THEN TO_CHAR("CapacityServicesChangedAt", 'DD/MM/YYYY')
          ELSE NULL
          END st06cap_changedate,
      CASE 
          WHEN "MainServiceFKValue" = 9
              OR (
                  SELECT COUNT(1)
                  FROM "EstablishmentServices"
                  WHERE "EstablishmentID" = e."EstablishmentID"
                      AND "ServiceID" = 9
                  ) = 1
              THEN TO_CHAR("CapacityServicesSavedAt", 'DD/MM/YYYY')
          ELSE NULL
          END st06cap_savedate,
      COALESCE((
              SELECT "Answer"
              FROM "EstablishmentCapacity" ec
              JOIN "ServicesCapacity" sc ON ec."ServiceCapacityID" = sc."ServiceCapacityID"
                  AND sc."ServiceID" = 9
                  AND sc."Type" = 'Utilisation'
              WHERE ec."EstablishmentID" = e."EstablishmentID"
              ), - 1) st06util,
      CASE 
          WHEN "MainServiceFKValue" = 9
              OR (
                  SELECT COUNT(1)
                  FROM "EstablishmentServices"
                  WHERE "EstablishmentID" = e."EstablishmentID"
                      AND "ServiceID" = 9
                  ) = 1
              THEN TO_CHAR("CapacityServicesChangedAt", 'DD/MM/YYYY')
          ELSE NULL
          END st06util_changedate,
      CASE 
          WHEN "MainServiceFKValue" = 9
              OR (
                  SELECT COUNT(1)
                  FROM "EstablishmentServices"
                  WHERE "EstablishmentID" = e."EstablishmentID"
                      AND "ServiceID" = 9
                  ) = 1
              THEN TO_CHAR("CapacityServicesSavedAt", 'DD/MM/YYYY')
          ELSE NULL
          END st06util_savedate,
      CASE 
          WHEN "MainServiceFKValue" = 10
              OR (
                  SELECT COUNT(1)
                  FROM "EstablishmentServices"
                  WHERE "EstablishmentID" = e."EstablishmentID"
                      AND "ServiceID" = 10
                  ) = 1
              THEN 1
          ELSE 0
          END st07flag,
      COALESCE((
              SELECT "Answer"
              FROM "EstablishmentCapacity" ec
              JOIN "ServicesCapacity" sc ON ec."ServiceCapacityID" = sc."ServiceCapacityID"
                  AND sc."ServiceID" = 10
                  AND sc."Type" = 'Capacity'
              WHERE ec."EstablishmentID" = e."EstablishmentID"
              ), - 1) st07cap,
      CASE 
          WHEN "MainServiceFKValue" = 10
              OR (
                  SELECT COUNT(1)
                  FROM "EstablishmentServices"
                  WHERE "EstablishmentID" = e."EstablishmentID"
                      AND "ServiceID" = 10
                  ) = 1
              THEN TO_CHAR("CapacityServicesChangedAt", 'DD/MM/YYYY')
          ELSE NULL
          END st07cap_changedate,
      CASE 
          WHEN "MainServiceFKValue" = 10
              OR (
                  SELECT COUNT(1)
                  FROM "EstablishmentServices"
                  WHERE "EstablishmentID" = e."EstablishmentID"
                      AND "ServiceID" = 10
                  ) = 1
              THEN TO_CHAR("CapacityServicesSavedAt", 'DD/MM/YYYY')
          ELSE NULL
          END st07cap_savedate,
      COALESCE((
              SELECT "Answer"
              FROM "EstablishmentCapacity" ec
              JOIN "ServicesCapacity" sc ON ec."ServiceCapacityID" = sc."ServiceCapacityID"
                  AND sc."ServiceID" = 10
                  AND sc."Type" = 'Utilisation'
              WHERE ec."EstablishmentID" = e."EstablishmentID"
              ), - 1) st07util,
      CASE 
          WHEN "MainServiceFKValue" = 10
              OR (
                  SELECT COUNT(1)
                  FROM "EstablishmentServices"
                  WHERE "EstablishmentID" = e."EstablishmentID"
                      AND "ServiceID" = 10
                  ) = 1
              THEN TO_CHAR("CapacityServicesChangedAt", 'DD/MM/YYYY')
          ELSE NULL
          END st07util_changedate,
      CASE 
          WHEN "MainServiceFKValue" = 10
              OR (
                  SELECT COUNT(1)
                  FROM "EstablishmentServices"
                  WHERE "EstablishmentID" = e."EstablishmentID"
                      AND "ServiceID" = 10
                  ) = 1
              THEN TO_CHAR("CapacityServicesSavedAt", 'DD/MM/YYYY')
          ELSE NULL
          END st07util_savedate,
      CASE 
          WHEN "MainServiceFKValue" = 11
              OR (
                  SELECT COUNT(1)
                  FROM "EstablishmentServices"
                  WHERE "EstablishmentID" = e."EstablishmentID"
                      AND "ServiceID" = 11
                  ) = 1
              THEN 1
          ELSE 0
          END st10flag,
      COALESCE((
              SELECT "Answer"
              FROM "EstablishmentCapacity" ec
              JOIN "ServicesCapacity" sc ON ec."ServiceCapacityID" = sc."ServiceCapacityID"
                  AND sc."ServiceID" = 11
                  AND sc."Type" = 'Utilisation'
              WHERE ec."EstablishmentID" = e."EstablishmentID"
              ), - 1) st10util,
      CASE 
          WHEN "MainServiceFKValue" = 11
              OR (
                  SELECT COUNT(1)
                  FROM "EstablishmentServices"
                  WHERE "EstablishmentID" = e."EstablishmentID"
                      AND "ServiceID" = 11
                  ) = 1
              THEN TO_CHAR("CapacityServicesChangedAt", 'DD/MM/YYYY')
          ELSE NULL
          END st10util_changedate,
      CASE 
          WHEN "MainServiceFKValue" = 11
              OR (
                  SELECT COUNT(1)
                  FROM "EstablishmentServices"
                  WHERE "EstablishmentID" = e."EstablishmentID"
                      AND "ServiceID" = 11
                  ) = 1
              THEN TO_CHAR("CapacityServicesSavedAt", 'DD/MM/YYYY')
          ELSE NULL
          END st10util_savedate,
      CASE 
          WHEN "MainServiceFKValue" = 20
              OR (
                  SELECT COUNT(1)
                  FROM "EstablishmentServices"
                  WHERE "EstablishmentID" = e."EstablishmentID"
                      AND "ServiceID" = 20
                  ) = 1
              THEN 1
          ELSE 0
          END st08flag,
      COALESCE((
              SELECT "Answer"
              FROM "EstablishmentCapacity" ec
              JOIN "ServicesCapacity" sc ON ec."ServiceCapacityID" = sc."ServiceCapacityID"
                  AND sc."ServiceID" = 20
                  AND sc."Type" = 'Utilisation'
              WHERE ec."EstablishmentID" = e."EstablishmentID"
              ), - 1) st08util,
      CASE 
          WHEN "MainServiceFKValue" = 20
              OR (
                  SELECT COUNT(1)
                  FROM "EstablishmentServices"
                  WHERE "EstablishmentID" = e."EstablishmentID"
                      AND "ServiceID" = 20
                  ) = 1
              THEN TO_CHAR("CapacityServicesChangedAt", 'DD/MM/YYYY')
          ELSE NULL
          END st08util_changedate,
      CASE 
          WHEN "MainServiceFKValue" = 20
              OR (
                  SELECT COUNT(1)
                  FROM "EstablishmentServices"
                  WHERE "EstablishmentID" = e."EstablishmentID"
                      AND "ServiceID" = 20
                  ) = 1
              THEN TO_CHAR("CapacityServicesSavedAt", 'DD/MM/YYYY')
          ELSE NULL
          END st08util_savedate,
      CASE 
          WHEN "MainServiceFKValue" = 21
              OR (
                  SELECT COUNT(1)
                  FROM "EstablishmentServices"
                  WHERE "EstablishmentID" = e."EstablishmentID"
                      AND "ServiceID" = 21
                  ) = 1
              THEN 1
          ELSE 0
          END st54flag,
      COALESCE((
              SELECT "Answer"
              FROM "EstablishmentCapacity" ec
              JOIN "ServicesCapacity" sc ON ec."ServiceCapacityID" = sc."ServiceCapacityID"
                  AND sc."ServiceID" = 21
                  AND sc."Type" = 'Utilisation'
              WHERE ec."EstablishmentID" = e."EstablishmentID"
              ), - 1) st54util,
      CASE 
          WHEN "MainServiceFKValue" = 21
              OR (
                  SELECT COUNT(1)
                  FROM "EstablishmentServices"
                  WHERE "EstablishmentID" = e."EstablishmentID"
                      AND "ServiceID" = 21
                  ) = 1
              THEN TO_CHAR("CapacityServicesChangedAt", 'DD/MM/YYYY')
          ELSE NULL
          END st54util_changedate,
      CASE 
          WHEN "MainServiceFKValue" = 21
              OR (
                  SELECT COUNT(1)
                  FROM "EstablishmentServices"
                  WHERE "EstablishmentID" = e."EstablishmentID"
                      AND "ServiceID" = 21
                  ) = 1
              THEN TO_CHAR("CapacityServicesSavedAt", 'DD/MM/YYYY')
          ELSE NULL
          END st54util_savedate,
      CASE 
          WHEN "MainServiceFKValue" = 22
              OR (
                  SELECT COUNT(1)
                  FROM "EstablishmentServices"
                  WHERE "EstablishmentID" = e."EstablishmentID"
                      AND "ServiceID" = 22
                  ) = 1
              THEN 1
          ELSE 0
          END st74flag,
      COALESCE((
              SELECT "Answer"
              FROM "EstablishmentCapacity" ec
              JOIN "ServicesCapacity" sc ON ec."ServiceCapacityID" = sc."ServiceCapacityID"
                  AND sc."ServiceID" = 22
                  AND sc."Type" = 'Utilisation'
              WHERE ec."EstablishmentID" = e."EstablishmentID"
              ), - 1) st74util,
      CASE 
          WHEN "MainServiceFKValue" = 22
              OR (
                  SELECT COUNT(1)
                  FROM "EstablishmentServices"
                  WHERE "EstablishmentID" = e."EstablishmentID"
                      AND "ServiceID" = 22
                  ) = 1
              THEN TO_CHAR("CapacityServicesChangedAt", 'DD/MM/YYYY')
          ELSE NULL
          END st74util_changedate,
      CASE 
          WHEN "MainServiceFKValue" = 22
              OR (
                  SELECT COUNT(1)
                  FROM "EstablishmentServices"
                  WHERE "EstablishmentID" = e."EstablishmentID"
                      AND "ServiceID" = 22
                  ) = 1
              THEN TO_CHAR("CapacityServicesSavedAt", 'DD/MM/YYYY')
          ELSE NULL
          END st74util_savedate,
      CASE 
          WHEN "MainServiceFKValue" = 23
              OR (
                  SELECT COUNT(1)
                  FROM "EstablishmentServices"
                  WHERE "EstablishmentID" = e."EstablishmentID"
                      AND "ServiceID" = 23
                  ) = 1
              THEN 1
          ELSE 0
          END st55flag,
      COALESCE((
              SELECT "Answer"
              FROM "EstablishmentCapacity" ec
              JOIN "ServicesCapacity" sc ON ec."ServiceCapacityID" = sc."ServiceCapacityID"
                  AND sc."ServiceID" = 23
                  AND sc."Type" = 'Utilisation'
              WHERE ec."EstablishmentID" = e."EstablishmentID"
              ), - 1) st55util,
      CASE 
          WHEN "MainServiceFKValue" = 23
              OR (
                  SELECT COUNT(1)
                  FROM "EstablishmentServices"
                  WHERE "EstablishmentID" = e."EstablishmentID"
                      AND "ServiceID" = 23
                  ) = 1
              THEN TO_CHAR("CapacityServicesChangedAt", 'DD/MM/YYYY')
          ELSE NULL
          END st55util_changedate,
      CASE 
          WHEN "MainServiceFKValue" = 23
              OR (
                  SELECT COUNT(1)
                  FROM "EstablishmentServices"
                  WHERE "EstablishmentID" = e."EstablishmentID"
                      AND "ServiceID" = 23
                  ) = 1
              THEN TO_CHAR("CapacityServicesSavedAt", 'DD/MM/YYYY')
          ELSE NULL
          END st55util_savedate,
      CASE 
          WHEN "MainServiceFKValue" = 35
              OR (
                  SELECT COUNT(1)
                  FROM "EstablishmentServices"
                  WHERE "EstablishmentID" = e."EstablishmentID"
                      AND "ServiceID" = 35
                  ) = 1
              THEN 1
          ELSE 0
          END st73flag,
      COALESCE((
              SELECT "Answer"
              FROM "EstablishmentCapacity" ec
              JOIN "ServicesCapacity" sc ON ec."ServiceCapacityID" = sc."ServiceCapacityID"
                  AND sc."ServiceID" = 35
                  AND sc."Type" = 'Utilisation'
              WHERE ec."EstablishmentID" = e."EstablishmentID"
              ), - 1) st73util,
      CASE 
          WHEN "MainServiceFKValue" = 35
              OR (
                  SELECT COUNT(1)
                  FROM "EstablishmentServices"
                  WHERE "EstablishmentID" = e."EstablishmentID"
                      AND "ServiceID" = 35
                  ) = 1
              THEN TO_CHAR("CapacityServicesChangedAt", 'DD/MM/YYYY')
          ELSE NULL
          END st73util_changedate,
      CASE 
          WHEN "MainServiceFKValue" = 35
              OR (
                  SELECT COUNT(1)
                  FROM "EstablishmentServices"
                  WHERE "EstablishmentID" = e."EstablishmentID"
                      AND "ServiceID" = 35
                  ) = 1
              THEN TO_CHAR("CapacityServicesSavedAt", 'DD/MM/YYYY')
          ELSE NULL
          END st73util_savedate,
      CASE 
          WHEN "MainServiceFKValue" = 18
              OR (
                  SELECT COUNT(1)
                  FROM "EstablishmentServices"
                  WHERE "EstablishmentID" = e."EstablishmentID"
                      AND "ServiceID" = 18
                  ) = 1
              THEN 1
          ELSE 0
          END st12flag,
      COALESCE((
              SELECT "Answer"
              FROM "EstablishmentCapacity" ec
              JOIN "ServicesCapacity" sc ON ec."ServiceCapacityID" = sc."ServiceCapacityID"
                  AND sc."ServiceID" = 18
                  AND sc."Type" = 'Utilisation'
              WHERE ec."EstablishmentID" = e."EstablishmentID"
              ), - 1) st12util,
      CASE 
          WHEN "MainServiceFKValue" = 18
              OR (
                  SELECT COUNT(1)
                  FROM "EstablishmentServices"
                  WHERE "EstablishmentID" = e."EstablishmentID"
                      AND "ServiceID" = 18
                  ) = 1
              THEN TO_CHAR("CapacityServicesChangedAt", 'DD/MM/YYYY')
          ELSE NULL
          END st12util_changedate,
      CASE 
          WHEN "MainServiceFKValue" = 18
              OR (
                  SELECT COUNT(1)
                  FROM "EstablishmentServices"
                  WHERE "EstablishmentID" = e."EstablishmentID"
                      AND "ServiceID" = 18
                  ) = 1
              THEN TO_CHAR("CapacityServicesSavedAt", 'DD/MM/YYYY')
          ELSE NULL
          END st12util_savedate,
      CASE 
          WHEN "MainServiceFKValue" = 1
              OR (
                  SELECT COUNT(1)
                  FROM "EstablishmentServices"
                  WHERE "EstablishmentID" = e."EstablishmentID"
                      AND "ServiceID" = 1
                  ) = 1
              THEN 1
          ELSE 0
          END st13flag,
      CASE 
          WHEN "MainServiceFKValue" = 2
              OR (
                  SELECT COUNT(1)
                  FROM "EstablishmentServices"
                  WHERE "EstablishmentID" = e."EstablishmentID"
                      AND "ServiceID" = 2
                  ) = 1
              THEN 1
          ELSE 0
          END st15flag,
      CASE 
          WHEN "MainServiceFKValue" = 3
              OR (
                  SELECT COUNT(1)
                  FROM "EstablishmentServices"
                  WHERE "EstablishmentID" = e."EstablishmentID"
                      AND "ServiceID" = 3
                  ) = 1
              THEN 1
          ELSE 0
          END st18flag,
      CASE 
          WHEN "MainServiceFKValue" = 4
              OR (
                  SELECT COUNT(1)
                  FROM "EstablishmentServices"
                  WHERE "EstablishmentID" = e."EstablishmentID"
                      AND "ServiceID" = 4
                  ) = 1
              THEN 1
          ELSE 0
          END st20flag,
      CASE 
          WHEN "MainServiceFKValue" = 5
              OR (
                  SELECT COUNT(1)
                  FROM "EstablishmentServices"
                  WHERE "EstablishmentID" = e."EstablishmentID"
                      AND "ServiceID" = 5
                  ) = 1
              THEN 1
          ELSE 0
          END st19flag,
      CASE 
          WHEN "MainServiceFKValue" = 19
              OR (
                  SELECT COUNT(1)
                  FROM "EstablishmentServices"
                  WHERE "EstablishmentID" = e."EstablishmentID"
                      AND "ServiceID" = 19
                  ) = 1
              THEN 1
          ELSE 0
          END st17flag,
      COALESCE((
              SELECT "Answer"
              FROM "EstablishmentCapacity" ec
              JOIN "ServicesCapacity" sc ON ec."ServiceCapacityID" = sc."ServiceCapacityID"
                  AND sc."ServiceID" = 19
                  AND sc."Type" = 'Capacity'
              WHERE ec."EstablishmentID" = e."EstablishmentID"
              ), - 1) st17cap,
      CASE 
          WHEN "MainServiceFKValue" = 19
              OR (
                  SELECT COUNT(1)
                  FROM "EstablishmentServices"
                  WHERE "EstablishmentID" = e."EstablishmentID"
                      AND "ServiceID" = 19
                  ) = 1
              THEN TO_CHAR("CapacityServicesChangedAt", 'DD/MM/YYYY')
          ELSE NULL
          END st17cap_changedate,
      CASE 
          WHEN "MainServiceFKValue" = 19
              OR (
                  SELECT COUNT(1)
                  FROM "EstablishmentServices"
                  WHERE "EstablishmentID" = e."EstablishmentID"
                      AND "ServiceID" = 19
                  ) = 1
              THEN TO_CHAR("CapacityServicesSavedAt", 'DD/MM/YYYY')
          ELSE NULL
          END st17cap_savedate,
      COALESCE((
              SELECT "Answer"
              FROM "EstablishmentCapacity" ec
              JOIN "ServicesCapacity" sc ON ec."ServiceCapacityID" = sc."ServiceCapacityID"
                  AND sc."ServiceID" = 19
                  AND sc."Type" = 'Utilisation'
              WHERE ec."EstablishmentID" = e."EstablishmentID"
              ), - 1) st17util,
      CASE 
          WHEN "MainServiceFKValue" = 19
              OR (
                  SELECT COUNT(1)
                  FROM "EstablishmentServices"
                  WHERE "EstablishmentID" = e."EstablishmentID"
                      AND "ServiceID" = 19
                  ) = 1
              THEN TO_CHAR("CapacityServicesChangedAt", 'DD/MM/YYYY')
          ELSE NULL
          END st17util_changedate,
      CASE 
          WHEN "MainServiceFKValue" = 19
              OR (
                  SELECT COUNT(1)
                  FROM "EstablishmentServices"
                  WHERE "EstablishmentID" = e."EstablishmentID"
                      AND "ServiceID" = 19
                  ) = 1
              THEN TO_CHAR("CapacityServicesSavedAt", 'DD/MM/YYYY')
          ELSE NULL
          END st17util_savedate,
      CASE 
          WHEN "MainServiceFKValue" = 7
              OR (
                  SELECT COUNT(1)
                  FROM "EstablishmentServices"
                  WHERE "EstablishmentID" = e."EstablishmentID"
                      AND "ServiceID" = 7
                  ) = 1
              THEN 1
          ELSE 0
          END st14flag,
      CASE 
          WHEN "MainServiceFKValue" = 8
              OR (
                  SELECT COUNT(1)
                  FROM "EstablishmentServices"
                  WHERE "EstablishmentID" = e."EstablishmentID"
                      AND "ServiceID" = 8
                  ) = 1
              THEN 1
          ELSE 0
          END st16flag,
      CASE 
          WHEN "MainServiceFKValue" = 6
              OR (
                  SELECT COUNT(1)
                  FROM "EstablishmentServices"
                  WHERE "EstablishmentID" = e."EstablishmentID"
                      AND "ServiceID" = 6
                  ) = 1
              THEN 1
          ELSE 0
          END st21flag,
      CASE 
          WHEN "MainServiceFKValue" = 26
              OR (
                  SELECT COUNT(1)
                  FROM "EstablishmentServices"
                  WHERE "EstablishmentID" = e."EstablishmentID"
                      AND "ServiceID" = 26
                  ) = 1
              THEN 1
          ELSE 0
          END st63flag,
      CASE 
          WHEN "MainServiceFKValue" = 27
              OR (
                  SELECT COUNT(1)
                  FROM "EstablishmentServices"
                  WHERE "EstablishmentID" = e."EstablishmentID"
                      AND "ServiceID" = 27
                  ) = 1
              THEN 1
          ELSE 0
          END st61flag,
      CASE 
          WHEN "MainServiceFKValue" = 28
              OR (
                  SELECT COUNT(1)
                  FROM "EstablishmentServices"
                  WHERE "EstablishmentID" = e."EstablishmentID"
                      AND "ServiceID" = 28
                  ) = 1
              THEN 1
          ELSE 0
          END st62flag,
      CASE 
          WHEN "MainServiceFKValue" = 29
              OR (
                  SELECT COUNT(1)
                  FROM "EstablishmentServices"
                  WHERE "EstablishmentID" = e."EstablishmentID"
                      AND "ServiceID" = 29
                  ) = 1
              THEN 1
          ELSE 0
          END st64flag,
      CASE 
          WHEN "MainServiceFKValue" = 30
              OR (
                  SELECT COUNT(1)
                  FROM "EstablishmentServices"
                  WHERE "EstablishmentID" = e."EstablishmentID"
                      AND "ServiceID" = 30
                  ) = 1
              THEN 1
          ELSE 0
          END st66flag,
      CASE 
          WHEN "MainServiceFKValue" = 31
              OR (
                  SELECT COUNT(1)
                  FROM "EstablishmentServices"
                  WHERE "EstablishmentID" = e."EstablishmentID"
                      AND "ServiceID" = 31
                  ) = 1
              THEN 1
          ELSE 0
          END st68flag,
      CASE 
          WHEN "MainServiceFKValue" = 32
              OR (
                  SELECT COUNT(1)
                  FROM "EstablishmentServices"
                  WHERE "EstablishmentID" = e."EstablishmentID"
                      AND "ServiceID" = 32
                  ) = 1
              THEN 1
          ELSE 0
          END st67flag,
      CASE 
          WHEN "MainServiceFKValue" = 33
              OR (
                  SELECT COUNT(1)
                  FROM "EstablishmentServices"
                  WHERE "EstablishmentID" = e."EstablishmentID"
                      AND "ServiceID" = 33
                  ) = 1
              THEN 1
          ELSE 0
          END st69flag,
      CASE 
          WHEN "MainServiceFKValue" = 34
              OR (
                  SELECT COUNT(1)
                  FROM "EstablishmentServices"
                  WHERE "EstablishmentID" = e."EstablishmentID"
                      AND "ServiceID" = 34
                  ) = 1
              THEN 1
          ELSE 0
          END st70flag,
      CASE 
          WHEN "MainServiceFKValue" = 17
              OR (
                  SELECT COUNT(1)
                  FROM "EstablishmentServices"
                  WHERE "EstablishmentID" = e."EstablishmentID"
                      AND "ServiceID" = 17
                  ) = 1
              THEN 1
          ELSE 0
          END st71flag,
      CASE 
          WHEN "MainServiceFKValue" = 14
              OR (
                  SELECT COUNT(1)
                  FROM "EstablishmentServices"
                  WHERE "EstablishmentID" = e."EstablishmentID"
                      AND "ServiceID" = 14
                  ) = 1
              THEN 1
          ELSE 0
          END st75flag,
      CASE 
          WHEN "MainServiceFKValue" = 16
              OR (
                  SELECT COUNT(1)
                  FROM "EstablishmentServices"
                  WHERE "EstablishmentID" = e."EstablishmentID"
                      AND "ServiceID" = 16
                  ) = 1
              THEN 1
          ELSE 0
          END st72flag,
      CASE 
          WHEN "MainServiceFKValue" = 36
              OR (
                  SELECT COUNT(1)
                  FROM "EstablishmentServices"
                  WHERE "EstablishmentID" = e."EstablishmentID"
                      AND "ServiceID" = 36
                  ) = 1
              THEN 1
          ELSE 0
          END st60flag,
      CASE 
          WHEN "MainServiceFKValue" = 15
              OR (
                  SELECT COUNT(1)
                  FROM "EstablishmentServices"
                  WHERE "EstablishmentID" = e."EstablishmentID"
                      AND "ServiceID" = 15
                  ) = 1
              THEN 1
          ELSE 0
          END st52flag,
      CASE 
          WHEN EXISTS (
                  SELECT 1
                  FROM cqc."MandatoryTraining"
                  WHERE "EstablishmentFK" = e."EstablishmentID" LIMIT 1
                  )
              THEN 1
          ELSE 0
          END hasmandatorytraining
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
