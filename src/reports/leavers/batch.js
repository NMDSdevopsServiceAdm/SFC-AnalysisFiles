const db = require('../db');

const populateBatch = async (numInBatch) => {
  await db.raw(
    `
       CREATE OR REPLACE FUNCTION create_batch_4_leavers(p_no_of_workers integer) RETURNS VOID AS $$
       DECLARE
         current_status INT := 1;
         no_of_batch_created INT := 0;
       BEGIN
         LOOP
             current_status := (SELECT COUNT(1) FROM "Afr3BatchiSkAi0mo" WHERE "BatchNo" IS NULL);
             IF current_status <> 0 THEN
               no_of_batch_created := no_of_batch_created + 1;
             END IF;
 
             EXIT WHEN current_status = 0;
 
             UPDATE "Afr3BatchiSkAi0mo"
             SET    "BatchNo" = (SELECT MAX(COALESCE("BatchNo",0)) + 1 FROM "Afr3BatchiSkAi0mo")
             WHERE  "RunningTotal" <= p_no_of_workers
             AND    "BatchNo" IS NULL;
 
             UPDATE "Afr3BatchiSkAi0mo"
             SET    "RunningTotal" = "RunningTotal" - p_no_of_workers
             WHERE  "BatchNo" IS NULL;
         END LOOP;
 
         RAISE NOTICE 'Created: [ % ] batch.', (SELECT COUNT(DISTINCT "BatchNo") FROM "Afr3BatchiSkAi0mo");
       END;
       $$ LANGUAGE plpgsql;
     `,
  );

  await db.raw('SELECT create_batch_4_leavers(?);', [numInBatch]);
};

const createBatches = async (runDate, numInBatch = 20000) => {
  await db.schema.dropTableIfExists('Afr3BatchiSkAi0mo');

  await db.raw(
    `
    CREATE TABLE "Afr3BatchiSkAi0mo" AS
SELECT "EstablishmentID",
       "NoOfWorkers",
       SUM("NoOfWorkers") OVER (ORDER BY "NoOfWorkers" ASC, "EstablishmentID" ASC) "RunningTotal",
       NULL::INT "BatchNo",
       TO_DATE(?, 'DD-MM-YYYY')::DATE AS "RunDate"
FROM   (
          SELECT e."EstablishmentID",COUNT(1) "NoOfWorkers"
          FROM   "Establishment" e JOIN "Worker" w ON
                 e."EstablishmentID" = w."EstablishmentFK" AND
                 w."Archived" = true AND
                 e."Status" IS NULL
          GROUP  BY 1
       ) x;
      `,
    [runDate],
  );

  await db.raw('CREATE INDEX "Afr3BatchiSkAi0mo_idx" ON "Afr3BatchiSkAi0mo"("BatchNo");');

  await populateBatch(numInBatch);
};

const dropBatch = async () => {
  await db.schema.dropTableIfExists('Afr3BatchiSkAi0mo');
};

const getBatches = async () => db.select('BatchNo').from('Afr3BatchiSkAi0mo').groupBy(1).orderBy(1);

const findLeaversByBatch = (batchNum) =>
  db
    .raw(
      `
    SELECT 'M' || DATE_PART('year',(b."RunDate" - INTERVAL '1 day')) || LPAD(DATE_PART('month',(b."RunDate" - INTERVAL '1 day'))::TEXT,2,'0') period,
        TO_CHAR((SELECT MAX("When") FROM "WorkerAudit" WHERE "EventType" = 'deleted' AND "WorkerFK" =  w."ID" LIMIT 1),'DD/MM/YYYY') deletedate,
       (SELECT "Reason" FROM "WorkerLeaveReasons" WHERE "ID" = w."LeaveReasonFK" LIMIT 1)  reason,
       w."LeaveReasonOther" reasonOther,
       e."EstablishmentID" establishmentid,
       w."TribalID" tribalid,
       e."ParentID" parentid,
       CASE WHEN e."IsParent" THEN e."EstablishmentID" ELSE CASE WHEN e."ParentID" IS NOT NULL THEN e."ParentID" ELSE e."EstablishmentID" END END orgid,
       e."NmdsID" nmdsid,
       w."ID" workerid,
       -- UPPER(MD5(REPLACE("NationalInsuranceNumberValue",' ','') || TO_CHAR("DateOfBirthValue", 'YYYYMMDD'))) wrkglbid,
       1 wkplacestat,
       TO_CHAR(w."created",'DD/MM/YYYY') createddate,
       TO_CHAR(GREATEST(
          w."NameOrIdChangedAt",
          w."ContractChangedAt",
          w."MainJobFKChangedAt",
          w."ApprovedMentalHealthWorkerChangedAt",
          w."MainJobStartDateChangedAt",
          w."OtherJobsChangedAt",
          w."NationalInsuranceNumberChangedAt",
          w."DateOfBirthChangedAt",
          w."PostcodeChangedAt",
          w."DisabilityChangedAt",
          w."GenderChangedAt",
          w."EthnicityFKChangedAt",
          w."NationalityChangedAt",
          w."CountryOfBirthChangedAt",
          w."RecruitedFromChangedAt",
          w."BritishCitizenshipChangedAt",
          w."YearArrivedChangedAt",
          w."SocialCareStartDateChangedAt",
          w."DaysSickChangedAt",
          w."ZeroHoursContractChangedAt",
          w."WeeklyHoursAverageChangedAt",
          w."WeeklyHoursContractedChangedAt",
          w."AnnualHourlyPayChangedAt",
          w."CareCertificateChangedAt",
          w."ApprenticeshipTrainingChangedAt",
          w."QualificationInSocialCareChangedAt",
          w."SocialCareQualificationFKChangedAt",
          w."OtherQualificationsChangedAt",
          w."HighestQualificationFKChangedAt",
          w."CompletedChangedAt",
          w."RegisteredNurseChangedAt",
          w."NurseSpecialismFKChangedAt",
          w."LocalIdentifierChangedAt",
          w."EstablishmentFkChangedAt",
          w."FluJabChangedAt"),'DD/MM/YYYY') updateddate,
       TO_CHAR(GREATEST(
          w."NameOrIdSavedAt",
          w."ContractSavedAt",
          w."MainJobFKSavedAt",
          w."ApprovedMentalHealthWorkerSavedAt",
          w."MainJobStartDateSavedAt",
          w."OtherJobsSavedAt",
          w."NationalInsuranceNumberSavedAt",
          w."DateOfBirthSavedAt",
          w."PostcodeSavedAt",
          w."DisabilitySavedAt",
          w."GenderSavedAt",
          w."EthnicityFKSavedAt",
          w."NationalitySavedAt",
          w."CountryOfBirthSavedAt",
          w."RecruitedFromSavedAt",
          w."BritishCitizenshipSavedAt",
          w."YearArrivedSavedAt",
          w."SocialCareStartDateSavedAt",
          w."DaysSickSavedAt",
          w."ZeroHoursContractSavedAt",
          w."WeeklyHoursAverageSavedAt",
          w."WeeklyHoursContractedSavedAt",
          w."AnnualHourlyPaySavedAt",
          w."CareCertificateSavedAt",
          w."ApprenticeshipTrainingSavedAt",
          w."QualificationInSocialCareSavedAt",
          w."SocialCareQualificationFKSavedAt",
          w."OtherQualificationsSavedAt",
          w."HighestQualificationFKSavedAt",
          w."CompletedSavedAt",
          w."RegisteredNurseSavedAt",
          w."NurseSpecialismFKSavedAt",
          w."LocalIdentifierSavedAt",
          w."EstablishmentFkSavedAt",
          w."FluJabSavedAt"),'DD/MM/YYYY') savedate,
       CASE e."ShareDataWithCQC" WHEN true THEN 1 ELSE 0 END cqcpermission,
       CASE e."ShareDataWithLA" WHEN true THEN 1 ELSE 0 END lapermission,
       CASE WHEN e."IsRegulated" is true THEN 2 ELSE 0 END regtype,
       e."ProvID" providerid,
       e."LocationID" locationid,
       CASE e."EmployerTypeValue"
          WHEN 'Local Authority (adult services)' THEN 1
          WHEN 'Local Authority (generic/other)' THEN 3
          WHEN 'Private Sector' THEN 6
          WHEN 'Voluntary / Charity' THEN 7
          WHEN 'Other' THEN 8
       END esttype,
       COALESCE((SELECT "RegionID" FROM "Cssr" WHERE "NmdsIDLetter" = SUBSTRING(e."NmdsID",1,1) LIMIT 1),NULL,-1) regionid,
       COALESCE(
           (SELECT "CssrID" FROM "Cssr", cqcref.pcodedata WHERE "NmdsIDLetter" = SUBSTRING(e."NmdsID",1,1) AND postcode = e."PostCode" AND pcodedata.local_custodian_code = "Cssr"."LocalCustodianCode" GROUP BY "Cssr"."CssrID", "Cssr"."CssR" LIMIT 1),NULL,-1) cssr,
       COALESCE((
          SELECT CASE "LocalAuthority"
                    WHEN 'Mid Bedfordshire' THEN 1
                    WHEN 'Bedford' THEN 2
                    WHEN 'South Bedfordshire' THEN 3
                    WHEN 'Cambridge' THEN 4
                    WHEN 'East Cambridgeshire' THEN 5
                    WHEN 'Fenland' THEN 6
                    WHEN 'Huntingdonshire' THEN 7
                    WHEN 'South Cambridgeshire' THEN 8
                    WHEN 'Basildon' THEN 9
                    WHEN 'Braintree' THEN 10
                    WHEN 'Brentwood' THEN 11
                    WHEN 'Castle Point' THEN 12
                    WHEN 'Chelmsford' THEN 13
                    WHEN 'Colchester' THEN 14
                    WHEN 'Epping Forest' THEN 15
                    WHEN 'Harlow' THEN 16
                    WHEN 'Maldon' THEN 17
                    WHEN 'Rochford' THEN 18
                    WHEN 'Tendring' THEN 19
                    WHEN 'Uttlesford' THEN 20
                    WHEN 'Broxbourne' THEN 21
                    WHEN 'Dacorum' THEN 22
                    WHEN 'East Hertfordshire' THEN 23
                    WHEN 'Hertsmere' THEN 24
                    WHEN 'North Hertfordshire' THEN 25
                    WHEN 'St Albans' THEN 26
                    WHEN 'Stevenage' THEN 27
                    WHEN 'Three Rivers' THEN 28
                    WHEN 'Watford' THEN 29
                    WHEN 'Welwyn Hatfield' THEN 30
                    WHEN 'Luton' THEN 31
                    WHEN 'Breckland' THEN 32
                    WHEN 'Broadland' THEN 33
                    WHEN 'Great Yarmouth' THEN 34
                    WHEN 'King\`s Lynn and West Norfolk' THEN 35
                    WHEN 'North Norfolk' THEN 36
                    WHEN 'Norwich' THEN 37
                    WHEN 'South Norfolk' THEN 38
                    WHEN 'Peterborough' THEN 39
                    WHEN 'Southend-on-Sea' THEN 40
                    WHEN 'Babergh' THEN 41
                    WHEN 'Forest Heath' THEN 42
                    WHEN 'Ipswich' THEN 43
                    WHEN 'Mid Suffolk' THEN 44
                    WHEN 'St. Edmundsbury' THEN 45
                    WHEN 'Suffolk Coastal' THEN 46
                    WHEN 'Waveney' THEN 47
                    WHEN 'Thurrock' THEN 48
                    WHEN 'Derby' THEN 49
                    WHEN 'Amber Valley' THEN 50
                    WHEN 'Bolsover' THEN 51
                    WHEN 'Chesterfield' THEN 52
                    WHEN 'Derbyshire Dales' THEN 53
                    WHEN 'Erewash' THEN 54
                    WHEN 'High Peak' THEN 55
                    WHEN 'North East Derbyshire' THEN 56
                    WHEN 'South Derbyshire' THEN 57
                    WHEN 'Leicester' THEN 58
                    WHEN 'Blaby' THEN 59
                    WHEN 'Charnwood' THEN 60
                    WHEN 'Harborough' THEN 61
                    WHEN 'Hinckley and Bosworth' THEN 62
                    WHEN 'Melton' THEN 63
                    WHEN 'North West Leicestershire' THEN 64
                    WHEN 'Oadby and Wigston' THEN 65
                    WHEN 'Boston' THEN 66
                    WHEN 'East Lindsey' THEN 67
                    WHEN 'Lincoln' THEN 68
                    WHEN 'North Kesteven' THEN 69
                    WHEN 'South Holland' THEN 70
                    WHEN 'South Kesteven' THEN 71
                    WHEN 'West Lindsey' THEN 72
                    WHEN 'Corby' THEN 73
                    WHEN 'Daventry' THEN 74
                    WHEN 'East Northamptonshire' THEN 75
                    WHEN 'Kettering' THEN 76
                    WHEN 'Northampton' THEN 77
                    WHEN 'South Northamptonshire' THEN 78
                    WHEN 'Wellingborough' THEN 79
                    WHEN 'Nottingham' THEN 80
                    WHEN 'Ashfield' THEN 81
                    WHEN 'Bassetlaw' THEN 82
                    WHEN 'Broxtowe' THEN 83
                    WHEN 'Gedling' THEN 84
                    WHEN 'Mansfield' THEN 85
                    WHEN 'Newark and Sherwood' THEN 86
                    WHEN 'Rushcliffe' THEN 87
                    WHEN 'Rutland' THEN 88
                    WHEN 'Barking and Dagenham' THEN 89
                    WHEN 'Barnet' THEN 90
                    WHEN 'Bexley' THEN 91
                    WHEN 'Brent' THEN 92
                    WHEN 'Bromley' THEN 93
                    WHEN 'Camden' THEN 94
                    WHEN 'City of London' THEN 95
                    WHEN 'Croydon' THEN 96
                    WHEN 'Ealing' THEN 97
                    WHEN 'Enfield' THEN 98
                    WHEN 'Greenwich' THEN 99
                    WHEN 'Hackney' THEN 100
                    WHEN 'Hammersmith and Fulham' THEN 101
                    WHEN 'Haringey' THEN 102
                    WHEN 'Harrow' THEN 103
                    WHEN 'Havering' THEN 104
                    WHEN 'Hillingdon' THEN 105
                    WHEN 'Hounslow' THEN 106
                    WHEN 'Islington' THEN 107
                    WHEN 'Kensington and Chelsea' THEN 108
                    WHEN 'Kingston upon Thames' THEN 109
                    WHEN 'Lambeth' THEN 110
                    WHEN 'Lewisham' THEN 111
                    WHEN 'Merton' THEN 112
                    WHEN 'Newham' THEN 113
                    WHEN 'Redbridge' THEN 114
                    WHEN 'Richmond upon Thames' THEN 115
                    WHEN 'Southwark' THEN 116
                    WHEN 'Sutton' THEN 117
                    WHEN 'Tower Hamlets' THEN 118
                    WHEN 'Waltham Forest' THEN 119
                    WHEN 'Wandsworth' THEN 120
                    WHEN 'Westminster' THEN 121
                    WHEN 'Darlington' THEN 122
                    WHEN 'Chester-le-Street' THEN 123
                    WHEN 'Derwentside' THEN 124
                    WHEN 'Durham' THEN 125
                    WHEN 'Easington' THEN 126
                    WHEN 'Sedgefield' THEN 127
                    WHEN 'Teesdale' THEN 128
                    WHEN 'Wear Valley' THEN 129
                    WHEN 'Gateshead' THEN 130
                    WHEN 'Hartlepool' THEN 131
                    WHEN 'middlesbrough' THEN 132
                    WHEN 'Newcastle upon Tyne' THEN 133
                    WHEN 'North Tyneside' THEN 134
                    WHEN 'Alnwick' THEN 135
                    WHEN 'Berwick-upon-Tweed' THEN 136
                    WHEN 'Blyth Valley' THEN 137
                    WHEN 'Castle Morpeth' THEN 138
                    WHEN 'Tynedale' THEN 139
                    WHEN 'Wansbeck' THEN 140
                    WHEN 'Redcar and Cleveland' THEN 141
                    WHEN 'South Tyneside' THEN 142
                    WHEN 'Stockton-on-Tees' THEN 143
                    WHEN 'Sunderland' THEN 144
                    WHEN 'Blackburn with Darwen' THEN 145
                    WHEN 'Blackpool' THEN 146
                    WHEN 'Bolton' THEN 147
                    WHEN 'Bury' THEN 148
                    WHEN 'Chester' THEN 149
                    WHEN 'Congleton' THEN 150
                    WHEN 'Crewe and Nantwich' THEN 151
                    WHEN 'Ellesmere Port & Neston' THEN 152
                    WHEN 'Macclesfield' THEN 153
                    WHEN 'Vale Royal' THEN 154
                    WHEN 'Allerdale' THEN 155
                    WHEN 'Barrow-in-Furness' THEN 156
                    WHEN 'Carlisle' THEN 157
                    WHEN 'Copeland' THEN 158
                    WHEN 'Eden' THEN 159
                    WHEN 'South Lakeland' THEN 160
                    WHEN 'Halton' THEN 161
                    WHEN 'Knowsley' THEN 162
                    WHEN 'Burnley' THEN 163
                    WHEN 'Chorley' THEN 164
                    WHEN 'Fylde' THEN 165
                    WHEN 'Hyndburn' THEN 166
                    WHEN 'Lancaster' THEN 167
                    WHEN 'Pendle' THEN 168
                    WHEN 'Preston' THEN 169
                    WHEN 'Ribble Valley' THEN 170
                    WHEN 'Rossendale' THEN 171
                    WHEN 'South Ribble' THEN 172
                    WHEN 'West Lancashire' THEN 173
                    WHEN 'Wyre' THEN 174
                    WHEN 'Liverpool' THEN 175
                    WHEN 'Manchester' THEN 176
                    WHEN 'Oldham' THEN 177
                    WHEN 'Rochdale' THEN 178
                    WHEN 'Salford' THEN 179
                    WHEN 'Sefton' THEN 180
                    WHEN 'St. Helens' THEN 181
                    WHEN 'Stockport' THEN 182
                    WHEN 'Tameside' THEN 183
                    WHEN 'Trafford' THEN 184
                    WHEN 'Warrington' THEN 185
                    WHEN 'Wigan' THEN 186
                    WHEN 'Wirral' THEN 187
                    WHEN 'Bracknell Forest' THEN 188
                    WHEN 'Brighton and Hove' THEN 189
                    WHEN 'Aylesbury Vale' THEN 190
                    WHEN 'Chiltern' THEN 191
                    WHEN 'South Bucks' THEN 192
                    WHEN 'Wycombe' THEN 193
                    WHEN 'Eastbourne' THEN 194
                    WHEN 'Hastings' THEN 195
                    WHEN 'Lewes' THEN 196
                    WHEN 'Rother' THEN 197
                    WHEN 'Wealden' THEN 198
                    WHEN 'Basingstoke and Deane' THEN 199
                    WHEN 'East Hampshire' THEN 200
                    WHEN 'Eastleigh' THEN 201
                    WHEN 'Fareham' THEN 202
                    WHEN 'Gosport' THEN 203
                    WHEN 'Hart' THEN 204
                    WHEN 'Havant' THEN 205
                    WHEN 'New Forest' THEN 206
                    WHEN 'Rushmoor' THEN 207
                    WHEN 'Test Valley' THEN 208
                    WHEN 'Winchester' THEN 209
                    WHEN 'Isle of Wight' THEN 210
                    WHEN 'Ashford' THEN 211
                    WHEN 'Canterbury' THEN 212
                    WHEN 'Dartford' THEN 213
                    WHEN 'Dover' THEN 214
                    WHEN 'Gravesham' THEN 215
                    WHEN 'Maidstone' THEN 216
                    WHEN 'Sevenoaks' THEN 217
                    WHEN 'Shepway' THEN 218
                    WHEN 'Swale' THEN 219
                    WHEN 'Thanet' THEN 220
                    WHEN 'Tonbridge and Malling' THEN 221
                    WHEN 'Tunbridge Wells' THEN 222
                    WHEN 'Medway' THEN 223
                    WHEN 'Milton Keynes' THEN 224
                    WHEN 'Cherwell' THEN 225
                    WHEN 'Oxford' THEN 226
                    WHEN 'South Oxfordshire' THEN 227
                    WHEN 'Vale of White Horse' THEN 228
                    WHEN 'West Oxfordshire' THEN 229
                    WHEN 'Portsmouth' THEN 230
                    WHEN 'Reading' THEN 231
                    WHEN 'Slough' THEN 232
                    WHEN 'Southampton' THEN 233
                    WHEN 'Elmbridge' THEN 234
                    WHEN 'Epsom and Ewell' THEN 235
                    WHEN 'Guildford' THEN 236
                    WHEN 'Mole Valley' THEN 237
                    WHEN 'Reigate and Banstead' THEN 238
                    WHEN 'Runnymede' THEN 239
                    WHEN 'Spelthorne' THEN 240
                    WHEN 'Surrey Heath' THEN 241
                    WHEN 'Tandridge' THEN 242
                    WHEN 'Waverley' THEN 243
                    WHEN 'Woking' THEN 244
                    WHEN 'West Berkshire' THEN 245
                    WHEN 'Adur' THEN 246
                    WHEN 'Arun' THEN 247
                    WHEN 'Chichester' THEN 248
                    WHEN 'Crawley' THEN 249
                    WHEN 'Horsham' THEN 250
                    WHEN 'Mid Sussex' THEN 251
                    WHEN 'Worthing' THEN 252
                    WHEN 'Windsor and Maidenhead' THEN 253
                    WHEN 'Wokingham' THEN 254
                    WHEN 'Bath and North East Somerset' THEN 255
                    WHEN 'Bournemouth' THEN 256
                    WHEN 'City of Bristol' THEN 257
                    WHEN 'Caradon' THEN 258
                    WHEN 'Carrick' THEN 259
                    WHEN 'Kerrier' THEN 260
                    WHEN 'North Cornwall' THEN 261
                    WHEN 'Penwith' THEN 262
                    WHEN 'Restormel' THEN 263
                    WHEN 'East Devon' THEN 264
                    WHEN 'Exeter' THEN 265
                    WHEN 'Mid Devon' THEN 266
                    WHEN 'North Devon' THEN 267
                    WHEN 'South Hams' THEN 268
                    WHEN 'Teignbridge' THEN 269
                    WHEN 'Torridge' THEN 270
                    WHEN 'West Devon' THEN 271
                    WHEN 'Christchurch' THEN 272
                    WHEN 'East Dorset' THEN 273
                    WHEN 'North Dorset' THEN 274
                    WHEN 'Purbeck' THEN 275
                    WHEN 'West Dorset' THEN 276
                    WHEN 'Weymouth and Portland' THEN 277
                    WHEN 'Cheltenham' THEN 278
                    WHEN 'Cotswold' THEN 279
                    WHEN 'Forest of Dean' THEN 280
                    WHEN 'Gloucester' THEN 281
                    WHEN 'Stroud' THEN 282
                    WHEN 'Tewkesbury' THEN 283
                    WHEN 'Isles of Scilly' THEN 284
                    WHEN 'North Somerset' THEN 285
                    WHEN 'Plymouth' THEN 286
                    WHEN 'Poole' THEN 287
                    WHEN 'Mendip' THEN 288
                    WHEN 'Sedgemoor' THEN 289
                    WHEN 'South Somerset' THEN 290
                    WHEN 'Taunton Deane' THEN 291
                    WHEN 'West Somerset' THEN 292
                    WHEN 'South Gloucestershire' THEN 293
                    WHEN 'Swindon' THEN 294
                    WHEN 'Torbay' THEN 295
                    WHEN 'Kennet' THEN 296
                    WHEN 'North Wiltshire' THEN 297
                    WHEN 'Salisbury' THEN 298
                    WHEN 'West Wiltshire' THEN 299
                    WHEN 'Birmingham' THEN 300
                    WHEN 'Coventry' THEN 301
                    WHEN 'Dudley' THEN 302
                    WHEN 'Herefordshire' THEN 303
                    WHEN 'Sandwell' THEN 304
                    WHEN 'Bridgnorth' THEN 305
                    WHEN 'North Shropshire' THEN 306
                    WHEN 'Oswestry' THEN 307
                    WHEN 'Shrewsbury and Atcham' THEN 308
                    WHEN 'South Shropshire' THEN 309
                    WHEN 'Solihull' THEN 310
                    WHEN 'Cannock Chase' THEN 311
                    WHEN 'East Staffordshire' THEN 312
                    WHEN 'Lichfield' THEN 313
                    WHEN 'Newcastle-under-Lyme' THEN 314
                    WHEN 'South Staffordshire' THEN 315
                    WHEN 'Stafford' THEN 316
                    WHEN 'Staffordshire Moorlands' THEN 317
                    WHEN 'Tamworth' THEN 318
                    WHEN 'Stoke-on-Trent' THEN 319
                    WHEN 'Telford and Wrekin' THEN 320
                    WHEN 'Walsall' THEN 321
                    WHEN 'North Warwickshire' THEN 322
                    WHEN 'Nuneaton and Bedworth' THEN 323
                    WHEN 'Rugby' THEN 324
                    WHEN 'Stratford-on-Avon' THEN 325
                    WHEN 'Warwick' THEN 326
                    WHEN 'Wolverhampton' THEN 327
                    WHEN 'Bromsgrove' THEN 328
                    WHEN 'Malvern Hills' THEN 329
                    WHEN 'Redditch' THEN 330
                    WHEN 'Worcester' THEN 331
                    WHEN 'Wychavon' THEN 332
                    WHEN 'Wyre Forest' THEN 333
                    WHEN 'Barnsley' THEN 334
                    WHEN 'Bradford' THEN 335
                    WHEN 'Calderdale' THEN 336
                    WHEN 'Doncaster' THEN 337
                    WHEN 'East Riding of Yorkshire' THEN 338
                    WHEN 'Kingston upon Hull' THEN 339
                    WHEN 'Kirklees' THEN 340
                    WHEN 'Leeds' THEN 341
                    WHEN 'North East Lincolnshire' THEN 342
                    WHEN 'North Lincolnshire' THEN 343
                    WHEN 'Craven' THEN 344
                    WHEN 'Hambleton' THEN 345
                    WHEN 'Harrogate' THEN 346
                    WHEN 'Richmondshire' THEN 347
                    WHEN 'Ryedale' THEN 348
                    WHEN 'Scarborough' THEN 349
                    WHEN 'Selby' THEN 350
                    WHEN 'Rotherham' THEN 351
                    WHEN 'Sheffield' THEN 352
                    WHEN 'Wakefield' THEN 353
                    WHEN 'York' THEN 354
                    WHEN 'Bedford' THEN 400
                    WHEN 'Central Bedfordshire' THEN 401
                    WHEN 'Cheshire East' THEN 402
                    WHEN 'Cheshire West and Chester' THEN 403
                    WHEN 'Cornwall' THEN 404
                    WHEN 'Isles of Scilly' THEN 405
                    WHEN 'County Durham' THEN 406
                    WHEN 'Northumberland' THEN 407
                    WHEN 'Shropshire' THEN 408
                    WHEN 'Wiltshire' THEN 409
                 END
          FROM   "Cssr", cqcref.pcodedata
          WHERE postcode = e."PostCode"
          AND  pcodedata.local_custodian_code = "LocalCustodianCode"
          GROUP BY "Cssr"."CssrID", "Cssr"."CssR", "LocalAuthority"
          LIMIT 1
       ),-1) lauthid,
       'na' parliamentaryconstituency,
       (
          SELECT CASE name
                    WHEN 'Care home services with nursing' THEN 1
                    WHEN 'Care home services without nursing' THEN 2
                    WHEN 'Other adult residential care services' THEN 5
                    WHEN 'Day care and day services' THEN 6
                    WHEN 'Other adult day care services' THEN 7
                    WHEN 'Domiciliary care services' THEN 8
                    WHEN 'Domestic services and home help' THEN 10
                    WHEN 'Other adult domiciliary care service' THEN 12
                    WHEN 'Carers support' THEN 13
                    WHEN 'Short breaks / respite care' THEN 14
                    WHEN 'Community support and outreach' THEN 15
                    WHEN 'Social work and care management' THEN 16
                    WHEN 'Shared lives' THEN 17
                    WHEN 'Disability adaptations / assistive technology services' THEN 18
                    WHEN 'Occupational / employment-related services' THEN 19
                    WHEN 'Information and advice services' THEN 20
                    WHEN 'Other adult community care service' THEN 21
                    WHEN 'Any other services' THEN 52
                    WHEN 'Sheltered housing' THEN 53
                    WHEN 'Extra care housing services' THEN 54
                    WHEN 'Supported living services' THEN 55
                    WHEN 'Specialist college services' THEN 60
                    WHEN 'Community based services for people with a learning disability' THEN 61
                    WHEN 'Community based services for people with mental health needs' THEN 62
                    WHEN 'Community based services for people who misuse substances' THEN 63
                    WHEN 'Community healthcare services' THEN 64
                    WHEN 'Hospice services' THEN 66
                    WHEN 'Long term conditions services' THEN 67
                    WHEN 'Hospital services for people with mental health needs, learning disabilities and/or problems with substance misuse' THEN 68
                    WHEN 'Rehabilitation services' THEN 69
                    WHEN 'Residential substance misuse treatment/ rehabilitation services' THEN 70
                    WHEN 'Other healthcare service' THEN 71
                    WHEN 'Head office services' THEN 72
                    WHEN 'Nurses agency' THEN 74
                 END
          FROM   services
          WHERE  id = e."MainServiceFKValue"
       ) mainstid,
       CASE w."ContractValue" WHEN 'Permanent' THEN 190 WHEN 'Temporary' THEN 191 WHEN 'Pool/Bank' THEN 192 WHEN 'Agency' THEN 193 WHEN 'Other' THEN 196 ELSE -1 END emplstat,
       TO_CHAR(w."ContractChangedAt",'DD/MM/YYYY') emplstat_changedate,
       TO_CHAR(w."ContractSavedAt",'DD/MM/YYYY') emplstat_savedate,
       COALESCE((
          SELECT CASE "JobName"
                    WHEN 'Senior Management' THEN 1
                    WHEN 'Middle Management' THEN 2
                    WHEN 'First Line Manager' THEN 3
                    WHEN 'Registered Manager' THEN 4
                    WHEN 'Supervisor' THEN 5
                    WHEN 'Social Worker' THEN 6
                    WHEN 'Senior Care Worker' THEN 7
                    WHEN 'Care Worker' THEN 8
                    WHEN 'Community, Support and Outreach Work' THEN 9
                    WHEN 'Employment Support' THEN 10
                    WHEN 'Advice, Guidance and Advocacy' THEN 11
                    WHEN 'Occupational Therapist' THEN 15
                    WHEN 'Registered Nurse' THEN 16
                    WHEN 'Allied Health Professional (not Occupational Therapist)' THEN 17
                    WHEN 'Technician' THEN 22
                    WHEN 'Other job roles directly involved in providing care' THEN 23
                    WHEN 'Managers and staff care-related but not care-providing' THEN 24
                    WHEN 'Administrative / office staff not care-providing' THEN 25
                    WHEN 'Ancillary staff not care-providing' THEN 26
                    WHEN 'Other job roles not directly involved in providing care' THEN 27
                    WHEN 'Activities worker or co-ordinator' THEN 34
                    WHEN 'Safeguarding & Reviewing Officer' THEN 35
                    WHEN 'Occupational Therapist Assistant' THEN 36
                    WHEN 'Nursing Associate' THEN 37
                    WHEN 'Nursing Assistant' THEN 38
                    WHEN 'Assessment Officer' THEN 39
                    WHEN 'Care Coordinator' THEN 40
                    WHEN 'Care Navigator' THEN 41
                    WHEN 'Any childrens / young people''s job role' THEN 42
                 END
          FROM   "Job"
          WHERE  "JobID" = w."MainJobFKValue"
       ),-1) mainjrid,
       TO_CHAR("MainJobFKChangedAt",'DD/MM/YYYY') mainjrid_changedate,
       TO_CHAR("MainJobFKSavedAt",'DD/MM/YYYY') mainjrid_savedate,
       TO_CHAR("MainJobStartDateValue",'DD/MM/YYYY') strtdate,
       TO_CHAR("MainJobStartDateChangedAt",'DD/MM/YYYY') strtdate_changedate,
       TO_CHAR("MainJobStartDateSavedAt",'DD/MM/YYYY') strtdate_savedate,
       EXTRACT(YEAR FROM AGE("DateOfBirthValue")) age,
       TO_CHAR("DateOfBirthChangedAt",'DD/MM/YYYY') age_changedate,
       TO_CHAR("DateOfBirthSavedAt",'DD/MM/YYYY') age_savedate,
       CASE "GenderValue" WHEN 'Male' THEN 1 WHEN 'Female' THEN 2 WHEN 'Don''t know' THEN 3 WHEN 'Other' THEN 4 ELSE -1 END gender,
       TO_CHAR("GenderChangedAt",'DD/MM/YYYY') gender_changedate,
       TO_CHAR("GenderSavedAt",'DD/MM/YYYY') gender_savedate,
       CASE "DisabilityValue" WHEN 'No' THEN 0 WHEN 'Yes' THEN 1 WHEN 'Undisclosed' THEN 2 WHEN 'Don''t know' THEN -2 ELSE -1 END disabled,
       TO_CHAR("DisabilityChangedAt",'DD/MM/YYYY') disabled_changedate,
       TO_CHAR("DisabilitySavedAt",'DD/MM/YYYY') disabled_savedate,
       COALESCE((
          SELECT CASE "Ethnicity"
                    WHEN 'English / Welsh / Scottish / Northern Irish / British' THEN 31
                    WHEN 'Irish' THEN 32
                    WHEN 'Gypsy or Irish Traveller' THEN 33
                    WHEN 'Any other White background' THEN 34
                    WHEN 'White and Black Caribbean' THEN 35
                    WHEN 'White and Black African' THEN 36
                    WHEN 'White and Asian' THEN 37
                    WHEN 'Any other Mixed/ multiple ethnic background' THEN 38
                    WHEN 'Indian' THEN 39
                    WHEN 'Pakistani' THEN 40
                    WHEN 'Bangladeshi' THEN 41
                    WHEN 'Chinese' THEN 42
                    WHEN 'Any other Asian background' THEN 43
                    WHEN 'African' THEN 44
                    WHEN 'Caribbean' THEN 45
                    WHEN 'Any other Black / African / Caribbean background' THEN 46
                    WHEN 'Arab' THEN 47
                    WHEN 'Any other ethnic group' THEN 98
                    WHEN 'Don''t know' THEN 99
                 END
          FROM   "Ethnicity"
          WHERE  "ID" = w."EthnicityFKValue"
       ),-1) ethnicity,
       TO_CHAR("EthnicityFKChangedAt",'DD/MM/YYYY') ethnicity_changedate,
       TO_CHAR("EthnicityFKSavedAt",'DD/MM/YYYY') ethnicity_savedate,
       CASE "NationalityValue" WHEN 'British' THEN 1 WHEN 'Other' THEN 0 WHEN 'Don''t know' THEN 2 ELSE -1 END isbritish,
       CASE "NationalityValue"
          WHEN 'British' THEN 826
          ELSE
             COALESCE((
                SELECT CASE "Nationality"
                          WHEN 'Afghan' THEN 4
                          WHEN 'Albanian' THEN 8
                          WHEN 'Algerian' THEN 12
                          WHEN 'American' THEN 16
                          WHEN 'Andorran' THEN 20
                          WHEN 'Angolan' THEN 24
                          WHEN 'Citizen of Antigua and Barbuda' THEN 28
                          WHEN 'Azerbaijani' THEN 31
                          WHEN 'Argentine' THEN 32
                          WHEN 'Australian' THEN 36
                          WHEN 'Austrian' THEN 40
                          WHEN 'Bahamian' THEN 44
                          WHEN 'Bahraini' THEN 48
                          WHEN 'Bangladeshi' THEN 50
                          WHEN 'Armenian' THEN 51
                          WHEN 'Barbadian' THEN 52
                          WHEN 'Belgian' THEN 56
                          WHEN 'Bermudian' THEN 60
                          WHEN 'Bhutanese' THEN 64
                          WHEN 'Bolivian' THEN 68
                          WHEN 'Citizen of Bosnia and Herzegovina' THEN 70
                          WHEN 'Botswanan' THEN 72
                          WHEN 'Brazilian' THEN 76
                          WHEN 'Belizean' THEN 84
                          WHEN 'Solomon Islander' THEN 90
                          WHEN 'Bruneian' THEN 96
                          WHEN 'Bulgarian' THEN 100
                          WHEN 'Burmese' THEN 104
                          WHEN 'Burundian' THEN 108
                          WHEN 'Belarusian' THEN 112
                          WHEN 'Cambodian' THEN 116
                          WHEN 'Cameroonian' THEN 120
                          WHEN 'Canadian' THEN 124
                          WHEN 'Cape Verdean' THEN 132
                          WHEN 'Cayman Islander' THEN 136
                          WHEN 'Central African' THEN 140
                          WHEN 'Sri Lankan' THEN 144
                          WHEN 'Chadian' THEN 148
                          WHEN 'Chilean' THEN 152
                          WHEN 'Chinese' THEN 156
                          WHEN 'Taiwanese' THEN 158
                          WHEN 'Colombian' THEN 170
                          WHEN 'Comoran' THEN 174
                          WHEN 'Congolese (Congo)' THEN 178
                          WHEN 'Congolese (DRC)' THEN 180
                          WHEN 'Cook Islander' THEN 184
                          WHEN 'Costa Rican' THEN 188
                          WHEN 'Croatian' THEN 191
                          WHEN 'Cuban' THEN 192
                          WHEN 'Cypriot' THEN 196
                          WHEN 'Czech' THEN 203
                          WHEN 'Beninese' THEN 204
                          WHEN 'Danish' THEN 208
                          WHEN 'Dominican' THEN 212
                          WHEN 'Citizen of the Dominican Republic' THEN 214
                          WHEN 'Ecuadorean' THEN 218
                          WHEN 'Salvadorean' THEN 222
                          WHEN 'Equatorial Guinean' THEN 226
                          WHEN 'Ethiopian' THEN 231
                          WHEN 'Eritrean' THEN 232
                          WHEN 'Estonian' THEN 233
                          WHEN 'Faroese' THEN 234
                          WHEN 'Fijian' THEN 242
                          WHEN 'Finnish' THEN 246
                          WHEN 'French' THEN 250
                          WHEN 'Djiboutian' THEN 262
                          WHEN 'Gabonese' THEN 266
                          WHEN 'Georgian' THEN 268
                          WHEN 'Gambian' THEN 270
                          WHEN 'Palestinian' THEN 275
                          WHEN 'German' THEN 276
                          WHEN 'Ghanaian' THEN 288
                          WHEN 'Gibraltarian' THEN 292
                          WHEN 'Citizen of Kiribati' THEN 296
                          WHEN 'Greek' THEN 300
                          WHEN 'Greenlandic' THEN 304
                          WHEN 'Grenadian' THEN 308
                          WHEN 'Guamanian' THEN 316
                          WHEN 'Guatemalan' THEN 320
                          WHEN 'Guinean' THEN 324
                          WHEN 'Guyanese' THEN 328
                          WHEN 'Haitian' THEN 332
                          WHEN 'Honduran' THEN 340
                          WHEN 'Hong Konger' THEN 344
                          WHEN 'Hungarian' THEN 348
                          WHEN 'Icelandic' THEN 352
                          WHEN 'Indian' THEN 356
                          WHEN 'Indonesian' THEN 360
                          WHEN 'Iranian' THEN 364
                          WHEN 'Iraqi' THEN 368
                          WHEN 'Irish' THEN 372
                          WHEN 'Israeli' THEN 376
                          WHEN 'Italian' THEN 380
                          WHEN 'Ivorian' THEN 384
                          WHEN 'Jamaican' THEN 388
                          WHEN 'Japanese' THEN 392
                          WHEN 'Kazakh' THEN 398
                          WHEN 'Jordanian' THEN 400
                          WHEN 'Kenyan' THEN 404
                          WHEN 'North Korean' THEN 408
                          WHEN 'South Korean' THEN 410
                          WHEN 'Kuwaiti' THEN 414
                          WHEN 'Kyrgyz' THEN 417
                          WHEN 'Lebanese' THEN 422
                          WHEN 'Mosotho' THEN 426
                          WHEN 'Latvian' THEN 428
                          WHEN 'Liberian' THEN 430
                          WHEN 'Libyan' THEN 434
                          WHEN 'Liechtenstein citizen' THEN 438
                          WHEN 'Lithuanian' THEN 440
                          WHEN 'Luxembourger' THEN 442
                          WHEN 'Macanese' THEN 446
                          WHEN 'Malagasy' THEN 450
                          WHEN 'Malawian' THEN 454
                          WHEN 'Malaysian' THEN 458
                          WHEN 'Maldivian' THEN 462
                          WHEN 'Malian' THEN 466
                          WHEN 'Maltese' THEN 470
                          WHEN 'Martiniquais' THEN 474
                          WHEN 'Mauritanian' THEN 478
                          WHEN 'Mauritian' THEN 480
                          WHEN 'Lao' THEN 481
                          WHEN 'Mexican' THEN 484
                          WHEN 'Monegasque' THEN 492
                          WHEN 'Mongolian' THEN 496
                          WHEN 'Moldovan' THEN 498
                          WHEN 'Montenegrin' THEN 499
                          WHEN 'Montserratian' THEN 500
                          WHEN 'Moroccan' THEN 504
                          WHEN 'Mozambican' THEN 508
                          WHEN 'Omani' THEN 512
                          WHEN 'Namibian' THEN 516
                          WHEN 'Nauruan' THEN 520
                          WHEN 'Nepalese' THEN 524
                          WHEN 'Dutch' THEN 528
                          WHEN 'Citizen of Vanuatu' THEN 548
                          WHEN 'New Zealander' THEN 554
                          WHEN 'Nicaraguan' THEN 558
                          WHEN 'Nigerien' THEN 562
                          WHEN 'Nigerian' THEN 566
                          WHEN 'Niuean' THEN 570
                          WHEN 'Norwegian' THEN 578
                          WHEN 'Micronesian' THEN 583
                          WHEN 'Marshallese' THEN 584
                          WHEN 'Palauan' THEN 585
                          WHEN 'Pakistani' THEN 586
                          WHEN 'Panamanian' THEN 591
                          WHEN 'Papua New Guinean' THEN 598
                          WHEN 'Paraguayan' THEN 600
                          WHEN 'Peruvian' THEN 604
                          WHEN 'Filipino' THEN 608
                          WHEN 'Polish' THEN 616
                          WHEN 'Portuguese' THEN 620
                          WHEN 'Guinea-Bissau' THEN 624
                          WHEN 'East Timorese' THEN 626
                          WHEN 'Puerto Rican' THEN 630
                          WHEN 'Qatari' THEN 634
                          WHEN 'Romanian' THEN 642
                          WHEN 'Russian' THEN 643
                          WHEN 'Rwandan' THEN 646
                          WHEN 'St Helenian' THEN 654
                          WHEN 'Kittitian' THEN 659
                          WHEN 'Anguillan' THEN 660
                          WHEN 'St Lucian' THEN 662
                          WHEN 'Vincentian' THEN 670
                          WHEN 'Sammarinese' THEN 674
                          WHEN 'Sao Tomean' THEN 678
                          WHEN 'Saudi Arabian' THEN 682
                          WHEN 'Senegalese' THEN 686
                          WHEN 'Serbian' THEN 688
                          WHEN 'Citizen of Seychelles' THEN 690
                          WHEN 'Sierra Leonean' THEN 694
                          WHEN 'Singaporean' THEN 702
                          WHEN 'Slovak' THEN 703
                          WHEN 'Vietnamese' THEN 704
                          WHEN 'Slovenian' THEN 705
                          WHEN 'Somali' THEN 706
                          WHEN 'South African' THEN 710
                          WHEN 'Zimbabwean' THEN 716
                          WHEN 'Spanish' THEN 724
                          WHEN 'South Sudanese' THEN 728
                          WHEN 'Sudanese' THEN 736
                          WHEN 'Surinamese' THEN 740
                          WHEN 'Swazi' THEN 748
                          WHEN 'Swedish' THEN 752
                          WHEN 'Swiss' THEN 756
                          WHEN 'Syrian' THEN 760
                          WHEN 'Tajik' THEN 762
                          WHEN 'Thai' THEN 764
                          WHEN 'Togolese' THEN 768
                          WHEN 'Tongan' THEN 776
                          WHEN 'Trinidadian' THEN 780
                          WHEN 'Emirati' THEN 784
                          WHEN 'Tunisian' THEN 788
                          WHEN 'Turkish' THEN 792
                          WHEN 'Turkmen' THEN 795
                          WHEN 'Turks and Caicos Islander' THEN 796
                          WHEN 'Tuvaluan' THEN 798
                          WHEN 'Ugandan' THEN 800
                          WHEN 'Ukrainian' THEN 804
                          WHEN 'Macedonian' THEN 807
                          WHEN 'Egyptian' THEN 818
                          WHEN 'British' THEN 826
                          WHEN 'Tanzanian' THEN 834
                          WHEN 'Burkinan' THEN 854
                          WHEN 'Uruguayan' THEN 858
                          WHEN 'Uzbek' THEN 860
                          WHEN 'Venezuelan' THEN 862
                          WHEN 'Wallisian' THEN 876
                          WHEN 'Samoan' THEN 882
                          WHEN 'Yemeni' THEN 887
                          WHEN 'Zambian' THEN 894
                          WHEN 'Kosovon' THEN 995
                          WHEN 'Workers nationality unknown' THEN 998
                       END
                FROM   "Nationality"
                WHERE  "ID" = w."NationalityOtherFK"
             ),-1)
       END nationality,
       TO_CHAR("NationalityChangedAt",'DD/MM/YYYY') isbritish_changedate,
       TO_CHAR("NationalitySavedAt",'DD/MM/YYYY') isbritish_savedate,
       CASE "BritishCitizenshipValue" WHEN 'No' THEN 0 WHEN 'Yes' THEN 1 WHEN 'Don''t know' THEN 2 ELSE -1 END britishcitizen,
       TO_CHAR("BritishCitizenshipChangedAt",'DD/MM/YYYY') britishcitizen_changedate,
       TO_CHAR("BritishCitizenshipSavedAt",'DD/MM/YYYY') britishcitizen_savedate,
       CASE "CountryOfBirthValue" WHEN 'Other' THEN 0 WHEN 'United Kingdom' THEN 1 WHEN 'Don''t know' THEN 2 ELSE -1 END borninuk,
       CASE "CountryOfBirthValue"
          WHEN 'United Kingdom' THEN 826
          ELSE
            COALESCE((
               SELECT CASE "Country"
                         WHEN 'Afghanistan' THEN 4
                         WHEN 'Albania' THEN 8
                         WHEN 'Antarctica' THEN 10
                         WHEN 'Algeria' THEN 12
                         WHEN 'American Samoa' THEN 16
                         WHEN 'Andorra' THEN 20
                         WHEN 'Angola' THEN 24
                         WHEN 'Antigua and Barbuda' THEN 28
                         WHEN 'Azerbaijan' THEN 31
                         WHEN 'Argentina' THEN 32
                         WHEN 'Australia' THEN 36
                         WHEN 'Austria' THEN 40
                         WHEN 'Bahamas' THEN 44
                         WHEN 'Bahrain' THEN 48
                         WHEN 'Bangladesh' THEN 50
                         WHEN 'Armenia' THEN 51
                         WHEN 'Barbados' THEN 52
                         WHEN 'Belgium' THEN 56
                         WHEN 'Bermuda' THEN 60
                         WHEN 'Bhutan' THEN 64
                         WHEN 'Bolivia' THEN 68
                         WHEN 'Bosnia and Herzegovina' THEN 70
                         WHEN 'Botswana' THEN 72
                         WHEN 'Bouvet Island' THEN 74
                         WHEN 'Brazil' THEN 76
                         WHEN 'Belize' THEN 84
                         WHEN 'British Indian Ocean Territory' THEN 86
                         WHEN 'Solomon Islands' THEN 90
                         WHEN 'Virgin Islands British' THEN 92
                         WHEN 'Brunei Darussalam' THEN 96
                         WHEN 'Bulgaria' THEN 100
                         WHEN 'Myanmar' THEN 104
                         WHEN 'Burundi' THEN 108
                         WHEN 'Belarus' THEN 112
                         WHEN 'Cambodia' THEN 116
                         WHEN 'Cameroon' THEN 120
                         WHEN 'Canada' THEN 124
                         WHEN 'Cape Verde' THEN 132
                         WHEN 'Cayman Islands' THEN 136
                         WHEN 'Central African Republic' THEN 140
                         WHEN 'Sri Lanka' THEN 144
                         WHEN 'Chad' THEN 148
                         WHEN 'Chile' THEN 152
                         WHEN 'China' THEN 156
                         WHEN 'Taiwan Province of China' THEN 158
                         WHEN 'Christmas Island' THEN 162
                         WHEN 'Cocos (Keeling) Islands' THEN 166
                         WHEN 'Colombia' THEN 170
                         WHEN 'Comoros' THEN 174
                         WHEN 'Mayotte' THEN 175
                         WHEN 'Congo' THEN 178
                         WHEN 'Congo Democratic Republic of the' THEN 180
                         WHEN 'Cook Islands' THEN 184
                         WHEN 'Costa Rica' THEN 188
                         WHEN 'Croatia' THEN 191
                         WHEN 'Cuba' THEN 192
                         WHEN 'Cyprus' THEN 196
                         WHEN 'Czech Republic' THEN 203
                         WHEN 'Benin' THEN 204
                         WHEN 'Denmark' THEN 208
                         WHEN 'Dominica' THEN 212
                         WHEN 'Dominican Republic' THEN 214
                         WHEN 'Ecuador' THEN 218
                         WHEN 'El Salvador' THEN 222
                         WHEN 'Equatorial Guinea' THEN 226
                         WHEN 'Ethiopia' THEN 231
                         WHEN 'Eritrea' THEN 232
                         WHEN 'Estonia' THEN 233
                         WHEN 'Faroe Islands' THEN 234
                         WHEN 'Falkland Islands (Malvinas)' THEN 238
                         WHEN 'South Georgia and the South Sandwich Islands' THEN 239
                         WHEN 'Fiji' THEN 242
                         WHEN 'Finland' THEN 246
                         WHEN 'Aland Islands' THEN 248
                         WHEN 'France' THEN 250
                         WHEN 'French Guiana' THEN 254
                         WHEN 'French Polynesia' THEN 258
                         WHEN 'French Southern Territories' THEN 260
                         WHEN 'Djibouti' THEN 262
                         WHEN 'Gabon' THEN 266
                         WHEN 'Georgia' THEN 268
                         WHEN 'Gambia' THEN 270
                         WHEN 'Palestine, State of' THEN 275
                         WHEN 'Germany' THEN 276
                         WHEN 'Ghana' THEN 288
                         WHEN 'Gibraltar' THEN 292
                         WHEN 'Kiribati' THEN 296
                         WHEN 'Greece' THEN 300
                         WHEN 'Greenland' THEN 304
                         WHEN 'Grenada' THEN 308
                         WHEN 'Guadeloupe' THEN 312
                         WHEN 'Guam' THEN 316
                         WHEN 'Guatemala' THEN 320
                         WHEN 'Guinea' THEN 324
                         WHEN 'Guyana' THEN 328
                         WHEN 'Haiti' THEN 332
                         WHEN 'Heard Island and McDonald Islands' THEN 334
                         WHEN 'Holy See (Vatican City State)' THEN 336
                         WHEN 'Honduras' THEN 340
                         WHEN 'Hong Kong' THEN 344
                         WHEN 'Hungary' THEN 348
                         WHEN 'Iceland' THEN 352
                         WHEN 'India' THEN 356
                         WHEN 'Indonesia' THEN 360
                         WHEN 'Iran Islamic Republic of' THEN 364
                         WHEN 'Iraq' THEN 368
                         WHEN 'Ireland' THEN 372
                         WHEN 'Israel' THEN 376
                         WHEN 'Italy' THEN 380
                         WHEN 'Cote d''Ivoire' THEN 384
                         WHEN 'Jamaica' THEN 388
                         WHEN 'Japan' THEN 392
                         WHEN 'Kazakhstan' THEN 398
                         WHEN 'Jordan' THEN 400
                         WHEN 'Kenya' THEN 404
                         WHEN 'Korea Democratic People''s Republic of' THEN 408
                         WHEN 'Korea Republic of' THEN 410
                         WHEN 'Kuwait' THEN 414
                         WHEN 'Kyrgyzstan' THEN 417
                         WHEN 'Laos' THEN 418
                         WHEN 'Lebanon' THEN 422
                         WHEN 'Lesotho' THEN 426
                         WHEN 'Latvia' THEN 428
                         WHEN 'Liberia' THEN 430
                         WHEN 'Libya' THEN 434
                         WHEN 'Liechtenstein' THEN 438
                         WHEN 'Lithuania' THEN 440
                         WHEN 'Luxembourg' THEN 442
                         WHEN 'Macao' THEN 446
                         WHEN 'Madagascar' THEN 450
                         WHEN 'Malawi' THEN 454
                         WHEN 'Malaysia' THEN 458
                         WHEN 'Maldives' THEN 462
                         WHEN 'Mali' THEN 466
                         WHEN 'Malta' THEN 470
                         WHEN 'Mauritania' THEN 478
                         WHEN 'Mauritius' THEN 480
                         WHEN 'Mexico' THEN 484
                         WHEN 'Monaco' THEN 492
                         WHEN 'Mongolia' THEN 496
                         WHEN 'Moldova' THEN 498
                         WHEN 'Montenegro' THEN 499
                         WHEN 'Montserrat' THEN 500
                         WHEN 'Morocco' THEN 504
                         WHEN 'Mozambique' THEN 508
                         WHEN 'Oman' THEN 512
                         WHEN 'Namibia' THEN 516
                         WHEN 'Nauru' THEN 520
                         WHEN 'Nepal' THEN 524
                         WHEN 'Netherlands' THEN 528
                         WHEN 'Curacao (Formerly Netherlands Antilles)' THEN 531
                         WHEN 'Aruba' THEN 533
                         WHEN 'Sint Maarten (Dutch part)' THEN 534
                         WHEN 'Bonaire, Sint Eustatius and Saba' THEN 535
                         WHEN 'New Caledonia' THEN 540
                         WHEN 'Vanuatu' THEN 548
                         WHEN 'New Zealand' THEN 554
                         WHEN 'Nicaragua' THEN 558
                         WHEN 'Niger' THEN 562
                         WHEN 'Nigeria' THEN 566
                         WHEN 'Niue' THEN 570
                         WHEN 'Norfolk Island' THEN 574
                         WHEN 'Norway' THEN 578
                         WHEN 'Northern Mariana Islands' THEN 580
                         WHEN 'United States Minor Outlying Islands' THEN 581
                         WHEN 'Micronesia Federated States of' THEN 583
                         WHEN 'Marshall Islands' THEN 584
                         WHEN 'Palau' THEN 585
                         WHEN 'Pakistan' THEN 586
                         WHEN 'Panama' THEN 591
                         WHEN 'Papua New Guinea' THEN 598
                         WHEN 'Paraguay' THEN 600
                         WHEN 'Peru' THEN 604
                         WHEN 'Philippines' THEN 608
                         WHEN 'Pitcairn' THEN 612
                         WHEN 'Poland' THEN 616
                         WHEN 'Portugal' THEN 620
                         WHEN 'Guinea-Bissau' THEN 624
                         WHEN 'Timor-Leste' THEN 626
                         WHEN 'Puerto Rico' THEN 630
                         WHEN 'Qatar' THEN 634
                         WHEN 'Reunion' THEN 638
                         WHEN 'Romania' THEN 642
                         WHEN 'Russian Federation' THEN 643
                         WHEN 'Rwanda' THEN 646
                         WHEN 'Saint Barthelemy' THEN 652
                         WHEN 'St Helena Ascension and Tristan da Cunha' THEN 654
                         WHEN 'Saint Kitts and Nevis' THEN 659
                         WHEN 'Anguilla' THEN 660
                         WHEN 'Saint Lucia' THEN 662
                         WHEN 'Saint Martin (French part)' THEN 663
                         WHEN 'Saint Pierre and Miquelon' THEN 666
                         WHEN 'Saint Vincent and the Grenadines' THEN 670
                         WHEN 'San Marino' THEN 674
                         WHEN 'Sao Tome and Principe' THEN 678
                         WHEN 'Saudi Arabia' THEN 682
                         WHEN 'Senegal' THEN 686
                         WHEN 'Serbia' THEN 688
                         WHEN 'Seychelles' THEN 690
                         WHEN 'Sierra Leone' THEN 694
                         WHEN 'Singapore' THEN 702
                         WHEN 'Slovakia' THEN 703
                         WHEN 'Viet Nam' THEN 704
                         WHEN 'Slovenia' THEN 705
                         WHEN 'Somalia' THEN 706
                         WHEN 'South Africa' THEN 710
                         WHEN 'Zimbabwe' THEN 716
                         WHEN 'Spain' THEN 724
                         WHEN 'South Sudan' THEN 728
                         WHEN 'Western Sahara' THEN 732
                         WHEN 'Sudan' THEN 736
                         WHEN 'Suriname' THEN 740
                         WHEN 'Svalbard and Jan Mayen' THEN 744
                         WHEN 'Swaziland' THEN 748
                         WHEN 'Sweden' THEN 752
                         WHEN 'Switzerland' THEN 756
                         WHEN 'Syrian Arab Republic' THEN 760
                         WHEN 'Tajikistan' THEN 762
                         WHEN 'Thailand' THEN 764
                         WHEN 'Togo' THEN 768
                         WHEN 'Tokelau' THEN 772
                         WHEN 'Tonga' THEN 776
                         WHEN 'Trinidad and Tobago' THEN 780
                         WHEN 'United Arab Emirates' THEN 784
                         WHEN 'Tunisia' THEN 788
                         WHEN 'Turkey' THEN 792
                         WHEN 'Turkmenistan' THEN 795
                         WHEN 'Turks and Caicos Islands' THEN 796
                         WHEN 'Tuvalu' THEN 798
                         WHEN 'Uganda' THEN 800
                         WHEN 'Ukraine' THEN 804
                         WHEN 'Macedonia the former Yugoslav Republic of' THEN 807
                         WHEN 'Egypt' THEN 818
                         WHEN 'United Kingdom' THEN 826
                         WHEN 'Guernsey' THEN 831
                         WHEN 'Jersey' THEN 832
                         WHEN 'Tanzania United Republic of' THEN 834
                         WHEN 'United States' THEN 840
                         WHEN 'Virgin Islands U.S.' THEN 850
                         WHEN 'Burkina Faso' THEN 854
                         WHEN 'Uruguay' THEN 858
                         WHEN 'Uzbekistan' THEN 860
                         WHEN 'Venezuela' THEN 862
                         WHEN 'Wallis and Futuna' THEN 876
                         WHEN 'Samoa' THEN 882
                         WHEN 'Yemen' THEN 887
                         WHEN 'Zambia' THEN 894
                         WHEN 'Kosovo' THEN 995
                         WHEN 'Country unknown' THEN 999
                      END
               FROM   "Country"
               WHERE  "ID" = w."CountryOfBirthOtherFK"
            ),-1)
       END countryofbirth,
       TO_CHAR("CountryOfBirthChangedAt",'DD/MM/YYYY') borninuk_changedate,
       TO_CHAR("CountryOfBirthSavedAt",'DD/MM/YYYY') borninuk_savedate,
       CASE "YearArrivedValue" WHEN 'Yes' THEN "YearArrivedYear" WHEN 'No' THEN -1 ELSE -2 END yearofentry,
       TO_CHAR("YearArrivedChangedAt",'DD/MM/YYYY') yearofentry_changedate,
       TO_CHAR("YearArrivedSavedAt",'DD/MM/YYYY') yearofentry_savedate,
       COALESCE((SELECT
            "Cssr"."RegionID" 
        FROM
            "Cssr" 
        WHERE
            EXISTS (SELECT
                1 
            FROM
                cqcref.pcodedata 
            WHERE
                (cqcref.pcodedata.postcode = w."PostcodeValue") 
                AND ("Cssr"."LocalCustodianCode" = cqcref.pcodedata.local_custodian_code)) LIMIT 1), -1) homeregionid,
       COALESCE((SELECT
            "Cssr"."CssrID" 
        FROM
            "Cssr" 
        WHERE
            EXISTS (SELECT
                1 
            FROM
                cqcref.pcodedata 
            WHERE
                (cqcref.pcodedata.postcode = w."PostcodeValue") 
                AND ("Cssr"."LocalCustodianCode" = cqcref.pcodedata.local_custodian_code)) LIMIT 1), -1) homecssrid,
       COALESCE((
          SELECT CASE "LocalAuthority"
                    WHEN 'Mid Bedfordshire' THEN 1
                    WHEN 'Bedford' THEN 2
                    WHEN 'South Bedfordshire' THEN 3
                    WHEN 'Cambridge' THEN 4
                    WHEN 'East Cambridgeshire' THEN 5
                    WHEN 'Fenland' THEN 6
                    WHEN 'Huntingdonshire' THEN 7
                    WHEN 'South Cambridgeshire' THEN 8
                    WHEN 'Basildon' THEN 9
                    WHEN 'Braintree' THEN 10
                    WHEN 'Brentwood' THEN 11
                    WHEN 'Castle Point' THEN 12
                    WHEN 'Chelmsford' THEN 13
                    WHEN 'Colchester' THEN 14
                    WHEN 'Epping Forest' THEN 15
                    WHEN 'Harlow' THEN 16
                    WHEN 'Maldon' THEN 17
                    WHEN 'Rochford' THEN 18
                    WHEN 'Tendring' THEN 19
                    WHEN 'Uttlesford' THEN 20
                    WHEN 'Broxbourne' THEN 21
                    WHEN 'Dacorum' THEN 22
                    WHEN 'East Hertfordshire' THEN 23
                    WHEN 'Hertsmere' THEN 24
                    WHEN 'North Hertfordshire' THEN 25
                    WHEN 'St Albans' THEN 26
                    WHEN 'Stevenage' THEN 27
                    WHEN 'Three Rivers' THEN 28
                    WHEN 'Watford' THEN 29
                    WHEN 'Welwyn Hatfield' THEN 30
                    WHEN 'Luton' THEN 31
                    WHEN 'Breckland' THEN 32
                    WHEN 'Broadland' THEN 33
                    WHEN 'Great Yarmouth' THEN 34
                    WHEN 'King\`s Lynn and West Norfolk' THEN 35
                    WHEN 'North Norfolk' THEN 36
                    WHEN 'Norwich' THEN 37
                    WHEN 'South Norfolk' THEN 38
                    WHEN 'Peterborough' THEN 39
                    WHEN 'Southend-on-Sea' THEN 40
                    WHEN 'Babergh' THEN 41
                    WHEN 'Forest Heath' THEN 42
                    WHEN 'Ipswich' THEN 43
                    WHEN 'Mid Suffolk' THEN 44
                    WHEN 'St. Edmundsbury' THEN 45
                    WHEN 'Suffolk Coastal' THEN 46
                    WHEN 'Waveney' THEN 47
                    WHEN 'Thurrock' THEN 48
                    WHEN 'Derby' THEN 49
                    WHEN 'Amber Valley' THEN 50
                    WHEN 'Bolsover' THEN 51
                    WHEN 'Chesterfield' THEN 52
                    WHEN 'Derbyshire Dales' THEN 53
                    WHEN 'Erewash' THEN 54
                    WHEN 'High Peak' THEN 55
                    WHEN 'North East Derbyshire' THEN 56
                    WHEN 'South Derbyshire' THEN 57
                    WHEN 'Leicester' THEN 58
                    WHEN 'Blaby' THEN 59
                    WHEN 'Charnwood' THEN 60
                    WHEN 'Harborough' THEN 61
                    WHEN 'Hinckley and Bosworth' THEN 62
                    WHEN 'Melton' THEN 63
                    WHEN 'North West Leicestershire' THEN 64
                    WHEN 'Oadby and Wigston' THEN 65
                    WHEN 'Boston' THEN 66
                    WHEN 'East Lindsey' THEN 67
                    WHEN 'Lincoln' THEN 68
                    WHEN 'North Kesteven' THEN 69
                    WHEN 'South Holland' THEN 70
                    WHEN 'South Kesteven' THEN 71
                    WHEN 'West Lindsey' THEN 72
                    WHEN 'Corby' THEN 73
                    WHEN 'Daventry' THEN 74
                    WHEN 'East Northamptonshire' THEN 75
                    WHEN 'Kettering' THEN 76
                    WHEN 'Northampton' THEN 77
                    WHEN 'South Northamptonshire' THEN 78
                    WHEN 'Wellingborough' THEN 79
                    WHEN 'Nottingham' THEN 80
                    WHEN 'Ashfield' THEN 81
                    WHEN 'Bassetlaw' THEN 82
                    WHEN 'Broxtowe' THEN 83
                    WHEN 'Gedling' THEN 84
                    WHEN 'Mansfield' THEN 85
                    WHEN 'Newark and Sherwood' THEN 86
                    WHEN 'Rushcliffe' THEN 87
                    WHEN 'Rutland' THEN 88
                    WHEN 'Barking and Dagenham' THEN 89
                    WHEN 'Barnet' THEN 90
                    WHEN 'Bexley' THEN 91
                    WHEN 'Brent' THEN 92
                    WHEN 'Bromley' THEN 93
                    WHEN 'Camden' THEN 94
                    WHEN 'City of London' THEN 95
                    WHEN 'Croydon' THEN 96
                    WHEN 'Ealing' THEN 97
                    WHEN 'Enfield' THEN 98
                    WHEN 'Greenwich' THEN 99
                    WHEN 'Hackney' THEN 100
                    WHEN 'Hammersmith and Fulham' THEN 101
                    WHEN 'Haringey' THEN 102
                    WHEN 'Harrow' THEN 103
                    WHEN 'Havering' THEN 104
                    WHEN 'Hillingdon' THEN 105
                    WHEN 'Hounslow' THEN 106
                    WHEN 'Islington' THEN 107
                    WHEN 'Kensington and Chelsea' THEN 108
                    WHEN 'Kingston upon Thames' THEN 109
                    WHEN 'Lambeth' THEN 110
                    WHEN 'Lewisham' THEN 111
                    WHEN 'Merton' THEN 112
                    WHEN 'Newham' THEN 113
                    WHEN 'Redbridge' THEN 114
                    WHEN 'Richmond upon Thames' THEN 115
                    WHEN 'Southwark' THEN 116
                    WHEN 'Sutton' THEN 117
                    WHEN 'Tower Hamlets' THEN 118
                    WHEN 'Waltham Forest' THEN 119
                    WHEN 'Wandsworth' THEN 120
                    WHEN 'Westminster' THEN 121
                    WHEN 'Darlington' THEN 122
                    WHEN 'Chester-le-Street' THEN 123
                    WHEN 'Derwentside' THEN 124
                    WHEN 'Durham' THEN 125
                    WHEN 'Easington' THEN 126
                    WHEN 'Sedgefield' THEN 127
                    WHEN 'Teesdale' THEN 128
                    WHEN 'Wear Valley' THEN 129
                    WHEN 'Gateshead' THEN 130
                    WHEN 'Hartlepool' THEN 131
                    WHEN 'middlesbrough' THEN 132
                    WHEN 'Newcastle upon Tyne' THEN 133
                    WHEN 'North Tyneside' THEN 134
                    WHEN 'Alnwick' THEN 135
                    WHEN 'Berwick-upon-Tweed' THEN 136
                    WHEN 'Blyth Valley' THEN 137
                    WHEN 'Castle Morpeth' THEN 138
                    WHEN 'Tynedale' THEN 139
                    WHEN 'Wansbeck' THEN 140
                    WHEN 'Redcar and Cleveland' THEN 141
                    WHEN 'South Tyneside' THEN 142
                    WHEN 'Stockton-on-Tees' THEN 143
                    WHEN 'Sunderland' THEN 144
                    WHEN 'Blackburn with Darwen' THEN 145
                    WHEN 'Blackpool' THEN 146
                    WHEN 'Bolton' THEN 147
                    WHEN 'Bury' THEN 148
                    WHEN 'Chester' THEN 149
                    WHEN 'Congleton' THEN 150
                    WHEN 'Crewe and Nantwich' THEN 151
                    WHEN 'Ellesmere Port & Neston' THEN 152
                    WHEN 'Macclesfield' THEN 153
                    WHEN 'Vale Royal' THEN 154
                    WHEN 'Allerdale' THEN 155
                    WHEN 'Barrow-in-Furness' THEN 156
                    WHEN 'Carlisle' THEN 157
                    WHEN 'Copeland' THEN 158
                    WHEN 'Eden' THEN 159
                    WHEN 'South Lakeland' THEN 160
                    WHEN 'Halton' THEN 161
                    WHEN 'Knowsley' THEN 162
                    WHEN 'Burnley' THEN 163
                    WHEN 'Chorley' THEN 164
                    WHEN 'Fylde' THEN 165
                    WHEN 'Hyndburn' THEN 166
                    WHEN 'Lancaster' THEN 167
                    WHEN 'Pendle' THEN 168
                    WHEN 'Preston' THEN 169
                    WHEN 'Ribble Valley' THEN 170
                    WHEN 'Rossendale' THEN 171
                    WHEN 'South Ribble' THEN 172
                    WHEN 'West Lancashire' THEN 173
                    WHEN 'Wyre' THEN 174
                    WHEN 'Liverpool' THEN 175
                    WHEN 'Manchester' THEN 176
                    WHEN 'Oldham' THEN 177
                    WHEN 'Rochdale' THEN 178
                    WHEN 'Salford' THEN 179
                    WHEN 'Sefton' THEN 180
                    WHEN 'St. Helens' THEN 181
                    WHEN 'Stockport' THEN 182
                    WHEN 'Tameside' THEN 183
                    WHEN 'Trafford' THEN 184
                    WHEN 'Warrington' THEN 185
                    WHEN 'Wigan' THEN 186
                    WHEN 'Wirral' THEN 187
                    WHEN 'Bracknell Forest' THEN 188
                    WHEN 'Brighton and Hove' THEN 189
                    WHEN 'Aylesbury Vale' THEN 190
                    WHEN 'Chiltern' THEN 191
                    WHEN 'South Bucks' THEN 192
                    WHEN 'Wycombe' THEN 193
                    WHEN 'Eastbourne' THEN 194
                    WHEN 'Hastings' THEN 195
                    WHEN 'Lewes' THEN 196
                    WHEN 'Rother' THEN 197
                    WHEN 'Wealden' THEN 198
                    WHEN 'Basingstoke and Deane' THEN 199
                    WHEN 'East Hampshire' THEN 200
                    WHEN 'Eastleigh' THEN 201
                    WHEN 'Fareham' THEN 202
                    WHEN 'Gosport' THEN 203
                    WHEN 'Hart' THEN 204
                    WHEN 'Havant' THEN 205
                    WHEN 'New Forest' THEN 206
                    WHEN 'Rushmoor' THEN 207
                    WHEN 'Test Valley' THEN 208
                    WHEN 'Winchester' THEN 209
                    WHEN 'Isle of Wight' THEN 210
                    WHEN 'Ashford' THEN 211
                    WHEN 'Canterbury' THEN 212
                    WHEN 'Dartford' THEN 213
                    WHEN 'Dover' THEN 214
                    WHEN 'Gravesham' THEN 215
                    WHEN 'Maidstone' THEN 216
                    WHEN 'Sevenoaks' THEN 217
                    WHEN 'Shepway' THEN 218
                    WHEN 'Swale' THEN 219
                    WHEN 'Thanet' THEN 220
                    WHEN 'Tonbridge and Malling' THEN 221
                    WHEN 'Tunbridge Wells' THEN 222
                    WHEN 'Medway' THEN 223
                    WHEN 'Milton Keynes' THEN 224
                    WHEN 'Cherwell' THEN 225
                    WHEN 'Oxford' THEN 226
                    WHEN 'South Oxfordshire' THEN 227
                    WHEN 'Vale of White Horse' THEN 228
                    WHEN 'West Oxfordshire' THEN 229
                    WHEN 'Portsmouth' THEN 230
                    WHEN 'Reading' THEN 231
                    WHEN 'Slough' THEN 232
                    WHEN 'Southampton' THEN 233
                    WHEN 'Elmbridge' THEN 234
                    WHEN 'Epsom and Ewell' THEN 235
                    WHEN 'Guildford' THEN 236
                    WHEN 'Mole Valley' THEN 237
                    WHEN 'Reigate and Banstead' THEN 238
                    WHEN 'Runnymede' THEN 239
                    WHEN 'Spelthorne' THEN 240
                    WHEN 'Surrey Heath' THEN 241
                    WHEN 'Tandridge' THEN 242
                    WHEN 'Waverley' THEN 243
                    WHEN 'Woking' THEN 244
                    WHEN 'West Berkshire' THEN 245
                    WHEN 'Adur' THEN 246
                    WHEN 'Arun' THEN 247
                    WHEN 'Chichester' THEN 248
                    WHEN 'Crawley' THEN 249
                    WHEN 'Horsham' THEN 250
                    WHEN 'Mid Sussex' THEN 251
                    WHEN 'Worthing' THEN 252
                    WHEN 'Windsor and Maidenhead' THEN 253
                    WHEN 'Wokingham' THEN 254
                    WHEN 'Bath and North East Somerset' THEN 255
                    WHEN 'Bournemouth' THEN 256
                    WHEN 'City of Bristol' THEN 257
                    WHEN 'Caradon' THEN 258
                    WHEN 'Carrick' THEN 259
                    WHEN 'Kerrier' THEN 260
                    WHEN 'North Cornwall' THEN 261
                    WHEN 'Penwith' THEN 262
                    WHEN 'Restormel' THEN 263
                    WHEN 'East Devon' THEN 264
                    WHEN 'Exeter' THEN 265
                    WHEN 'Mid Devon' THEN 266
                    WHEN 'North Devon' THEN 267
                    WHEN 'South Hams' THEN 268
                    WHEN 'Teignbridge' THEN 269
                    WHEN 'Torridge' THEN 270
                    WHEN 'West Devon' THEN 271
                    WHEN 'Christchurch' THEN 272
                    WHEN 'East Dorset' THEN 273
                    WHEN 'North Dorset' THEN 274
                    WHEN 'Purbeck' THEN 275
                    WHEN 'West Dorset' THEN 276
                    WHEN 'Weymouth and Portland' THEN 277
                    WHEN 'Cheltenham' THEN 278
                    WHEN 'Cotswold' THEN 279
                    WHEN 'Forest of Dean' THEN 280
                    WHEN 'Gloucester' THEN 281
                    WHEN 'Stroud' THEN 282
                    WHEN 'Tewkesbury' THEN 283
                    WHEN 'Isles of Scilly' THEN 284
                    WHEN 'North Somerset' THEN 285
                    WHEN 'Plymouth' THEN 286
                    WHEN 'Poole' THEN 287
                    WHEN 'Mendip' THEN 288
                    WHEN 'Sedgemoor' THEN 289
                    WHEN 'South Somerset' THEN 290
                    WHEN 'Taunton Deane' THEN 291
                    WHEN 'West Somerset' THEN 292
                    WHEN 'South Gloucestershire' THEN 293
                    WHEN 'Swindon' THEN 294
                    WHEN 'Torbay' THEN 295
                    WHEN 'Kennet' THEN 296
                    WHEN 'North Wiltshire' THEN 297
                    WHEN 'Salisbury' THEN 298
                    WHEN 'West Wiltshire' THEN 299
                    WHEN 'Birmingham' THEN 300
                    WHEN 'Coventry' THEN 301
                    WHEN 'Dudley' THEN 302
                    WHEN 'Herefordshire' THEN 303
                    WHEN 'Sandwell' THEN 304
                    WHEN 'Bridgnorth' THEN 305
                    WHEN 'North Shropshire' THEN 306
                    WHEN 'Oswestry' THEN 307
                    WHEN 'Shrewsbury and Atcham' THEN 308
                    WHEN 'South Shropshire' THEN 309
                    WHEN 'Solihull' THEN 310
                    WHEN 'Cannock Chase' THEN 311
                    WHEN 'East Staffordshire' THEN 312
                    WHEN 'Lichfield' THEN 313
                    WHEN 'Newcastle-under-Lyme' THEN 314
                    WHEN 'South Staffordshire' THEN 315
                    WHEN 'Stafford' THEN 316
                    WHEN 'Staffordshire Moorlands' THEN 317
                    WHEN 'Tamworth' THEN 318
                    WHEN 'Stoke-on-Trent' THEN 319
                    WHEN 'Telford and Wrekin' THEN 320
                    WHEN 'Walsall' THEN 321
                    WHEN 'North Warwickshire' THEN 322
                    WHEN 'Nuneaton and Bedworth' THEN 323
                    WHEN 'Rugby' THEN 324
                    WHEN 'Stratford-on-Avon' THEN 325
                    WHEN 'Warwick' THEN 326
                    WHEN 'Wolverhampton' THEN 327
                    WHEN 'Bromsgrove' THEN 328
                    WHEN 'Malvern Hills' THEN 329
                    WHEN 'Redditch' THEN 330
                    WHEN 'Worcester' THEN 331
                    WHEN 'Wychavon' THEN 332
                    WHEN 'Wyre Forest' THEN 333
                    WHEN 'Barnsley' THEN 334
                    WHEN 'Bradford' THEN 335
                    WHEN 'Calderdale' THEN 336
                    WHEN 'Doncaster' THEN 337
                    WHEN 'East Riding of Yorkshire' THEN 338
                    WHEN 'Kingston upon Hull' THEN 339
                    WHEN 'Kirklees' THEN 340
                    WHEN 'Leeds' THEN 341
                    WHEN 'North East Lincolnshire' THEN 342
                    WHEN 'North Lincolnshire' THEN 343
                    WHEN 'Craven' THEN 344
                    WHEN 'Hambleton' THEN 345
                    WHEN 'Harrogate' THEN 346
                    WHEN 'Richmondshire' THEN 347
                    WHEN 'Ryedale' THEN 348
                    WHEN 'Scarborough' THEN 349
                    WHEN 'Selby' THEN 350
                    WHEN 'Rotherham' THEN 351
                    WHEN 'Sheffield' THEN 352
                    WHEN 'Wakefield' THEN 353
                    WHEN 'York' THEN 354
                    WHEN 'Bedford' THEN 400
                    WHEN 'Central Bedfordshire' THEN 401
                    WHEN 'Cheshire East' THEN 402
                    WHEN 'Cheshire West and Chester' THEN 403
                    WHEN 'Cornwall' THEN 404
                    WHEN 'Isles of Scilly' THEN 405
                    WHEN 'County Durham' THEN 406
                    WHEN 'Northumberland' THEN 407
                    WHEN 'Shropshire' THEN 408
                    WHEN 'Wiltshire' THEN 409
                 END
          FROM   "Cssr"
          WHERE
            EXISTS (SELECT
                1 
            FROM
                cqcref.pcodedata 
            WHERE
                (cqcref.pcodedata.postcode = w."PostcodeValue") 
                AND ("LocalCustodianCode" = cqcref.pcodedata.local_custodian_code)) LIMIT 1), -1) homelauthid,
       'na' homeparliamentaryconstituency,
       (select (point(e."Longitude",e."Latitude") <@> point(w."Longitude",w."Latitude")) as "distwrkk") distwrkk,
       CASE
          WHEN "RecruitedFromValue" IS NULL THEN -1
          WHEN "RecruitedFromValue" = 'No' THEN 225
          WHEN 'Yes' THEN
             (
                SELECT CASE "From"
                          WHEN 'Adult care sector: Local Authority' THEN 210
                          WHEN 'Adult care sector: private or voluntary sector' THEN 211
                          WHEN 'Health sector' THEN 214
                          WHEN 'Other sector' THEN 216
                          WHEN 'Internal promotion or transfer or career development' THEN 217
                          WHEN 'Not previously employed' THEN 219
                          WHEN 'Agency' THEN 221
                          WHEN 'Other sources' THEN 224
                          WHEN 'Childrens/young people''s social care' THEN 226
                          WHEN 'First role after education' THEN 227
                       END
                FROM   "RecruitedFrom"
                WHERE  "ID" = w."RecruitedFromOtherFK"
             )
       END scerec,
       TO_CHAR("RecruitedFromChangedAt",'DD/MM/YYYY') scerec_changedate,
       TO_CHAR("RecruitedFromSavedAt",'DD/MM/YYYY') scerec_savedate,
       CASE "SocialCareStartDateValue" WHEN 'Yes' THEN "SocialCareStartDateYear" WHEN 'No' THEN -2 ELSE  -1 END startsec,
       TO_CHAR("SocialCareStartDateChangedAt",'DD/MM/YYYY') startsec_changedate,
       TO_CHAR("SocialCareStartDateSavedAt",'DD/MM/YYYY') startsec_savedate,
       CASE "DaysSickValue" WHEN 'Yes' THEN "DaysSickDays" WHEN 'No' THEN -2 ELSE  -1 END dayssick,
       TO_CHAR("DaysSickChangedAt",'DD/MM/YYYY') dayssick_changedate,
       TO_CHAR("DaysSickSavedAt",'DD/MM/YYYY') dayssick_savedate,
       CASE "ZeroHoursContractValue" WHEN 'Yes' THEN 1 WHEN 'No' THEN 0 WHEN 'Don''t know' THEN -2 ELSE -1 END zerohours,
       TO_CHAR("ZeroHoursContractChangedAt",'DD/MM/YYYY') zerohours_changedate,
       TO_CHAR("ZeroHoursContractSavedAt",'DD/MM/YYYY') zerohours_savedate,
       CASE "WeeklyHoursAverageValue" WHEN 'Yes' THEN "WeeklyHoursAverageHours" WHEN 'No' THEN -2 ELSE -1 END averagehours,
       TO_CHAR("WeeklyHoursAverageChangedAt",'DD/MM/YYYY') zero_averagehours_changedate,
       TO_CHAR("WeeklyHoursAverageSavedAt",'DD/MM/YYYY') zero_averagehours_savedate,
       CASE "WeeklyHoursContractedValue" WHEN 'Yes' THEN "WeeklyHoursContractedHours" WHEN 'No' THEN -2 ELSE -1 END conthrs,
       TO_CHAR("WeeklyHoursContractedChangedAt",'DD/MM/YYYY') conthrs_changedate,
       TO_CHAR("WeeklyHoursContractedSavedAt",'DD/MM/YYYY') conthrs_savedate,
       CASE "AnnualHourlyPayValue" WHEN 'Annually' THEN 250 WHEN 'Hourly' THEN 252 WHEN 'Don''t know' THEN -2 ELSE -1 END salaryint,
       CASE "AnnualHourlyPayValue" WHEN 'Annually' THEN "AnnualHourlyPayRate" ELSE NULL END salary,
       CASE "AnnualHourlyPayValue" WHEN 'Hourly' THEN "AnnualHourlyPayRate" ELSE NULL END hrlyrate,
       TO_CHAR("AnnualHourlyPayChangedAt",'DD/MM/YYYY') pay_changedate,
       TO_CHAR("AnnualHourlyPaySavedAt",'DD/MM/YYYY') pay_savedate,
       CASE "CareCertificateValue" WHEN 'Yes, completed' THEN 1 WHEN 'No' THEN 2 WHEN 'Yes, in progress or partially completed' THEN 3 ELSE -1 END ccstatus,
       TO_CHAR("CareCertificateChangedAt",'DD/MM/YYYY') ccstatus_changedate,
       TO_CHAR("CareCertificateSavedAt",'DD/MM/YYYY') ccstatus_savedate,
       CASE "ApprenticeshipTrainingValue" WHEN 'Yes' THEN 1 WHEN 'No' THEN 2 WHEN 'Don''t know' THEN 3 ELSE -1 END apprentice,
       TO_CHAR("ApprenticeshipTrainingChangedAt",'DD/MM/YYYY') apprentice_changedate,
       TO_CHAR("ApprenticeshipTrainingSavedAt",'DD/MM/YYYY') apprentice_savedate,
       CASE "QualificationInSocialCareValue" WHEN 'Yes' THEN 1 WHEN 'No' THEN 2 WHEN 'Don''t know' THEN 3 ELSE -1 END scqheld,
       TO_CHAR("QualificationInSocialCareChangedAt",'DD/MM/YYYY') scqheld_changedate,
       TO_CHAR("QualificationInSocialCareSavedAt",'DD/MM/YYYY') scqheld_savedate,
       COALESCE((
          SELECT CASE "Level"
                    WHEN 'Entry level' THEN 0
                    WHEN 'Level 1' THEN 1
                    WHEN 'Level 2' THEN 2
                    WHEN 'Level 3' THEN 3
                    WHEN 'Level 4' THEN 4
                    WHEN 'Level 5' THEN 5
                    WHEN 'Level 6' THEN 6
                    WHEN 'Level 7' THEN 7
                    WHEN 'Level 8 or above' THEN 8
                    WHEN 'Don''t know' THEN 10
                 END
          FROM   "Qualification"
          WHERE  "ID" = w."SocialCareQualificationFKValue"
       ),-1) levelscqheld,
       TO_CHAR("SocialCareQualificationFKChangedAt",'DD/MM/YYYY') levelscqheld_changedate,
       TO_CHAR("SocialCareQualificationFKSavedAt",'DD/MM/YYYY') levelscqheld_savedate,
       CASE "OtherQualificationsValue" WHEN 'Yes' THEN 1 WHEN 'No' THEN 2 WHEN 'Don''t know' THEN 3 ELSE -1 END nonscqheld,
       TO_CHAR("OtherQualificationsChangedAt",'DD/MM/YYYY') nonscqheld_changedate,
       TO_CHAR("OtherQualificationsSavedAt",'DD/MM/YYYY') nonscqheld_savedate,
       COALESCE((
          SELECT CASE "Level"
                    WHEN 'Entry level' THEN 0
                    WHEN 'Level 1' THEN 1
                    WHEN 'Level 2' THEN 2
                    WHEN 'Level 3' THEN 3
                    WHEN 'Level 4' THEN 4
                    WHEN 'Level 5' THEN 5
                    WHEN 'Level 6' THEN 6
                    WHEN 'Level 7' THEN 7
                    WHEN 'Level 8 or above' THEN 8
                    WHEN 'Don''t know' THEN 10
                 END
          FROM   "Qualification"
          WHERE  "ID" = w."HighestQualificationFKValue"
       ),-1) levelnonscqheld,
       TO_CHAR("HighestQualificationFKChangedAt",'DD/MM/YYYY') levelnonscqheld_changedate,
       TO_CHAR("HighestQualificationFKSavedAt",'DD/MM/YYYY') levelnonscqheld_savedate,
       COALESCE((SELECT 1 FROM "WorkerQualifications" WHERE "WorkerFK" = w."ID" LIMIT 1),0) listqualsachflag,
       (SELECT TO_CHAR(MAX(updated),'DD/MM/YYYY') FROM "WorkerQualifications" WHERE "WorkerFK" = w."ID") listqualsachflag_changedate,
       (SELECT TO_CHAR(MAX(created),'DD/MM/YYYY') FROM "WorkerQualifications" WHERE "WorkerFK" = w."ID") listqualsachflag_savedate,
       COALESCE((
          SELECT CAST((CASE WHEN b."Level" = 'E' THEN '1' WHEN b."Level" IS NULL THEN '-1' ELSE b."Level" END) AS INTEGER)
          FROM   "WorkerQualifications" a JOIN "Qualifications" b ON a."QualificationsFK" = b."ID" AND a."WorkerFK" = w."ID"
          ORDER  BY 1 DESC LIMIT 1
       ),-1) listhiqualev,
       (
          SELECT TO_CHAR(updated,'DD/MM/YYYY')
          FROM   (
                    SELECT CAST((CASE WHEN b."Level" = 'E' THEN '1' WHEN b."Level" IS NULL THEN '-1' ELSE b."Level" END) AS INTEGER),a.updated
                    FROM   "WorkerQualifications" a JOIN "Qualifications" b ON a."QualificationsFK" = b."ID" AND a."WorkerFK" = w."ID"
                    ORDER  BY 1 DESC,2 DESC LIMIT 1
                 ) z
       ) listhiqualev_changedate,
       (
          SELECT TO_CHAR(created,'DD/MM/YYYY')
          FROM   (
                    SELECT CAST((CASE WHEN b."Level" = 'E' THEN '1' WHEN b."Level" IS NULL THEN '-1' ELSE b."Level" END) AS INTEGER),a.created
                    FROM   "WorkerQualifications" a JOIN "Qualifications" b ON a."QualificationsFK" = b."ID" AND a."WorkerFK" = w."ID"
                    ORDER  BY 1 DESC,2 DESC LIMIT 1
                 ) z
       ) listhiqualev_savedate,
       CASE "MainJobFKValue"
          WHEN 26 THEN 1
          ELSE CASE "OtherJobsValue" WHEN 'Yes' THEN CASE WHEN EXISTS (SELECT 1 FROM "WorkerJobs" WHERE "WorkerFK" = w."ID" AND "JobFK" = 26 LIMIT 1) THEN 1 ELSE 0 END ELSE 0 END
       END jr01flag,
       CASE "MainJobFKValue"
          WHEN 15 THEN 1
          ELSE CASE "OtherJobsValue" WHEN 'Yes' THEN CASE WHEN EXISTS (SELECT 1 FROM "WorkerJobs" WHERE "WorkerFK" = w."ID" AND "JobFK" = 15 LIMIT 1) THEN 1 ELSE 0 END ELSE 0 END
       END jr02flag,
       CASE "MainJobFKValue"
          WHEN 13 THEN 1
          ELSE CASE "OtherJobsValue" WHEN 'Yes' THEN CASE WHEN EXISTS (SELECT 1 FROM "WorkerJobs" WHERE "WorkerFK" = w."ID" AND "JobFK" = 13 LIMIT 1) THEN 1 ELSE 0 END ELSE 0 END
       END jr03flag,
       CASE "MainJobFKValue"
          WHEN 22 THEN 1
          ELSE CASE "OtherJobsValue" WHEN 'Yes' THEN CASE WHEN EXISTS (SELECT 1 FROM "WorkerJobs" WHERE "WorkerFK" = w."ID" AND "JobFK" = 22 LIMIT 1) THEN 1 ELSE 0 END ELSE 0 END
       END jr04flag,
       CASE "MainJobFKValue"
          WHEN 28 THEN 1
          ELSE CASE "OtherJobsValue" WHEN 'Yes' THEN CASE WHEN EXISTS (SELECT 1 FROM "WorkerJobs" WHERE "WorkerFK" = w."ID" AND "JobFK" = 28 LIMIT 1) THEN 1 ELSE 0 END ELSE 0 END
       END jr05flag,
       CASE "MainJobFKValue"
          WHEN 27 THEN 1
          ELSE CASE "OtherJobsValue" WHEN 'Yes' THEN CASE WHEN EXISTS (SELECT 1 FROM "WorkerJobs" WHERE "WorkerFK" = w."ID" AND "JobFK" = 27 LIMIT 1) THEN 1 ELSE 0 END ELSE 0 END
       END jr06flag,
       CASE "MainJobFKValue"
          WHEN 25 THEN 1
          ELSE CASE "OtherJobsValue" WHEN 'Yes' THEN CASE WHEN EXISTS (SELECT 1 FROM "WorkerJobs" WHERE "WorkerFK" = w."ID" AND "JobFK" = 25 LIMIT 1) THEN 1 ELSE 0 END ELSE 0 END
       END jr07flag,
       CASE "MainJobFKValue"
          WHEN 10 THEN 1
          ELSE CASE "OtherJobsValue" WHEN 'Yes' THEN CASE WHEN EXISTS (SELECT 1 FROM "WorkerJobs" WHERE "WorkerFK" = w."ID" AND "JobFK" = 10 LIMIT 1) THEN 1 ELSE 0 END ELSE 0 END
       END jr08flag,
       CASE "MainJobFKValue"
          WHEN 11 THEN 1
          ELSE CASE "OtherJobsValue" WHEN 'Yes' THEN CASE WHEN EXISTS (SELECT 1 FROM "WorkerJobs" WHERE "WorkerFK" = w."ID" AND "JobFK" = 11 LIMIT 1) THEN 1 ELSE 0 END ELSE 0 END
       END jr09flag,
       CASE "MainJobFKValue"
          WHEN 12 THEN 1
          ELSE CASE "OtherJobsValue" WHEN 'Yes' THEN CASE WHEN EXISTS (SELECT 1 FROM "WorkerJobs" WHERE "WorkerFK" = w."ID" AND "JobFK" = 12 LIMIT 1) THEN 1 ELSE 0 END ELSE 0 END
       END jr10flag,
       CASE "MainJobFKValue"
          WHEN 3 THEN 1
          ELSE CASE "OtherJobsValue" WHEN 'Yes' THEN CASE WHEN EXISTS (SELECT 1 FROM "WorkerJobs" WHERE "WorkerFK" = w."ID" AND "JobFK" = 3 LIMIT 1) THEN 1 ELSE 0 END ELSE 0 END
       END jr11flag,
       CASE "MainJobFKValue"
          WHEN 18 THEN 1
          ELSE CASE "OtherJobsValue" WHEN 'Yes' THEN CASE WHEN EXISTS (SELECT 1 FROM "WorkerJobs" WHERE "WorkerFK" = w."ID" AND "JobFK" = 18 LIMIT 1) THEN 1 ELSE 0 END ELSE 0 END
       END jr15flag,
       CASE "MainJobFKValue"
          WHEN 23 THEN 1
          ELSE CASE "OtherJobsValue" WHEN 'Yes' THEN CASE WHEN EXISTS (SELECT 1 FROM "WorkerJobs" WHERE "WorkerFK" = w."ID" AND "JobFK" = 23 LIMIT 1) THEN 1 ELSE 0 END ELSE 0 END
       END jr16flag,
       CASE "MainJobFKValue"
          WHEN 4 THEN 1
          ELSE CASE "OtherJobsValue" WHEN 'Yes' THEN CASE WHEN EXISTS (SELECT 1 FROM "WorkerJobs" WHERE "WorkerFK" = w."ID" AND "JobFK" = 4 LIMIT 1) THEN 1 ELSE 0 END ELSE 0 END
       END jr17flag,
       CASE "MainJobFKValue"
          WHEN 29 THEN 1
          ELSE CASE "OtherJobsValue" WHEN 'Yes' THEN CASE WHEN EXISTS (SELECT 1 FROM "WorkerJobs" WHERE "WorkerFK" = w."ID" AND "JobFK" = 29 LIMIT 1) THEN 1 ELSE 0 END ELSE 0 END
       END jr22flag,
       CASE "MainJobFKValue"
          WHEN 20 THEN 1
          ELSE CASE "OtherJobsValue" WHEN 'Yes' THEN CASE WHEN EXISTS (SELECT 1 FROM "WorkerJobs" WHERE "WorkerFK" = w."ID" AND "JobFK" = 20 LIMIT 1) THEN 1 ELSE 0 END ELSE 0 END
       END jr23flag,
       CASE "MainJobFKValue"
          WHEN 14 THEN 1
          ELSE CASE "OtherJobsValue" WHEN 'Yes' THEN CASE WHEN EXISTS (SELECT 1 FROM "WorkerJobs" WHERE "WorkerFK" = w."ID" AND "JobFK" = 14 LIMIT 1) THEN 1 ELSE 0 END ELSE 0 END
       END jr24flag,
       CASE "MainJobFKValue"
          WHEN 2 THEN 1
          ELSE CASE "OtherJobsValue" WHEN 'Yes' THEN CASE WHEN EXISTS (SELECT 1 FROM "WorkerJobs" WHERE "WorkerFK" = w."ID" AND "JobFK" = 2 LIMIT 1) THEN 1 ELSE 0 END ELSE 0 END
       END jr25flag,
       CASE "MainJobFKValue"
          WHEN 5 THEN 1
          ELSE CASE "OtherJobsValue" WHEN 'Yes' THEN CASE WHEN EXISTS (SELECT 1 FROM "WorkerJobs" WHERE "WorkerFK" = w."ID" AND "JobFK" = 5 LIMIT 1) THEN 1 ELSE 0 END ELSE 0 END
       END jr26flag,
       CASE "MainJobFKValue"
          WHEN 21 THEN 1
          ELSE CASE "OtherJobsValue" WHEN 'Yes' THEN CASE WHEN EXISTS (SELECT 1 FROM "WorkerJobs" WHERE "WorkerFK" = w."ID" AND "JobFK" = 21 LIMIT 1) THEN 1 ELSE 0 END ELSE 0 END
       END jr27flag,
       -- Removed jr33flag as confirmed by Will Fenton on 24/10/2019
       CASE "MainJobFKValue"
          WHEN 1 THEN 1
          ELSE CASE "OtherJobsValue" WHEN 'Yes' THEN CASE WHEN EXISTS (SELECT 1 FROM "WorkerJobs" WHERE "WorkerFK" = w."ID" AND "JobFK" = 1 LIMIT 1) THEN 1 ELSE 0 END ELSE 0 END
       END jr34flag,
       CASE "MainJobFKValue"
          WHEN 24 THEN 1
          ELSE CASE "OtherJobsValue" WHEN 'Yes' THEN CASE WHEN EXISTS (SELECT 1 FROM "WorkerJobs" WHERE "WorkerFK" = w."ID" AND "JobFK" = 24 LIMIT 1) THEN 1 ELSE 0 END ELSE 0 END
       END jr35flag,
       CASE "MainJobFKValue"
          WHEN 19 THEN 1
          ELSE CASE "OtherJobsValue" WHEN 'Yes' THEN CASE WHEN EXISTS (SELECT 1 FROM "WorkerJobs" WHERE "WorkerFK" = w."ID" AND "JobFK" = 19 LIMIT 1) THEN 1 ELSE 0 END ELSE 0 END
       END jr36flag,
       CASE "MainJobFKValue"
          WHEN 17 THEN 1
          ELSE CASE "OtherJobsValue" WHEN 'Yes' THEN CASE WHEN EXISTS (SELECT 1 FROM "WorkerJobs" WHERE "WorkerFK" = w."ID" AND "JobFK" = 17 LIMIT 1) THEN 1 ELSE 0 END ELSE 0 END
       END jr37flag,
       CASE "MainJobFKValue"
          WHEN 16 THEN 1
          ELSE CASE "OtherJobsValue" WHEN 'Yes' THEN CASE WHEN EXISTS (SELECT 1 FROM "WorkerJobs" WHERE "WorkerFK" = w."ID" AND "JobFK" = 16 LIMIT 1) THEN 1 ELSE 0 END ELSE 0 END
       END jr38flag,
       CASE "MainJobFKValue"
          WHEN 7 THEN 1
          ELSE CASE "OtherJobsValue" WHEN 'Yes' THEN CASE WHEN EXISTS (SELECT 1 FROM "WorkerJobs" WHERE "WorkerFK" = w."ID" AND "JobFK" = 7 LIMIT 1) THEN 1 ELSE 0 END ELSE 0 END
       END jr39flag,
       CASE "MainJobFKValue"
          WHEN 8 THEN 1
          ELSE CASE "OtherJobsValue" WHEN 'Yes' THEN CASE WHEN EXISTS (SELECT 1 FROM "WorkerJobs" WHERE "WorkerFK" = w."ID" AND "JobFK" = 8 LIMIT 1) THEN 1 ELSE 0 END ELSE 0 END
       END jr40flag,
       CASE "MainJobFKValue"
          WHEN 9 THEN 1
          ELSE CASE "OtherJobsValue" WHEN 'Yes' THEN CASE WHEN EXISTS (SELECT 1 FROM "WorkerJobs" WHERE "WorkerFK" = w."ID" AND "JobFK" = 9 LIMIT 1) THEN 1 ELSE 0 END ELSE 0 END
       END jr41flag,
       CASE "MainJobFKValue"
          WHEN 6 THEN 1
          ELSE CASE "OtherJobsValue" WHEN 'Yes' THEN CASE WHEN EXISTS (SELECT 1 FROM "WorkerJobs" WHERE "WorkerFK" = w."ID" AND "JobFK" = 6 LIMIT 1) THEN 1 ELSE 0 END ELSE 0 END
       END jr42flag,
       CASE "RegisteredNurseValue"
          WHEN 'Adult Nurse' THEN 1
          WHEN 'Mental Health Nurse' THEN 2
          WHEN 'Learning Disabilities Nurse' THEN 3
          WHEN 'Children''s Nurse' THEN 4
          WHEN 'Enrolled Nurse' THEN 5
          ELSE -1
       END jd16registered,
       TO_CHAR("RegisteredNurseChangedAt",'DD/MM/YYYY') jd16registered_changedate,
       TO_CHAR("RegisteredNurseSavedAt",'DD/MM/YYYY') jd16registered_savedate,
       CASE "NurseSpecialismFKValue" WHEN 1 THEN 1 ELSE 0 END jr16cat1,
       CASE "NurseSpecialismFKValue" WHEN 2 THEN 1 ELSE 0 END jr16cat2,
       CASE "NurseSpecialismFKValue" WHEN 3 THEN 1 ELSE 0 END jr16cat3,
       CASE "NurseSpecialismFKValue" WHEN 4 THEN 1 ELSE 0 END jr16cat4,
       CASE "NurseSpecialismFKValue" WHEN 5 THEN 1 ELSE 0 END jr16cat5,
       CASE "NurseSpecialismFKValue" WHEN 6 THEN 1 ELSE 0 END jr16cat6,
       CASE "NurseSpecialismFKValue" WHEN 7 THEN 1 ELSE 0 END jr16cat7,
       CASE "NurseSpecialismFKValue" WHEN 8 THEN 1 ELSE 0 END jr16cat8,
       TO_CHAR("NurseSpecialismFKChangedAt",'DD/MM/YYYY') jr16cat_changedate,
       TO_CHAR("NurseSpecialismFKSavedAt",'DD/MM/YYYY') jr16cat_savedate,
       CASE "ApprovedMentalHealthWorkerValue" WHEN 'Yes' THEN 1 WHEN 'No' THEN 0 WHEN 'Don''t know' THEN -2 ELSE -1 END amhp,
       TO_CHAR("ApprovedMentalHealthWorkerChangedAt",'DD/MM/YYYY') amhp_changedate,
       TO_CHAR("ApprovedMentalHealthWorkerSavedAt",'DD/MM/YYYY') amhp_savedate,
       CASE WHEN EXISTS (SELECT 1 FROM "EstablishmentServiceUsers" WHERE "EstablishmentID" = e."EstablishmentID" AND "ServiceUserID" = 1 LIMIT 1) THEN 1 ELSE 0 END ut01flag,
       CASE WHEN EXISTS (SELECT 1 FROM "EstablishmentServiceUsers" WHERE "EstablishmentID" = e."EstablishmentID" AND "ServiceUserID" = 2 LIMIT 1) THEN 1 ELSE 0 END ut02flag,
       CASE WHEN EXISTS (SELECT 1 FROM "EstablishmentServiceUsers" WHERE "EstablishmentID" = e."EstablishmentID" AND "ServiceUserID" = 3 LIMIT 1) THEN 1 ELSE 0 END ut22flag,
       CASE WHEN EXISTS (SELECT 1 FROM "EstablishmentServiceUsers" WHERE "EstablishmentID" = e."EstablishmentID" AND "ServiceUserID" = 4 LIMIT 1) THEN 1 ELSE 0 END ut23flag,
       CASE WHEN EXISTS (SELECT 1 FROM "EstablishmentServiceUsers" WHERE "EstablishmentID" = e."EstablishmentID" AND "ServiceUserID" = 5 LIMIT 1) THEN 1 ELSE 0 END ut25flag,
       CASE WHEN EXISTS (SELECT 1 FROM "EstablishmentServiceUsers" WHERE "EstablishmentID" = e."EstablishmentID" AND "ServiceUserID" = 6 LIMIT 1) THEN 1 ELSE 0 END ut26flag,
       CASE WHEN EXISTS (SELECT 1 FROM "EstablishmentServiceUsers" WHERE "EstablishmentID" = e."EstablishmentID" AND "ServiceUserID" = 7 LIMIT 1) THEN 1 ELSE 0 END ut27flag,
       CASE WHEN EXISTS (SELECT 1 FROM "EstablishmentServiceUsers" WHERE "EstablishmentID" = e."EstablishmentID" AND "ServiceUserID" = 8 LIMIT 1) THEN 1 ELSE 0 END ut46flag,
       CASE WHEN EXISTS (SELECT 1 FROM "EstablishmentServiceUsers" WHERE "EstablishmentID" = e."EstablishmentID" AND "ServiceUserID" = 9 LIMIT 1) THEN 1 ELSE 0 END ut03flag,
       CASE WHEN EXISTS (SELECT 1 FROM "EstablishmentServiceUsers" WHERE "EstablishmentID" = e."EstablishmentID" AND "ServiceUserID" = 14 LIMIT 1) THEN 1 ELSE 0 END ut04flag,
       CASE WHEN EXISTS (SELECT 1 FROM "EstablishmentServiceUsers" WHERE "EstablishmentID" = e."EstablishmentID" AND "ServiceUserID" = 13 LIMIT 1) THEN 1 ELSE 0 END ut05flag,
       CASE WHEN EXISTS (SELECT 1 FROM "EstablishmentServiceUsers" WHERE "EstablishmentID" = e."EstablishmentID" AND "ServiceUserID" = 11 LIMIT 1) THEN 1 ELSE 0 END ut06flag,
       CASE WHEN EXISTS (SELECT 1 FROM "EstablishmentServiceUsers" WHERE "EstablishmentID" = e."EstablishmentID" AND "ServiceUserID" = 15 LIMIT 1) THEN 1 ELSE 0 END ut07flag,
       CASE WHEN EXISTS (SELECT 1 FROM "EstablishmentServiceUsers" WHERE "EstablishmentID" = e."EstablishmentID" AND "ServiceUserID" = 16 LIMIT 1) THEN 1 ELSE 0 END ut08flag,
       CASE WHEN EXISTS (SELECT 1 FROM "EstablishmentServiceUsers" WHERE "EstablishmentID" = e."EstablishmentID" AND "ServiceUserID" = 10 LIMIT 1) THEN 1 ELSE 0 END ut28flag,
       CASE WHEN EXISTS (SELECT 1 FROM "EstablishmentServiceUsers" WHERE "EstablishmentID" = e."EstablishmentID" AND "ServiceUserID" = 12 LIMIT 1) THEN 1 ELSE 0 END ut29flag,
       CASE WHEN EXISTS (SELECT 1 FROM "EstablishmentServiceUsers" WHERE "EstablishmentID" = e."EstablishmentID" AND "ServiceUserID" = 17 LIMIT 1) THEN 1 ELSE 0 END ut31flag,
       CASE WHEN EXISTS (SELECT 1 FROM "EstablishmentServiceUsers" WHERE "EstablishmentID" = e."EstablishmentID" AND "ServiceUserID" = 18 LIMIT 1) THEN 1 ELSE 0 END ut09flag,
       CASE WHEN EXISTS (SELECT 1 FROM "EstablishmentServiceUsers" WHERE "EstablishmentID" = e."EstablishmentID" AND "ServiceUserID" = 20 LIMIT 1) THEN 1 ELSE 0 END ut18flag,
       CASE WHEN EXISTS (SELECT 1 FROM "EstablishmentServiceUsers" WHERE "EstablishmentID" = e."EstablishmentID" AND "ServiceUserID" = 21 LIMIT 1) THEN 1 ELSE 0 END ut19flag,
       CASE WHEN EXISTS (SELECT 1 FROM "EstablishmentServiceUsers" WHERE "EstablishmentID" = e."EstablishmentID" AND "ServiceUserID" = 22 LIMIT 1) THEN 1 ELSE 0 END ut20flag,
       CASE WHEN EXISTS (SELECT 1 FROM "EstablishmentServiceUsers" WHERE "EstablishmentID" = e."EstablishmentID" AND "ServiceUserID" = 19 LIMIT 1) THEN 1 ELSE 0 END ut45flag,
       CASE WHEN EXISTS (SELECT 1 FROM "EstablishmentServiceUsers" WHERE "EstablishmentID" = e."EstablishmentID" AND "ServiceUserID" = 23 LIMIT 1) THEN 1 ELSE 0 END ut21flag,
       TO_CHAR((SELECT MAX(updated) FROM "WorkerQualifications" WHERE "WorkerFK" = w."ID"),'DD/MM/YYYY') qlach_changedate,
       TO_CHAR((SELECT MAX(created) FROM "WorkerQualifications" WHERE "WorkerFK" = w."ID"),'DD/MM/YYYY') qlach_savedate,
       CASE WHEN EXISTS (SELECT 1 FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 97 LIMIT 1) THEN 1 ELSE 0 END ql01achq2,
       (SELECT "Year" FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 97 LIMIT 1) ql01year2,
       CASE WHEN EXISTS (SELECT 1 FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 98 LIMIT 1) THEN 1 ELSE 0 END ql02achq3,
       (SELECT "Year" FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 98 LIMIT 1) ql02year3,
       CASE WHEN EXISTS (SELECT 1 FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 96 LIMIT 1) THEN 1 ELSE 0 END ql03achq4,
       (SELECT "Year" FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 96 LIMIT 1) ql03year4,
       CASE WHEN EXISTS (SELECT 1 FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 93 LIMIT 1) THEN 1 ELSE 0 END ql04achq2,
       (SELECT "Year" FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 93 LIMIT 1) ql04year2,
       CASE WHEN EXISTS (SELECT 1 FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 94 LIMIT 1) THEN 1 ELSE 0 END ql05achq3,
       (SELECT "Year" FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 94 LIMIT 1) ql05year3,
       CASE WHEN EXISTS (SELECT 1 FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 95 LIMIT 1) THEN 1 ELSE 0 END ql06achq4,
       (SELECT "Year" FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 95 LIMIT 1) ql06year4,
       COALESCE((SELECT SUM(total_quals) FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 24), 0) ql08achq,
       (SELECT MAX("Year") FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 24) ql08year,
       COALESCE((SELECT SUM(total_quals) FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 99), 0) ql09achq,
       (SELECT MAX("Year") FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 99) ql09year,
       CASE WHEN EXISTS (SELECT 1 FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 100 LIMIT 1) THEN 1 ELSE 0 END ql10achq4,
       (SELECT "Year" FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 100 LIMIT 1) ql10year4,
       COALESCE((SELECT SUM(total_quals) FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 25), 0) ql12achq3,
       (SELECT MAX("Year") FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 25) ql12year3,
       CASE WHEN EXISTS (SELECT 1 FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 102 LIMIT 1) THEN 1 ELSE 0 END ql13achq3,
       (SELECT "Year" FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 102 LIMIT 1) ql13year3,
       CASE WHEN EXISTS (SELECT 1 FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 107 LIMIT 1) THEN 1 ELSE 0 END ql14achq3,
       (SELECT "Year" FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 107 LIMIT 1) ql14year3,
       CASE WHEN EXISTS (SELECT 1 FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 106 LIMIT 1) THEN 1 ELSE 0 END ql15achq3,
       (SELECT "Year" FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 106 LIMIT 1) ql15year3,
       CASE WHEN EXISTS (SELECT 1 FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 72 LIMIT 1) THEN 1 ELSE 0 END ql16achq4,
       (SELECT "Year" FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 72 LIMIT 1) ql16year4,
       COALESCE((SELECT SUM(total_quals) FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 89), 0) ql17achq4,
       (SELECT MAX("Year") FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 89) ql17year4,
       CASE WHEN EXISTS (SELECT 1 FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 71 LIMIT 1) THEN 1 ELSE 0 END ql18achq4,
       (SELECT "Year" FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 71 LIMIT 1) ql18year4,
       CASE WHEN EXISTS (SELECT 1 FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 16 LIMIT 1) THEN 1 ELSE 0 END ql19achq4,
       (SELECT "Year" FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 16 LIMIT 1) ql19year4,
       CASE WHEN EXISTS (SELECT 1 FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 1 LIMIT 1) THEN 1 ELSE 0 END ql20achq4,
       (SELECT "Year" FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 1 LIMIT 1) ql20year4,
       CASE WHEN EXISTS (SELECT 1 FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 14 LIMIT 1) THEN 1 ELSE 0 END ql22achq4,
       (SELECT "Year" FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 14 LIMIT 1) ql22year4,
       CASE WHEN EXISTS (SELECT 1 FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 15 LIMIT 1) THEN 1 ELSE 0 END ql25achq4,
       (SELECT "Year" FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 15 LIMIT 1) ql25year4,
       COALESCE((SELECT SUM(total_quals) FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 26), 0) ql26achq4,
       (SELECT MAX("Year") FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 26) ql26year4,
       COALESCE((SELECT SUM(total_quals) FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 114), 0) ql27achq4,
       (SELECT MAX("Year") FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 114) ql27year4,
       COALESCE((SELECT SUM(total_quals) FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 116), 0) ql28achq4,
       (SELECT MAX("Year") FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 116) ql28year4,
       COALESCE((SELECT SUM(total_quals) FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 115), 0) ql32achq3,
       (SELECT MAX("Year") FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 115) ql32year3,
       COALESCE((SELECT SUM(total_quals) FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 113), 0) ql33achq4,
       (SELECT MAX("Year") FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 113) ql33year4,
       CASE WHEN EXISTS (SELECT 1 FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 111 LIMIT 1) THEN 1 ELSE 0 END ql34achqe,
       (SELECT "Year" FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 111 LIMIT 1) ql34yeare,
       CASE WHEN EXISTS (SELECT 1 FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 109 LIMIT 1) THEN 1 ELSE 0 END ql35achq1,
       (SELECT "Year" FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 109 LIMIT 1) ql35year1,
       CASE WHEN EXISTS (SELECT 1 FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 110 LIMIT 1) THEN 1 ELSE 0 END ql36achq2,
       (SELECT "Year" FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 110 LIMIT 1) ql36year2,
       COALESCE((SELECT SUM(total_quals) FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 117), 0) ql37achq,
       (SELECT MAX("Year") FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 117) ql37year,
       COALESCE((SELECT SUM(total_quals) FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 118), 0) ql38achq,
       (SELECT MAX("Year") FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 118) ql38year,
       COALESCE((SELECT SUM(total_quals) FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 119), 0) ql39achq,
       (SELECT MAX("Year") FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 119) ql39year,
       CASE WHEN EXISTS (SELECT 1 FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 20 LIMIT 1) THEN 1 ELSE 0 END ql41achq2,
       (SELECT "Year" FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 20 LIMIT 1) ql41year2,
       CASE WHEN EXISTS (SELECT 1 FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 30 LIMIT 1) THEN 1 ELSE 0 END ql42achq3,
       (SELECT "Year" FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 30 LIMIT 1) ql42year3,
       CASE WHEN EXISTS (SELECT 1 FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 4 LIMIT 1) THEN 1 ELSE 0 END ql48achq2,
       (SELECT "Year" FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 4 LIMIT 1) ql48year2,
       CASE WHEN EXISTS (SELECT 1 FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 5 LIMIT 1) THEN 1 ELSE 0 END ql49achq3,
       (SELECT "Year" FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 5 LIMIT 1) ql49year3,
       CASE WHEN EXISTS (SELECT 1 FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 60 LIMIT 1) THEN 1 ELSE 0 END ql50achq2,
       (SELECT "Year" FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 60 LIMIT 1) ql50year2,
       CASE WHEN EXISTS (SELECT 1 FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 61 LIMIT 1) THEN 1 ELSE 0 END ql51achq3,
       (SELECT "Year" FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 61 LIMIT 1) ql51year3,
       CASE WHEN EXISTS (SELECT 1 FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 10 LIMIT 1) THEN 1 ELSE 0 END ql52achq2,
       (SELECT "Year" FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 10 LIMIT 1) ql52year2,
       CASE WHEN EXISTS (SELECT 1 FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 80 LIMIT 1) THEN 1 ELSE 0 END ql53achq2,
       (SELECT "Year" FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 80 LIMIT 1) ql53year2,
       CASE WHEN EXISTS (SELECT 1 FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 81 LIMIT 1) THEN 1 ELSE 0 END ql54achq3,
       (SELECT "Year" FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 81 LIMIT 1) ql54year3,
       CASE WHEN EXISTS (SELECT 1 FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 82 LIMIT 1) THEN 1 ELSE 0 END ql55achq2,
       (SELECT "Year" FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 82 LIMIT 1) ql55year2,
       CASE WHEN EXISTS (SELECT 1 FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 83 LIMIT 1) THEN 1 ELSE 0 END ql56achq3,
       (SELECT "Year" FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 83 LIMIT 1) ql56year3,
       CASE WHEN EXISTS (SELECT 1 FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 84 LIMIT 1) THEN 1 ELSE 0 END ql57achq2,
       (SELECT "Year" FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 84 LIMIT 1) ql57year2,
       CASE WHEN EXISTS (SELECT 1 FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 85 LIMIT 1) THEN 1 ELSE 0 END ql58achq3,
       (SELECT "Year" FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 85 LIMIT 1) ql58year3,
       CASE WHEN EXISTS (SELECT 1 FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 86 LIMIT 1) THEN 1 ELSE 0 END ql62achq5,
       (SELECT "Year" FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 86 LIMIT 1) ql62year5,
       CASE WHEN EXISTS (SELECT 1 FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 87 LIMIT 1) THEN 1 ELSE 0 END ql63achq5,
       (SELECT "Year" FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 87 LIMIT 1) ql63year5,
       CASE WHEN EXISTS (SELECT 1 FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 88 LIMIT 1) THEN 1 ELSE 0 END ql64achq5,
       (SELECT "Year" FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 88 LIMIT 1) ql64year5,
       CASE WHEN EXISTS (SELECT 1 FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 66 LIMIT 1) THEN 1 ELSE 0 END ql67achq2,
       (SELECT "Year" FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 66 LIMIT 1) ql67year2,
       CASE WHEN EXISTS (SELECT 1 FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 67 LIMIT 1) THEN 1 ELSE 0 END ql68achq3,
       (SELECT "Year" FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 67 LIMIT 1) ql68year3,
       CASE WHEN EXISTS (SELECT 1 FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 23 LIMIT 1) THEN 1 ELSE 0 END ql72achq2,
       (SELECT "Year" FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 23 LIMIT 1) ql72year2,
       CASE WHEN EXISTS (SELECT 1 FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 32 LIMIT 1) THEN 1 ELSE 0 END ql73achq2,
       (SELECT "Year" FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 32 LIMIT 1) ql73year2,
       CASE WHEN EXISTS (SELECT 1 FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 19 LIMIT 1) THEN 1 ELSE 0 END ql74achq3,
       (SELECT "Year" FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 19 LIMIT 1) ql74year3,
       CASE WHEN EXISTS (SELECT 1 FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 64 LIMIT 1) THEN 1 ELSE 0 END ql76achq2,
       (SELECT "Year" FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 64 LIMIT 1) ql76year2,
       CASE WHEN EXISTS (SELECT 1 FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 65 LIMIT 1) THEN 1 ELSE 0 END ql77achq3,
       (SELECT "Year" FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 65 LIMIT 1) ql77year3,
       COALESCE((SELECT SUM(total_quals) FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 103), 0) ql82achq,
       (SELECT MAX("Year") FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 103) ql82year,
       COALESCE((SELECT SUM(total_quals) FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 104), 0) ql83achq,
       (SELECT MAX("Year") FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 104) ql83year,
       COALESCE((SELECT SUM(total_quals) FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 105), 0) ql84achq,
       (SELECT MAX("Year") FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 105) ql84year,
       CASE WHEN EXISTS (SELECT 1 FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 17 LIMIT 1) THEN 1 ELSE 0 END ql85achq1,
       (SELECT "Year" FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 17 LIMIT 1) ql85year1,
       CASE WHEN EXISTS (SELECT 1 FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 2 LIMIT 1) THEN 1 ELSE 0 END ql86achq2,
       (SELECT "Year" FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 2 LIMIT 1) ql86year2,
       CASE WHEN EXISTS (SELECT 1 FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 45 LIMIT 1) THEN 1 ELSE 0 END ql87achq3,
       (SELECT "Year" FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 45 LIMIT 1) ql87year3,
       CASE WHEN EXISTS (SELECT 1 FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 9 LIMIT 1) THEN 1 ELSE 0 END ql88achq2,
       (SELECT "Year" FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 9 LIMIT 1) ql88year2,
       CASE WHEN EXISTS (SELECT 1 FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 69 LIMIT 1) THEN 1 ELSE 0 END ql89achq3,
       (SELECT "Year" FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 69 LIMIT 1) ql89year3,
       CASE WHEN EXISTS (SELECT 1 FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 12 LIMIT 1) THEN 1 ELSE 0 END ql90achq2,
       (SELECT "Year" FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 12 LIMIT 1) ql90year2,
       CASE WHEN EXISTS (SELECT 1 FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 18 LIMIT 1) THEN 1 ELSE 0 END ql91achq2,
       (SELECT "Year" FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 18 LIMIT 1) ql91year2,
       CASE WHEN EXISTS (SELECT 1 FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 13 LIMIT 1) THEN 1 ELSE 0 END ql92achq1,
       (SELECT "Year" FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 13 LIMIT 1) ql92year1,
       CASE WHEN EXISTS (SELECT 1 FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 62 LIMIT 1) THEN 1 ELSE 0 END ql93achq1,
       (SELECT "Year" FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 62 LIMIT 1) ql93year1,
       CASE WHEN EXISTS (SELECT 1 FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 21 LIMIT 1) THEN 1 ELSE 0 END ql94achq2,
       (SELECT "Year" FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 21 LIMIT 1) ql94year2,
       CASE WHEN EXISTS (SELECT 1 FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 22 LIMIT 1) THEN 1 ELSE 0 END ql95achq3,
       (SELECT "Year" FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 22 LIMIT 1) ql95year3,
       CASE WHEN EXISTS (SELECT 1 FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 11 LIMIT 1) THEN 1 ELSE 0 END ql96achq2,
       (SELECT "Year" FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 11 LIMIT 1) ql96year2,
       CASE WHEN EXISTS (SELECT 1 FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 59 LIMIT 1) THEN 1 ELSE 0 END ql98achq2,
       (SELECT "Year" FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 59 LIMIT 1) ql98year2,
       CASE WHEN EXISTS (SELECT 1 FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 6 LIMIT 1) THEN 1 ELSE 0 END ql99achq2,
       (SELECT "Year" FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 6 LIMIT 1) ql99year2,
       CASE WHEN EXISTS (SELECT 1 FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 7 LIMIT 1) THEN 1 ELSE 0 END ql100achq3,
       (SELECT "Year" FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 7 LIMIT 1) ql100year3,
       CASE WHEN EXISTS (SELECT 1 FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 68 LIMIT 1) THEN 1 ELSE 0 END ql101achq3,
       (SELECT "Year" FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 68 LIMIT 1) ql101year3,
       CASE WHEN EXISTS (SELECT 1 FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 63 LIMIT 1) THEN 1 ELSE 0 END ql102achq5,
       (SELECT "Year" FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 63 LIMIT 1) ql102year5,
       CASE WHEN EXISTS (SELECT 1 FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 8 LIMIT 1) THEN 1 ELSE 0 END ql103achq3,
       (SELECT "Year" FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 8 LIMIT 1) ql103year3,
       CASE WHEN EXISTS (SELECT 1 FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 75 LIMIT 1) THEN 1 ELSE 0 END ql104achq4,
       (SELECT "Year" FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 75 LIMIT 1) ql104year4,
       CASE WHEN EXISTS (SELECT 1 FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 76 LIMIT 1) THEN 1 ELSE 0 END ql105achq4,
       (SELECT "Year" FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 76 LIMIT 1) ql105year4,
       CASE WHEN EXISTS (SELECT 1 FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 3 LIMIT 1) THEN 1 ELSE 0 END ql107achq3,
       (SELECT "Year" FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 3 LIMIT 1) ql107year3,
       CASE WHEN EXISTS (SELECT 1 FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 47 LIMIT 1) THEN 1 ELSE 0 END ql108achq3,
       (SELECT "Year" FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 47 LIMIT 1) ql108year3,
       CASE WHEN EXISTS (SELECT 1 FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 74 LIMIT 1) THEN 1 ELSE 0 END ql109achq4,
       (SELECT "Year" FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 74 LIMIT 1) ql109year4,
       CASE WHEN EXISTS (SELECT 1 FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 31 LIMIT 1) THEN 1 ELSE 0 END ql110achq4,
       (SELECT "Year" FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 31 LIMIT 1) ql110year4,
       COALESCE((SELECT SUM(total_quals) FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 27), 0) ql111achq,
       (SELECT MAX("Year") FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 27) ql111year,
       COALESCE((SELECT SUM(total_quals) FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 28), 0) ql112achq,
       (SELECT MAX("Year") FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 28) ql112year,
       COALESCE((SELECT SUM(total_quals) FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 134), 0) ql113achq,
       (SELECT MAX("Year") FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 134) ql113year,
       COALESCE((SELECT SUM(total_quals) FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 135), 0) ql114achq,
       (SELECT MAX("Year") FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 135) ql114year,
       COALESCE((SELECT SUM(total_quals) FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 90), 0) ql115achq,
       (SELECT MAX("Year") FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 90) ql115year,
       COALESCE((SELECT SUM(total_quals) FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 91), 0) ql116achq,
       (SELECT MAX("Year") FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 91) ql116year,
       COALESCE((SELECT SUM(total_quals) FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 112), 0) ql117achq,
       (SELECT MAX("Year") FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 112) ql117year,
       CASE WHEN EXISTS (SELECT 1 FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 39 LIMIT 1) THEN 1 ELSE 0 END ql118achq,
       (SELECT "Year" FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 39 LIMIT 1) ql118year,
       CASE WHEN EXISTS (SELECT 1 FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 33 LIMIT 1) THEN 1 ELSE 0 END ql119achq,
       (SELECT "Year" FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 33 LIMIT 1) ql119year,
       CASE WHEN EXISTS (SELECT 1 FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 49 LIMIT 1) THEN 1 ELSE 0 END ql120achq,
       (SELECT "Year" FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 49 LIMIT 1) ql120year,
       CASE WHEN EXISTS (SELECT 1 FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 34 LIMIT 1) THEN 1 ELSE 0 END ql121achq,
       (SELECT "Year" FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 34 LIMIT 1) ql121year,
       CASE WHEN EXISTS (SELECT 1 FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 50 LIMIT 1) THEN 1 ELSE 0 END ql122achq,
       (SELECT "Year" FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 50 LIMIT 1) ql122year,
       CASE WHEN EXISTS (SELECT 1 FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 36 LIMIT 1) THEN 1 ELSE 0 END ql123achq,
       (SELECT "Year" FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 36 LIMIT 1) ql123year,
       CASE WHEN EXISTS (SELECT 1 FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 37 LIMIT 1) THEN 1 ELSE 0 END ql124achq,
       (SELECT "Year" FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 37 LIMIT 1) ql124year,
       CASE WHEN EXISTS (SELECT 1 FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 38 LIMIT 1) THEN 1 ELSE 0 END ql125achq,
       (SELECT "Year" FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 38 LIMIT 1) ql125year,
       CASE WHEN EXISTS (SELECT 1 FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 51 LIMIT 1) THEN 1 ELSE 0 END ql126achq,
       (SELECT "Year" FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 51 LIMIT 1) ql126year,
       CASE WHEN EXISTS (SELECT 1 FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 53 LIMIT 1) THEN 1 ELSE 0 END ql127achq,
       (SELECT "Year" FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 53 LIMIT 1) ql127year,
       CASE WHEN EXISTS (SELECT 1 FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 52 LIMIT 1) THEN 1 ELSE 0 END ql128achq,
       (SELECT "Year" FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 52 LIMIT 1) ql128year,
       CASE WHEN EXISTS (SELECT 1 FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 77 LIMIT 1) THEN 1 ELSE 0 END ql129achq,
       (SELECT "Year" FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 77 LIMIT 1) ql129year,
       CASE WHEN EXISTS (SELECT 1 FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 78 LIMIT 1) THEN 1 ELSE 0 END ql130achq,
       (SELECT "Year" FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 78 LIMIT 1) ql130year,
       CASE WHEN EXISTS (SELECT 1 FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 41 LIMIT 1) THEN 1 ELSE 0 END ql131achq,
       (SELECT "Year" FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 41 LIMIT 1) ql131year,
       CASE WHEN EXISTS (SELECT 1 FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 79 LIMIT 1) THEN 1 ELSE 0 END ql132achq,
       (SELECT "Year" FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 79 LIMIT 1) ql132year,
       CASE WHEN EXISTS (SELECT 1 FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 55 LIMIT 1) THEN 1 ELSE 0 END ql133achq,
       (SELECT "Year" FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 55 LIMIT 1) ql133year,
       CASE WHEN EXISTS (SELECT 1 FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 42 LIMIT 1) THEN 1 ELSE 0 END ql134achq,
       (SELECT "Year" FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 42 LIMIT 1) ql134year,
       CASE WHEN EXISTS (SELECT 1 FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 56 LIMIT 1) THEN 1 ELSE 0 END ql135achq,
       (SELECT "Year" FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 56 LIMIT 1) ql135year,
       CASE WHEN EXISTS (SELECT 1 FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 35 LIMIT 1) THEN 1 ELSE 0 END ql136achq,
       (SELECT "Year" FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 35 LIMIT 1) ql136year,
       CASE WHEN EXISTS (SELECT 1 FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 40 LIMIT 1) THEN 1 ELSE 0 END ql137achq,
       (SELECT "Year" FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 40 LIMIT 1) ql137year,
       CASE WHEN EXISTS (SELECT 1 FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 43 LIMIT 1) THEN 1 ELSE 0 END ql138achq,
       (SELECT "Year" FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 43 LIMIT 1) ql138year,
       CASE WHEN EXISTS (SELECT 1 FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 57 LIMIT 1) THEN 1 ELSE 0 END ql139achq,
       (SELECT "Year" FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 57 LIMIT 1) ql139year,
       CASE WHEN EXISTS (SELECT 1 FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 58 LIMIT 1) THEN 1 ELSE 0 END ql140achq,
       (SELECT "Year" FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 58 LIMIT 1) ql140year,
       CASE WHEN EXISTS (SELECT 1 FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 48 LIMIT 1) THEN 1 ELSE 0 END ql141achq,
       (SELECT "Year" FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 48 LIMIT 1) ql141year,
       CASE WHEN EXISTS (SELECT 1 FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 54 LIMIT 1) THEN 1 ELSE 0 END ql142achq,
       (SELECT "Year" FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 54 LIMIT 1) ql142year,
       CASE WHEN EXISTS (SELECT 1 FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 44 LIMIT 1) THEN 1 ELSE 0 END ql143achq,
       (SELECT "Year" FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 44 LIMIT 1) ql143year,
       CASE WHEN EXISTS (SELECT 1 FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 127 LIMIT 1) THEN 1 ELSE 0 END ql301app,
       (SELECT "Year" FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 127 LIMIT 1) ql301year,
       CASE WHEN EXISTS (SELECT 1 FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 121 LIMIT 1) THEN 1 ELSE 0 END ql302app,
       (SELECT "Year" FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 121 LIMIT 1) ql302year,
       CASE WHEN EXISTS (SELECT 1 FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 123 LIMIT 1) THEN 1 ELSE 0 END ql303app,
       (SELECT "Year" FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 123 LIMIT 1) ql303year,
       CASE WHEN EXISTS (SELECT 1 FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 122 LIMIT 1) THEN 1 ELSE 0 END ql304app,
       (SELECT "Year" FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 122 LIMIT 1) ql304year,
       CASE WHEN EXISTS (SELECT 1 FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 128 LIMIT 1) THEN 1 ELSE 0 END ql305app,
       (SELECT "Year" FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 128 LIMIT 1) ql305year,
       CASE WHEN EXISTS (SELECT 1 FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 126 LIMIT 1) THEN 1 ELSE 0 END ql306app,
       (SELECT "Year" FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 126 LIMIT 1) ql306year,
       CASE WHEN EXISTS (SELECT 1 FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 129 LIMIT 1) THEN 1 ELSE 0 END ql307app,
       (SELECT "Year" FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 129 LIMIT 1) ql307year,
       CASE WHEN EXISTS (SELECT 1 FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 125 LIMIT 1) THEN 1 ELSE 0 END ql308app,
       (SELECT "Year" FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 125 LIMIT 1) ql308year,
       CASE WHEN EXISTS (SELECT 1 FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 130 LIMIT 1) THEN 1 ELSE 0 END ql309app,
       (SELECT "Year" FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 130 LIMIT 1) ql309year,
       CASE WHEN EXISTS (SELECT 1 FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 124 LIMIT 1) THEN 1 ELSE 0 END ql310app,
       (SELECT "Year" FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 124 LIMIT 1) ql310year,
       CASE WHEN EXISTS (SELECT 1 FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 133 LIMIT 1) THEN 1 ELSE 0 END ql311app,
       (SELECT "Year" FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 133 LIMIT 1) ql311year,
       CASE WHEN EXISTS (SELECT 1 FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 131 LIMIT 1) THEN 1 ELSE 0 END ql312app,
       (SELECT "Year" FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 131 LIMIT 1) ql312year,
       CASE WHEN EXISTS (SELECT 1 FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 132 LIMIT 1) THEN 1 ELSE 0 END ql313app,
       (SELECT "Year" FROM "WorkerQualificationStats" WHERE "WorkerFK" = w."ID" AND "QualificationsFK" = 132 LIMIT 1) ql313year,
       TO_CHAR((SELECT MAX(updated) FROM "WorkerTraining" WHERE "WorkerFK" = w."ID"),'DD/MM/YYYY') train_changedate,
       TO_CHAR((SELECT MAX(created) FROM "WorkerTraining" WHERE "WorkerFK" = w."ID"),'DD/MM/YYYY') train_savedate,
       CASE WHEN EXISTS (SELECT 1 FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" LIMIT 1) THEN 1 ELSE 0 END trainflag,
       CASE WHEN EXISTS (SELECT 1 FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 8 LIMIT 1) THEN 1 ELSE 0 END tr01flag,
       (SELECT latest_training_date FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 8 LIMIT 1) tr01latestdate,
       COALESCE((SELECT total_training FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 8 LIMIT 1), 0) tr01count,
       COALESCE((SELECT total_accredited_yes FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 8 LIMIT 1), 0) tr01ac,
       COALESCE((SELECT total_accredited_no FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 8 LIMIT 1), 0) tr01nac,
       COALESCE((SELECT total_accredited_unknown FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 8 LIMIT 1), 0) tr01dn,
       CASE WHEN EXISTS (SELECT 1 FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 10 LIMIT 1) THEN 1 ELSE 0 END tr02flag,
       (SELECT latest_training_date FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 10 LIMIT 1) tr02latestdate,
       COALESCE((SELECT total_training FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 10 LIMIT 1), 0) tr02count,
       COALESCE((SELECT total_accredited_yes FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 10 LIMIT 1), 0) tr02ac,
       COALESCE((SELECT total_accredited_no FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 10 LIMIT 1), 0) tr02nac,
       COALESCE((SELECT total_accredited_unknown FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 10 LIMIT 1), 0) tr02dn,
       -- TR03 dataset (451 to 456) removed after confirmation from Roy Price.
       CASE WHEN EXISTS (SELECT 1 FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 14 LIMIT 1) THEN 1 ELSE 0 END tr05flag,
       (SELECT latest_training_date FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 14 LIMIT 1) tr05latestdate,
       COALESCE((SELECT total_training FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 14 LIMIT 1), 0) tr05count,
       COALESCE((SELECT total_accredited_yes FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 14 LIMIT 1), 0) tr05ac,
       COALESCE((SELECT total_accredited_no FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 14 LIMIT 1), 0) tr05nac,
       COALESCE((SELECT total_accredited_unknown FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 14 LIMIT 1), 0) tr05dn,
       CASE WHEN EXISTS (SELECT 1 FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 17 LIMIT 1) THEN 1 ELSE 0 END tr06flag,
       (SELECT latest_training_date FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 17 LIMIT 1) tr06latestdate,
       COALESCE((SELECT total_training FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 17 LIMIT 1), 0) tr06count,
       COALESCE((SELECT total_accredited_yes FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 17 LIMIT 1), 0) tr06ac,
       COALESCE((SELECT total_accredited_no FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 17 LIMIT 1), 0) tr06nac,
       COALESCE((SELECT total_accredited_unknown FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 17 LIMIT 1), 0) tr06dn,
       CASE WHEN EXISTS (SELECT 1 FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 18 LIMIT 1) THEN 1 ELSE 0 END tr07flag,
       (SELECT latest_training_date FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 18 LIMIT 1) tr07latestdate,
       COALESCE((SELECT total_training FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 18 LIMIT 1), 0) tr07count,
       COALESCE((SELECT total_accredited_yes FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 18 LIMIT 1), 0) tr07ac,
       COALESCE((SELECT total_accredited_no FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 18 LIMIT 1), 0) tr07nac,
       COALESCE((SELECT total_accredited_unknown FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 18 LIMIT 1), 0) tr07dn,
       CASE WHEN EXISTS (SELECT 1 FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 19 LIMIT 1) THEN 1 ELSE 0 END tr08flag,
       (SELECT latest_training_date FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 19 LIMIT 1) tr08latestdate,
       COALESCE((SELECT total_training FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 19 LIMIT 1), 0) tr08count,
       COALESCE((SELECT total_accredited_yes FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 19 LIMIT 1), 0) tr08ac,
       COALESCE((SELECT total_accredited_no FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 19 LIMIT 1), 0) tr08nac,
       COALESCE((SELECT total_accredited_unknown FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 19 LIMIT 1), 0) tr08dn,
       CASE WHEN EXISTS (SELECT 1 FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 20 LIMIT 1) THEN 1 ELSE 0 END tr09flag,
       (SELECT latest_training_date FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 20 LIMIT 1) tr09latestdate,
       COALESCE((SELECT total_training FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 20 LIMIT 1), 0) tr09count,
       COALESCE((SELECT total_accredited_yes FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 20 LIMIT 1), 0) tr09ac,
       COALESCE((SELECT total_accredited_no FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 20 LIMIT 1), 0) tr09nac,
       COALESCE((SELECT total_accredited_unknown FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 20 LIMIT 1), 0) tr09dn,
       CASE WHEN EXISTS (SELECT 1 FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 21 LIMIT 1) THEN 1 ELSE 0 END tr10flag,
       (SELECT latest_training_date FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 21 LIMIT 1) tr10latestdate,
       COALESCE((SELECT total_training FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 21 LIMIT 1), 0) tr10count,
       COALESCE((SELECT total_accredited_yes FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 21 LIMIT 1), 0) tr10ac,
       COALESCE((SELECT total_accredited_no FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 21 LIMIT 1), 0) tr10nac,
       COALESCE((SELECT total_accredited_unknown FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 21 LIMIT 1), 0) tr10dn,
       CASE WHEN EXISTS (SELECT 1 FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 22 LIMIT 1) THEN 1 ELSE 0 END tr11flag,
       (SELECT latest_training_date FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 22 LIMIT 1) tr11latestdate,
       COALESCE((SELECT total_training FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 22 LIMIT 1), 0) tr11count,
       COALESCE((SELECT total_accredited_yes FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 22 LIMIT 1), 0) tr11ac,
       COALESCE((SELECT total_accredited_no FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 22 LIMIT 1), 0) tr11nac,
       COALESCE((SELECT total_accredited_unknown FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 22 LIMIT 1), 0) tr11dn,
       CASE WHEN EXISTS (SELECT 1 FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 23 LIMIT 1) THEN 1 ELSE 0 END tr12flag,
       (SELECT latest_training_date FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 23 LIMIT 1) tr12latestdate,
       COALESCE((SELECT total_training FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 23 LIMIT 1), 0) tr12count,
       COALESCE((SELECT total_accredited_yes FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 23 LIMIT 1), 0) tr12ac,
       COALESCE((SELECT total_accredited_no FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 23 LIMIT 1), 0) tr12nac,
       COALESCE((SELECT total_accredited_unknown FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 23 LIMIT 1), 0) tr12dn,
       CASE WHEN EXISTS (SELECT 1 FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 24 LIMIT 1) THEN 1 ELSE 0 END tr13flag,
       (SELECT latest_training_date FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 24 LIMIT 1) tr13latestdate,
       COALESCE((SELECT total_training FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 24 LIMIT 1), 0) tr13count,
       COALESCE((SELECT total_accredited_yes FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 24 LIMIT 1), 0) tr13ac,
       COALESCE((SELECT total_accredited_no FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 24 LIMIT 1), 0) tr13nac,
       COALESCE((SELECT total_accredited_unknown FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 24 LIMIT 1), 0) tr13dn,
       CASE WHEN EXISTS (SELECT 1 FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 25 LIMIT 1) THEN 1 ELSE 0 END tr14flag,
       (SELECT latest_training_date FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 25 LIMIT 1) tr14latestdate,
       COALESCE((SELECT total_training FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 25 LIMIT 1), 0) tr14count,
       COALESCE((SELECT total_accredited_yes FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 25 LIMIT 1), 0) tr14ac,
       COALESCE((SELECT total_accredited_no FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 25 LIMIT 1), 0) tr14nac,
       COALESCE((SELECT total_accredited_unknown FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 25 LIMIT 1), 0) tr14dn,
       CASE WHEN EXISTS (SELECT 1 FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 27 LIMIT 1) THEN 1 ELSE 0 END tr15flag,
       (SELECT latest_training_date FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 27 LIMIT 1) tr15latestdate,
       COALESCE((SELECT total_training FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 27 LIMIT 1), 0) tr15count,
       COALESCE((SELECT total_accredited_yes FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 27 LIMIT 1), 0) tr15ac,
       COALESCE((SELECT total_accredited_no FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 27 LIMIT 1), 0) tr15nac,
       COALESCE((SELECT total_accredited_unknown FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 27 LIMIT 1), 0) tr15dn,
       CASE WHEN EXISTS (SELECT 1 FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 28 LIMIT 1) THEN 1 ELSE 0 END tr16flag,
       (SELECT latest_training_date FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 28 LIMIT 1) tr16latestdate,
       COALESCE((SELECT total_training FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 28 LIMIT 1), 0) tr16count,
       COALESCE((SELECT total_accredited_yes FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 28 LIMIT 1), 0) tr16ac,
       COALESCE((SELECT total_accredited_no FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 28 LIMIT 1), 0) tr16nac,
       COALESCE((SELECT total_accredited_unknown FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 28 LIMIT 1), 0) tr16dn,
       CASE WHEN EXISTS (SELECT 1 FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 29 LIMIT 1) THEN 1 ELSE 0 END tr17flag,
       (SELECT latest_training_date FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 29 LIMIT 1) tr17latestdate,
       COALESCE((SELECT total_training FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 29 LIMIT 1), 0) tr17count,
       COALESCE((SELECT total_accredited_yes FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 29 LIMIT 1), 0) tr17ac,
       COALESCE((SELECT total_accredited_no FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 29 LIMIT 1), 0) tr17nac,
       COALESCE((SELECT total_accredited_unknown FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 29 LIMIT 1), 0) tr17dn,
       CASE WHEN EXISTS (SELECT 1 FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 31 LIMIT 1) THEN 1 ELSE 0 END tr18flag,
       (SELECT latest_training_date FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 31 LIMIT 1) tr18latestdate,
       COALESCE((SELECT total_training FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 31 LIMIT 1), 0) tr18count,
       COALESCE((SELECT total_accredited_yes FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 31 LIMIT 1), 0) tr18ac,
       COALESCE((SELECT total_accredited_no FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 31 LIMIT 1), 0) tr18nac,
       COALESCE((SELECT total_accredited_unknown FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 31 LIMIT 1), 0) tr18dn,
       CASE WHEN EXISTS (SELECT 1 FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 32 LIMIT 1) THEN 1 ELSE 0 END tr19flag,
       (SELECT latest_training_date FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 32 LIMIT 1) tr19latestdate,
       COALESCE((SELECT total_training FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 32 LIMIT 1), 0) tr19count,
       COALESCE((SELECT total_accredited_yes FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 32 LIMIT 1), 0) tr19ac,
       COALESCE((SELECT total_accredited_no FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 32 LIMIT 1), 0) tr19nac,
       COALESCE((SELECT total_accredited_unknown FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 32 LIMIT 1), 0) tr19dn,
       CASE WHEN EXISTS (SELECT 1 FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 33 LIMIT 1) THEN 1 ELSE 0 END tr20flag,
       (SELECT latest_training_date FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 33 LIMIT 1) tr20latestdate,
       COALESCE((SELECT total_training FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 33 LIMIT 1), 0) tr20count,
       COALESCE((SELECT total_accredited_yes FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 33 LIMIT 1), 0) tr20ac,
       COALESCE((SELECT total_accredited_no FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 33 LIMIT 1), 0) tr20nac,
       COALESCE((SELECT total_accredited_unknown FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 33 LIMIT 1), 0) tr20dn,
       CASE WHEN EXISTS (SELECT 1 FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 37 LIMIT 1) THEN 1 ELSE 0 END tr21flag,
       (SELECT latest_training_date FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 37 LIMIT 1) tr21latestdate,
       COALESCE((SELECT total_training FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 37 LIMIT 1), 0) tr21count,
       COALESCE((SELECT total_accredited_yes FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 37 LIMIT 1), 0) tr21ac,
       COALESCE((SELECT total_accredited_no FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 37 LIMIT 1), 0) tr21nac,
       COALESCE((SELECT total_accredited_unknown FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 37 LIMIT 1), 0) tr21dn,
       CASE WHEN EXISTS (SELECT 1 FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 12 LIMIT 1) THEN 1 ELSE 0 END tr22flag,
       (SELECT latest_training_date FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 12 LIMIT 1) tr22latestdate,
       COALESCE((SELECT total_training FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 12 LIMIT 1), 0) tr22count,
       COALESCE((SELECT total_accredited_yes FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 12 LIMIT 1), 0) tr22ac,
       COALESCE((SELECT total_accredited_no FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 12 LIMIT 1), 0) tr22nac,
       COALESCE((SELECT total_accredited_unknown FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 12 LIMIT 1), 0) tr22dn,
       CASE WHEN EXISTS (SELECT 1 FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 16 LIMIT 1) THEN 1 ELSE 0 END tr23flag,
       (SELECT latest_training_date FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 16 LIMIT 1) tr23latestdate,
       COALESCE((SELECT total_training FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 16 LIMIT 1), 0) tr23count,
       COALESCE((SELECT total_accredited_yes FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 16 LIMIT 1), 0) tr23ac,
       COALESCE((SELECT total_accredited_no FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 16 LIMIT 1), 0) tr23nac,
       COALESCE((SELECT total_accredited_unknown FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 16 LIMIT 1), 0) tr23dn,
       CASE WHEN EXISTS (SELECT 1 FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 3 LIMIT 1) THEN 1 ELSE 0 END tr25flag,
       (SELECT latest_training_date FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 3 LIMIT 1) tr25latestdate,
       COALESCE((SELECT total_training FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 3 LIMIT 1), 0) tr25count,
       COALESCE((SELECT total_accredited_yes FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 3 LIMIT 1), 0) tr25ac,
       COALESCE((SELECT total_accredited_no FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 3 LIMIT 1), 0) tr25nac,
       COALESCE((SELECT total_accredited_unknown FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 3 LIMIT 1), 0) tr25dn,
       CASE WHEN EXISTS (SELECT 1 FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 6 LIMIT 1) THEN 1 ELSE 0 END tr26flag,
       (SELECT latest_training_date FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 6 LIMIT 1) tr26latestdate,
       COALESCE((SELECT total_training FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 6 LIMIT 1), 0) tr26count,
       COALESCE((SELECT total_accredited_yes FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 6 LIMIT 1), 0) tr26ac,
       COALESCE((SELECT total_accredited_no FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 6 LIMIT 1), 0) tr26nac,
       COALESCE((SELECT total_accredited_unknown FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 6 LIMIT 1), 0) tr26dn,
       CASE WHEN EXISTS (SELECT 1 FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 15 LIMIT 1) THEN 1 ELSE 0 END tr27flag,
       (SELECT latest_training_date FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 15 LIMIT 1) tr27latestdate,
       COALESCE((SELECT total_training FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 15 LIMIT 1), 0) tr27count,
       COALESCE((SELECT total_accredited_yes FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 15 LIMIT 1), 0) tr27ac,
       COALESCE((SELECT total_accredited_no FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 15 LIMIT 1), 0) tr27nac,
       COALESCE((SELECT total_accredited_unknown FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 15 LIMIT 1), 0) tr27dn,
       CASE WHEN EXISTS (SELECT 1 FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 4 LIMIT 1) THEN 1 ELSE 0 END tr28flag,
       (SELECT latest_training_date FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 4 LIMIT 1) tr28latestdate,
       COALESCE((SELECT total_training FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 4 LIMIT 1), 0) tr28count,
       COALESCE((SELECT total_accredited_yes FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 4 LIMIT 1), 0) tr28ac,
       COALESCE((SELECT total_accredited_no FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 4 LIMIT 1), 0) tr28nac,
       COALESCE((SELECT total_accredited_unknown FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 4 LIMIT 1), 0) tr28dn,
       CASE WHEN EXISTS (SELECT 1 FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 11 LIMIT 1) THEN 1 ELSE 0 END tr29flag,
       (SELECT latest_training_date FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 11 LIMIT 1) tr29latestdate,
       COALESCE((SELECT total_training FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 11 LIMIT 1), 0) tr29count,
       COALESCE((SELECT total_accredited_yes FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 11 LIMIT 1), 0) tr29ac,
       COALESCE((SELECT total_accredited_no FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 11 LIMIT 1), 0) tr29nac,
       COALESCE((SELECT total_accredited_unknown FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 11 LIMIT 1), 0) tr29dn,
       CASE WHEN EXISTS (SELECT 1 FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 9 LIMIT 1) THEN 1 ELSE 0 END tr30flag,
       (SELECT latest_training_date FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 9 LIMIT 1) tr30latestdate,
       COALESCE((SELECT total_training FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 9 LIMIT 1), 0) tr30count,
       COALESCE((SELECT total_accredited_yes FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 9 LIMIT 1), 0) tr30ac,
       COALESCE((SELECT total_accredited_no FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 9 LIMIT 1), 0) tr30nac,
       COALESCE((SELECT total_accredited_unknown FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 9 LIMIT 1), 0) tr30dn,
       CASE WHEN EXISTS (SELECT 1 FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 26 LIMIT 1) THEN 1 ELSE 0 END tr31flag,
       (SELECT latest_training_date FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 26 LIMIT 1) tr31latestdate,
       COALESCE((SELECT total_training FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 26 LIMIT 1), 0) tr31count,
       COALESCE((SELECT total_accredited_yes FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 26 LIMIT 1), 0) tr31ac,
       COALESCE((SELECT total_accredited_no FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 26 LIMIT 1), 0) tr31nac,
       COALESCE((SELECT total_accredited_unknown FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 26 LIMIT 1), 0) tr31dn,
       CASE WHEN EXISTS (SELECT 1 FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 2 LIMIT 1) THEN 1 ELSE 0 END tr32flag,
       (SELECT latest_training_date FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 2 LIMIT 1) tr32latestdate,
       COALESCE((SELECT total_training FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 2 LIMIT 1), 0) tr32count,
       COALESCE((SELECT total_accredited_yes FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 2 LIMIT 1), 0) tr32ac,
       COALESCE((SELECT total_accredited_no FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 2 LIMIT 1), 0) tr32nac,
       COALESCE((SELECT total_accredited_unknown FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 2 LIMIT 1), 0) tr32dn,
       CASE WHEN EXISTS (SELECT 1 FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 7 LIMIT 1) THEN 1 ELSE 0 END tr33flag,
       (SELECT latest_training_date FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 7 LIMIT 1) tr33latestdate,
       COALESCE((SELECT total_training FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 7 LIMIT 1), 0) tr33count,
       COALESCE((SELECT total_accredited_yes FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 7 LIMIT 1), 0) tr33ac,
       COALESCE((SELECT total_accredited_no FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 7 LIMIT 1), 0) tr33nac,
       COALESCE((SELECT total_accredited_unknown FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 7 LIMIT 1), 0) tr33dn,
       CASE WHEN EXISTS (SELECT 1 FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 13 LIMIT 1) THEN 1 ELSE 0 END tr34flag,
       (SELECT latest_training_date FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 13 LIMIT 1) tr34latestdate,
       COALESCE((SELECT total_training FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 13 LIMIT 1), 0) tr34count,
       COALESCE((SELECT total_accredited_yes FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 13 LIMIT 1), 0) tr34ac,
       COALESCE((SELECT total_accredited_no FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 13 LIMIT 1), 0) tr34nac,
       COALESCE((SELECT total_accredited_unknown FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 13 LIMIT 1), 0) tr34dn,
       CASE WHEN EXISTS (SELECT 1 FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 36 LIMIT 1) THEN 1 ELSE 0 END tr35flag,
       (SELECT latest_training_date FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 36 LIMIT 1) tr35latestdate,
       COALESCE((SELECT total_training FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 36 LIMIT 1), 0) tr35count,
       COALESCE((SELECT total_accredited_yes FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 36 LIMIT 1), 0) tr35ac,
       COALESCE((SELECT total_accredited_no FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 36 LIMIT 1), 0) tr35nac,
       COALESCE((SELECT total_accredited_unknown FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 36 LIMIT 1), 0) tr35dn,
       CASE WHEN EXISTS (SELECT 1 FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 35 LIMIT 1) THEN 1 ELSE 0 END tr36flag,
       (SELECT latest_training_date FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 35 LIMIT 1) tr36latestdate,
       COALESCE((SELECT total_training FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 35 LIMIT 1), 0) tr36count,
       COALESCE((SELECT total_accredited_yes FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 35 LIMIT 1), 0) tr36ac,
       COALESCE((SELECT total_accredited_no FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 35 LIMIT 1), 0) tr36nac,
       COALESCE((SELECT total_accredited_unknown FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 35 LIMIT 1), 0) tr36dn,
       CASE WHEN EXISTS (SELECT 1 FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 5 LIMIT 1) THEN 1 ELSE 0 END tr37flag,
       (SELECT latest_training_date FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 5 LIMIT 1) tr37latestdate,
       COALESCE((SELECT total_training FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 5 LIMIT 1), 0) tr37count,
       COALESCE((SELECT total_accredited_yes FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 5 LIMIT 1), 0) tr37ac,
       COALESCE((SELECT total_accredited_no FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 5 LIMIT 1), 0) tr37nac,
       COALESCE((SELECT total_accredited_unknown FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 5 LIMIT 1), 0) tr37dn,
       CASE WHEN EXISTS (SELECT 1 FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 30 LIMIT 1) THEN 1 ELSE 0 END tr38flag,
       (SELECT latest_training_date FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 30 LIMIT 1) tr38latestdate,
       COALESCE((SELECT total_training FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 30 LIMIT 1), 0) tr38count,
       COALESCE((SELECT total_accredited_yes FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 30 LIMIT 1), 0) tr38ac,
       COALESCE((SELECT total_accredited_no FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 30 LIMIT 1), 0) tr38nac,
       COALESCE((SELECT total_accredited_unknown FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 30 LIMIT 1), 0) tr38dn,
       CASE WHEN EXISTS (SELECT 1 FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 1 LIMIT 1) THEN 1 ELSE 0 END tr39flag,
       (SELECT latest_training_date FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 1 LIMIT 1) tr39latestdate,
       COALESCE((SELECT total_training FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 1 LIMIT 1), 0) tr39count,
       COALESCE((SELECT total_accredited_yes FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 1 LIMIT 1), 0) tr39ac,
       COALESCE((SELECT total_accredited_no FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 1 LIMIT 1), 0) tr39nac,
       COALESCE((SELECT total_accredited_unknown FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 1 LIMIT 1), 0) tr39dn,
       CASE WHEN EXISTS (SELECT 1 FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 34 LIMIT 1) THEN 1 ELSE 0 END tr40flag,
       (SELECT latest_training_date FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 34 LIMIT 1) tr40latestdate,
       COALESCE((SELECT total_training FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 34 LIMIT 1), 0) tr40count,
       COALESCE((SELECT total_accredited_yes FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 34 LIMIT 1), 0) tr40ac,
       COALESCE((SELECT total_accredited_no FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 34 LIMIT 1), 0) tr40nac,
       COALESCE((SELECT total_accredited_unknown FROM "WorkerTrainingStats" WHERE "WorkerFK" = w."ID" AND "CategoryFK" = 34 LIMIT 1), 0) tr40dn
FROM   "Establishment" e
JOIN "Worker" w ON e."EstablishmentID" = w."EstablishmentFK" AND w."Archived" = true
JOIN "Afr3BatchiSkAi0mo" b ON e."EstablishmentID" = b."EstablishmentID" AND b."BatchNo" = ${batchNum};
    `,
    )
    .stream();

module.exports = {
  createBatches,
  dropBatch,
  getBatches,
  findLeaversByBatch,
};
