# Analysis Reports

## Overview

The Analysis Reports provide the Analysis team with a number of CSVs to provide insight into the data that has been entered into the SFC-WDS service.

The following 3 reports are created:

- Workplace report
- Workers report
- Leavers report

Once the reports have been generated, they will be uploaded to S3 for the Analysis team to download.

# Technology

[Bree](https://github.com/breejs/bree) is a scheduler that allows us to configure jobs to run on a specific schedule.

[Fast CSV](https://github.com/C2FO/fast-csv) is the library we use to generate CSV files.

[Knex](http://knexjs.org) is the library we use to communicate with the DB.

# Running the Scheduler

Install the project dependencies

```
npm i
```

Set up ENV vars

```
export DATABASE_URL=
export AWS_ACCESS_KEY_ID=
export AWS_SECRET_ACCESS_KEY=
export REPORTS_S3_BUCKET=
export CRON=
export NODE_ENV=
```

Run the scheduler

```
node src/index.js
```

Change the schedule

The jobs and the schedule that they run on can be found in `src/index.js`

# Running as a task

```
cf run-task sfcreports-staging "node --max-old-space-size=8192 jobs/generate_analysis_files.js" --name run-analysis-files -m 8G -k 4G
```

# Deployment

## Staging

```
cf push --no-route -f manifest.staging.yml -u process
```

## Pre-prod

```
cf push --no-route -f manifest.pre-prod.yml -u process
```

## Production

```
cf push --no-route -f manifest.prod.yml -u process
```
