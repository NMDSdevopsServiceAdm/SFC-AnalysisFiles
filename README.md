# Analysis Reports

## Overview

The Analysis Reports provide the Analysis team with a number of CSVs to provide insight into the data that has been entered into the SFC-WDS service.

The following 3 reports are created:

- Workplace report
- Workers report
- Leavers report

Once the reports have been generated, they will be uploaded to S3 for the Analysis team to download.

## Technology

[Bree](https://github.com/breejs/bree) is a scheduler that allows us to configure jobs to run on a specific schedule.

[Fast CSV](https://github.com/C2FO/fast-csv) is the library we use to generate CSV files.

[Knex](http://knexjs.org) is the library we use to communicate with the DB.

## Installing ansible and running deployments

Install ansible on your machine using homebrew 

```
brew install ansible
```

To run the deployment cd into the ansible directory and make sure you are allowed to SSH into EC2

```
ansible-playbook --inventory-file inventory --private-key ~/.ssh/sfc-db  playbook.yml --user ubuntu -vvv
```

For the changes to take place you should use the command below in the root of the directory after doing the SSH

```
sudo systemctl reload-or-restart sfcreports.service
```

## Logging and troubleshooting

To see the logs for each environment run the command below in the terminal after the SSH

```
journalctl -u  sfcreports.service  --since "1 hour ago"
```

To check the status of the application use command below

```
systemctl status sfcreports.service
```

## Running jobs locally for testing

### generate-analysis-files

For development purposes, use the below command to run the `generate_analysis_files` job locally.

This will prevent uploading the files or sending messages to slack.

```shell
npm run generate-local-analysis-files
```

This will output 3 .csv files in the filepath '/tmp/generate_analysis_files/output' on your local machine.


To check the content of the .csv files, we can use Excel's import feature by doing the following:

1. Open a new file in Excel

2. From the upper menu bar, select 
  File 
   --> Import 
   --> CSV File 
   --> Select the output .csv file we want to view
   --> Click "Get Data"
   
3. This should start a "Text Import Wizard" prompt. Select "Delimited", then press Next.
   The second step will let you choose from several delimiters. Untick "Tab", select "Other", and then put a pipe symbol `|` beside it. This will allow excel to parse the columns as separated by `|`.
   Select Next, and then select Finish.
   When Excel prompt you where to put the data, select a new sheet. Select "Import".

4. This should parse the .csv file in a tabular form, where you can search for the column name you want and see the data populated.

