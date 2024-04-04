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

# Installing the ansible and running the deployment

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

# Logging and troubleshooting

To see the logs for each environment run the command below in the terminal after the SSH

```
journalctl -u  sfcreports.service  --since "1 hour ago"
```

To check the status of the application use command below

```
systemctl status sfcreports.service
```