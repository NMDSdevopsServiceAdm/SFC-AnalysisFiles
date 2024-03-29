const models = require('../models/index');
const slack = require('../utils/slack/slack-logger');
const config =  require('../../config/index');

const updateComplete = async (locations, startDate, endDate) => {
    console.log('Update complete');
    console.log('Checking to see failure status');

    const startDateToDate = new Date(startDate);
    const endDateToDate = new Date(endDate);
    const finishDate = new Date();

    const failedLocations = locations.filter((location) => {
      return location.status != 'success'
    });
    const failed = failedLocations.length ? true : false;
  
    if (failed) {
      console.log(`${failedLocations.length} updates failed`);
      await slack.error(
        `${config.get('db.name')} - CQC changes`, 
        `${failedLocations.length} updates failed`
      );
      await models.cqclog.create(false);
      return;
    }
  
    console.log('All went successfully');
    await slack.info(
      `${config.get('db.name')} - CQC changes`, 
      `Successfully updated ${locations.length} locations between ` +
      `${startDateToDate.getDate()}/${startDateToDate.getMonth() + 1}/${startDateToDate.getFullYear()} and ` +
      `${endDateToDate.getDate()}/${endDateToDate.getMonth() + 1}/${endDateToDate.getFullYear()}. ` +
      `Finished at ${finishDate.getHours()}:${(finishDate.getMinutes() < 10? '0' : '') + finishDate.getMinutes()}` 
    )
    await models.cqclog.createRecord(true, endDate);
  }

  module.exports.updateComplete = updateComplete;