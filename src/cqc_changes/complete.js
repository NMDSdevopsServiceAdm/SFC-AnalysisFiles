const models = require('../models/index');

const updateComplete = async (locations, endDate) => {
    console.log('Update complete');
    console.log('Checking to see failure status');
  
    const failedLocations = locations.filter((location) => {
      return location.status != 'success'
    });
    const failed = failedLocations.length ? true : false;
  
    if (failed) {
      console.log('One or more updates failed');
      await models.cqclog.create(false);
      return;
    }
  
    console.log('All went successfully');
    await models.cqclog.createRecord(true, endDate);
  }

  module.exports.updateComplete = updateComplete;