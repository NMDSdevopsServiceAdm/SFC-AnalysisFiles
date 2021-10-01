const models = require('../models/index');
const { getChangedIds } = require('./changes');
const { updateLocation } = require('./update');
const { updateComplete } = require('./complete');
const { pRateLimit } = require('p-ratelimit');

const run = async () => {
    console.log('Looking for latest run');
    const log = await models.cqclog.findLatestRun();
    
    const limit = pRateLimit({
        interval: 1000,
        rate: 15,
    });
  
    // const startDate = log ? log.dataValues.lastUpdatedAt : null;
    const startDate = '2021-09-30T11:10:43Z';
    const endDate = new Date().toISOString().split('.')[0]+'Z';
    console.log('Was last run on ' + startDate);
  
    const locations = await getChangedIds(startDate, endDate);
    await Promise.all(locations.map(async (location) => {
      return await limit(() => updateLocation(location));
    }));
  
    await updateComplete(locations, startDate, endDate);
    models.sequelize.close();
}

module.exports.run = run