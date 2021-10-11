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
        rate: 10,
    });
  
    const startDate = log ? log.dataValues.lastUpdatedAt : null;
    const endDate = new Date().toISOString().split('.')[0]+'Z';
    console.log('Was last run on ' + startDate);
  
    const locations = await getChangedIds(startDate, endDate);
    let runCount = 0;
    const rateLimitExceededLocations = [];
    await Promise.all(locations.map(async (location) => {
      return await limit(() => {
        runCount++;
        return updateLocation(location, runCount, rateLimitExceededLocations, false);
      });
    }));

    let retries = 0;
    console.log(`Retrying ${rateLimitExceededLocations.length} rate limited locations`);
    while (rateLimitExceededLocations.length > 0) {
      retries++;
      await updateLocation(rateLimitExceededLocations[0], retries, rateLimitExceededLocations, true);
      rateLimitExceededLocations.shift();
    }
  
    await updateComplete(locations, startDate, endDate);
    models.sequelize.close();
}

module.exports.run = run