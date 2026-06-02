const axios = require('axios');
const axiosRetry = require('axios-retry');

const config = require('../../config/index');
const models = require('../models/index');
const slack = require('../utils/slack/slack-logger');

axiosRetry(axios, { retries: 3, retryDelay: axiosRetry.exponentialDelay });

const cqcEndpoint = config.get('cqcApi.url');
const cqcSubscriptionKey = config.get('cqcApi.subscriptionKey');

const updateLocation = async (location, runCount, rateLimitExceededLocations, retrying) => {
    try {
      if (retrying || (runCount % 100 === 0)) {
        console.log(`Updated ${runCount} locations`);
      }

      console.log(`Fetching location ${location.locationId}`);

      const individualLocation = await axios.get(cqcEndpoint + '/locations/' + location.locationId, { 'headers': { 'Ocp-Apim-Subscription-Key': cqcSubscriptionKey }});

      if (!individualLocation.data.deregistrationDate) {
        // not deregistered so must exist
        await models.location.updateLocation(individualLocation);
      } else {
        await models.location.deleteLocation(location.locationId);
      }
      
      updateStatus(location, 'success');
    } catch (error) {

        console.log('Failed location:', location.locationId);
  console.log('Status:', error.response?.status);
  console.log('Response:', error.response?.data);
  
     if (
  error.response?.data?.message?.includes('No Locations found')
) {
  console.log(`Deleting ${location.locationId}`);

  await models.location.deleteLocation(location.locationId);

  console.log(`Deleted ${location.locationId}`);

  updateStatus(location, 'success');

  console.log(`Marked ${location.locationId} success`);

} else if (error.response?.status === 429) {
        console.log('Adding location to rateLimitExceededLocations array');
        rateLimitExceededLocations.push(location);
      } else {
        console.log(error);
        await slack.error(
          `${config.get('db.name')} - CQC changes`, 
          `${error}`
        );
        updateStatus(location, `failed: ${error.message}`);
      }
    }
  };

  const updateStatus = (location, status) => {
    // updates status regardless of whether in locations array or rateLimitExceededLocations array
    location.status = status;
  }

  module.exports.updateLocation = updateLocation;