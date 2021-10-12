const axios = require('axios');
const axiosRetry = require('axios-retry');

const config = require('../../config/index');
const models = require('../models/index');
const slack = require('../utils/slack/slack-logger');

axiosRetry(axios, { retries: 3, retryDelay: axiosRetry.exponentialDelay });

const cqcEndpoint = config.get('cqcApiUrl')

const updateLocation = async (location, runCount, rateLimitExceededLocations, retrying) => {
    try {
      if (retrying || (runCount % 100 === 0)) {
        console.log(`Updated ${runCount} locations`);
      }

      const individualLocation = await axios.get(cqcEndpoint + '/locations/' + location.locationId);

      if (!individualLocation.data.deregistrationDate) {
        // not deregistered so must exist
        await models.location.updateLocation(individualLocation);
      } else {
        await models.location.deleteLocation(location.locationId);
      }
      
      updateStatus(location, 'success');
    } catch (error) {
      if (error.response.data.message && error.response.data.message.indexOf('No Locations found') > -1) {
        await models.location.deleteLocation(location.locationId);
        updateStatus(location, 'success');
      } else if (error.response.status === 429) {
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