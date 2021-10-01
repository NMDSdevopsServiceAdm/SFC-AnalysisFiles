const axios = require('axios');
const axiosRetry = require('axios-retry');

const config = require('../../config/index');
const models = require('../models/index');
const slack = require('../utils/slack/slack-logger');

axiosRetry(axios, { retries: 3 });

const cqcEndpoint = config.get('cqcApiUrl')

const updateLocation = async (location) => {
    try {
      console.log('Getting information about ' + location.locationId + ' from CQC');
      const individualLocation = await axios.get(cqcEndpoint + '/locations/' + location.locationId);

      if (!individualLocation.data.deregistrationDate) {
        // not deregistered so must exist
        console.log('Updating/inserting information into database');
        await models.location.updateLocation(individualLocation);
      } else {
        await models.location.deleteLocation(location.locationId);
      }
      
      updateStatus(location, 'success');
    } catch (error) {
      if (error.response.data.message && error.response.data.message.indexOf('No Locations found') > -1) {
        await models.location.deleteLocation(location.locationId);
        updateStatus(location, 'success');
      } else {
        console.log(error);
        await slack.error(
          'CQC changes', 
          `${error}`
        );
        updateStatus(location, `failed: ${error.message}`);
      }
    }
  };

  const updateStatus = (location, status) => {
    location.status = status;
  }

  module.exports.updateLocation = updateLocation;