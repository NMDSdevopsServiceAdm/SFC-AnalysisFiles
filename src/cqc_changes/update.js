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
        console.log('Updating/Inserting information into database');
        await models.location.upsert({
          locationid: individualLocation.data.locationId,
          locationname: individualLocation.data.name,
          addressline1: individualLocation.data.postalAddressLine1,
          addressline2: individualLocation.data.postalAddressLine2,
          towncity: individualLocation.data.postalAddressTownCity,
          county: individualLocation.data.postalAddressCounty,
          postalcode: individualLocation.data.postalCode,
          mainservice:
            individualLocation.data.gacServiceTypes.length > 0 ? individualLocation.data.gacServiceTypes[0].name : null,
        });
      } else {
        await models.location.deleteLocation(location.locationId);
      }
      updateStatus(location, 'success');
    } catch (error) {
      await slack.error(
        'CQC changes', 
        `${error}`
      );
      if (error.response.data.message && error.response.data.message.indexOf('No Locations found') > -1) {
        await models.location.deleteLocation(location.locationId);
        updateStatus(location, 'success');
      } else {
        console.log(error);
        updateStatus(location, `failed: ${error.message}`);
      }
    }
  };

  const updateStatus = (location, status) => {
    location.status = status;
  }

  module.exports.updateLocation = updateLocation;