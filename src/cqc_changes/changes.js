const axios = require('axios');
const axiosRetry = require('axios-retry');
const config = require('../../config/index')

axiosRetry(axios, { retries: 3, retryDelay: axiosRetry.exponentialDelay });

const cqcEndpoint = config.get('cqcApiUrl')

const getChangedIds = async (startTimestamp, endTimestamp) => {
    let changes = [];
    let nextPage='/changes/location?page=1&perPage=1000&startTimestamp=' + startTimestamp + '&endTimestamp=' + endTimestamp;
    console.log('Getting the changes');

    do {
      const changeUrl = cqcEndpoint + nextPage;
      const response = await axios.get(changeUrl);
      nextPage = response.data.nextPageUri;
            
      response.data.changes.forEach((location) => {
        changes.push({
          'locationId': location,
          'status': ''
        });
      });
      console.log(`Got ${changes.length} locations out of ${response.data.total}`);
    } while (nextPage != null);
    console.log('There are ' + changes.length + ' changes');
    return changes;
  }

  module.exports.getChangedIds = getChangedIds;