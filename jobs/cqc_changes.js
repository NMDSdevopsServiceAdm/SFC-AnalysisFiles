const axios = require('axios');
const axiosRetry = require('axios-retry');
const models = require('./models/index');

//CQC Endpoint
const url = 'https://api.cqc.org.uk/public/v1';

axiosRetry(axios, { retries: 3 });

const changes = async () => {
  console.log('Looking for latest run');
  const log = await models.cqclog.findAll({
    limit: 1,
    where: {
      success: true
    },
    order: [ [ 'createdat', 'DESC' ]]
  });

  const startDate = log ? log[0].dataValues.lastUpdatedAt : null;
  const endDate = new Date().toISOString().split('.')[0]+'Z';
  console.log('Was last run on ' + startDate);

  const locations = await getChangedIds(startDate, endDate);
  await Promise.all(locations.map(async (location) => {
    await updateLocation(location);
  }));

  await updateComplete(locations, endDate);
  models.sequelize.close();

  return {
    status: 200,
    body: 'Call Successful'
  };
}

// Get a list of all the CQC Location ID's that jhave changed between 2 timestamps
const getChangedIds = async (startTimestamp, endTimestamp) => {
  let changes = [];
  let nextPage='/changes/location?page=1&perPage=1000&startTimestamp=' + startTimestamp + '&endTimestamp=' + endTimestamp;
  console.log('Getting the changes');
  do {
    const changeUrl = url + nextPage;
    const response = await axios.get(changeUrl);
    nextPage = response.data.nextPageUri;

    response.data.changes.forEach((location) => {
      changes.push({
        'locationId': location,
        'status': ''
      });
    })
  } while (nextPage != null);
  console.log('There are ' + changes.length + ' changes');
  return changes;
}

const updateLocation = async (location) => {
  try {
    console.log('Getting information about ' + location.locationId + ' from CQC');
    const individualLocation = await axios.get(url + '/locations/' + location.locationId);
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
      await deleteLocation(location);
    }

    updateStatus(location, 'success');
    return {
      status: 200,
      body: 'Call Successful',
    };
  } catch (error) {
    console.log(error);
    if (error.response.data.message && error.response.data.message.indexOf('No Locations found') > -1) {
      await deleteLocation(location);
      updateStatus(location, 'success');
    } else {
      updateStatus(location, `failed: ${error.message}`);
    }
    return error.message;
  }
};

const deleteLocation = async (location) => {
  await models.location.destroy({
    where: {
      locationid: location.locationId,
    },
  });
}

const updateComplete = async (locations, endDate) => {
  console.log('Update complete');
  console.log('Checking to see failure status');

  let completionCount = 0;
  let failed = false;

  locations.forEach((location) => {
    if (location.status !== '') {
      completionCount++;
    }
    if (location.status !== 'success') {
      failed = true;
    }
  });

  console.log('Checked ' + completionCount);
  if (failed || completionCount !== locations.length) {
    console.log('One or more updates failed');
    await models.cqclog.create({
      success: false,
      message: 'Failed',
    });
    return;
  }

  console.log('All went successfully');
  await models.cqclog.create({
    success: true,
    message: 'Call Successful',
    lastUpdatedAt: endDate,
  });
}

const updateStatus = (location, status) => {
  location.status = status;
}

const run = async () => {
  try {
    return await changes();
  } catch (error) {
    return  error.message;
  }
};

(async () => {
  run()
    .then(() => {
      process.exit(0);
    })
    .catch((err) => {
      console.error(err);
      process.exit(1);
    });
})();