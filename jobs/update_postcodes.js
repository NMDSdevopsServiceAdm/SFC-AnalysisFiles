const db = require('../src/generate_analysis_files/db');
const axios = require('axios');
const config = require('../config');
const { postcodes } = require('./postcodes');

const getLongitudeLatitude = async () => {
  console.log('Start update');
  const transaction = await db.transaction();

  const api_key = config.get('getAddress.apiKey');

  for (let postcode of postcodes) {
    const { data } = await axios.get(`https://api.getAddress.io/find/${postcode}?api-key=${api_key}&expand=true`);
    await transaction('postcodes')
      .withSchema('cqcref')
      .where({ postcode })
      .update({ longitude: data.longitude, latitude: data.latitude });

    // api only allows a maximum of 300 requests per minute, so pause for 0.25s before making next call
    await pause();
  }

  await transaction.commit();
  console.log('db updated');
};

const pause = () => {
  return new Promise((resolve) => setTimeout(resolve, 250));
};

(async () => {
  getLongitudeLatitude()
    .then(() => {
      process.exit(0);
    })
    .catch(async (err) => {
      console.error(err);
      process.exit(1);
    });
})();
