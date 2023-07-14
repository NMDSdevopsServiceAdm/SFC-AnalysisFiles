const db = require('../src/generate_analysis_files/db');
const axios = require('axios');
const config = require('../config');

const postcodes = ['CR03FP'];

const getLongitudeLatitude = async () => {
  console.log('Start update');
  const transaction = await db.transaction();

  const api_key = config.get('getAddress.apiKey');
  await Promise.all(
    postcodes.map(async (postcode) => {
      const { data } = await axios.get(`https://api.getAddress.io/find/${postcode}?api-key=${api_key}&expand=true`);
      let formattedPostcode;
      if (postcode.length === 7) {
        formattedPostcode = postcode.substring(0, 4) + ' ' + postcode.substring(4);
      } else if (postcode.length === 5) {
        formattedPostcode = postcode.substring(0, 2) + ' ' + postcode.substring(2);
      } else {
        formattedPostcode = postcode.substring(0, 3) + ' ' + postcode.substring(3);
      }
      await transaction('postcodes')
        .withSchema('cqcref')
        .where({ postcode: formattedPostcode })
        .update({ longitude: data.longitude, latitude: data.latitude });
    }),
  );

  await transaction.commit();
  console.log('db updated');
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
