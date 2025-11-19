const config = require('../../../config');
const fs = require('fs');

const pg = require('knex')({
  client: 'pg',
  connection: {connectionString: config.get('db.url'),
      ssl: {
      rejectUnauthorized: true,
      ca: fs.readFileSync(config.get('db.sslCAPath'), 'utf8'),
    },
  },

  searchPath: ['public', 'cqc'],
});

module.exports = pg;



