const config = require('../../../config');
const fs = require('fs');

const isLocalhost = config.get('db.url').includes('localhost')
const sslConfig = isLocalhost ? null : {
      rejectUnauthorized: true,
      ca: fs.readFileSync(config.get('db.sslCAPath'), 'utf8'),
    }

const pg = require('knex')({
  client: 'pg',
  connection: {connectionString: config.get('db.url'),
      ssl: sslConfig ,
  },

  searchPath: ['public', 'cqc'],
});

module.exports = pg;



