const config = require('../../../config');
const fs = require('fs');


console.log("CA exists:", config.get("db.sslCAPath"), fs.existsSync(config.get("db.sslCAPath")));


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



