const config = require('../../config');

const pg = require('knex')({
  client: 'pg',
  connection: config.get('db.url'),
  searchPath: ['public', 'cqc'],
});

module.exports = pg;
