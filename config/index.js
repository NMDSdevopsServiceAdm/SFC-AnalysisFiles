var convict = require('convict');

var config = convict({
  db: {
    url: {
      doc: 'Database URL',
      format: '*',
      default: null,
      env: 'DATABASE_URL',
    },
  },
  s3: {
    bucket: {
      doc: 'Bucket to upload the reports to',
      default: 'sfcreports',
      env: 'REPORTS_S3_BUCKET',
    },
  },
  cron: {
    doc: 'When it should run',
    default: '0 0 1,15 * *',
    env: 'CRON',
  },
  environment: {
    doc: 'Which environment is this?',
    default: '',
    env: 'NODE_ENV',
  },
});

// Perform validation
config.validate({ allowed: 'strict' });

module.exports = config;
