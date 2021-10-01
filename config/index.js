var convict = require('convict');
const AWSSecrets = require('../aws/secrets');
const fs = require('fs');
const yaml = require('js-yaml');

convict.addFormat(require('convict-format-with-validator').url);

var config = convict({
  log: {
    sequelize: {
      doc: 'Whether to log sequelize SQL statements',
      format: 'Boolean',
      default: false
    }
  },
  db: {
    url: {
      doc: 'Database URL',
      format: '*',
      default: null,
      env: 'DATABASE_URL',
    },
    host: {
      doc: 'Database host name/IP',
      format: String,
      default: 'localhost',
      env: 'DB_HOST'
    },
    port: {
      doc: 'Database port',
      format: 'port',
      default: 5432,
      env: 'DB_PORT'
    },
    database: {
      doc: 'Database name',
      format: String,
      default: 'sfcdevdb',
      env: 'DB_NAME'
    },
    username: {
      doc: 'Database username',
      format: String,
      default: 'sfcadmin',
      env: 'DB_USER'
    },
    password: {
      doc: 'Database username',
      format: '*',
      default: 'unknown',
      env: 'DB_PASS'
    },
    dialect: {
      doc: 'Database dialect (sequelize)',
      format: String,
      default: 'postgres'
    },
    ssl: {
      doc: 'Use SSL?',
      format: 'Boolean',
      default: false
    },
    client_ssl: {
      status: {
        doc: 'Client SSL enabled or not',
        format: 'Boolean',
        default: false
      },
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
  cronCqcChanges: {
    doc: 'When CQC locations update should run',
    default: '0 0 * * *',
    env: 'CRON_CQC_CHANGES',
  },
  environment: {
    doc: 'Which environment is this?',
    default: '',
    env: 'NODE_ENV',
  },
  aws: {
    region: {
      doc: 'AWS region',
      format: '*',
      default: 'eu-west-2',
    },
    secrets: {
      use: {
        doc:
          'Whether to use AWS Secret Manager to retrieve sensitive information, e.g. ENCRYPTION_PRIVATE_KEY. If false, expect to read from environment variables.',
        format: 'Boolean',
        default: true,
      },
      wallet: {
        doc: 'The name of the AWS Secrets Manager wallet to recall from',
        format: String,
        default: 'bob',
      },
    },
  },
  cqcApiUrl: {
    doc: 'The API endpoint for CQC',
    default: 'https://api.cqc.org.uk/public/v1',
  },
  slack: {
    url: {
      doc: 'The slack notification endpoint',
      format: 'url',
      default: 'unknown', // note - bug in notify - must provide a default value for it to use env var
      env: 'SLACK_URL',
    },
    level: {
      doc: 'The level of notifications to be sent to Slack: 0 - disabled, 1-error, 2-warning, 3-info, 5 - trace',
      format: function check(val) {
        if (![0, 1, 2, 3, 5].includes(val)) throw new TypeError('Slack level must be one of 0, 1, 2, 3 or 5');
      },
      env: 'SLACK_LEVEL',
      default: 3,
    },
  },
});
// Load environment dependent configuration
var env = config.get('environment');

const envConfigfile = yaml.safeLoad(fs.readFileSync(__dirname + '/' + env + '.yaml'));

// load common file first, then env (so env overrides common)
config.load(envConfigfile);

// Perform validation
config.validate({ allowed: 'strict' });

if (config.get('aws.secrets.use')) {
  console.log('Using AWS Secrets');
  AWSSecrets.initialiseSecrets(config.get('aws.region'), config.get('aws.secrets.wallet')).then(() => {
    // DB rebind
    console.log('Setting AWS details');
    config.set('encryption.private', AWSSecrets.encryptionPrivate());
    config.set('encryption.public', AWSSecrets.encryptionPublic());
    config.set('encryption.passphrase', AWSSecrets.encryptionPassphrase());
  });
}

module.exports = config;
