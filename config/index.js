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
      default: false,
    },
  },
  db: {
    url: {
      doc: 'Database URL',
      format: '*',
      default: 'postgres://sfcadmin:unknown@localhost:5432/sfcdevdb',
      env: 'DATABASE_URL',
    },
    name: {
      doc: 'Service name',
      format: String,
      default: 'localhost',
      env: 'SERVICE_NAME',
    },
    dialect: {
      doc: 'Database dialect (sequelize)',
      format: String,
      default: 'postgres',
    },
  },
  s3: {
    bucket: {
      doc: 'Bucket to upload the reports to',
      default: 'sfcreports',
      env: 'REPORTS_S3_BUCKET',
    },
    benchmarksBucket: {
      doc: 'Bucket with latest benchmarks files',
      default: 'sfc-benchmark-upload-dev',
      env: 'BENCHMARKS_S3_BUCKET',
    },
    dataEngineeringBucket: {
      doc: 'Data engineering bucket for analysis reports',
      default: 'sfc-data-engineering-raw',
      env: 'DATA_ENGINEERING_S3_BUCKET',
    },
  },
  cron: {
    doc: 'When it should run',
    default: '0 0 1,8,15,23 * *',
    env: 'CRON',
  },
  cronBenchmarks: {
    doc: 'When benchmarks update should run',
    default: '0 0 * * *',
    env: 'CRON_BENCHMARKS',
  },
  cronCqcChanges: {
    doc: 'When CQC locations update should run',
    default: '0 0 * * *',
    env: 'CRON_CQC_CHANGES',
  },
  dataEngineering: {
    accessKey: {
      doc: 'Access key for data engineering AWS',
      default: 'bob',
      env: 'DATA_ENGINEERING_ACCESS_KEY',
    },
    secretKey: {
      doc: 'Secret key for data engineering AWS',
      default: 'bob',
      env: 'DATA_ENGINEERING_SECRET_KEY',
    },
    uploadToDataEngineering: {
      doc: 'Whether to upload reports to data engineering AWS',
      format: 'Boolean',
      default: false,
    },
  },
  reports: {
    accessKey: {
      doc: 'Access key for reports AWS',
      default: 'change_me',
      env: 'REPORTS_ACCESS_KEY',
    },
    secretKey: {
      doc: 'Secret key for reports AWS',
      default: 'change_me',
      env: 'REPORTS_SECRET_KEY',
    },
  },
  environment: {
    doc: 'Which environment is this?',
    default: 'example',
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
        default: false,
      },
      wallet: {
        doc: 'The name of the AWS Secrets Manager wallet to recall from',
        format: String,
        default: 'bob',
      },
    },
  },
  cqcApi: {
    url: {
      doc: 'The API endpoint for CQC',
      format: 'url',
      default: 'https://api.service.cqc.org.uk/public/v1',
    },
    subscriptionKey: {
      doc: 'The subscription key passed in CQC API calls',
      format: String,
      default: '',
      env: 'CQC_SUBSCRIPTION_KEY',
    },
  },
  slack: {
    url: {
      doc: 'The slack notification endpoint',
      format: 'url',
      default: 'unknown', // note - bug in notify - must provide a default value for it to use env var
      env: 'SLACK_URL',
    },
    benchmarksUrl: {
      doc: 'update benchmarks slack notification endpoint',
      format: 'url',
      default: 'unknown',
      env: 'SLACK_BENCHMARKS_URL',
    },
    analysisFileUrl: {
      doc: 'update analysis file slack notification endpoint',
      format: 'url',
      default: 'unknown',
      env: 'SLACK_ANALYSIS_FILE_URL',
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
  getAddress: {
    apiKey: {
      doc: 'getAddress api key',
      env: 'GET_ADDRESS_API_KEY',
      default: 'api_key',
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
    console.log('Setting AWS details');
    // config.set('encryption.private', AWSSecrets.encryptionPrivate());
    // config.set('encryption.public', AWSSecrets.encryptionPublic());
    // config.set('encryption.passphrase', AWSSecrets.encryptionPassphrase());

    if (config.get('dataEngineering.uploadToDataEngineering')) {
      config.set('dataEngineering.accessKey', AWSSecrets.dataEngineeringAccessKey());
      config.set('dataEngineering.secretKey', AWSSecrets.dataEngineeringSecretKey());
    }
  });
}

module.exports = config;
