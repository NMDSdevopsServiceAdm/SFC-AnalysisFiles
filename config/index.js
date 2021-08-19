var convict = require('convict');
const AWSSecrets = require('../aws/secrets');
const fs = require('fs');
const yaml = require('js-yaml');

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
  // encryption: {
  //   private: {
  //     doc: 'encryption private key',
  //     default: '',
  //     env: 'ENCRYPTION_PRIVATE_KEY',
  //   },
  //   public: {
  //     doc: 'encryption public key',
  //     default: '',
  //     env: 'ENCRYPTION_PUBLIC_KEY',
  //   },
  //   passphrase: {
  //     doc: 'encryption passphrase key',
  //     default: '',
  //     env: 'ENCRYPTION_PASSPHRASE',
  //   },
  // },
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
