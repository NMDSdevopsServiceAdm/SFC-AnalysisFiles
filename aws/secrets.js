'use strict';

const AWS = require('aws-sdk');
const config = require('../config/index');

let myLocalSecrets = null;

const initialiseSecrets = async (region, wallet) => {
  const secrets = new AWS.SecretsManager({
    region,
  });
  console.log('Initialising AWS Secret');
  console.log({dbConfigName:config.get('db.name'),dbConfig:config.get('db.url') });
  try {
  console.log('Initialising inside the try AWS Secret');
  console.log({dbConfigName:config.get('db.name'),dbConfig:config.get('db.url') });
    if (!wallet) throw new Error('wallet must be defined');
    const mySecretsValue = await secrets
      .getSecretValue({ SecretId: wallet })
      .promise()
      .then((mySecretsValue) => {
        return mySecretsValue;
      })
      .catch((error) => {
        console.error(error);
        throw error;
      });

    console.log('Checking Secret');
    console.log({dbConfigName:config.get('db.name'),dbConfig:config.get('db.url') });
    if (typeof mySecretsValue.SecretString !== 'undefined') {
      const mySecrets = JSON.parse(mySecretsValue.SecretString);

      if (typeof mySecrets == 'undefined') {
        throw new Error(`Unexpected parsing of secrets wallet: ${wallet}`);
      }

      myLocalSecrets = {
        ENCRYPTION_PRIVATE_KEY: mySecrets.ENCRYPTION_PRIVATE_KEY,
        ENCRYPTION_PUBLIC_KEY: mySecrets.ENCRYPTION_PUBLIC_KEY,
        ENCRYPTION_PASSPHRASE: mySecrets.ENCRYPTION_PASSPHRASE,
        DATA_ENGINEERING_ACCESS_KEY: mySecrets.DATA_ENGINEERING_ACCESS_KEY,
        DATA_ENGINEERING_SECRET_KEY: mySecrets.DATA_ENGINEERING_SECRET_KEY,
      };
    }
  } catch (err) {
    console.error('Failed to load AWS secrets: ', err);
  }
};

const getSecret = (secretName) => {
  if (myLocalSecrets === null) throw new Error('Unknown secrets');
  if (!myLocalSecrets[secretName]) throw new Error(`Unknown ${secretName} secret`);

  return myLocalSecrets[secretName];
}

const encryptionPrivate = () => getSecret('ENCRYPTION_PRIVATE_KEY');
const encryptionPublic = () => getSecret('ENCRYPTION_PUBLIC_KEY');
const encryptionPassphrase = () => getSecret('ENCRYPTION_PASSPHRASE');
const dataEngineeringAccessKey = () => getSecret('DATA_ENGINEERING_ACCESS_KEY');
const dataEngineeringSecretKey = () => getSecret('DATA_ENGINEERING_SECRET_KEY');

module.exports = {
  initialiseSecrets,
  encryptionPassphrase,
  encryptionPublic,
  encryptionPrivate,
  dataEngineeringAccessKey,
  dataEngineeringSecretKey,
};
