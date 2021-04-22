const AWS = require('aws-sdk');

let myLocalSecrets = null;

const initialiseSecrets = async (region, wallet) => {
  const secrets = new AWS.SecretsManager({
    region,
  });
  console.log('Initialising AWS Secret');
  try {
    if (!wallet) throw new Error('wallet must be defined');
    const mySecretsValue = await secrets
      .getSecretValue({ SecretId: wallet })
      .promise()
      .then((mySecretsValue) => {
        console.log('Got secrets from AWS');
        return mySecretsValue;
      })
      .catch((error) => {
        console.error(error);
        throw error;
      });

    console.log('Checking Secret');
    if (typeof mySecretsValue.SecretString !== 'undefined') {
      console.log('Got Secrets');
      const mySecrets = JSON.parse(mySecretsValue.SecretString);

      if (typeof mySecrets == 'undefined') {
        throw new Error(`Unexpected parsing of secrets wallet: ${wallet}`);
      }

      myLocalSecrets = {
        ENCRYPTION_PRIVATE_KEY: mySecrets.ENCRYPTION_PRIVATE_KEY,
        ENCRYPTION_PUBLIC_KEY: mySecrets.ENCRYPTION_PUBLIC_KEY,
        ENCRYPTION_PASSPHRASE: mySecrets.ENCRYPTION_PASSPHRASE,
      };
    }
  } catch (err) {
    console.error('Failed to load AWS secrets: ', err);
  }
};

const encryptionPrivate = () => {
  if (myLocalSecrets !== null) {
    if (!myLocalSecrets.ENCRYPTION_PRIVATE_KEY) {
      throw new Error('Unknown ENCRYPTION_PRIVATE_KEY secret');
    } else {
      return myLocalSecrets.ENCRYPTION_PRIVATE_KEY;
    }
  } else {
    throw new Error('Unknown secrets');
  }
};

const encryptionPublic = () => {
  if (myLocalSecrets !== null) {
    if (!myLocalSecrets.ENCRYPTION_PUBLIC_KEY) {
      throw new Error('Unknown ENCRYPTION_PUBLIC_KEY secret');
    } else {
      return myLocalSecrets.ENCRYPTION_PUBLIC_KEY;
    }
  } else {
    throw new Error('Unknown secrets');
  }
};

const encryptionPassphrase = () => {
  if (myLocalSecrets !== null) {
    if (!myLocalSecrets.ENCRYPTION_PASSPHRASE) {
      throw new Error('Unknown ENCRYPTION_PASSPHRASE secret');
    } else {
      return myLocalSecrets.ENCRYPTION_PASSPHRASE;
    }
  } else {
    throw new Error('Unknown secrets');
  }
};

module.exports = {
  initialiseSecrets,
  encryptionPassphrase,
  encryptionPublic,
  encryptionPrivate,
};
