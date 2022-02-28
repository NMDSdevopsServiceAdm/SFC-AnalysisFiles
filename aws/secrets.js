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
        return mySecretsValue;
      })
      .catch((error) => {
        console.error(error);
        throw error;
      });

    console.log('Checking Secret');
    if (typeof mySecretsValue.SecretString !== 'undefined') {
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

const getSecret = (secretName) => {
  if (myLocalSecrets === null) throw new Error('Unknown secrets');
  if (!myLocalSecrets[secretName]) throw new Error(`Unknown ${secretName} secret`);

  return myLocalSecrets[secretName];
}

const encryptionPrivate = () => {
  return getSecret('ENCRYPTION_PRIVATE_KEY');
};

const encryptionPublic = () => {
  return getSecret('ENCRYPTION_PUBLIC_KEY');
};

const encryptionPassphrase = () => {
  return getSecret('ENCRYPTION_PASSPHRASE');
};

module.exports = {
  initialiseSecrets,
  encryptionPassphrase,
  encryptionPublic,
  encryptionPrivate,
};
