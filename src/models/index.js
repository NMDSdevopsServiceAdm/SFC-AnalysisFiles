'use strict';

const config = require('../../config/index');
const fs = require('fs');
const path = require('path');
const Sequelize = require('sequelize');
const basename = path.basename(__filename);
const db = {};
const cfenv =  require('cfenv');
const { log } = require('util');

const dbConfig = {
  pool: {
    max: 10000,
    min: 0,
    idle: 200000,
    acquire: 200000
  },
  retry: { max: 3 },
  uri: cfenv.getAppEnv({
    vcap: {
      services: {
        postgres: [
          {
            name: 'localhost',
            credentials: {
              uri: config.get('db.url')
            }
          }
        ]
      }
    }
  }).getServiceCreds(config.get('db.url')).uri,
  dialect: config.get('db.dialect'),
  logging: config.get('log.sequelize'),

};

console.log({dbConfig:dbConfig});
const sequelize = new Sequelize(dbConfig.uri, dbConfig);

fs
  .readdirSync(__dirname)
  .filter(file => {
    return (file.indexOf('.') !== 0) && (file !== basename) && (file.slice(-3) === '.js');
  })
  .forEach(file => {
    const model = require(path.join(__dirname, file))(sequelize, Sequelize.DataTypes);
    db[model.name] = model;
  });

Object.keys(db).forEach(modelName => {
  if (db[modelName].associate) {
    db[modelName].associate(db);
  }
});

db.sequelize = sequelize;
db.Sequelize = Sequelize;

module.exports = db;
