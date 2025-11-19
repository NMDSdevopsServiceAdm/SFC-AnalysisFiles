'use strict';

const config = require('../../config/index');
const fs = require('fs');
const path = require('path');
const Sequelize = require('sequelize');
const basename = path.basename(__filename);
const db = {};

const dbUri = process.env.DATABASE_URL || config.get('db.url');

const dbConfig = {
  pool: {
    max: 10000,
    min: 0,
    idle: 200000,
    acquire: 200000,
  },
  retry: { max: 3 },
  dialect: config.get('db.dialect'),
  logging: config.get('log.sequelize'),
  dialectOptions: {
    ssl: {
      rejectUnauthorized: true,
      ca: fs.readFileSync(config.get('db.sslCAPath'), 'utf8'),
    }
  }
};


const sequelize = new Sequelize(dbUri, dbConfig);

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
