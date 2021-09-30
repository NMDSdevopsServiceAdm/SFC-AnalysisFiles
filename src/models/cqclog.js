'use strict';
module.exports = (sequelize, DataTypes) => {
  const cqclog = sequelize.define('cqclog', {
    success: DataTypes.BOOLEAN,
    message: DataTypes.STRING,
    lastUpdatedAt: DataTypes.STRING
  }, {
    schema: 'cqc',
    tableName: 'CqcLog',
    createdAt: 'createdat',
    updatedAt: false,
    freezeTableName: true
  });
  cqclog.associate = function(models) {
    // associations can be defined here
  };

  cqclog.findLatestRun = async function() {
    return await this.findOne({
      where: {
        success: true
      },
      order: [ [ 'createdat', 'DESC' ]]
    });
  }

  cqclog.createRecord = async function(success, endDate) {
    return await this.create({
      success,
      message: success ? 'Call Successful' : 'Failed',
      lastUpdatedAt: success ? endDate : undefined
    });
  }
  
  return cqclog;
};
