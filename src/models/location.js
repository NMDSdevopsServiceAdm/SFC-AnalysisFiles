'use strict';
module.exports = (sequelize, DataTypes) => {
  const location = sequelize.define('location', {
    locationname: {
      type: DataTypes.TEXT,
      allowNull: true
    },
    addressline1: {
      type: DataTypes.TEXT,
      allowNull: true
    },
    addressline2: {
      type: DataTypes.TEXT,
      allowNull: true
    },
    towncity: {
      type: DataTypes.TEXT,
      allowNull: true
    },
    county: {
      type: DataTypes.TEXT,
      allowNull: true
    },
    postalcode: {
      type: DataTypes.TEXT,
      allowNull: true
    },
    mainservice: {
      type: DataTypes.TEXT,
      allowNull: true
    },
    createdat: {
      type: DataTypes.DATE,
      allowNull: false
    },
    updatedat: {
      type: DataTypes.DATE,
      allowNull: true
    },
    locationid: {
      type: DataTypes.TEXT,
      allowNull: true,
      unique: true
    }
  }, {
    schema: 'cqcref',
    createdAt: 'createdat',
    updatedAt: 'updatedat',
    freezeTableName: true
  });

  location.removeAttribute('id');

  location.deleteLocation = async function(locationid) {
    return await this.destroy({
      where: {
        locationid,
      },
    })
  }

  location.updateLocation = async function(location) {
    return await this.upsert({
      locationid: location.data.locationId,
      locationname: location.data.name,
      addressline1: location.data.postalAddressLine1,
      addressline2: location.data.postalAddressLine2,
      towncity: location.data.postalAddressTownCity,
      county: location.data.postalAddressCounty,
      postalcode: location.data.postalCode,
      mainservice:
        location.data.gacServiceTypes.length > 0 ? location.data.gacServiceTypes[0].name : null,
    });
  }

  return location;
};
