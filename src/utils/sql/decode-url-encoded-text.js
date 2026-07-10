const { Transform } = require('stream');


const cleanTrainingName = (value) => {
  if (!value) return value;

  try {
    return decodeURIComponent(value).replace(/,/g, '');
  } catch {
    return value.replace(/,/g, '');
  }
};

const createDecodeTrainingNames = () =>
  new Transform({
    objectMode: true,
    transform(row, enc, cb) {
      if (row.training_name) {
        row.training_name = cleanTrainingName(row.training_name);
      }

      cb(null, row);
    },
  });

module.exports = {
  createDecodeTrainingNames
};