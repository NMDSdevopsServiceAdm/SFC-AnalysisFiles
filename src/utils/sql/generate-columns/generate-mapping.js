const OLD_YESNO_TYPE_MAPPING = {
  Yes: 1,
  No: 2,
  "Don''t know": -1,
};

const NEW_YESNO_TYPE_MAPPING = {
  No: 0,
  Yes: 1,
  "Don''t know": -2,
};

const RATE_TYPE_MAPPING = {
  'Hourly rate': 1,
  'Flat rate': 2,
  'I do not know': -2,
};

module.exports = {
  OLD_YESNO_TYPE_MAPPING,
  NEW_YESNO_TYPE_MAPPING,
  RATE_TYPE_MAPPING,
};
