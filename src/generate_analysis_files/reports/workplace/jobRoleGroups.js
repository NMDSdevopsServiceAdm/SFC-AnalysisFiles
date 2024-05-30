// this should use the ASC id rather than the BUDI value
const jobRoleGroups = {
  directCare: [25, 10, 11, 12, 3, 29, 20, 16, 39].join(', '),
  manager: [26, 15, 13, 22, 28, 14, 30, 32, 34, 36, 37].join(', '),
  other: [2, 5, 21, 1, 19, 7, 8, 9, 6, 31, 33, 35, 38].join(', '),
  professional: [27, 18, 23, 4, 24, 17].join(', '),
};
exports.jobRoleGroups = jobRoleGroups;
