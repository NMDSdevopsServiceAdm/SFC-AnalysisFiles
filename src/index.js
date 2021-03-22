const Bree = require('bree');
const config = require('../config');

const scheduler = new Bree({
  jobs: [
    {
      name: 'generate_analysis_files',
      cron: config.get('cron'),
    },
  ],
});

scheduler.start();
