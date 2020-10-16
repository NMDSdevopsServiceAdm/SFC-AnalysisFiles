const Bree = require('bree');

const scheduler = new Bree({
  jobs: [
    {
      name: 'generate_analysis_files',
      cron: '0 0 1,15 * *',
    },
  ],
});

scheduler.start();
