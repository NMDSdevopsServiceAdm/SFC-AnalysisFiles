const Bree = require('bree');

const scheduler = new Bree({
  jobs: [
    {
      name: 'generate_analysis_files',
      interval: '8h',
      timeout: 0,
    },
  ],
});

scheduler.start();
