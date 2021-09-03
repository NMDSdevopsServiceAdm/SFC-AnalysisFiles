const Bree = require('bree');
const config = require('../config');

const scheduler = new Bree({
  jobs: [
    {
      name: 'generate_analysis_files',
      cron: config.get('cron'),
    },
    {
      name: 'update_benchmarks',
      interval: '1m',
      // cron: config.get('cronBenchmarks'),
    },
  ],
});

scheduler.start();
