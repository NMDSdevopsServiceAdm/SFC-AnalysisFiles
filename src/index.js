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
      cron: config.get('cronBenchmarks'),
    },
    {
      name: 'cqc_changes',
      cron: config.get('cronCqcChanges'),
    },
    {
      name: 'nhs_bsa_report',
      cron: config.get('cronUploadToNhsBsaBucket'),
    },
  ],
});

scheduler.start();
