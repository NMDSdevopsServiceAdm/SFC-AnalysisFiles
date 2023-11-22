
'use strict';

const {run}  = require('./jobs/generate_analysis_files')



module.exports.handler = async () => {
    const handler = await run();
    return handler();
  }

