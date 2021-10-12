const { run } = require('../src/cqc_changes/index');

(async () => {
  run()
    .then(() => {
      process.exit(0);
    })
    .catch((err) => {
      console.error(err);
      process.exit(1);
    });
})();