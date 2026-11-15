const { execSync } = require('child_process');
const path = require('path');
const { holdsLock, release } = require('./run-lock');

module.exports = async () => {
    if (!holdsLock()) return; // never tear down containers that another run owns
    release();
    if (process.env.KEEP_TEST_CONTAINERS) return; // handy while iterating: KEEP_TEST_CONTAINERS=1 npm run test:integration
    execSync(`docker compose -f ${path.join(__dirname, 'docker-compose.yml')} down -v`, { stdio: 'inherit' });
};
