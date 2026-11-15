const { execSync } = require('child_process');
const path = require('path');
const { acquire } = require('./run-lock');

module.exports = async () => {
    acquire();
    const compose = path.join(__dirname, 'docker-compose.yml');
    console.log('\nStarting Kafka x3 + Mongo + MySQL (docker compose)...');
    execSync(`docker compose -f ${compose} up -d --wait --wait-timeout 240`, { stdio: 'inherit' });
};
