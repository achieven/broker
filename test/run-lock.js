// The suite uses fixed container names and ports (and one test stops a broker), so two runs at the same time
// corrupt each other: one run's teardown kills the other's containers, a stopped broker fails the other's tests.
// Refuse to start while another run is alive instead of producing mysterious failures.
const fs = require('fs');
const path = require('path');

const LOCK = path.join(__dirname, '.run.lock');

const alive = (pid) => { try { process.kill(pid, 0); return true; } catch (e) { return e.code === 'EPERM'; } };

exports.acquire = () => {
    try {
        const other = Number(fs.readFileSync(LOCK, 'utf8'));
        if (other && other !== process.pid && alive(other)) {
            throw new Error(
                `Another integration test run is in progress (pid ${other}). The runs share containers and ports, ` +
                `so they would break each other. Wait for it to finish (or kill it) and retry.`,
            );
        }
    } catch (e) {
        if (e.code !== 'ENOENT') throw e;
    }
    fs.writeFileSync(LOCK, String(process.pid));
};

exports.holdsLock = () => { try { return Number(fs.readFileSync(LOCK, 'utf8')) === process.pid; } catch (e) { return false; } };

exports.release = () => {
    try { if (Number(fs.readFileSync(LOCK, 'utf8')) === process.pid) fs.unlinkSync(LOCK); } catch (e) { /* already gone */ }
};
