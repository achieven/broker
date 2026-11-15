module.exports = {
    moduleFileExtensions: ['js', 'json', 'ts'],
    rootDir: '.',
    testRegex: 'test/.*\\.int-spec\\.ts$',
    transform: { '^.+\\.ts$': ['ts-jest', { tsconfig: 'test/tsconfig.json' }] },
    testEnvironment: 'node',
    globalSetup: '<rootDir>/test/global-setup.js',
    globalTeardown: '<rootDir>/test/global-teardown.js',
    testTimeout: 180000,
    setupFilesAfterEnv: ['<rootDir>/test/after-each.ts'],
    setupFiles: ['<rootDir>/test/quiet-logs.js'],
    maxWorkers: 1, // suites share one cluster and one set of proxy ports
};
