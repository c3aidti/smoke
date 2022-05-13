/**
 * Function to call stress test in the BatchQueue
 * @param nBatches: number of batches to be sent
 * @param batchSize: number of repetitions in each batch
 *
 * @return: BatchJob object
 */
 function runBatch(nBatches, batchSize) {
    var jobOptions = StressTestJobOptions.make({nBatches: nBatches, batchSize: batchSize});
    var job = StressTestJob.make({options: jobOptions}).upsert();
    job.start();
    return job;
}

/**
 * Function to call a single instance of the test
 */
function run() {
    // level 0
    StressTestPlainCompute.run();
    // level 1
    StressTestSklearnPipe.run();
    // level 2
    StressTestPostGres.run();
    // level 3
    StressTestCassandra.run();

    return;
}