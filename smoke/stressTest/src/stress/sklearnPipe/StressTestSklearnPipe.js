/**
 * Function to call SklearnPipe test in the BatchQueue
 * @param nBatches: number of batches to be sent
 * @param batchSize: number of calculations in each batch
 *
 * @return: BatchJob object
 */
 function runBatch(nBatches, batchSize) {
    var jobOptions = StressTestSPJobOptions.make({nBatches: nBatches, batchSize: batchSize});
    var job = StressTestSPJob.make({options: jobOptions}).upsert();
    job.start();
    return job;
}