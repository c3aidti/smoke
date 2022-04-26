/**
 * Function to call Plain Computation test in the BatchQueue
 * @param nBatches: number of batches to be sent
 * @param batchSize: number of calculations in each batch
 *
 * @return: BatchJob object
 */
function runBatch(nBatches, batchSize) {
    var jobOptions = StressTestPCJobOptions.make({nBatches: nBatches, batchSize: batchSize});
    var job = StressTestPCJob.make({options: jobOptions}).upsert();
    job.start();
    return job;
}
