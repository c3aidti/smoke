/**
 * Function call Plain Computation in the BatchQueue
 * @param nBatches: number of batches to be sent
 * @param batchSize: number of calculations in each batch
 *
 * @return: BatchJob object
 */
function runBatch(nBatches, batchSize) {
    var jobOptions = StressTestJobOptions.make({nBatches: nBatches, batchSize: batchSize});
    var job = StressTestJob.make({options: jobOptions}).upsert();
    job.start();
    return job;
}
