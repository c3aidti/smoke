/**
 * Function to call Cassandra test in the BatchQueue
 * @param nBatches: number of batches to be sent
 * @param batchSize: number of calculations in each batch
 *
 * @return: BatchJob object
 */
 function runBatch(nBatches, batchSize) {
    var jobOptions = StressTestCSJobOptions.make({nBatches: nBatches, batchSize: batchSize});
    var job = StressTestCSJob.make({options: jobOptions}).upsert();
    job.start();
    return job;
}