/**
 * Implementation of CreateSimulationFileTable.c3typ
 * @param {StressTestJob} jobtype
 * @param {StressTestJobOptions} joboptions
 */
function doStart(job, options) {
    for (var i = 0; i < options.nBatches; i++) {
        var batch = [];
        for (var j = 0; j < options.batchSize; j++) {
            batch.push(j);
        }
        var batchSpec = StressTestJobBatch.make({values: batch});
        job.scheduleBatch(batchSpec);
    };
}

/**
 * @param {StressTestJobBatch} batch
 * @param {StressTestJob} job
 * @param {StressTestJobOptions} options
 */
 function processBatch(batch, job, options) {
    batch.values.forEach(function() {
        StressTestPlainCompute.run();
    });
}
