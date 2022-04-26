/**
 * Implementation of CreateSimulationFileTable.c3typ
 * @param {StressTestPCJob} jobtype
 * @param {StressTestPCJobOptions} joboptions
 */
function doStart(job, options) {
    for (var i = 0; i < options.nBatches; i++) {
        var batch = [];
        for (var j = 0; j < options.batchSize; j++) {
            batch.push(j);
        }
        var batchSpec = StressTestPCJobBatch.make({values: batch});
        job.scheduleBatch(batchSpec);
    };
}

/**
 * @param {StressTestPCJobBatch} batch
 * @param {StressTestPCJob} job
 * @param {StressTestPCJobOptions} options
 */
 function processBatch(batch, job, options) {
    batch.values.forEach(function() {
        StressTestPlainCompute.run();
    });
}


/**
 * @param {StressTestPCJob} job
 * @param {StressTestJobPCOptions} options
 */
function allComplete(job, options) {
    var goodJob = StressTestBatchRecord.make({
        testType: "plainCompute",
        dateStarted: job.status().started,
        dateComplete: job.status().completed,
        nBatches: options.nBatches,
        batchSize: options.batchSize,
        jobObject: job,
        testFailed: false,
        failedBatches: 0
    });
    goodJob.upsert();
}


/**
 * @param {StressTestPCJob} job
 * @param {BatchJobStatus} jobStatus
 * @param {StressTestPCJobOptions} options
 */
 function failed(job, status, options) {
    var badJob = StressTestBatchRecord.make({
        testType: "plainCompute",
        dateStarted: job.status().started,
        dateComplete: job.status().completed,
        nBatches: options.nBatches,
        batchSize: options.batchSize,
        jobObject: job,
        testFailed: true,
        failedBatches: status.errors.count
    });
    badJob.upsert();
}