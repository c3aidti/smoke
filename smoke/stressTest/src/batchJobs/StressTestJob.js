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


/**
 * @param {StressTestJob} job
 * @param {StressTestJobOptions} options
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
 * @param {StressTestJob} job
 * @param {BatchJobStatus} jobStatus
 * @param {StressTestJobOptions} options
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