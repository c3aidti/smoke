/**
 * @param {StressTestSPJob} jobtype
 * @param {StressTestSPJobOptions} joboptions
 */
 function doStart(job, options) {
    for (var i = 0; i < options.nBatches; i++) {
        var batch = [];
        for (var j = 0; j < options.batchSize; j++) {
            batch.push(j);
        }
        var batchSpec = StressTestSPJobBatch.make({values: batch});
        job.scheduleBatch(batchSpec);
    };
}

/**
 * @param {StressTestSPJobBatch} batch
 * @param {StressTestSPJob} job
 * @param {StressTestSPJobOptions} options
 */
 function processBatch(batch, job, options) {
    batch.values.forEach(function() {
        StressTestSklearnPipe.run();
    });
}


/**
 * @param {StressTestSPJob} job
 * @param {StressTestJobSPOptions} options
 */
function allComplete(job, options) {
    var goodJob = StressTestBatchRecord.make({
        testType: "sklearnPipe",
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
 * @param {StressTestSPJob} job
 * @param {BatchJobStatus} jobStatus
 * @param {StressTestSPJobOptions} options
 */
 function failed(job, status, options) {
    var badJob = StressTestBatchRecord.make({
        testType: "sklearnPipe",
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