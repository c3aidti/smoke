/**
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
        StressTestSuite.run();
    });
}


/**
 * @param {StressTestJob} job
 * @param {StressTestJobOptions} options
 */
function allComplete(job, options) {
    var goodJob = StressTestBatchRecord.make({
        testType: "all",
        dateStarted: job.status().started,
        dateComplete: job.status().completed,
        nBatches: options.nBatches,
        batchSize: options.batchSize,
        jobReference: c3Make("map<string, string>",
        {"jobType": "StressTestJob",
            "id": job.id}),
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
        testType: "all",
        dateStarted: job.status().started,
        dateComplete: job.status().completed,
        nBatches: options.nBatches,
        batchSize: options.batchSize,
        jobReference: c3Make("map<string, string>",
        {"jobType": "StressTestJob",
            "id": job.id}),
        testFailed: true,
        failedBatches: status.errors.count
    });
    badJob.upsert();
}