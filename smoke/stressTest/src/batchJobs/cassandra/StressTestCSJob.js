/**
 * @param {StressTestCSJob} jobtype
 * @param {StressTestCSJobOptions} joboptions
 */
 function doStart(job, options) {
    for (var i = 0; i < options.nBatches; i++) {
        var batch = [];
        for (var j = 0; j < options.batchSize; j++) {
            batch.push(j);
        }
        var batchSpec = StressTestCSJobBatch.make({values: batch});
        job.scheduleBatch(batchSpec);
    };
}

/**
 * @param {StressTestCSJobBatch} batch
 * @param {StressTestCSJob} job
 * @param {StressTestCSJobOptions} options
 */
 function processBatch(batch, job, options) {
    batch.values.forEach(function() {
        StressTestCassandra.run();
    });
}


/**
 * @param {StressTestCSJob} job
 * @param {StressTestCSJobOptions} options
 */
function allComplete(job, options) {
    var goodJob = StressTestBatchRecord.make({
        testType: "cassandra",
        dateStarted: job.status().started,
        dateComplete: job.status().completed,
        nBatches: options.nBatches,
        batchSize: options.batchSize,
        jobReference: c3Make("map<string, string>",
        {"jobType": "StressTestCSJob",
            "id": job.id}),
        testFailed: false,
        failedBatches: 0
    });
    goodJob.upsert();
}


/**
 * @param {StressTestCSJob} job
 * @param {BatchJobStatus} jobStatus
 * @param {StressTestCSJobOptions} options
 */
 function failed(job, status, options) {
    var badJob = StressTestBatchRecord.make({
        testType: "cassandra",
        dateStarted: job.status().started,
        dateComplete: job.status().completed,
        nBatches: options.nBatches,
        batchSize: options.batchSize,
        jobReference: c3Make("map<string, string>",
        {"jobType": "StressTestCSJob",
            "id": job.id}),
        testFailed: true,
        failedBatches: status.errors.count
    });
    badJob.upsert();
}