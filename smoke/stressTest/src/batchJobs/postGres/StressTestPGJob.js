/**
 * @param {StressTestPGJob} jobtype
 * @param {StressTestPGJobOptions} joboptions
 */
 function doStart(job, options) {
    for (var i = 0; i < options.nBatches; i++) {
        var batch = [];
        for (var j = 0; j < options.batchSize; j++) {
            batch.push(j);
        }
        var batchSpec = StressTestPGJobBatch.make({values: batch});
        job.scheduleBatch(batchSpec);
    };
}

/**
 * @param {StressTestPGJobBatch} batch
 * @param {StressTestPGJob} job
 * @param {StressTestPGJobOptions} options
 */
 function processBatch(batch, job, options) {
    batch.values.forEach(function() {
        StressTestPostGres.run();
    });
}


/**
 * @param {StressTestPGJob} job
 * @param {StressTestPGJobOptions} options
 */
function allComplete(job, options) {
    var goodJob = StressTestBatchRecord.make({
        testType: "postGres",
        dateStarted: job.status().started,
        dateComplete: job.status().completed,
        nBatches: options.nBatches,
        batchSize: options.batchSize,
        jobReference: c3Make("map<string, string>",
        {"jobType": "StressTestPGJob",
            "id": job.id}),
        testFailed: false,
        failedBatches: 0
    });
    goodJob.upsert();
}


/**
 * @param {StressTestPGJob} job
 * @param {BatchJobStatus} jobStatus
 * @param {StressTestPGJobOptions} options
 */
 function failed(job, status, options) {
    var badJob = StressTestBatchRecord.make({
        testType: "postGres",
        dateStarted: job.status().started,
        dateComplete: job.status().completed,
        nBatches: options.nBatches,
        batchSize: options.batchSize,
        jobReference: c3Make("map<string, string>",
        {"jobType": "StressTestPGJob",
            "id": job.id}),
        testFailed: true,
        failedBatches: status.errors.count
    });
    badJob.upsert();
}