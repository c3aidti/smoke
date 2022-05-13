/**
 * @param {InfiniteLoopJob} jobtype
 * @param {InfiniteLoopJobOptions} joboptions
 */
 function doStart(job, options) {
    for (var i = 0; i < options.nBatches; i++) {
        var batch = [];
        for (var j = 0; j < options.batchSize; j++) {
            batch.push(j);
        }
        var batchSpec = InfiniteLoopJobBatch.make({values: batch});
        job.scheduleBatch(batchSpec);
    };
}

/**
 * @param {InfiniteLoopJobBatch} batch
 * @param {InfiniteLoopJob} job
 * @param {InfiniteLoopJobOptions} options
 */
 function processBatch(batch, job, options) {
    batch.values.forEach(function() {
        for (;;) {}
    });
}
