/**
* Copyright (c) 2022-2024, C3 AI DTI, Development Operations Team
* All rights reserved. License: https://github.com/c3aidti/.github
**/

function doStart(job, options) {
    job.setHardwareProfile(options.hardwareProfileId);
    var batch = [];

    /** Compute batch size based on number of parallel "streams" or upsert tasks. */
    var cnt = SmokePPESimulationSample.fetchCount({
        filter: options.filter
    });
    var q = parseInt(cnt/options.parallelStreams);
    var r = cnt % options.parallelStreams;
    var batchSize = cnt;
    if (r > 0) {
        batchSize = q + 1;
    }
    else {
        batchSize = q;
    }

    var dataset = SmokePPESimulationSample.fetchObjStream({
        filter: options.filter,
        limit: options.limit,
        offset: options.offset
    });

    while(dataset.hasNext()) {
        batch.push(dataset.next());

        if (batch.length >= batchSize || !dataset.hasNext()) {
            var batchSpec = UpsertSmokePPEDataToSmokePPEGeoTimeBatch.make({values: batch});
            job.scheduleBatch(batchSpec);
            
            batch = [];
        }
    }
}

function processBatch(batch, job, options) {
    batch.values.forEach(function(simSample) {
        simSample[options.method](options.pseudoLevelIndex,options.batchSize);
    });
}