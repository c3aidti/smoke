/**
* Copyright (c) 2022, C3 AI DTI, Development Operations Team
* All rights reserved. License: https://github.com/c3aidti/.github
**/
/**
 * Implementation of UpsertAODDataAfterHeaders.c3typ
 * @param {UpsertAODDataAfterHeaders} job
 * @param {UpsertAODDataAfterHeadersOptions} options
 */
 function doStart(job, options) {
    var batch = [];

    var finalFilter = options.filter.and().eq("container", "aod-3hourly");

    var dataset = SimulationOutputFile.fetchObjStream({
        filter: finalFilter,
        limit: options.limit,
        offset: options.offset
    });

    while(dataset.hasNext()) {
        batch.push(dataset.next());

        if (batch.length >= options.batchSize || !dataset.hasNext()) {
            var batchSpec = UpsertAODDataAfterHeadersBatch.make({values: batch});
            job.scheduleBatch(batchSpec);
            
            batch = [];
        }
    }
}

/**
 * @param {UpsertAODDataAfterHeadersBatch} batch
 * @param {UpsertAODDataAfterHeaders} job
 * @param {UpsertAODDataAfterHeadersOptions} options
 */
function processBatch(batch, job, options) {
    batch.values.forEach(function(file) {
        file.upsert3HourlyAODDataAfterHeadersCreated();
    });
}