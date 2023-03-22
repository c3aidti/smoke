/**
* Copyright (c) 2022, C3 AI DTI, Development Operations Team
* All rights reserved. License: https://github.com/c3aidti/.github
**/
/**
 * Implementation of CreateAODHeaders.c3typ
 * @param {UpsertSmokePPEDataAfterHeaders} job
 * @param {UpsertSmokePPEDataAfterHeadersOptions} options
 */
 function doStart(job, options) {
    job.setHardwareProfile(options.hardwareProfileId);
    var batch = [];

    var dataset = SmokePPESimulationSample.fetchObjStream({
        filter: options.filter,
        limit: options.limit,
        offset: options.offset
    });

    while(dataset.hasNext()) {
        batch.push(dataset.next());

        if (batch.length >= options.batchSize || !dataset.hasNext()) {
            var batchSpec = UpsertSmokePPEDataAfterHeadersBatch.make({values: batch});
            job.scheduleBatch(batchSpec);
            
            batch = [];
        }
    }
}

/**
 * @param {UpsertSmokePPEDataAfterHeadersBatch} batch
 * @param {UpsertSmokePPEDataAfterHeaders} job
 * @param {UpsertSmokePPEDataAfterHeadersOptions} options
 */
function processBatch(batch, job, options) {
    batch.values.forEach(function(simSample) {
        simSample.upsertDataAfterHeadersCreated(options.pseudoLevelIndex);
    });
}