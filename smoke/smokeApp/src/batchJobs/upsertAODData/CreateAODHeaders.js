/**
* Copyright (c) 2022, C3 AI DTI, Development Operations Team
* All rights reserved. License: https://github.com/c3aidti/.github
**/
/**
 * Implementation of CreateAODHeaders.c3typ
 * @param {CreateAODHeaders} job
 * @param {CreateAODHeadersOptions} options
 */
 function doStart(job, options) {
    job.setHardwareProfile(options.hardwareProfileId);
    var batch = [];

    var finalFilter = options.filter.and().paren(eq("container", "aod-3hourly").or(eq("container","smoke-ppe")));

    var dataset = SimulationOutputFile.fetchObjStream({
        filter: finalFilter,
        limit: options.limit,
        offset: options.offset
    });

    while(dataset.hasNext()) {
        batch.push(dataset.next());

        if (batch.length >= options.batchSize || !dataset.hasNext()) {
            var batchSpec = CreateAODHeadersBatch.make({values: batch});
            job.scheduleBatch(batchSpec);
            
            batch = [];
        }
    }
}

/**
 * @param {CreateAODHeadersBatch} batch
 * @param {CreateAODHeaders} job
 * @param {CreateAODHeadersOptions} options
 */
function processBatch(batch, job, options) {
    batch.values.forEach(function(file) {
        file.createAODDataCassandraHeaders();
    });
}