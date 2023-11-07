/**
 * Implementation of UpsertAcureData.c3typ
 * @param {UpsertAcureData} job
 * @param {UpsertAcureDataOptions} options
 */
 function doStart(job, options) {
    job.setHardwareProfile(options.hardwareProfileId);
    var batch = [];

    var dataset = SimulationOutputFile.fetchObjStream({
        limit: -1,
        filter: "container == 'acure-aircraft'"
    });

    while(dataset.hasNext()) {
        batch.push(dataset.next());

        if (batch.length >= options.batchSize || !dataset.hasNext()) {
            var batchSpec = UpsertAcureDataBatch.make({values: batch});
            job.scheduleBatch(batchSpec);
            
            batch = [];
        }
    }
}



/**
 * @param {UpsertAcureDataBatch} batch
 * @param {UpsertAcureData} job
 * @param {UpsertAcureDataOptions} options
 */
function processBatch(batch, job, options) {
    batch.values.forEach(function(file) {
        file.upsertAcureAircraftData();
    });
}