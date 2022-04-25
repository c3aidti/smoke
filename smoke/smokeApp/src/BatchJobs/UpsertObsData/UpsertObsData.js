/**
 * UpsertData.js
 * Implementation of UpsertData.c3typ
 * @param {UpsertObsData} job
 * @param {UpsertDataOptions} options
 */
 function doStart(job, options) {
    var batch = [];

    var dataset = ObservationOutputFile.fetchObjStream({
//        filter: 'processed == false',
        limit: -1
    });

    while(dataset.hasNext()) {
        batch.push(dataset.next());

        if (batch.length >= options.batchSize || !dataset.hasNext()) {
            var batchSpec = UpsertObsDataBatch.make({values: batch});
            job.scheduleBatch(batchSpec);
            
            batch = [];
        }
    }
}



/**
 * @param {UpsertObsDataBatch} batch
 * @param {UpsertObsData} job
 * @param {UpsertDataOptions} options
 */
function processBatch(batch, job, options) {
    batch.values.forEach(function(file) {
        file.upsertORACLESData();
    });
}