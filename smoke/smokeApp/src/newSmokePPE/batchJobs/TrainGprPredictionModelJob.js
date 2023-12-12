/**
* Copyright (c) 2022-2024, C3 AI DTI, Development Operations Team
* All rights reserved. License: https://github.com/c3aidti/.github
**/
/**
* Copyright (c) 2022-2024, C3 AI DTI, Development Operations Team
* All rights reserved. License: https://github.com/c3aidti/.github
**/

function doStart(job, options) {
    var dsetObj = SimulationEnsembleDataset.fetch({
        "filter": Filter.eq('id',dsetId),
        "include":"id"
    }).objs[0];
    var dsetTypeName = dsetObj.type().typeName();
    var dsetType = TypeRef.make({"typeName": dsetTypeName}).toType();

    // Stage rows for training
    dsetType.stageTrainedPredictionModelRowsForTechnique(
        options.geoTimeGridFetchSpec,
        options.technique 
    );

    job.setHardwareProfile(options.hardwareProfileId);

    var predictionModelTypeName = dsetType.getTrainedPredictionModelTypeName();
    var predictionModelType = TypeRef.make({"typeName": predictionModelTypeName}).toType();

    // update options with predictionModelType
    var newOptions = TrainGprPredictionModelJobOptions.make(options.toJson());
    newOptions.predictionModelType = predictionModelType;
    var updateJob = TrainGprPredictionModelJob.make({"id":job.id,"options":newOptions.toJson()});
    updateJob.merge();

    specj.type = 'FetchStreamSpec'
    var streamSpec = FetchStreamSpec.make(specj)
    var rows = predictionModelType.fetchObjStream(streamSpec);

    var batch = [];
    while(rows.hasNext()) {
        batch.push(rows.next().id);

        if (batch.length >= options.batchSize || !gstps.hasNext()) {
            var batchSpec = TrainGprPredictionModelJobBatch.make({values: batch});
            job.scheduleBatch(batchSpec);
            
            batch = [];
        }
    }

}

function processBatch(batch, job, options) {
    batch.values.forEach(function(rowId) {
        var row = options.predictionModelType.get(rowId);
        row.train(options.X);
    });
}