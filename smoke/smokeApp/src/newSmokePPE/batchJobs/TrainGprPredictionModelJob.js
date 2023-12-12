/**
* Copyright (c) 2022-2024, C3 AI DTI, Development Operations Team
* All rights reserved. License: https://github.com/c3aidti/.github
**/

function doStart(job, options) {
    // var dsetObj = SimulationEnsembleDataset.fetch({
    //     "filter": Filter.eq('id',options.datasetId),
    //     "include":"id"
    // }).objs[0];
    // var dsetTypeName = dsetObj.type().typeName();
    // var dsetType = TypeRef.make({"typeName": dsetTypeName}).toType();
    // dsetObj = dsetType.get(options.datasetId);
    var dsetObj = options.dataset;

    // Stage rows for training
    dsetObj.stageTrainedPredictionModelRowsForTechnique(
        options.geoTimeGridFetchSpec,
        options.gprTechnique 
    );

    job.setHardwareProfile(options.hardwareProfileId);

    var predictionModelTypeName = dsetObj.getTrainedPredictionModelTypeName();
    var predictionModelType = TypeRef.make({"typeName": predictionModelTypeName}).toType();

    // update options with predictionModelType
    // var newOptions = TrainGprPredictionModelJobOptions.make(options.toJson());
    // newOptions.predictionModelType = predictionModelType;
    // var updateJob = TrainGprPredictionModelJob.make({"id":job.id,"options":newOptions.toJson()});
    // updateJob.merge();

    var specj = options.geoTimeGridFetchSpec.toJson();
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
    var dsetObj = options.dataset;
    var predictionModelTypeName = dsetObj.getTrainedPredictionModelTypeName();
    var predictionModelType = TypeRef.make({"typeName": predictionModelTypeName}).toType();
    batch.values.forEach(function(rowId) {
        var row = predictionModelType.get(rowId);
        row.train(options.X);
    });
}