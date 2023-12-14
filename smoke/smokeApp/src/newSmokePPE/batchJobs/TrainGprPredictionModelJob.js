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

    // See if the options.geoTimeGridFetchSpec contianes a filter
    // If it does append an and() clause to filter on, if not, create new filter
    var filter = options.geoTimeGridFetchSpec.filter;
    if (filter === undefined) {
        filter = Filter.eq('isTrained', false);
    }
    else {
        filter = filter.and().eq('isTrained', false);
    }

    var specj = options.geoTimeGridFetchSpec.toJson();
    specj.type = 'FetchStreamSpec'
    var streamSpec = FetchStreamSpec.make(specj)
    var rows = predictionModelType.fetchObjStream(streamSpec);

    var batch = [];
    while(rows.hasNext()) {
        batch.push(rows.next().id);

        if (batch.length >= options.batchSize || !rows.hasNext()) {
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