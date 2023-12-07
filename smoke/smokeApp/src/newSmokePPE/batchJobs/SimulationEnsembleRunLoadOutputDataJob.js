/**
* Copyright (c) 2022-2024, C3 AI DTI, Development Operations Team
* All rights reserved. License: https://github.com/c3aidti/.github
**/

function doStart(job, options) {
    var simulationRunType = TypeRef.make({"typeName": options.typeName}).toType()
    var dsFilter = Filter.eq("id", options.datasetId);
    var datasetObj = SimulationEnsembleDataset.fetch(
        {
            "filter": dsFilter,
            "include": "id"
        }
    ).objs[0];
    var datasetTypeName = datasetObj.type().typeName();
    var datasetType = TypeRef.make({"typeName": datasetTypeName}).toType();
    datasetObj = datasetType.fetch({"filter": dsFilter}).objs[0];
    job.setHardwareProfile(options.hardwareProfileId);

    /** Compute batch size based on number of parallel "streams" or upsert tasks. */
    var filter = Filter.eq("ensemble.id", datasetObj.ensemble.id);

    var cnt = simulationRunType.fetchCount({
        "filter": filter
    });

    var batch = [];
    var q = parseInt(cnt/options.parallelStreams);
    var r = cnt % options.parallelStreams;
    var batchSize = cnt;
    if (r > 0) {
        batchSize = q + 1;
    }
    else {
        batchSize = q;
    }

    var dataset = simulationRunType.fetchObjStream({
        "filter": options.filter
    });

    while(dataset.hasNext()) {
        batch.push(dataset.next().id);

        if (batch.length >= batchSize || !dataset.hasNext()) {
            var batchSpec = SimulationEnsembleRunLoadOutputDataJobBatch.make({values: batch});
            job.scheduleBatch(batchSpec);
            
            batch = [];
        }
    }
}

function processBatch(batch, job, options) {
    var simulationRunType = TypeRef.make({"typeName": options.typeName}).toType();
    batch.values.forEach(function(simId) {
        var sim = simulationRunType.get(simId);
        sim[options.method](options.datasetId,options.pseudoLevelIndex,options.batchSize);
    });
}