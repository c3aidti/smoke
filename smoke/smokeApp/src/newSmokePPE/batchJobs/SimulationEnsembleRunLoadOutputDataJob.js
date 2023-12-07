/**
* Copyright (c) 2022-2024, C3 AI DTI, Development Operations Team
* All rights reserved. License: https://github.com/c3aidti/.github
**/

function doStart(job, options) {
    var simulationRunType = TypeRef.make({"typeName": options.typeName}).toType()
    var datasetObj = SimulationEnsembleDataset.fetch(
        {
            "filter":"id=='datasetId'",
            "include": "id"
        }
    ).objs[0]
    var datasetType = datasetObj.type().typeName()
    datasetObj = datasetType.fetch({"filter":"id=='datasetId'"}).objs[0]
    job.setHardwareProfile(options.hardwareProfileId);
    var batch = [];

    /** Compute batch size based on number of parallel "streams" or upsert tasks. */
    var filter = Filter.eq("ensemble.id", datasetObj.ensemble.id);
    var cnt = SmokePPESimulationSample.fetchCount({
        filter: filter
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

    var dataset = simulationRunType.fetchObjStream({
        filter: options.filter,
        limit: options.limit,
        offset: options.offset
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
    var simulationRunType = TypeRef.make({"typeName": options.typeName}).toType()
    batch.values.forEach(function(simId) {
        var sim = simulationRunType.get(simId);
        sim[options.method](options.datasetId,options.pseudoLevelIndex,options.batchSize);
    });
}