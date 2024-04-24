/**
* Copyright (c) 2022-2024, C3 AI DTI, Development Operations Team
* All rights reserved. License: https://github.com/c3aidti/.github
**/

function doStart(job, options) {
    // Setup/lookup dataset variables
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

    // Lookup sims and flightTracks based on filters
    var sims = simulationRunType.fetchObjStream({
        "filter": options.simulationFilter
    });

    var flightTracks = FlightTrack.fetchObjStream({
        "filter": options.flightTrackFilter
    });

    // Batch scheduling loop
    var batch = [];
    var batchCtr = 0;
    var stashIndex = 0;
    var flightTrackId;
    var stashId;
    var simId;
    var batchSpec;
    while(stashIndex < options.stashIds.length) {
        stashId = options.stashIds[stashIndex];

        while(sims.hasNext()) {
            simId = sims.next().id

            while(flightTracks.hasNext()) {
                batchCtr++;
                flightTrackId = flightTracks.next().id;
                batchValue = SimulationEnsembleRunInterpToFlightTracksJobBatchValue.make({simId: simId, flightTrackId: flightTrackId, stashId: stashId});
                batch.push(batchValue);

                if (batchCtr >= options.batchSize || (!sims.hasNext() && !flightTracks.hasNext() && stashIndex === options.stashIds.length - 1)) {
                    batchSpec = SimulationEnsembleRunLoadOutputDataJobBatch.make({values: batch});
                    job.scheduleBatch(batchSpec);
                    batchCtr = 0;
                    batch = [];
                }
            }
        }

        stashIndex++;
    }

    // Schedule any remaining items that didn't reach the batch size
    if (batchCtr > 0) {
        batchSpec = SimulationEnsembleRunLoadOutputDataJobBatch.make({values: batch});
        job.scheduleBatch(batchSpec);
    }
}

function processBatch(batch, job, options) {
    var simulationRunType = TypeRef.make({"typeName": options.typeName}).toType();
    batch.values.forEach(function(val) {
        var sim = simulationRunType.get(val.simId);
        var flightTrack = FlightTrack.get(val.flightTrackId);
        sim[options.method](options.datasetId,val.stashId,flightTrack.flightDate,flightTrack.campaign);
    });
}