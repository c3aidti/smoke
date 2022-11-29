/**
* Copyright (c) 2022, C3 AI DTI, Development Operations Team
* All rights reserved. License: https://github.com/c3aidti/.github
**/
/**
 * Implementation of AODGaussianMLTrainingJob
 * @param {AODGaussianMLTrainingJob} job
 * @param {AODGaussianMLTrainingJobOptions} options
 */
 function doStart(job, options) {
    job.setHardwareProfile(options.hardwareProfileId);
    var batch = [];

    var gstps = GeoSurfaceTimePoint.fetchObjStream({
        filter: options.gstpFilter,
        limit: -1
    });

    while(gstps.hasNext()) {
        batch.push(gstps.next());

        if (batch.length >= options.batchSize || !gstps.hasNext()) {
            var batchSpec = AODGaussianMLTrainingJobBatch.make({values: batch});
            job.scheduleBatch(batchSpec);
            
            batch = [];
        }
    }
}


/**
 * Implementation of what to do in each batch
 * @param {AODGaussianMLTrainingJobBatch} batch
 * @param {AODGaussianMLTrainingJob} job
 * @param {AODGaussianMLTrainingJobOptions} options
 */
function processBatch(batch, job, options) {
    batch.values.forEach(function(gstp) {

        // define target
        var targetType = TypeRef.make({"typeName": "Simulation3HourlyAODOutput"});
        var targetFilter = Filter.eq("geoSurfaceTimePoint.id", gstp.id);
        var targetSpec = FetchSpec.make({
            "limit": -1,
            "order": "simulationSample.id",
            "filter": targetFilter.toString()
        });

        // find the simulations
        var simulationsSpec = FetchSpec.make({
            "limit": -1,
            "order": "simulationSample.id",
            "filter": targetFilter.toString(),
            "include": "simulationSample"
        });
        var samples = targetType.toType().fetch(simulationsSpec).objs;
        var simIds = [];
        for(var i = 0; i < samples.length; i++) {
            simIds.push(samples[i].simulationSample.id);
        }

        var featuresType = TypeRef.make({"typeName": "SimulationModelParameters"});
        var allSamples = featuresType.toType().fetch({
            "limit": -1,
            "order": "id",
            "include": "id"
        }).objs;
        var allSimIds = [];
        for(var i = 0; i < allSamples.length; i++) {
            allSimIds.push(allSamples[i].id);
        };
        var excludeIds = [];
        for(var i = 0; i < allSimIds.length; i++) {
            if(simIds.indexOf(allSimIds[i]) === -1) {
                excludeIds.push(allSimIds[i]);
            }
        };

        // define the features
        var featuresFilter = Filter.not().intersects("id", excludeIds);
        var featuresSpec = FetchSpec.make({
            "limit": -1,
            "order": "id",
            "filter": featuresFilter
        });

        // define the data source spec
        var sourceSpec = GPRDataSourceSpec.make({
            "featuresType": featuresType,
            "featuresSpec": featuresSpec,
            "excludeFeatures": options.excludeFeatures,
            "targetType": targetType,
            "targetSpec": targetSpec,
            "targetName": options.targetName
        }).upsert()

        // create the pipe
        var GPR_pipe = GaussianProcessRegressionPipe.make({
            "technique": options.gprTechnique,
            "dataSourceSpec": sourceSpec
        })

        // get target and features
        var X = GPR_pipe.getFeatures();
        var y = GPR_pipe.getTarget();

        // train and save
        var GPR_pipe_trained = GPR_pipe.train(X, y);
        GPR_pipe_trained.upsert();

    });
}