##
# Copyright (c) 2022, C3 AI DTI, Development Operations Team
# All rights reserved. License: https://github.com/c3aidti/.github
##
def extractLearnedParametersJob(excFeats, gstpFilter, targetName, technique, batchSize , smokeppe=False):
    """
    Dynamic map-reduce job to extract learned hyper parameters.
    """
    if not smokeppe:
        def cassandra_mapper(batch, objs, job):
            models = []
            for obj in objs:
                model = c3.AODGPRModelFinder.getPipe(
                    job.context.value["excludeFeatures"],
                    obj.id,
                    job.context.value["targetName"],
                    job.context.value["technique"]
                )
                models.append(model)
            
            return {batch: models}
    else:
        def cassandra_mapper(batch, objs, job):
            models = []
            for obj in objs:
                model = c3.SmokePPEGPRPredictor.getPipe(
                    job.context.value["excludeFeatures"],
                    obj.id,
                    job.context.value["targetName"],
                    job.context.value["technique"]
                )
                models.append(model)
            
            return {batch: models}

    def cassandra_reducer(key, interValues, job):
        values = []
        for iv in interValues:
            for val in iv:
                for m in val:
                    pickledModel = m["trainedModel"]["model"]
                    model = c3.PythonSerialization.deserialize(serialized=pickledModel)
                    hp = model.kernel_.get_params()['k2__length_scale']
                    model_id = m["id"]

                    # find GSTP
                    dssId = m["dataSourceSpec"]["id"]
                    dss = c3.GPRDataSourceSpec.get(dssId)
                    gstpId = dss.targetSpec.filter.split(" == ")[1].replace('"', '')
                    gstp = c3.GeoSurfaceTimePoint.get(gstpId)
                    lat = gstp.latitude
                    lon = gstp.longitude
                    time = gstp.time
                    values.append((hp, model_id, lat, lon, time))

        return values

    map_lambda = c3.Lambda.fromPython(cassandra_mapper)
    reduce_lambda = c3.Lambda.fromPython(cassandra_reducer, runtime="gordon-ML_1_0_0")

    job_context = c3.MappObj(
        value={
            'excludeFeatures': excFeats,
            'targetName': targetName,
            'technique': technique
        }
    )
    job = c3.DynMapReduce.startFromSpec(
        c3.DynMapReduceSpec(
            targetType="GeoSurfaceTimePoint",       
            filter=gstpFilter, 
            mapLambda=map_lambda,
            reduceLambda=reduce_lambda,
            batchSize=batchSize,
            context=job_context,
            hardwareProfile="appc8m642-w"
        )
    )

    return job


def getDataframeFromJob(job):
    """
    Iterates over job result and builds dataframe.
    """
    import pandas as pd
    import numpy as np

    lengthScales = []
    ids = []
    lats = []
    lons = []
    times = []
    if job.status().status == "completed":
        for key, value in job.results().items():
            for subvalue in value:
                ls = np.array(subvalue[0]).astype(float)
                model_id = np.array([subvalue[1]]).astype(str)
                lengthScales.append(ls)
                ids.append(model_id[0])
                lats.append(np.array(subvalue[2]).astype(float))
                lons.append(np.array(subvalue[3]).astype(float))
                times.append(np.array(subvalue[4]))
                
        df = pd.DataFrame(lengthScales)
        df["modelId"] = ids
        df["latitude"] = lats
        df["longitude"] = lons
        df["time"] = times
        return df
    else:
        return False