##
# Copyright (c) 2022, C3 AI DTI, Development Operations Team
# All rights reserved. License: https://github.com/c3aidti/.github
##
def makePredictionsJob(
    excFeats, gstpFilter, targetName, synthDataset, technique, batchSize
):
    """
    Dynamic map-reduce job to get predictions on synthDataset.
    """

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

    def cassandra_reducer(key, interValues, job):
        values = []
        synthDataframe = c3.Dataset.toPandas(job.context.value["syntheticDataset"])
        for iv in interValues:
            for val in iv:
                for m in val:
                    # predictions
                    model_id = m["id"]
                    centered = m["technique"]["centerTarget"]
                    if centered:
                        center = m["trainedModel"].parameters["targetMean"].asfloat()
                    else:
                        center = 0
                    pickledModel = m["trainedModel"]["model"]
                    model = c3.PythonSerialization.deserialize(serialized=pickledModel)
                    mean, sd = model.predict(synthDataframe, return_std=True)

                    # location
                    dssId = m["dataSourceSpec"]["id"]
                    dss = c3.GPRDataSourceSpec.get(dssId)
                    gstpId = dss.targetSpec.filter.split(" == ")[1].replace('"', '')
                    gstp = c3.GeoSurfaceTimePoint.get(gstpId)
                    lat = gstp.latitude
                    lon = gstp.longitude
                    time = gstp.time
                    values.append((model_id, mean, center, sd, lat, lon, time))
                    

        return values

    map_lambda = c3.Lambda.fromPython(cassandra_mapper)
    reduce_lambda = c3.Lambda.fromPython(cassandra_reducer, runtime="gordon-ML_1_0_0")

    job_context = c3.MappObj(
        value={
            'excludeFeatures': excFeats,
            'targetName': targetName,
            'technique': technique,
            'syntheticDataset': synthDataset
        }
    )
    job = c3.DynMapReduce.startFromSpec(
        c3.DynMapReduceSpec(
            targetType="GeoSurfaceTimePoint",       
            filter=gstpFilter, 
            mapLambda=map_lambda,
            reduceLambda=reduce_lambda,
            batchSize=batchSize,
            context=job_context
        )
    )

    return job


def getPredictionsDataframeFromJob(job):
    """
    Iterates over job result and builds dataframe.
    """
    import pandas as pd
    import numpy as np

    predictions = []

    if job.status().status == "completed":
        for key, value in job.results().items():
            for subvalue in value:
                df_m = pd.DataFrame()
                df_m["meanResponse"] = np.array(subvalue[1]).flatten()
                df_m["meanResponse"] += subvalue[2]
                df_m["sdResponse"] = subvalue[3]
                df_m["latitude"] = subvalue[4]
                df_m["longitude"] = subvalue[5]
                df_m["time"] = subvalue[6]
                df_m["modelId"] = subvalue[0]
                df_m["variant"] = list(range(df_m.shape[0]))

            predictions.append(df_m)

        df = pd.concat(predictions, axis=0).reset_index(drop=True)
        return df
    else:
        return False