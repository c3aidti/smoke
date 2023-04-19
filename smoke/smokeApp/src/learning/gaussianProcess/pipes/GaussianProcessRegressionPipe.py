##
# Copyright (c) 2022, C3 AI DTI, Development Operations Team
# All rights reserved. License: https://github.com/c3aidti/.github
##
def train(this, input, targetOutput, spec):
    """
    Performs Scikit-Learn's GaussianProcessRegressor's fit().
    https://scikit-learn.org/stable/modules/generated/sklearn.gaussian_process.GaussianProcessRegressor.html
    """
    from sklearn.gaussian_process import GaussianProcessRegressor

    technique = c3.GaussianProcessRegressionTechnique.get(this.technique.id)
    serializedKernel = c3.SklearnGPRKernel.get(technique.kernel.id)

    # get data
    X = c3.Dataset.toNumpy(dataset=input)
    y = c3.Dataset.toNumpy(dataset=targetOutput)

    if (technique.centerTarget):
        targetMean = float(y.mean())
        y = y - y.mean()
    
    if (technique.validation):
        rng = np.random.RandomState(technique.randomSeed)
        rng.shuffle(X)
        X = X[0:int((1.0 - technique.splitFraction)*len(X))]
        rng.shuffle(y)
        y = y[0:int((1.0 - technique.splitFraction)*len(y))]


    # get kernel object from c3, make it python again
    kernel = c3.PythonSerialization.deserialize(serialized=serializedKernel.pickledKernel)

    # build and train GPR
    gp = GaussianProcessRegressor(kernel=kernel)
    gp.fit(X, y)

    if (technique.centerTarget):
        params = {}
        params["targetMean"] = targetMean
        this.trainedModel = c3.MLTrainedModelArtifact(
            model=c3.PythonSerialization.serialize(obj=gp),
            parameters=params
        )
    else:
        this.trainedModel = c3.MLTrainedModelArtifact(
            model=c3.PythonSerialization.serialize(obj=gp),
        )

    return this


def process(this, input, spec, computeStd=False, computeCov=False):
    """
    Performs Scikit-Learn's GaussianProcessRegressor's predict().
    https://scikit-learn.org/stable/modules/generated/sklearn.gaussian_process.GaussianProcessRegressor.html
    """
    import numpy as np

    # unpickle the model
    gp = c3.PythonSerialization.deserialize(serialized=this.trainedModel.model)

    # format data
    X = c3.Dataset.toNumpy(dataset=input)

    # get predictions: notice that cov and std simultaneously is not supported by Sklearn https://github.com/scikit-learn/scikit-learn/blob/baf0ea25d/sklearn/gaussian_process/_gpr.py#L327
    if computeStd and not computeCov:
        predictions, std = gp.predict(X, return_std=True)
        result = np.c_[predictions,std]

        return c3.Dataset.fromPython(pythonData=result)

    elif not computeStd and computeCov:
        predictions, cov = gp.predict(X, return_cov=True)
        result = np.c_[predictions,cov]

        return c3.Dataset.fromPython(pythonData=result)

    else:
        predictions = gp.predict(X)

        return c3.Dataset.fromPython(pythonData=predictions)


def isProcessable(this):
    """"
    Guarantees that process() can only be called after train()
    """

    return this.isTrained()


def getFeatures(this):
    """
    Gets the features to train the GPR model.
    """
    import pandas as pd

    dataSourceSpec = c3.GPRDataSourceSpec.get(this.dataSourceSpec.id)
    featuresType = dataSourceSpec.featuresType.toType()
    
    if (featuresType.name == "StagedFeatures"):
        features = c3.StagedFeatures.fetch({
            "limit": -1,
            "order": "id"
        }).objs.toJson()

        df = pd.DataFrame(features)
        keys = df.iloc[0]["features"].keys()

        for key in keys:
            df[key] = df["features"].apply(lambda x: x[key])
        
        df.drop("version", axis=1, inplace=True)
        df = df.select_dtypes(["number"])

        return c3.Dataset.fromPython(df)


    inputTableC3 = featuresType.fetch(dataSourceSpec.featuresSpec).objs
    if inputTableC3 is not None:
        inputTablePandas = pd.DataFrame(inputTableC3.toJson())
        inputTablePandas = inputTablePandas.drop("version", axis=1)
        # collect only the numeric fields
        inputTablePandas = inputTablePandas.select_dtypes(["number"])
        # drop ignored features
        if (dataSourceSpec.excludeFeatures):
            inputTablePandas.drop(columns=dataSourceSpec.excludeFeatures, inplace=True)
        return c3.Dataset.fromPython(inputTablePandas)
    else:
        return c3.Dataset.fromPython(pd.DataFrame([]))


def getTarget(this):
    """
    Get the targets to train the GPR model.
    """
    import pandas as pd

    dataSourceSpec = c3.GPRDataSourceSpec.get(this.dataSourceSpec.id)
    targetType = dataSourceSpec.targetType.toType()

    if (targetType.name == "StagedTargets"):
        targets = c3.StagedTargets.fetch({
            "limit": -1,
            "order": "id"
        }).objs.toJson()

        df = pd.DataFrame(targets)
        keys = df.iloc[0]["targets"].keys()

        for key in keys:
            df[key] = df["targets"].apply(lambda x: x[key])
        
        df.drop("version", axis=1, inplace=True)
        df = df.select_dtypes(["number"])

        return c3.Dataset.fromPython(df)
 
        
    outputTableC3 = targetType.fetch(dataSourceSpec.targetSpec).objs
    if outputTableC3 is not None:
        outputTablePandas = pd.DataFrame(outputTableC3.toJson())
        outputTablePandas = outputTablePandas.drop("version", axis=1)
        # collect only the numeric fields
        outputTablePandas = outputTablePandas.select_dtypes(include=["number"], exclude=["int"])
        if dataSourceSpec.targetName == "all":
            outputTablePandas = pd.DataFrame(
                outputTablePandas.sum(axis=1),
                columns=[dataSourceSpec.targetName]
            )
        else:
            outputTablePandas = pd.DataFrame(outputTablePandas[dataSourceSpec.targetName])
        return c3.Dataset.fromPython(outputTablePandas)
    else:
        return c3.Dataset.fromPython(pd.DataFrame([]))


def trainWithStagedAOD(this, modelIds):
    """
    This method trains a large model with data coming from previously trained
    GPR models with AOD data.
    Inputs:
        ids: list of GaussianProcessRegressionPipes ids
    Returns:
        int: 0 if method worked, 1 otherwise
    """
    from sklearn.gaussian_process import GaussianProcessRegressor

    # stage features and targets
    c3.StagedFeatures.stageFromAODGPRModelIdsList(modelIds)
    c3.StagedTargets.stageFromAODGPRModelIdsList(modelIds)
    # get data
    X = c3.Dataset.toNumpy(dataset=this.getFeatures())
    y = c3.Dataset.toNumpy(dataset=this.getTarget())

    # generate training technique
    technique = c3.GaussianProcessRegressionTechnique.get(this.technique.id)
    serializedKernel = c3.SklearnGPRKernel.get(technique.kernel.id)

    if (technique.centerTarget):
        targetMean = float(y.mean())
        y = y - y.mean()
    
    # get kernel object from c3, make it python again
    kernel = c3.PythonSerialization.deserialize(serialized=serializedKernel.pickledKernel)

    # build and train GPR
    gp = GaussianProcessRegressor(kernel=kernel)
    gp.fit(X, y)

    if (technique.centerTarget):
        params = {}
        params["targetMean"] = targetMean
        this.trainedModel = c3.MLTrainedModelArtifact(
            model=c3.PythonSerialization.serialize(obj=gp),
            parameters=params
        )
    else:
        this.trainedModel = c3.MLTrainedModelArtifact(
            model=c3.PythonSerialization.serialize(obj=gp),
        )

    this.upsert()

    return 0

def trainWithListOfAODModels(this, modelIds, excludeFeatures):
    """
    This method trains a large model with data coming from previously trained
    GPR models with AOD data.
    Inputs:
        ids: list of GaussianProcessRegressionPipes ids
    Returns:
        int: 0 if method worked, 1 otherwise
    """
    from sklearn.gaussian_process import GaussianProcessRegressor
    import pandas as pd
    from datetime import timedelta

    # get data
    X = pd.DataFrame()
    y = pd.DataFrame()
    for model_id in modelIds:
        model = c3.GaussianProcessRegressionPipe.get(model_id)
        data_source_spec = c3.GPRDataSourceSpec.get(model.dataSourceSpec.id, "targetSpec")
        gstp_id = data_source_spec.targetSpec.filter.split(" == ")[1].replace('"', '')
        gstp = c3.GeoSurfaceTimePoint.get(gstp_id)
        my_time = gstp.time.timetuple()
        px = c3.Dataset.toPandas(model.getFeatures())
        px["latitude"] = gstp.latitude
        px["longitude"] = gstp.longitude
        px["time"] = timedelta(
            days=my_time.tm_yday,
            minutes=my_time.tm_min,
            hours=my_time.tm_hour
        ).total_seconds() / 3600
        X = pd.concat([X,px], ignore_index=True)

        py = c3.Dataset.toPandas(model.getTarget())
        y = pd.concat([y,py], ignore_index=True)
    X.drop(columns=excludeFeatures, inplace=True)
    X = X.to_numpy()
    y = y.to_numpy()

    # generate training technique
    technique = c3.GaussianProcessRegressionTechnique.get(this.technique.id)
    serializedKernel = c3.SklearnGPRKernel.get(technique.kernel.id)

    if (technique.centerTarget):
        targetMean = float(y.mean())
        y = y - y.mean()

    # get kernel object from c3, make it python again
    kernel = c3.PythonSerialization.deserialize(serialized=serializedKernel.pickledKernel)

    # build and train GPR
    gp = GaussianProcessRegressor(kernel=kernel)
    gp.fit(X, y)

    if (technique.centerTarget):
        params = {}
        params["targetMean"] = targetMean
        this.trainedModel = c3.MLTrainedModelArtifact(
            model=c3.PythonSerialization.serialize(obj=gp),
            parameters=params
        )
    else:
        this.trainedModel = c3.MLTrainedModelArtifact(
            model=c3.PythonSerialization.serialize(obj=gp),
        )

    this.upsert()

    return 0