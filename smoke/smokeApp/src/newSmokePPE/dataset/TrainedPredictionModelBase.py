def getTarget(this):
    techniqueTypeName = this.technique.toJson()['type']
    technique = getattr(c3,techniqueTypeName).get(this.technique.id)
    datasetTypeName = this.dataset.toJson()['type']
    dataset = getattr(c3,datasetTypeName).get(this.dataset.id)
    return dataset.getTargetForTechnique(technique,this.geoTimeGridPoint)

def train(this,X):
    from sklearn.gaussian_process import GaussianProcessRegressor
    typeName = this.toJson()['type']
    techniqueTypeName = this.technique.toJson()['type']
    technique = getattr(c3,techniqueTypeName).get(this.technique.id)
    datasetTypeName = this.dataset.toJson()['type']
    dataset = getattr(c3,datasetTypeName).get(this.dataset.id)

    updateThis = getattr(c3,typeName)(
        id = this.id,
        dataset = this.dataset,
        technique = this.technique
    )
    
    y = dataset.getTargetForTechnique(technique,this.geoTimeGridPoint)
    
    X_np = c3.Dataset.toNumpy(X)
    y_np = c3.Dataset.toNumpy(y)
    
    if (technique.centerTarget):
        targetMean = float(y_np.mean())
        updateThis.targetMean = targetMean

    
    if (technique.standardizeTarget):
        targetStd = float(y_np.std())
        updateThis.targetStd = targetStd


    if (technique.centerTarget and technique.standardizeTarget):
        y_np = (y_np - targetMean) / targetStd
    elif (technique.centerTarget):
        y_np = y_np - targetMean
    elif (technique.standardizeTarget):
        y_np = y_np / targetStd

    if (technique.validation):
        rng = np.random.RandomState(technique.randomSeed)
        rng.shuffle(X_np)
        X_np = X_np[0:int((1.0 - technique.splitFraction)*len(X_np))]
        rng.shuffle(y_np)
        y_np = y_np[0:int((1.0 - technique.splitFraction)*len(y_np))]
        
    serializedKernel = technique.getSerializedKernel()
    kernel = c3.PythonSerialization.deserialize(serialized=serializedKernel)
    
    # build and train GPR
    gp = GaussianProcessRegressor(kernel=kernel)
    try:
        gp.fit(X_np, y_np)
    except:
        print("Error in training")
        return False
    
    updateThis.trainedModel = c3.PythonSerialization.serialize(obj=gp),
    
    updateThis.merge()
    
    return True

def doPredict(this, X):
    import numpy as np
    import pandas as pd
    
    model = c3.PythonSerialization.deserialize(
        serialized=this.trainedModel
    )
    # 
    technique = c3.GprPredictionModelParameters.get(this.technique.id)
    if technique.centerTarget:
        targetMean = this.targetMean
    if technique.standardizeTarget:
        targetStd = this.targetStd
        
    mean, sd = model.predict(
        c3.Dataset.toPandas(X),
        return_std=True
    )

    if technique.centerTarget and technique.standardizeTarget:
        mean = mean * targetStd + targetMean
        sd = sd * targetStd
    elif technique.centerTarget:
        mean = mean + targetMean
    elif technique.standardizeTarget:
        mean = mean * targetStd
        sd = sd * targetStd

    df = pd.DataFrame()
    df["meanResponse"] = np.array(mean).flatten()
    df["sdResponse"] = sd
    df["variant"] = list(range(df.shape[0]))
    
    return c3.PythonSerialization.serialize(obj=df)

def predict(this, X):
    import numpy as np
    import pandas as pd

    return this.doPredict(X)

def predictAsync(this, X):
    import numpy as np
    import pandas as pd
    import time
    
    spec = {
        "typeName": this.dataset.getTrainedPredictionModelTypeName(),
        "action": 'doPredict',
        "args": {"this": this, "X": X}
    }
    action = c3.AsyncAction.submit(spec)
    
    # Poll for completion
    while not action.hasCompleted():
        time.sleep(1)
    action = action.get()
    result = action.result
    action.remove()
    return result
    