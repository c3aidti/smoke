def train(this,X):
    from sklearn.gaussian_process import GaussianProcessRegressor
    typeName = this.toJson()['type']
    techniqueTypeName = this.technique.toJson()['type']
    technique = getattr(c3,techniqueTypeName).get(this.technique.id)
    datasetTypeName = this.dataset.toJson()['type']
    dataset = getattr(c3,datasetTypeName).get(this.dataset.id)
    
    y = dataset.getTargetForTechnique(technique,this.geoTimeGridPoint)
    
    X_np = c3.Dataset.toNumpy(X)
    y_np = c3.Dataset.toNumpy(y)
    
    if (technique.centerTarget):
        targetMean = float(y_np.mean())
        y_np = y_np - y_np.mean()
        
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
    gp.fit(X_np, y_np)
    
    updateThis = getattr(c3,typeName)(
        id = this.id,
        dataset = this.dataset,
        technique = this.technique
    )
    
    updateThis.trainedModel = c3.PythonSerialization.serialize(obj=gp),
    
    updateThis.merge()
    
    return getattr(c3,typeName).get(this.id)