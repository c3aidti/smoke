def createTrainingTechnique(
        this,
        kernelName, 
        serializedKernel, 
        featureExcludeList=None, 
        featureIncludeList=None, 
        simulationExcludeList=None,
        simulationIncludeList=None,
        targetName='sumAll',
        randomSeed=42, 
        centerTarget=False,
        standardizeTarget=False,
        validation=False, 
        splitFraction=0.2
        ):
    """
    Create a training technique object with the given parameters.
    """
    ens = c3.SimulationEnsemble.fetch(
    spec={
        "filter":c3.Filter.inst().eq('model.id',this.id).and_().exists('datasets'),
        "include": "this,datasets"
        }
    ).objs[0]
    ds = c3.SimulationEnsembleDataset.get(ens.datasets[0].id)

    # featureIncludeList and featureExcludeList are mutually exclusive
    if featureIncludeList and featureExcludeList:
        raise ValueError("featureIncludeList and featureExcludeList are mutually exclusive")
    
    # simulationIncludeList and simulationExcludeList are mutually exclusive
    if simulationIncludeList and simulationExcludeList:
        raise ValueError("simulationIncludeList and simulationExcludeList are mutually exclusive")
    
    # If featureIncludeList is defined, use it
    if featureIncludeList:
        featureList = featureIncludeList
    else:
        featureList = ds.getSimulationParameterList()

    # If an exclude list is defined, use it to filter the python list
    # both feature list and featured Exclude list are lists of strings
    if featureExcludeList:
        featureList = [x for x in featureList if x not in featureExcludeList]
    serializedFeatureList = c3.PythonSerialization.serialize(obj=featureList)

    # If simulationIncludeList is defined, use it
    if simulationIncludeList:
        simulationList = simulationIncludeList
    else:
        simulationList = ds.getSimulationList()
    serializedSimulationList = c3.PythonSerialization.serialize(obj=simulationList)

    return c3.GprPredictionModelParameters.createTechnique(
        this,
        kernelName=kernelName,
        serializedKernel=serializedKernel,
        serializedFeatureList=serializedFeatureList,
        serializedSimulationList=serializedSimulationList,
        targetName=targetName,
        randomSeed=randomSeed,
        centerTarget=centerTarget,
        standardizeTarget=standardizeTarget,
        validation=validation,
        splitFraction=splitFraction
    )