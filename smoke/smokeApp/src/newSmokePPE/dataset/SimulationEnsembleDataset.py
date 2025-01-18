def getFeaturesForTechnique(this,technique):
    """
    Get the features for the GPR prediction model.
    """
    import pandas as pd
    simulationRunTypeName = this.getSimulationRunTypeName()
    featureList =  c3.PythonSerialization.deserialize(serialized=technique.serializedFeatureList)
    simulationList = c3.PythonSerialization.deserialize(serialized=technique.serializedSimulationList)

    # create filter and include spec to retrive the simulation output fields from the simulation output type
    filter = c3.Filter.inst().eq('ensemble.id',this.ensemble.id)
    # if the simulation list is not the same as the full list of available simulations, add a filter
    availableSimulations = this.getSimulationList()
    if simulationList != availableSimulations:
        filter = filter.and_().intersects('simulationNumber',simulationList)

    # convert the featureList to a string and set include to it
    include = ",".join(featureList)

    # fetch the simulation output fields
    simulationOutputFields = getattr(c3,simulationRunTypeName).fetch({
        "filter":filter,
        "include":include + ',simulationNumber'
    }).objs.toJson()

    df = pd.DataFrame(simulationOutputFields)
    df.drop(["type","id","typeIdent","meta","version"],axis=1, inplace=True)
    df.sort_values(['simulationNumber'],inplace=True)
    df.reset_index(drop=True,inplace=True)
    df.drop(['simulationNumber'],axis=1,inplace=True)
    return c3.Dataset.fromPython(df)

def getTargetForTechnique(this,technique,geoTimeGridId):
    import pandas as pd
    simulationOutputTypeName = this.getSimulationOutputTypeName()
    simulationList = c3.PythonSerialization.deserialize(serialized=technique.serializedSimulationList)

    filter = c3.Filter.inst().eq('geoTimeGridPoint',geoTimeGridId)

    availableSimulations = this.getSimulationList()
    if simulationList != availableSimulations:
        filter = filter.and_().intersects('simulationRun.simulationNumber',simulationList)

    res = getattr(c3,simulationOutputTypeName).fetch(spec={
        "filter": filter,
        "include": technique.targetName+",simulationRun.simulationNumber",
        "limit": -1
    })
    df = pd.DataFrame(res.objs.toJson())
    df['sim_num'] = df.get('simulationRun').apply(lambda x: int(x['id'].split('_')[-1]))
    df.sort_values('sim_num',inplace=True)
    df.reset_index(drop=True,inplace=True)
    df.drop(["type","id","meta","version","simulationRun","sim_num"],axis=1, inplace=True)
    return c3.Dataset.fromPython(df)
