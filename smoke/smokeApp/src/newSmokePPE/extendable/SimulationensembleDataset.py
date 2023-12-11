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
        "include":include
    }).objs.toJson()

    df = pd.DataFrame(simulationOutputFields)
    df.drop(["type","id","typeIdent","meta","version"],axis=1, inplace=True)
    return c3.Dataset.fromPython(df)
