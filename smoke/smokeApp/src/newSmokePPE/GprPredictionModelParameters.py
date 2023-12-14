def createTechnique(
        model,
        kernelName,
        serializedKernel,
        serializedFeatureList,
        serializedSimulationList,
        targetName,
        randomSeed,
        centerTarget,
        validation,
        splitFraction
):
    """
    Create a technique for the GPR prediction model.
    """

    # if kernelName is not one of the possible kernels, throw an error
    possible_kernels = c3.GprPredictionModelParameters.fieldType('kernelName').declaredAnnotations.constraint.possible
    if kernelName not in possible_kernels:
        raise ValueError(f"kernelName must be one of {possible_kernels}")

    # Try to retrive a GprPredictionModelParameters object with the same parameters
    existing = c3.GprPredictionModelParameters.fetch(
        spec={
            "filter": c3.Filter.inst().eq("model.id", model.id).and_(
            ).eq("kernelName", kernelName).and_(
            ).eq("serializedKernel", serializedKernel).and_(
            ).eq("serializedFeatureList", serializedFeatureList).and_(
            ).eq("serializedSimulationList", serializedSimulationList).and_(
            ).eq("targetName", targetName).and_(
            ).eq("randomSeed", randomSeed).and_(
            ).eq("centerTarget", centerTarget).and_(
            ).eq("validation", validation).and_(
            ).eq("splitFraction", splitFraction)
        }
    ).objs
    if existing:
        return existing[0]
    
    obj = c3.GprPredictionModelParameters(
        model = model,
        kernelName = kernelName,
        serializedKernel = serializedKernel,
        serializedFeatureList = serializedFeatureList,
        featureList = c3.PythonSerialization.deserialize(serialized=serializedFeatureList),
        serializedSimulationList = serializedSimulationList,
        simulationList = c3.PythonSerialization.deserialize(serialized=serializedSimulationList),
        targetName = targetName,
        randomSeed = randomSeed,
        centerTarget = centerTarget,
        validation = validation,
        splitFraction = splitFraction
    ).upsert()
    return c3.GprPredictionModelParameters.get(obj.id)



    