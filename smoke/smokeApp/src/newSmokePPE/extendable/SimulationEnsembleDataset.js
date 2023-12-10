/**
 * 
 */
function initializeGrid(coarseGrainOptions) {
    var typeName = this.type().typeName()
    var thisType = TypeRef.make({"typeName": typeName}).toType()
    if (typeof coarseGrainOptions === 'undefined') {
        coarseGrainOptions = null;
    }
    else {
      thisType.make({"id": this.id, "ensemble": this.enemble, "coarseGrainOptions": coarseGrainOptions}).merge()
    }
    // Use  TypeRef to access the bindings of this type
    var geoTimeGridType = thisType.mixins()[2].bindings()['GT'].name()
    
    //Fetch a simulation Run from the ensemble and get its' typeRef to access it's bindings
    var filter = Filter.eq("ensemble.id", "smoke_ppe_tatz")
    var simulationRunTypeName = SimulationRun.fetch({"filter": filter,"include":"id"}).objs[0].type().typeName()
    var simRun = TypeRef.make({"typeName": simulationRunTypeName}).toType()
    
    // Access the type name of the outputFile type used for this ensemble
    var outputFileTypeName = simRun.mixins()[1].bindings()['FT'].name()
    var outputFileType = TypeRef.make({"typeName": outputFileTypeName}).toType()
    
    // Get first file and call upsertGridData
    var file = outputFileType.fetch({"limit":1}).objs[0]
    file.upsertGridData(
      typeName,
      geoTimeGridType,
      this.id,
      coarseGrainOptions
    )
    return true
}

function getSimulationRunTypeName() {
  // var thisTypeName = this.type().typeName()
  // var thisType = TypeRef.make({"typeName": thisTypeName}).toType()
  var filter = Filter.eq("ensemble.id", this.ensemble.id)
  return SimulationRun.fetch({"filter": filter,"include":"id"}).objs[0].type().typeName()
}

function getSimulationParameterList() {
  var simulationRunTypeName = this.getSimulationRunTypeName()
  var simulationRunType = TypeRef.make({"typeName": simulationRunTypeName}).toType()
  var parameterTypeName = simulationRunType.mixins()[2].typeName()
  var fields = simulationRunType.mixins()[2].fieldTypes()

  var filteredFields = fields.filter(function(field) {
      return simulationRunType.fieldTypeDeclaredOn(field.name()).typeName() === parameterTypeName;
  });

  var fieldNames = filteredFields.map(function(field) {
      return field.name();
  });

  return fieldNames;
}

function startLoadOutputDataJob(pseudoLevelIndex, hardwareProfileId, parallelStreams, batchSize) {
  var simulationRunTypeName = this.getSimulationRunTypeName();
  var options = SimulationEnsembleRunLoadOutputDataJobOptions.make({
    typeName: simulationRunTypeName,
    datasetId: this.id,
    hardwareProfileId: hardwareProfileId,
    pseudoLevelIndex: pseudoLevelIndex, 
    parallelStreams: parallelStreams,
    batchSize: batchSize
  });
  job = SimulationEnsembleRunLoadOutputDataJob.make({
    options: options
  }).upsert();
  return job;
}