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

function getSimulationOutputTypeName() {
  var thisTypeName = this.type().typeName()
  var thisType = TypeRef.make({"typeName": thisTypeName}).toType()
  return thisType.mixins()[2].bindings()['OT'].typeName()
}

function getGeoTimeGridTypeName() {
  var thisTypeName = this.type().typeName()
  var thisType = TypeRef.make({"typeName": thisTypeName}).toType()
  return thisType.mixins()[2].bindings()['GT'].typeName()
}

function getTrainedPredictionModelTypeName() {
  var thisTypeName = this.type().typeName()
  var thisType = TypeRef.make({"typeName": thisTypeName}).toType()
  return thisType.mixins()[2].bindings()['MT'].typeName()
}
function getSimulationEnsemble() {
  return SimulationEnsemble.get(this.ensemble.id)
}

function getSimulationModel() {
  var ensemble = this.getSimulationEnsemble()
  return SimulationModel.get(ensemble.model.id)
}

function getSimulationList () {
  var simulationRunTypeName = this.getSimulationRunTypeName();
  var simulationRunType = TypeRef.make({"typeName": simulationRunTypeName}).toType();
  // create list of o.simulationNumner, order by simulationNumber
  return simulationRunType.fetch({
    filter: Filter.eq("ensemble.id", this.ensemble.id)
  }).objs.map(function(o) { return o.simulationNumber }).sort(function(a, b) { return a - b });
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
  var job = SimulationEnsembleRunLoadOutputDataJob.make({
    options: options
  }).upsert();
  job.start();
  return job;
}

function startTrainGprPredictionModelJob(gprTechnique, geoTimeGridFetchSpec, hardwareProfileId, batchSize) {
  var options = TrainGprPredictionModelJobOptions.make({
    dataset: this,
    gprTechnique: gprTechnique,
    geoTimeGridFetchSpec: geoTimeGridFetchSpec,
    hardwareProfileId: hardwareProfileId,
    batchSize: batchSize,
    X: this.getFeaturesForTechnique(gprTechnique)
  });
  var job = TrainGprPredictionModelJob.make({
    options: options
  }).upsert();
  job.start();
  return job;
}

function stageTrainedPredictionModelRowsForTechnique(geoTimeGridFetchSpec,technique, batchSize) {
  var geoGridTypeName = this.getGeoTimeGridTypeName();
  var geoGridType = TypeRef.make({"typeName": geoGridTypeName}).toType()
  // var gridPointCount = geoGridType.fetchCount(geoTimeGridFetchSpec)
  var modelTypeName = this.getTrainedPredictionModelTypeName();
  var modelType = TypeRef.make({"typeName": modelTypeName}).toType()

  var specj = geoTimeGridFetchSpec.toJson();
  specj.type = 'FetchStreamSpec'
  var streamSpec = FetchStreamSpec.make(specj)
  var gridPoints = geoGridType.fetchObjStream(streamSpec);

  var batch = [];
  var upcount = 0;
  while(gridPoints.hasNext()) {
    var gridPoint = gridPoints.next();
    // Create a composite key for the upsert
    var compositeKey = StringUtil.safeId(this.id + "_" + technique.id + "_" + gridPoint.id + "_");
    var row = modelType.make({
      "id": compositeKey,
      "dataset": this,
      "geoTimeGridPoint": gridPoint,
      "technique": technique
    });
    batch.push(row);
    if (batch.length >= batchSize || !gridPoints.hasNext()) {
      var uplist = modelType.mergeBatch(batch);
      upcount += uplist.count();
      batch = [];
    }
  }

  return upcount;
    
  }