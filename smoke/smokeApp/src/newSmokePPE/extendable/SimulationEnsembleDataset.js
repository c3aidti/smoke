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
      var updated = TypeRef.make({"typeName": typeName}).toType()
      updated.id = this.id
      updated.coarseGrainOptions = coarseGrainOptions
      updated.merge() 
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
    return file
}