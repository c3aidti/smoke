/**
 * Callback that is called synchronously during a request 
 * that creates objs after those objs are created. 
 *
 * @param objs
 *  List of objs that were created.  
 *  The objs will already have been created.  
 *  By default, only the id is present in the objs. 
 *  If more fields are desired a dependency annotation can  be specified 
 * (e.g. `@dependency(include = "field1, field2...")`. 
 * Then the objs will have at least those requested fields.
 * @return List of any errors that were encountered.
 */

function afterCreate(objs) {
  var files = objs.map(createFiles);
  files.forEach(upsertBatch);
  return;
    
  function upsertBatch(batch) {
    ObservationOutputFile.upsertBatch(batch);
  }
    
  function createFiles(obj) {
    // AZURE DIRECTORY PATH HERE
    var pathToFiles = 'azure://' + obj.prePathToFiles + '/' + obj.name;
  
    var observationFiles = FileSystem.inst().listFiles(pathToFiles).files;
    // Remove non-NetCDF files from list and filter correct versionTag
    for (var i = 0; i < observationFiles.length; i++) {
      var of = observationFiles[i];
      if (of.url.slice(-3) !== ".nc") {
        observationFiles.splice(i,1);
      } else if (of.url.slice(-6,-3) !== obj.versionTag) {
        observationFiles.splice(i,1);
      }
    }
    return observationFiles.map(createObsOutFiles);
    
    function createObsOutFiles(file) {
      var year = file.url.slice(-15,-11);
      var month = file.url.slice(-11,-9);
      var day = file.url.slice(-9,-7);
      var date_str = year + "-" + month + "-" + day;
      return ObservationOutputFile.make({
                    "observationSet": obj,
                    "file": File.make({
                            "url": file.url
                    }),
                    "dateTag": DateTime.make({
                            "value": date_str
                    })
      });
    }
  }
};
  
  
  
  
  
/**
* Function to upsert ObservationOutput with data from all ObservaitonOutputFiles
* corresponding to this Simulation Sample
* @param this, outputFiles
*  List of ObservationOutputFiles corresponding to this ObservationSet. 
*
* @return Number of files that were processed
*/
  
function upsertObservationData() {
  var results = this.outputFiles.map(upsert);
  
  function upsert(file) {
    var actual_file = ObservationOutputFile.get(file.id);
    return actual_file.upsertORACLESData();
  };
  
  var total = results.reduce(function (previousValue, currentValue) {
    return previousValue + currentValue;
  }, 0)
  
  return total;
};
  


/**
 * removeAllSeededData()
 * Removes all seeded data for this project.
 */
 function removeAllSeededData() {
  ObservationSet.removeAll();
  ObservationOutput.removeAll();
  ObservationOutputFile.removeAll();
  ObservationOutputSeries.removeAll();

  return 0;
}