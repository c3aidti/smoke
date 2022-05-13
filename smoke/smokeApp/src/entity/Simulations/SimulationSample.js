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
    SimulationOutputFile.upsertBatch(batch);
  }
  
  function createFiles(obj) {
    var ensemble = SimulationEnsemble.fetch({
                      filter: Filter.eq("id",obj.ensemble.id) 
                    }).objs[0]

    // ACURE-AIRCRAFT CONTAINER                
    var ensemblePath = FileSystem.inst().rootUrl() + 'gordon-group/' + ensemble.name + '/';
    var prePathToAllFiles = ensemblePath + ensemble.prePathToFiles;
    var pathToSample = prePathToAllFiles + padStart(String(obj.simulationNumber),3,'0');
    var allAAFiles = FileSystem.inst().listFiles(pathToSample).files;
    var sampleFiles = new Array();

    // Remove non-NetCDF files from list
    for (var i = 0; i < allAAFiles.length; i++) {
      var sf = allAAFiles[i];
      if (sf.url.slice(-3) === ".nc") {
        sampleFiles.push(sf);
      };
    };

    // MONTHLY-MEAN CONTAINER...
    var simString = padStart(String(obj.simulationNumber), 3, '0');
    var sampleFiles2 = new Array();

    var months = ['jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec'];
    var containerRoot = "azure://gordon/aod-3hourly/";

    for (var i = 0; i < months.length; i++) {
      var month = months[i];
      var pathToFiles = containerRoot + month + "/";
      var fileStream = FileSystem.inst().listFilesStream(pathToFiles);
      while (fileStream.hasNext()) {
        var file = fileStream.next();
        if (file.url.slice(-6,-3) === simString && file.url.slice(-3) === ".nc" && file.url.slice(37,42) !== 'ACURE') {
          sampleFiles2.push(file);
        };
      };
    };

    

    // put two containers together
    sampleFiles = sampleFiles.concat(sampleFiles2);

    return sampleFiles.map(createSimOutFiles);
  
    function padStart(text, length, pad) {
      return (pad.repeat(Math.max(0, length - text.length)) + text).slice(-length);
    }
  
    /// THIS NEEDS WORK -- container will jave diff mount point
    function createSimOutFiles(file) {
      if (file.url.slice(0,32) === "azure://monthly-mean-simulations") {
        var year = file.url.slice(-18,-14);
        var month = file.url.slice(-14,-12);
        var day = file.url.slice(-12,-10);
        var date_str = year + "-" + month + "-" + day;
        var container = "aod-3hourly";
        return SimulationOutputFile.make({
          "simulationSample": obj,
          "file": File.make({
                  "url": file.url
          }),
          "dateTag": DateTime.make({
                  "value": date_str
          }),
          "container": container
        });
      }
      else {
        var year = file.url.slice(-11,-7);
        var month = file.url.slice(-7,-5);
        var day = file.url.slice(-5,-3);
        var date_str = year + "-" + month + "-" + day;
        var container = "acure-aircraft";
        return SimulationOutputFile.make({
                  "simulationSample": obj,
                  "file": File.make({
                          "url": file.url
                  }),
                  "dateTag": DateTime.make({
                          "value": date_str
                  }),
                  "container": container
        });
      }
    }
  }
};





/**
 * Function to upsert SimulationModelOutput with data from all SimulationOutputFiles
 * corresponding to this Simulation Sample
 * @param this, outputFiles
 *  List of SimulationOutputFiles corresponding to this SimulationSample. 
 *
 * @return Number of files that were processed
 */

function upsertSampleData() {
  var results = this.outputFiles.map(upsert);

  function upsert(file) {
    var actual_file = SimulationOutputFile.get(file.id);
    return actual_file.upsertData();
  };

  var total = results.reduce(function (previousValue, currentValue) {
    return previousValue + currentValue;
  }, 0)

  return total;
};
