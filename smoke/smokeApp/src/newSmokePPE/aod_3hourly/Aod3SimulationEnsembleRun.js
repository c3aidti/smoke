/**
* Copyright (c) 2022, C3 AI DTI, Development Operations Team
* All rights reserved. License: https://github.com/c3aidti/.github
**/
/**
* Implementation of upsertFileTable
* @param this: {@link AOD3SimulationRun} instance
* @return: integer 
*/
function upsertFileTable() {
    var simSample = this;
 
   // SMOKE-PPE CONTAINER
    var containerRoot = "azure://aod-3hourly/jul/";
    var pathToFiles = containerRoot;
    var fileStream = FileSystem.inst().listFilesStream(pathToFiles,-1);
    var aod3Files = new Array();

    var paddedSimNumber = String(this.simulationNumber).padStart(3, '0');

    while(fileStream.hasNext()) {
        var file = fileStream.next();
        if(file.url.endsWith(paddedSimNumber + ".nc")) {
            aod3Files.push(file);
        };
    };

    var fileObjects = aod3Files.map(createSimOutFile);
    Aod3SimulationEnsembleOutputFile.upsertBatch(fileObjects);

    return 0;
 
    function createSimOutFile(file) {
        if (file.url.includes("azure://aod-3hourly/jul/")) {
            var container = "aod-3hourly";
            var filename = file.url.split("azure://aod-3hourly/jul/")[1];
            var id = filename;
            // parse the stashCode out of the filename, where the name is of the form "ens_#_..._<m01...>.nc"
            var stashCode = "all_aod";
            return SppeSimulationEnsembleOutputFile.make({
                "id": id,
                "simulationRun": simSample,
                "stashCode": stashCode,
                "file": File.make({
                    "url": file.url
                }),
                "container": container
            });
        };
    };
}