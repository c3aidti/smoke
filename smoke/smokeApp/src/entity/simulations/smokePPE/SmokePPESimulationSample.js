/**
* Copyright (c) 2022, C3 AI DTI, Development Operations Team
* All rights reserved. License: https://github.com/c3aidti/.github
**/
/**
* Implementation of upsertFileTable
* @param this: {@link SmokePPESimulationSample} instance
* @return: integer 
*/
function upsertFileTable() {
    var simSample = this;
 
   // SMOKE-PPE CONTAINER
    var containerRoot = FileSystem.urlFromMountAndRelativeEncodedPath("SMK_PPE");
    var pathToFiles = containerRoot + "ens_" + String(this.simulationNumber) + "_glm_atmosphere";
    var fileStream = FileSystem.inst().listFilesStream(pathToFiles,-1);
    var smokePPEFiles = new Array();

    while(fileStream.hasNext()) {
        var file = fileStream.next();
        if(file.url.endsWith(".nc")) {
            smokePPEFiles.push(file);
        };
    };

    var fileObjects = smokePPEFiles.map(createSimOutFile);
    SmokePPESimulationOutputFile.upsertBatch(fileObjects);

    return 0;
 
   
    function createSimOutFile(file) {
        if (file.url.includes("azure://smoke-ppe/")) {
            var container = "smoke-ppe";
            return SmokePPESimulationOutputFile.make({
                "simulationSample": simSample,
                "file": File.make({
                    "url": file.url
                }),
                "container": container
            });
        };
    };
}