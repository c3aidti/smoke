/**
* Copyright (c) 2022, C3 AI DTI, Development Operations Team
* All rights reserved. License: https://github.com/c3aidti/.github
**/
/**
* Implementation of upsertFileTable
* @param this: {@link SmokePPESimulationRun} instance
* @return: integer 
*/
function upsertFileTable() {
    var simSample = this;
 
   // SMOKE-PPE CONTAINER
    var containerRoot = FileSystem.urlFromMountAndRelativeEncodedPath("SMK_PPE");
    var pathToFiles = containerRoot + "ens_" + String(this.simulationNumber) + "_glm_atmosphere";
    var pathToFiles_air = containerRoot + "ens_" + String(this.simulationNumber) + "_glm_air";
    var pathToFiles_mf = containerRoot + "ens_" + String(this.simulationNumber) + "_glm_mass_fraction_of_cloud_liquid_water_in_air";
    var pathToFiles_exc = containerRoot + "ens_" + String(this.simulationNumber) + "_glm_m01s02i530_m01s02i530";
    var pathToFiles_cdnc_ctw = containerRoot + "ens_" + String(this.simulationNumber) + "_glm_m01s01i298_m01s01i298";
    var pathToFiles_cdnc_wghts = containerRoot + "ens_" + String(this.simulationNumber) + "_glm_m01s01i299_m01s01i299";
    var pathToFiles_swrf = containerRoot + "ens_" + String(this.simulationNumber) + "_glm_m01s01i205_m01s01i205";
    var fileStream = FileSystem.inst().listFilesStream(pathToFiles,-1);
    var fileStream_air = FileSystem.inst().listFilesStream(pathToFiles_air,-1);
    var fileStream_mf = FileSystem.inst().listFilesStream(pathToFiles_mf,-1);
    var fileStream_exc = FileSystem.inst().listFilesStream(pathToFiles_exc,-1);
    var fileStream_cdnc_ctw = FileSystem.inst().listFilesStream(pathToFiles_cdnc_ctw,-1);
    var fileStream_cdnc_wghts = FileSystem.inst().listFilesStream(pathToFiles_cdnc_wghts,-1);
    var fileStream_swrf = FileSystem.inst().listFilesStream(pathToFiles_swrf,-1);
    var smokePPEFiles = new Array();
    var swrfFiles = new Array();

    while(fileStream.hasNext()) {
        var file = fileStream.next();
        if(file.url.endsWith(".nc")) {
            smokePPEFiles.push(file);
        };
    };

    while(fileStream_air.hasNext()) {
        var file = fileStream_air.next();
        if(file.url.endsWith(".nc")) {
            smokePPEFiles.push(file);
        };
    };

    while(fileStream_mf.hasNext()) {
        var file = fileStream_mf.next();
        if(file.url.endsWith(".nc")) {
            smokePPEFiles.push(file);
        };
    };

    while(fileStream_exc.hasNext()) {
        var file = fileStream_exc.next();
        if(file.url.endsWith(".nc")) {
            smokePPEFiles.push(file);
        };
    };

    while(fileStream_cdnc_ctw.hasNext()) {
        var file = fileStream_cdnc_ctw.next();
        if(file.url.endsWith(".nc")) {
            smokePPEFiles.push(file);
        };
    };

    while(fileStream_cdnc_wghts.hasNext()) {
        var file = fileStream_cdnc_wghts.next();
        if(file.url.endsWith(".nc")) {
            smokePPEFiles.push(file);
        };
    };    

    while(fileStream_swrf.hasNext()) {
        var file = fileStream_swrf.next();
        if(file.url.endsWith(".nc")) {
            swrfFiles.push(file);
        };
    };

    var fileObjects = smokePPEFiles.map(createSimOutFile);
    SppeSimulationEnsembleOutputFile.upsertBatch(fileObjects);

    var fileSwrfObjects = swrfFiles.map(createSwrfOutFile);
    SppeSimulationEnsembleSwrfFile.upsertBatch(fileSwrfObjects)

    return 0;
 
    function createSimOutFile(file) {
        if (file.url.includes("azure://smoke-ppe/")) {
            var container = "smoke-ppe";
            var filename = file.url.split("azure://smoke-ppe/")[1];
            return SppeSimulationEnsembleOutputFile.make({
                "id": filename,
                "simulationRun": simSample,
                "file": File.make({
                    "url": file.url
                }),
                "container": container
            });
        };
    };

    function createSwrfOutFile(file) {
        if (file.url.includes("azure://smoke-ppe/")) {
            var container = "smoke-ppe";
            var filename = file.url.split("azure://smoke-ppe/")[1];
            return SppeSimulationEnsembleSwrfFile.make({
                "id": filename,
                "simulationRun": simSample,
                "file": File.make({
                    "url": file.url
                }),
                "container": container
            });
        };
    };
}