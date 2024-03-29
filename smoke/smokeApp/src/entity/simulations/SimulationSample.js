/**
* Copyright (c) 2022, C3 AI DTI, Development Operations Team
* All rights reserved. License: https://github.com/c3aidti/.github
**/

/**
* Implementation of upsertFileTable
* @param this: {@link SimulationSample} instance
* @return: integer 
*/
function upsertFileTable() {
    var simSample = this;
    var ensemble = SimulationEnsemble.get(this.ensemble.id);
    var simString = padStart(String(this.simulationNumber), 3, '0');
 
   // ACURE-AIRCRAFT CONTAINER
    var containerRoot = FileSystem.urlFromMountAndRelativeEncodedPath("ACR_RCFT_SMLTNS");
    var ensemblePath = containerRoot + ensemble.name + '/';
    var prePathToFiles = ensemblePath + ensemble.prePathToFiles;
    var pathToSample = prePathToFiles + simString;
    var fileStream = FileSystem.inst().listFilesStream(pathToSample,-1);
    var acureFiles = new Array();

    while(fileStream.hasNext()) {
        var file = fileStream.next();
        if(file.url.endsWith(".nc")) {
            acureFiles.push(file);
        }
    }

    var fileObjects = acureFiles.map(createSimOutFile);
    SimulationOutputFile.upsertBatch(fileObjects);
 
   // 3HOURLY-AOD CONTAINER
    var aodFiles = new Array();
    var months = ['jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec'];
    var containerRoot = FileSystem.urlFromMountAndRelativeEncodedPath("AOD_3HRLY");
    for (var i = 0; i < months.length; i++) {
        var month = months[i];
        var pathToFiles = containerRoot + month + "/";
        var fileStream = FileSystem.inst().listFilesStream(pathToFiles, -1);
        while (fileStream.hasNext()) {
            var file = fileStream.next();
            var simString2 = file.url.split("_")[1];
            simString2 = simString2.slice(0,3);
            if (simString2 === simString && file.url.includes(".nc") && !file.url.includes('ACURE')) {
                aodFiles.push(file);
            };
        };
    };
 
    var fileObjects = aodFiles.map(createSimOutFile);
    SimulationOutputFile.upsertBatch(fileObjects);

    return 0;
 
 
    function padStart(text, length, pad) {
        return (pad.repeat(Math.max(0, length - text.length)) + text).slice(-length);
    }
   
    function createSimOutFile(file) {
        if (file.url.includes("azure://aod-3hourly")) {
            var date = file.url.split("a.pb")[1]
            date = date.split(".pp")[0]
            var year = date.slice(0,4);
            var month = date.slice(4,6);
            var day = date.slice(6,8);
            var date_str = year + "-" + month + "-" + day;
            var container = "aod-3hourly";
            return SimulationOutputFile.make({
                "simulationSample": simSample,
                "file": File.make({
                    "url": file.url
                }),
                "dateTag": DateTime.make({
                    "value": date_str
                }),
                "container": container
            });
        } else {
            var year = file.url.slice(-11,-7);
            var month = file.url.slice(-7,-5);
            var day = file.url.slice(-5,-3);
            var date_str = year + "-" + month + "-" + day;
            var container = "acure-aircraft";
            return SimulationOutputFile.make({
                "simulationSample": simSample,
                "file": File.make({
                    "url": file.url
                }),
                "dateTag": DateTime.make({
                    "value": date_str
                }),
                "container": container
            });  
        };
    };
};
