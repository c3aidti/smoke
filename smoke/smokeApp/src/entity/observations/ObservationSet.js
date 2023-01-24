/**
* Copyright (c) 2022, C3 AI DTI, Development Operations Team
* All rights reserved. License: https://github.com/c3aidti/.github
**/
/**
* Implementation of upsertFileTable
* @param this: {@link ObservationSet} instance
* @return: integer 
*/
function upsertFileTable() {
    var obsSet = this;
    if (obsSet.names.slice(0,5) === "MODIS") {
        var containerRoot = FileSystem.urlFromMountAndRelativeEncodedPath("MDS_DLY_LVL3");
        var fileStream = FileSystem.inst().listFilesStream(containerRoot, -1);
        var setFiles = new Array();

        while(fileStream.hasNext()) {
            var file = fileStream.next();
            var url = file.url;
            if(url.endsWith(".nc" && url.split(".")[0] == obsSet.prePathToFiles)) {
                setFiles.push(file);
            }
        };
    } 
    else {
        var containerRoot = FileSystem.urlFromMountAndRelativeEncodedPath("RCFT_BSRVTNS");
        var pathToFiles = containerRoot + obsSet.name;
        var fileStream = FileSystem.inst().listFilesStream(pathToFiles,-1);
        var setFiles = new Array();

        while(fileStream.hasNext()) {
            var file = fileStream.next();
            var url = file.url
            if(url.endsWith(".nc") && url.slice(-6,-3) == this.versionTag) {
                setFiles.push(file);
            }
        };
    };

    var fileObjects = setFiles.map(createObsOutFile);
    ObservationOutputFile.upsertBatch(fileObjects);


    function createObsOutFile(file) {
        if(obsSet.name == "ATom_60s") {
            return ObservationOutputFile.make({
                "observationSet": obsSet,
                "file": File.make({
                    "url": file.url
                }),
            });
        }
        else if (obsSet.name == "MODIS_daily_08_D3") {
            var year = file.url.split(".")[1].slice(1,5);
            var dayNumber = parseInt(file.url.split(".")[1].slice(5,8));
            var oneDay = 1000 * 60 * 60 * 24;
            var start = new Date(year, 0, 0);
            var date = new Date(start.getTime() + dayNumber * oneDay);
            return ObservationOutputFile.make({
                "observationSet": obsSet,   
                "file": File.make({
                    "url": file.url
                }),
                "dateTag": date
            });
        }
        else {
            var year = file.url.slice(-15,-11);
            var month = file.url.slice(-11,-9);
            var day = file.url.slice(-9,-7);
            var date_str = year + "-" + month + "-" + day;
            return ObservationOutputFile.make({
                "observationSet": obsSet,
                "file": File.make({
                    "url": file.url
                }),
                "dateTag": DateTime.make({
                    "value": date_str
                })
            });
        };
    };

    return 0;
}