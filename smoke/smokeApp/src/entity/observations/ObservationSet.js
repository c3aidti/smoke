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