/**
* Copyright (c) 2022, C3 AI DTI, Development Operations Team
* All rights reserved. License: https://github.com/c3aidti/.github
**/
function getPipe(excFeats, gstpId, targetName, technique) {
    // find the data source specs
    var gstpKey = "geoSurfaceTimePoint.id == \"" + gstpId + "\"";
    var filter = Filter.eq("featuresType.typeName", "SmokePPESimulationModelParameters")
        .and().eq("targetType.typeName", "SmokePPESimulationOutput")
        //.and().intersects("excludeFeatures", excFeats)
        .and().eq("targetName", targetName)
        .and().eq("targetSpec.filter", gstpKey);

    var sourceSpecIds = GPRDataSourceSpec.fetch({
        "filter": filter,
        "limit": -1,
        "include": "id"
    }).objs.map(obj => obj.id);

    // find the kernels
    var fullTech = GaussianProcessRegressionTechnique.get(technique.id);
    var fullKernel = SklearnGPRKernel.get(fullTech.kernel.id);
    filter = Filter.eq("pickledKernel", fullKernel.pickledKernel);
    var kernelIds = SklearnGPRKernel.fetch({
        "filter": filter.value,
        "limit": -1,
        "include": "id"
    }).objs.map(obj => obj.id);

    // find the techniques
    filter = Filter.intersects("kernel.id", kernelIds)
        .and().eq("centerTarget", technique.centerTarget);
    var techIds = GaussianProcessRegressionTechnique.fetch({
        "filter": filter.value,
        "limit": -1,
        "include": "id"
    }).objs.map(obj => obj.id);

    // now find the models
    filter = Filter.intersects("technique.id", techIds)
        .and().intersects("dataSourceSpec.id", sourceSpecIds);
    var pipes = GaussianProcessRegressionPipe.fetch({
        "filter": filter.value,
        "limit": -1
    }).objs;

    return pipes
}

function getPipes(excFeats, gstpFilter, targetName, technique) {
    var gstpIds = GeoSurfaceTimePoint.fetch({
        "filter": gstpFilter,
        "limit": -1,
        "include": "id"
    }).objs.map(obj => obj.id);

    var pipes = gstpIds.map(id => SmokePPEGPRPredictor.getPipe(excFeats, id, targetName, technique));
    var nonNulls = pipes.filter(function (el) {
        return el.length != 0;
    });

    return nonNulls
}