function transform(source){
    var types = [];
    // For each simulation number, read the field: ens_<sim#>_m01s02i530_550nm
    // and appedn types array with json of new type
    for (var i = 0; i <= 120; i++){
        var field = "ens_" + i + "_m01s02i530_550nm";
        types.push({
            "type": "InterpolatedFlightSimulationOutput",
            "simulationRun": {"id": "smoke_ppe_tatz_" + i},
            "dataset" : {"id": "smoke_ppe_tatz_flight"},
            "geoTimeGridPoint":{"id": source.latitude + "_" + source.longitude + "_" + source.time},
            "exCoeff550": source[field]
        });

    }
    return types;
}