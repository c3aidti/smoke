function getSerializedKernel(){
    var typeName = this.type().typeName();
    var thisType = TypeRef.make({"typeName": typeName}).toType();
    var thisObj = thisType.fetch({"filter":Filter.eq('id',this.id),"include":"serializedKernel"}).objs[0];
    return thisObj.serializedKernel;
}
function afterRemove(objs) {
    objs.forEach(function(o) {
       var filter = Filter.eq("dataset.id", o.id);
       // Get ensembles that belong to the model associated with this technique
       var ensembles = SimulationEnsemble.fetch({
           "filter": Filter.eq("model.id", o.model.id),
           "include": "id"
       }).objs;

       // For each ensemble, the list of datasets that belong to the ensemble
       ensembles.forEach(function(ensemble){
            var datasets = SimulationEnsembleDataset.fetch({
                "filter": Filter.eq("ensemble.id", ensemble.id),
                "include": "id"
            }).objs;
    
            // inspect the datasets to retrive the associated TrainedPredictionModel type
            // from each  
            datasets.forEach(function(dataset){
                var trainedPredictionModelTypeName = dataset.getTrainedPredictionModelTypeName();
                var trainedPredictionModelType = TypeRef.make({"typeName": trainedPredictionModelTypeName}).toType();
                var recordsFilter = Filter.eq("dataset.id", dataset.id).and().eq("technique.id", o.id);
                // remove the matching trainedPredictionModels
                trainedPredictionModelType.removeAll(recordsFilter);
            });
       });
    });
};