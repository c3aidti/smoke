/**
 * SimulationEnsemble.js
 * removeAllSeededData()
 * Removes all seeded data for this project.
 */
function removeAllSeededData() {
    SimulationEnsemble.removeAll();
    SimulationSample.removeAll();
    SimulationModelParameters.removeAll();
    SimulationOutputFile.removeAll();
    SimulationModelOutput.removeAll();
    SimulationModelOutputSeries.removeAll();

    return 0;
}


/**
 * upsertEnsembleData()
 * Upsert data into SimModOut for all SimOutFils corresponding to all samples 
 * in this ensemble
 */
function upsertEnsembleData() {
    var results = this.samples.map(upsert);
  
    function upsert(sample) {
      var actual_sample = SimulationSample.get(sample.id);
      return actual_sample.upsertSampleData();
    };
  
    var total = results.reduce(function (previousValue, currentValue) {
      return previousValue + currentValue;
    }, 0)
  
    return total;
};