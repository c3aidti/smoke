import os
from c3python import get_c3
import pandas as pd
import numpy as np

c3 = get_c3(
    url = "https://devsmokeapprc1-smokeapp.devrc01.c3aids.cloud",
    tenant = "smokeApp",
    tag = "devsmokeApprc1",
    keyfile = os.path.expanduser('~/.c3/c3-rsa.new-smoke')
)

datasetId = 'smoke_ppe_tatz_coarse'
dset = c3.SimulationEnsembleDataset.get(datasetId)
tech = c3.GprPredictionModelParameters.get('TECH-C')
num_features = len(tech.getFeatureList())

def createSynthDataset(num_variants,num_features):
    import numpy as np
    synth_dataset = np.random.rand(num_variants,num_features)
    return c3.Dataset.fromPython(pythonData=synth_dataset)

trainedModel = getattr(c3,dset.getTrainedPredictionModelTypeName()).fetch(spec={"limit":1}).objs[0]

# Predict for single point
ds = trainedModel.predict(
    createSynthDataset(10,num_features)
)
print(c3.PythonSerialization.deserialize(ds))

# predict for set of models
trainedModel = getattr(c3,dset.getTrainedPredictionModelTypeName()).fetch(spec={"limit":2}).objs[0]