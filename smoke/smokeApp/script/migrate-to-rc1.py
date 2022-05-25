import os
from c3python import C3Migrate
#from c3python import AzureBlob

MIGRATE_CLUSTER = True

from_url = "https://tc02d-dev.c3dti.ai"
from_tenant = "dev"
from_tag = "tc02d"
from_token = '303303021ba1154cf7e287b290c48f79542e7284a9f131d36ea60347223b7fededb04957f40929f3173a0c31ed3608417e7e40c81d95f512c8ca544508c4bd9108be'

to_url = "https://testsmokeapprc1-smokeapp.devrc01.c3aids.cloud"
to_tenant = "smokeApp"
to_tag = "test/smokeApp/rc1"
to_token = '30333cb45af6bb0b9855bb146887acdf6647948362b7375de64a4d80c0829076aadf8819bc56698ef01ccd4cc4d8faccde9c'

c3m = C3Migrate(
    from_url, to_url,
    from_tenant, to_tenant,
    from_tag, to_tag,
    from_auth from_token,
    to_auth = to_token,
    # toBlobConnectionStringFile=os.path.expanduser('~/.c3/dti-qa-container.id'),
    # toBlobContainer='sbox-azdti',
    retrys=12
)

types = [
   "UpsertObsData",
    "ObservationOutput",
    "TransformSourceSimulationModelParametersMapToSimulationModelOutputSeries",
    "TestAODData",
    "UpsertData",
    "SimulationModelOutput",
    "SimulationOutputFile",
    "PrincipalComponentAnalysisPipe",
    "ObservationSet",
    "TransformSourceObservationSetToObservationOutputSeries",
    "Simulation3HourlyAODOutput",
    "SMOMLDataSourceSpec",
    "TransformSourceObservationSetToObservationSet",
    "GaussianProcessRegressionPipe",
    "SimulationEnsemble",
    "ObservationOutputFile",
    "GeoSurfaceTimePoint",
    "SimulationModelOutputSeries",
    "TransformSourceSimulationModelParametersMapToSimulationModelParameters",
    "SimulationSample",
    "ObservationOutputSeries",
    "UpsertAODData",
    "SimulationModelParameters",
    "CreateSimulationFileTable",
    "TransformSourceSimulationEnsembleToSimulationEnsemble",
    "TestGSTP",
    "TransformSourceSimulationModelParametersMapToSimulationSample",
]

c3m.migrate_types(types, remove_to=False)