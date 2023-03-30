# Our scalable cloud-based inference framework

The purpose of this directory is to provide adequate code and post-processed data to reproduce a number of examples and results presented in the work "Statistical constraints on climate model parameters using a scalable cloud-based inference framework" (2023). **Note:** Components of our framework are exclusively compatible with the proprietary cloud computing resources available on the C3 Enterprise AI platform.

## Notebooks

The flow chart for our pipeline for building frequentist confidence sets on climate model parameters is below.

![](./data_model.png?raw=true "Inference framework pipeline")
*Figure 1. Pipeline flow chart from Section 1.1.*

The `SimulatedGrid` for our application is the A-CURE PPE output, available on the CEDA archive (Regayre et al, 2023). The `SatelliteGrid` is the set of MODIS AOD measurements, openly available through the MODIS web page (NASA, 2023). Matching is performed using methods built into the [Observer](./notebooks/classes/Observer.ipynb) notebook. Some `MatchedSatelliteTimeseries` data can be found in the post-processed [observations](./postProcessedData/observations.csv) data set. The five pipes of our pipeline are reproduced as follows.

The [`EmulatorTraining`](./notebooks/classes/Emulator.ipynb) pipes (Sections 2.1-2.2) provide means to scalably train an emulator from the `SimulatedGrid` set. The [`EmulatorEvaluation`](./notebooks/pipeline/EmulatorEvaluation.ipynb) pipe features fast methods to evaluate the emulator over a large collection of test parameter vectors and includes code used to determine outliers. **Note:** These pipes employ some methods exclusively compatible with the proprietary cloud computing resources available on the C3 Enterprise AI platform.

The [`DataDrivenModelDiscrep`](./notebooks/pipeline/DataDrivenModelDiscrep.ipynb) pipe (Section 2.3) performs numerical maximum likelihood estimation using the `scipy` library, specifically using the [`scipy.optimize`](https://docs.scipy.org/doc/scipy/reference/generated/scipy.optimize.minimize_scalar.html#scipy.optimize.minimize_scalar) routine. The [`PlausibilityTest`](./notebooks/pipeline/PlausibilityTest.ipynb) pipe features code for computing the strict bounds implausiblity metric defined in Section 2.4 and the history matching implausibility statistic described in the Appendix. The [`FrequentistConfSet`](./notebooks/pipeline/FrequentistConfSet.ipynb) furnishes means for visualizing the parameter constraints resultant of the `PlausibilityTest` results for the purpose of replicating Figure 3 or Figure 4.

 Also included in this directory are i) some partial code for a class devoted to performing lengthscale parameter clustering and plotting (see the [Clusterer](./notebooks/classes/Clusterer.ipynb) notebook); ii) partial code for a class devoted to reporting confidence sets (see the [Statistician](./notebooks/classes/Statistician.ipynb) notebook); and iii) code used to reproduce Figure 2 (see the [`MissingnessMapPlot`](./notebooks/MissingnessMapPlot.ipynb) notebook). Some examples for the different classes are offered in the [examples](./notebooks/examples/) folder.

## postProcessedData

In addition to the available code found in the notebooks described above, a minimal number of data are provided to reproduce some examples which do not require the complete C3 Enterprise AI platform:

 - A collection of 5,000 parameter [vectors](./postProcessedData/model_variants.csv) used for constructing confidence sets as well as their rescaled [counterparts](./postProcessedData/inputs.csv). The parameters are perturbed on various intervals (see Table 1) but are normalized to the interval [0, 1] when provided as input to the emulator. 
 - Implausibilites obtained from our [strict bounds method](./postProcessedData/implausibilities.csv) and from the adapted [history matching method](./postProcessedData/hm_implausibilities.csv)
 - [Matched observations](./postProcessedData/observations.csv) and a table labelling [missing and outlier observations](./postProcessedData/outliers.csv)
 - The estimated maximum likelihood [estimate](./postProcessedData/mle.csv) for the unaccounted-for variance term $\widehat{\delta_{\text{MLE}}^2}$

## Feedback

Please consider leaving feedback or reporting bugs to [this address](mailto:jcarzon@andrew.cmu.edu). We welcome contributions.

## References

 - [NASA MODIS Aerosol Product (last access: 17 January 2023)](https://modis.gsfc.nasa.gov/data/dataprod/mod04.php)
 - [C3 AI Enterprise (last access: 13 December 2022)](https://c3.ai/)
 - [Identifying climate model structural inconsistencies allows for tight constraint of aerosol radiative forcing (2023), EGUsphere](https://egusphere.copernicus.org/preprints/2023/egusphere-2023-77/)