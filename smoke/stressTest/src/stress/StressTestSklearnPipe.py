def run():
    """
    Starts a single instance of StressTestSklearnPipe
    """
    import numpy as np
    from sklearn.model_selection import train_test_split

    # create synthetic data
    nrows = 2**20
    ncols = 60
    grid = np.linspace(0,1.0, nrows)
    features = np.empty((nrows,ncols))

    for i in range(ncols):
        a = np.random.rand()
        b = np.random.rand()
        c = np.random.rand()
        feat = a*grid + b + np.random.normal(loc=0, scale=c, size=grid.size)
        features[:,i] = feat
    
    response = np.empty(nrows)
    a = np.random.rand()
    b = np.random.rand()
    c = np.random.rand()
    response = a*grid + b + np.random.normal(loc=0, scale=c, size=grid.size)

    # create c3 datasets
    datasets = train_test_split(features, response, test_size=0.1, random_state=42)
    X_train = c3.Dataset.fromPython(datasets[0])
    X_test = c3.Dataset.fromPython(datasets[1])
    y_train = c3.Dataset.fromPython(datasets[2])
    y_test = c3.Dataset.fromPython(datasets[3])

    # build ML pipeline
    linearRegression = c3.SklearnPipe(
                        name="linearRegression",
                        technique=c3.SklearnTechnique(
                            name="linear_model.LinearRegression",
                            processingFunctionName="predict"
                        )
                     )
    lrPipeline = c3.MLSerialPipeline(
                name="lrPipeline",
                steps=[c3.MLStep(name="linearRegression",
                                 pipe=linearRegression)],
                scoringMetrics=c3.MLScoringMetric.toScoringMetricMap(scoringMetricList=[c3.MLRSquaredMetric()])
             )

    # train and score
    trainedLr = lrPipeline.train(input=X_train, targetOutput=y_train)
    scoreLr = trainedLr.score(input=X_test, targetOutput=y_test)

    return scoreLr['MLRSquaredMetric']