def train(this, input, targetOutput, spec):
    """
    Performs Scikit-Learn's PCA fit().
    https://scikit-learn.org/stable/modules/generated/sklearn.decomposition.PCA.html
    """
    from sklearn.decomposition import PCA

    # get number of PCA components
    nComps = this.technique.nComponents
    
    # cast features into pandas df
    data = c3.Dataset.toPandas(dataset=input)

    # call pca
    pca = PCA(n_components=nComps)
    #data = pca.fit_transform(data)
    pca.fit(data)

    # serialize this training
    this.trainedModel = c3.MLTrainedModelArtifact(model=c3.PythonSerialization.serialize(obj=pca))

    return this


def process(this, input, spec):
    """
    Performs Scikit-Learn's PCA transform().
    https://scikit-learn.org/stable/modules/generated/sklearn.decomposition.PCA.html
    """

    data = c3.Dataset.toPandas(dataset=input)
    pca = c3.PythonSerialization.deserialize(serialized=this.trainedModel.model)
    data = pca.transform(data)

    return c3.Dataset.fromPython(pythonData=data)


def isProcessable(this):
    """"
    Guarantees that process() can only be called after train()
    """

    return this.isTrained()