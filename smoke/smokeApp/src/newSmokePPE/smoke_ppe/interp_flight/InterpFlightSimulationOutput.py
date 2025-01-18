def upsertGridPointsfromOutput(datasetId):
    """
    Grab the unique grid points from the interpolated output
    """
    def evaluate_projection(c3_type, projection_dict, column_name = [], ignore_ref = True):
        """
        evaluate_projection(c3_type, projection_dict, column_name = [])
        evalaute_projection_data unpack data from evalaute projection to pandas dataframe
        parameters
        ----------
        c3_type : c3.Type
            Type to evalaute projection
        projection_dict : dict
            A dictionary contain projection details 
        column_name : [string], default = []
            A list of string contains column name for projection. If not specified, 
            the function extract columns name from column header. 
        ignore_ref : bool, default = True
            if ignore_ref is True, the return data frame will unpack the reference column. In other words,
            instead of getting c3.PhysicalAsset(id: "abcd"), you will get "abcd"
        returns
        -------
        df : pandas dataframe
            output of evalaute projection 
        examples
        --------
        >>> projection_dict = {'projection': "assetId, count())",
                                 'group': 'assetId',
                                 'filter': "startsWith(workOrderType, '{}')".format('correction')}
        >>> c3_type = c3.WorkOrder 
        >>> evaluate_projection(c3_type, projection_dict)
            assetId    count()
        0   ABHDG        5
        1   UUYSH101B   10
        """
        import pandas as pd
        import warnings 
        import re
        #Input check#
        if not isinstance(c3_type, c3.Type):
            raise ValueError("The input c3_type is not c3.Type (e.g., c3.PhysicalAsset)")
        if len(column_name) != 0:
            nCol = len(projection_dict['projection'].split(","))
            if nCol != len(column_name):
                raise ValueError("Number of column names in column_name are not equal to number of projections")
        if not "projection" in projection_dict:
            raise ValueError("projection_dict must contain 'projection' as one of the key")
        #### Evaluate projection ####
        data = c3_type.evaluate(projection_dict)
        if data.count == 0:
            warnings.warn("No output from evalaute projection")
            return pd.DataFrame([])
        N = len(data.tuples[0].cells)          #number of projection's columns 
        #one line of unpack data 
        df = pd.DataFrame([[x.cells[i].value() for x in data.tuples] for i in range(N)]).transpose()
        #unpack reference columns (Forien key column)
        if ignore_ref == True:
            for i in range(N):
                if "c3." in str(type(df.iloc[0, i])):
                    df[i] = df[i].apply(lambda x: [list(x) for x in zip(*x.__dict__.items())][-1][0])    
        #Renaming column 
        if len(column_name) == 0:
            r = re.compile(r'(?:[^,(]|\([^)]*\))+')
            column_name = r.findall(projection_dict['projection'])
        rename_dict = {k: v for (k, v) in zip(range(N), column_name)}
        df.rename(rename_dict, axis = 1, inplace = True) 
        return df
    datasetObj = c3.SimulationEnsembleDataset.fetch(
        spec = {
            "filter":c3.Filter.inst().eq('id',datasetId),
            "include": "id"
        }
    ).objs[0]
    datasetType = datasetObj.toJson()['type']
    dataset = getattr(c3,datasetType).get(datasetId)
    geoTimeGridType = getattr(c3,datasetType).mixins[2].genericVarBindings[1].name #SppeTatzCoarseGeoTimeGrid
    simulationOutputType = getattr(c3,datasetType).mixins[2].genericVarBindings[2].name #SppeTatzCoarseSimulationOutput
    projection_dict = {
                         'projection':"geoTimeGridPoint.id,max(latitude),max(longitude),max(time)",
                         'group':"geoTimeGridPoint.id",
                      }
    c3_type = getattr(c3,simulationOutputType)
    df_grid = evaluate_projection(c3_type, projection_dict)
    
    df_grid = df_grid.rename(columns={
        'max(latitude)': 'latitude',
        'max(longitude)':'longitude',
        'max(time)':'time',
        'geoTimeGridPoint.id': 'id'
        }
    )
    df_grid['dataset'] = dataset
    
    # Upsert Grid locations
    batch_records = df_grid.to_dict(orient="records")
    getattr(c3, geoTimeGridType).mergeBatch(objs=batch_records)
    
    return True