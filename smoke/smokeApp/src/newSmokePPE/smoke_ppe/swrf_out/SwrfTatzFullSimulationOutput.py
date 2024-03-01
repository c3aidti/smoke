def upsertSimulationOutput(this, batchSize=80276):
    import pandas as pd
    import datetime as dt
    import numpy as np
        
    thisType = this.toJson()['type'] #type of this obj instance
    outputFileType = getattr(c3,thisType).mixins[1].genericVarBindings[1].name #SppeSimulationEnsembleOutputFile
    datasetObj = c3.SimulationEnsembleDataset.fetch(
        spec = {
            "filter":c3.Filter.inst().eq('id',datasetId),
            "include": "id"
        }
    ).objs[0]
    datasetType = datasetObj.toJson()['type'] #SppeTatzCoarseSimulationEnsembleDataset
    geoTimeGridType = getattr(c3,datasetType).mixins[2].genericVarBindings[1].name #SppeTatzCoarseGeoTimeGrid
    simulationOutputType = this.toJson()['type'] #SppeTatzCoarseSimulationOutput
    thisDataset = getattr(c3,datasetType).get(datasetId)
    coarseGrainOptions = thisDataset.coarseGrainOptions

    def make_gstp(objId):
        return getattr(c3,geoTimeGridType)(id=objId)


    # grab the relevant files
    files = getattr(c3,outputFileType).fetch({
        "filter": c3.Filter().eq("simulationRun.id", this.id)
    }).objs

    swrf_url = files[0].file.url
    
    #------------------------------SWRF Calcs------------------------------------
    # create GSTP objects
    gstpFile = swrf_url
    sample = c3.NetCDFUtil.openFile(gstpFile)
    lat = sample["latitude"][:]
    lon = [x*(x < 180) + (x - 360)*(x >= 180) for x in sample["longitude"][:]]
    tim = sample["time"][:]
    c3.NetCDFUtil.closeFile(sample,gstpFile)
    
    # construct correct times array
    zero_time = dt.datetime(1970,1,1,0,0)
    times = []
    for t in tim:
        target_time = zero_time + dt.timedelta(hours=t)
        times.append(target_time)
    

    if coarseGrainOptions:
        # Coarse-graining: Reduce the resolution of the lat-lon grid
        original_lat = lat.copy()
        original_lon = lon.copy()
        lat = interp_coord(lat,coarseGrainOptions.coarseFactor)
        lon = interp_coord(lon,coarseGrainOptions.coarseFactor)

    df_st = pd.DataFrame()
        
    df_st["time"] = [t for t in times for n in range(0, len(lat)*len(lon))]
    df_st["latitude"] = [l for l in lat for n in range(0, len(lon))]*len(times)
    df_st["longitude"] = [l for l in lon]*len(times)*len(lat)
    df_st["id"] = datasetId + '_' + round(df_st["latitude"],3).astype(str) + "_" + round(df_st["longitude"],3).astype(str) + "_" + df_st["time"].astype(str).apply(lambda x: x.replace(" ", 'T'))
    df_st["geoTimeGridPoint"] = df_st["id"].apply(make_gstp)
    df_st = df_st.drop(columns=["id"])

    # add dataset
    df_st["dataset"] = getattr(c3,datasetType)(id=datasetId)
    
    # add simulation
    df_st["simulationRun"] = getattr(c3,thisType)(id=this.id)
    
    # add unique id
    df_st["id"] = datasetId + '_' + this.id + '_' + round(df_st["latitude"],3).astype(str) + "_" + round(df_st["longitude"],3).astype(str) + "_" + df_st["time"].astype(str).apply(lambda x: x.replace(" ", 'T'))

    df_st = df_st.drop(columns=["time", "latitude", "longitude"])
        
    data = c3.NetCDFUtil.openFile(swrf_url)
    tensor = data['toa_outgoing_shortwave_flux']

    if coarseGrainOptions:
        interpolated_data = []
        for time_slice in tensor:
            interp_data_time_slice = interp_targ_data(time_slice,coarseGrainOptions.coarseFactor,coarseGrainOptions.coarseFactor)
            interpolated_data.append(interp_data_time_slice)

        # Convert the list of interpolated slices into a 3D numpy array
        tensor = np.array(interpolated_data)

    # Flatten the tensor for adding to DataFrame
    df_st['swrfOut'] = tensor.reshape(-1)

    c3.NetCDFUtil.closeFile(data, swrf_url)

    #------------------------------Batch Funcs------------------------------------
    # Initialize an index to track batches
    start_index = 0
    end_index = batchSize

    # Number of records
    total_records = len(df_st)
    
    # upsert data
    while start_index < total_records:
#         print(f"Upserting Batch {start_index}")
        # Create a smaller DataFrame for the current batch
        batch_df = df_st.iloc[start_index:end_index]

        # Convert the batch DataFrame to a list of dictionaries
        batch_records = batch_df.to_dict(orient="records")

        # Upsert the batch
        getattr(c3, simulationOutputType).upsertBatch(objs=batch_records)

        # Update indices for the next batch
        start_index = end_index
        end_index = min(end_index + batchSize, total_records)
        
    return True

#------------------------------Helper Functions------------------------------------
def interp_targ_data(targ_data, lats_step, lons_step):
        """
        targ_data: iterable
        Must be a two dimensional array of the data being regridded in the dimensions of (lats,lons)
        lats_step: int
        The number of points to be included in an average.
        lons_step: int
        The number of points to be included in an average.
        """
        import numpy as np
        lats_dim, lons_dim = np.shape(targ_data)
        
        lats_inds = list(range(0, lats_dim, lats_step))
        lons_inds = list(range(0, lons_dim, lons_step))
        
        rg_targ_data = np.zeros((len(lats_inds), len(lons_inds)))
        m = 0
        for i in lats_inds:
            n = 0
            for j in lons_inds:
                rg_targ_data[m,n] = np.mean(targ_data[i:i+lats_step,j:j+lons_step])
                n +=1
            m += 1
        return rg_targ_data

def interp_coord(data_arr, step):
        """
        data_arr: iterable
        Must be a 1 dimensional iterable.
        step: int
        The number of points to be included in one average.
        """
        import numpy as np
        rg_list = []
        
        ind_list = list(range(0, len(data_arr), step))
        for ind in ind_list:
            rg_list.append(
                np.mean(data_arr[ind:ind+step])
            )
        return rg_list