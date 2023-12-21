def upsertSimulationOutput(this, datasetId, pseudoLevelIndex, batchSize=80276):
    import pandas as pd
    import datetime as dt
    import numpy as np
    
    thisType = this.toJson()['type']
    outputFileType = getattr(c3,thisType).mixins[1].genericVarBindings[1].name #SppeSimulationEnsembleOutputFile
    datasetObj = c3.SimulationEnsembleDataset.fetch(
        spec = {
            "filter":c3.Filter.inst().eq('id',datasetId),
            "include": "id"
        }
    ).objs[0]
    datasetType = datasetObj.toJson()['type'] #SppeTatzCoarseSimulationEnsembleDataset
    geoTimeGridType = getattr(c3,datasetType).mixins[2].genericVarBindings[1].name #SppeTatzCoarseGeoTimeGrid
    simulationOutputType = getattr(c3,datasetType).mixins[2].genericVarBindings[2].name #SppeTatzCoarseSimulationOutput
    thisDataset = getattr(c3,datasetType).get(datasetId) #
    coarseGrainOptions = thisDataset.coarseGrainOptions

    def make_gstp(objId):
        return getattr(c3,geoTimeGridType)(id=objId)

    # variables necessary to calculate clwp
    var_names_ppe = {
            "pressure" : "air_pressure",
            "level_height" : "level_height",
            "potential_temp" : "air_potential_temperature",
            "mass_frac_water" : "mass_fraction_of_cloud_liquid_water_in_air"
    }
    var_names_ppe_inv = {v: k for k, v in var_names_ppe.items()}

    # grab the relevant files
    files = getattr(c3,outputFileType).fetch({
        "filter": c3.Filter().eq("simulationRun.id", this.id)
    }).objs

    # create GSTP objects
    gstpFile = files[0]
    sample = c3.NetCDFUtil.openFile(gstpFile.file.url)
    lat = sample["latitude"][:]
    lon = [x*(x < 180) + (x - 360)*(x >= 180) for x in sample["longitude"][:]]
    tim = sample["time"][:]
    c3.NetCDFUtil.closeFile(sample,gstpFile.file.url)
    
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
    
    # add pseudoLevelIndex
    df_st["pseudoLevelIndex"] = pseudoLevelIndex
    
    # add unique id
    df_st["id"] = datasetId + '_' + this.id + '_' + round(df_st["latitude"],3).astype(str) + "_" + round(df_st["longitude"],3).astype(str) + "_" + df_st["time"].astype(str).apply(lambda x: x.replace(" ", 'T'))

    df_st = df_st.drop(columns=["time", "latitude", "longitude"])

    for file in files:
        var_name = file.file.url.split('glm_')[-1].split('_m01')[0]
        
        if var_name not in var_names_ppe_inv.keys():
            continue

        file_url_dict = {var_name: file.file.url}
            
        # data = c3.NetCDFUtil.openFile(file.file.url)
        # tensor = data[var_name]

        # # Extracting the 3D tensor for the pseudoLevelIndex
        # tensor_3d = np.array(tensor[:, pseudoLevelIndex, :, :])  # shape: (time, lat, lon)

        # if coarseGrainOptions:
        #     interpolated_data = []
        #     for time_slice in tensor_3d:
        #         time_slice
        #         interp_data_time_slice = interp_targ_data(time_slice,coarseGrainOptions.coarseFactor,coarseGrainOptions.coarseFactor)
        #         interpolated_data.append(interp_data_time_slice)
            
        #     # Convert the list of interpolated slices into a 3D numpy array
        #     tensor_3d = np.array(interpolated_data)

        # # Flatten the tensor for adding to DataFrame
        # df_st[var_names_ppe_inv[var_name]] = tensor_3d.reshape(-1)

        # c3.NetCDFUtil.closeFile(data, file.file.url)

    # get air pressure data
    data = c3.NetCDFUtil.openFile(file_url_dict['air_pressure'])
    air_press = data['air_pressure'] #dimensions (time,model_level_number,latitude,longitude)
    c3.NetCDFUtil.closeFile(data, file_url_dict['air_pressure'])
    # get pot temp data
    data = c3.NetCDFUtil.openFile(file_url_dict['air_potential_temperature'])
    pot_temp = data['air_potential_temperature']
    c3.NetCDFUtil.closeFile(data, file_url_dict['air_potential_temperature'])
    # get mass frac data
    data = c3.NetCDFUtil.openFile(file_url_dict['mass_fraction_of_cloud_liquid_water_in_air'])
    mass_frac = data['mass_fraction_of_cloud_liquid_water_in_air']
    c3.NetCDFUtil.closeFile(data, file_url_dict['mass_fraction_of_cloud_liquid_water_in_air'])

    # save model level heights
    level_heights = data['level_height']

    #put data through clwp func together for calculation
    clwp_data = get_clwp(mass_frac,pot_temp,air_press,level_heights,times)

    # coarse grain clwp data here
    interp_data = []
    for time_slice in clwp_data:
        interp_data.append(interp_targ_data(time_slice,coarseGrainOptions.coarseFactor,coarseGrainOptions.coarseFactor))
    clwp_coarse = np.array(interp_data)

    df_st['clwp'] = clwp_coarse.reshape(-1)
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
def get_clwp(mass_frac_arr,pot_temp_arr,press_arr,level_heights_arr,times):
    
    # constants
    P_0 = 100000 #Pa
    exp_term = 0.286
    MW_air = 28.96 #g/mol
    R = 8.314472 #m3 Pa / K mol
    
    mass_frac_data = mass_frac_arr[:,:,:,:]
    press_data = press_arr[:,:,:,:]
    pot_temp_data = pot_temp_arr[:,:,:,:]
    spatial_dimensions = list(pot_temp_data.shape)[-2:]
    
    # finding temperature
    denominator = ((press_data**-1)*P_0)**exp_term
    temp_data = pot_temp_data / denominator
    
    #finding air density
    rho_air = press_data * MW_air / (R * temp_data)
    
    lwc_data = mass_frac_data * rho_air / 1000
    level_heights = level_heights_arr[:]
    
    time_dfs = []
    clwp_all_times = []
    
    for i in range(len(times)):
        lwc_this_time = lwc_data[i,:,:,:]

        clwp_data = np.zeros(spatial_dimensions)
        for level in range(len(level_heights)):
            clwp_data += lwc_this_time[level,:,:] * level_heights[level]
        clwp_all_times.append(clwp_data)
    clwp_all_times = np.array(clwp_all_times)
    return clwp_all_times

def interp_targ_data(targ_data, lats_step, lons_step):
        """
        targ_data: iterable
        Must be a two dimensional array of the data being regridded in the dimensions of (lats,lons)
        lats_step: int
        The number of points to be included in an average.
        lons_step: int
        The number of points to be included in an average.
        """
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
        rg_list = []
        
        ind_list = list(range(0, len(data_arr), step))
        for ind in ind_list:
            rg_list.append(
                np.mean(data_arr[ind:ind+step])
            )
        return rg_list