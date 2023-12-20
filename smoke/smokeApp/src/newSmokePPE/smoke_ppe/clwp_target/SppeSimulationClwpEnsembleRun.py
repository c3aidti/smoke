def upsertSimulationOutput(this, datasetId, pseudoLevelIndex, batchSize=80276):
    import pandas as pd
    import datetime as dt
    import numpy as np
    
    thisType = this.toJson()['type']
    outputFileType = getattr(c3,thisType).mixins[1].genericVarBindings[1].name #SppeSimulationOutputFile
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
        # lat = lat[::coarseGrainOptions.coarseFactor]
        # lon = lon[::coarseGrainOptions.coarseFactor]

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
            
        data = c3.NetCDFUtil.openFile(file.file.url)
        tensor = data[var_name]

        # Extracting the 3D tensor for the pseudoLevelIndex
        tensor_3d = np.array(tensor[:, pseudoLevelIndex, :, :])  # shape: (time, lat, lon)

        if coarseGrainOptions:
            interpolated_data = []
            for time_slice in tensor_3d:
                time_slice
                interp_data_time_slice = interp_targ_data(time_slice,coarseGrainOptions.coarseFactor,coarseGrainOptions.coarseFactor)
                interpolated_data.append(interp_data_time_slice)
            
            # Convert the list of interpolated slices into a 3D numpy array
            tensor_3d = np.array(interpolated_data)

        # Flatten the tensor for adding to DataFrame
        df_st[var_names_ppe_inv[var_name]] = tensor_3d.reshape(-1)

        c3.NetCDFUtil.closeFile(data, file.file.url)
      
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
def get_clwp(ens_num):
    
    # constants
    P_0 = 100000 #Pa
    exp_term = 0.286
    MW_air = 28.96 #g/mol
    R = 8.314472 #m3 Pa / K mol
    
    for url in mass_frac_water_urls:
        if 'ens_' + str(ens_num) in url:
            mf_url = url
            
    for url in pressure_urls:
        if 'ens_' + str(ens_num) in url:
            press_url = url
            
    for url in pot_temp_urls:
        if 'ens_' + str(ens_num) in url:
            pt_url = url
    
    # read files
    mass_frac_file = c3.NetCDFUtil.openFile(mf_url)
    press_file = c3.NetCDFUtil.openFile(press_url)
    pot_temp_file = c3.NetCDFUtil.openFile(pt_url)
    
    mass_frac_data = mass_frac_file['mass_fraction_of_cloud_liquid_water_in_air'][:,:,:,:]
    press_data = press_file['air_pressure'][:,:,:,:]
    pot_temp_data = pot_temp_file['air_potential_temperature'][:,:,:,:]
    
    lats = mass_frac_file['latitude'][:]
    lons = mass_frac_file['longitude'][:]
    
    lats_df = [lat for lon in lons for lat in lats]
    lons_df = [lon for lon in lons for lat in lats]
    
    # for regridding
    lats_global = np.array(range(-90,90,4))
    lons_global = np.array(range(-180,180,4))

    lats_global = lats_global[(lats_global > np.min(lats)) & (lats_global < np.max(lats))]
    lons_global = lons_global[(lons_global > np.min(lons)) & (lons_global < np.max(lons))]

    lats_rg = [lat for lon in lons_global for lat in lats_global]
    lons_rg = [lon for lon in lons_global for lat in lats_global]
    
    ens_num = mf_url.split('_')[1]
    
    # finding temperature
    denominator = ((press_data**-1)*P_0)**exp_term
    temp_data = pot_temp_data / denominator
    
    #finding air density
    rho_air = press_data * MW_air / (R * temp_data)
    
    lwc_data = mass_frac_data * rho_air / 1000
    level_heights = mass_frac_file['level_height'][:]
    
    times = mass_frac_file['time'][:]
    time_dfs = []
    clwp_all_times = []
    
    for i in range(len(times)):
        lwc_this_time = lwc_data[i,:,:,:]
        clwp_data = np.zeros([len(lats),len(lons)])
        for level in range(len(level_heights)):
            clwp_data += lwc_this_time[level,:,:] * level_heights[level]
        clwp_all_times.append(clwp_data)
    clwp_all_times = np.array(clwp_all_times)
    return lats, lons, clwp_all_times

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