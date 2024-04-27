def upsertSimulationOutput(this, datasetId, pseudoLevelIndex, batchSize=80276):
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
    simulationOutputType = getattr(c3,datasetType).mixins[2].genericVarBindings[2].name #SppeTatzCoarseSimulationOutput
    thisDataset = getattr(c3,datasetType).get(datasetId)
    coarseGrainOptions = thisDataset.coarseGrainOptions

    def make_gstp(objId):
        return getattr(c3,geoTimeGridType)(id=objId)

    # var name dicts
    aod_var_names = {
            "dust" : "atmosphere_optical_thickness_due_to_dust_ambient_aerosol",
            "solubleAitkenMode" : "atmosphere_optical_thickness_due_to_soluble_aitken_mode_sulphate_aerosol",
            "solubleAccumulationMode" : "atmosphere_optical_thickness_due_to_soluble_accumulation_mode_sulphate_aerosol",
            "solubleCoarseMode" : "atmosphere_optical_thickness_due_to_soluble_coarse_mode_sulphate_aerosol",
            "insolubleAitkenMode" : "atmosphere_optical_thickness_due_to_insoluble_aitken_mode_sulphate_aerosol"
    }
    aod_var_names_inv = {v: k for k, v in aod_var_names.items()}

    clwp_var_names = {
            "pressure" : "air_pressure",
            "level_height" : "level_height",
            "potential_temp" : "air_potential_temperature",
            "mass_frac_water" : "mass_fraction_of_cloud_liquid_water_in_air"
    }

    clwp_var_names_inv = {v: k for k, v in clwp_var_names.items()}

    ex_coeff_var_names = {
        "fill in" : "here"
    }

    ex_coeff_var_names_inv = {v: k for k, v in ex_coeff_var_names.items()}

    # store urls
    aod_urls = []
    urls_dict = {'AOD':[]}

    # grab the relevant files
    files = getattr(c3,outputFileType).fetch({
        "filter": c3.Filter().eq("simulationRun.id", this.id)
    }).objs
    
    for file in files:
        url = file.file.url
        if 'atmosphere' in url:
            aod_urls.append(url)
        elif 'air_pressure' in url:
            urls_dict['pressure'] = url
        elif 'air_potential' in url:
            urls_dict['potential_temp'] = url
        elif 'mass_fraction' in url:
            urls_dict['mass_frac_water'] = url
        elif 'm01s01i298' in url:
            urls_dict['cdnc_ctw'] = url
        elif 'm01s01i299' in url:
            urls_dict['cdnc_wghts'] = url
        elif 'm01s01i205' in url:
            urls_dict['swrf_out'] = url
        else:
            urls_dict['ex_coeff'] = url
    urls_dict['AOD'] = aod_urls
    #------------------------------AOD Calcs------------------------------------
    # create GSTP objects
    gstpFile = urls_dict['AOD'][0]
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
    
    # add pseudoLevelIndex
    df_st["pseudoLevelIndex"] = pseudoLevelIndex
    
    # add unique id
    df_st["id"] = datasetId + '_' + this.id + '_' + round(df_st["latitude"],3).astype(str) + "_" + round(df_st["longitude"],3).astype(str) + "_" + df_st["time"].astype(str).apply(lambda x: x.replace(" ", 'T'))

    df_st = df_st.drop(columns=["time", "latitude", "longitude"])

    
    for url in urls_dict['AOD']:
        var_name = url.split('glm_')[-1].split('_m01')[0]
        
        if var_name not in aod_var_names_inv.keys():
            continue
            
        data = c3.NetCDFUtil.openFile(url)
        tensor = data[var_name]

        # Extracting the 3D tensor for the pseudoLevelIndex
        tensor_3d = np.array(tensor[:, pseudoLevelIndex, :, :])  # shape: (time, lat, lon)

        if coarseGrainOptions:
            interpolated_data = []
            for time_slice in tensor_3d:
                interp_data_time_slice = interp_targ_data(time_slice,coarseGrainOptions.coarseFactor,coarseGrainOptions.coarseFactor)
                interpolated_data.append(interp_data_time_slice)

            # Convert the list of interpolated slices into a 3D numpy array
            tensor_3d = np.array(interpolated_data)

        # Flatten the tensor for adding to DataFrame
        df_st[aod_var_names_inv[var_name]] = tensor_3d.reshape(-1)

        c3.NetCDFUtil.closeFile(data, url)

    #------------------------------CLWP Calcs------------------------------------
    # create GSTP objects
    gstpFile = urls_dict['pressure']
    sample = c3.NetCDFUtil.openFile(gstpFile)
    lat = sample["latitude"][:]
    lon = [x*(x < 180) + (x - 360)*(x >= 180) for x in sample["longitude"][:]]
    tim = sample["time"][:]
    c3.NetCDFUtil.closeFile(sample,gstpFile)
    
    # construct correct times array
    zero_time = dt.datetime(1970,1,1,0,0)
    times = []
    for t in tim:
        target_time = zero_time + dt.timedelta(hours=t,minutes=20)
        times.append(target_time)

    if coarseGrainOptions:
        # Coarse-graining: Reduce the resolution of the lat-lon grid
        original_lat = lat.copy()
        original_lon = lon.copy()
        lat = interp_coord(lat,coarseGrainOptions.coarseFactor)
        lon = interp_coord(lon,coarseGrainOptions.coarseFactor)

    df_st2 = pd.DataFrame()
        
    df_st2["time"] = [t for t in times for n in range(0, len(lat)*len(lon))]
    df_st2["latitude"] = [l for l in lat for n in range(0, len(lon))]*len(times)
    df_st2["longitude"] = [l for l in lon]*len(times)*len(lat)
    df_st2["id"] = datasetId + '_' + round(df_st2["latitude"],3).astype(str) + "_" + round(df_st2["longitude"],3).astype(str) + "_" + df_st2["time"].astype(str).apply(lambda x: x.replace(" ", 'T'))
    df_st2["geoTimeGridPoint"] = df_st2["id"].apply(make_gstp)
    df_st2 = df_st2.drop(columns=["id"])
    
    # add unique id
    df_st2["id"] = datasetId + '_' + this.id + '_' + round(df_st2["latitude"],3).astype(str) + "_" + round(df_st2["longitude"],3).astype(str) + "_" + df_st2["time"].astype(str).apply(lambda x: x.replace(" ", 'T'))

    df_st2 = df_st2.drop(columns=["time", "latitude", "longitude"])

    # get air pressure data
    data = c3.NetCDFUtil.openFile(urls_dict['pressure'])
    air_press = data['air_pressure'][:,:,:,:] #dimensions (time,model_level_number,latitude,longitude)
    c3.NetCDFUtil.closeFile(data, urls_dict['pressure'])
    # get pot temp data
    data = c3.NetCDFUtil.openFile(urls_dict['potential_temp'])
    pot_temp = data['air_potential_temperature'][:,:,:,:]
    c3.NetCDFUtil.closeFile(data, urls_dict['potential_temp'])
    # get mass frac data
    data = c3.NetCDFUtil.openFile(urls_dict['mass_frac_water'])
    mass_frac = data['mass_fraction_of_cloud_liquid_water_in_air'][:,:,:,:]
    # save model level heights
    level_heights = data['level_height'][:]

    c3.NetCDFUtil.closeFile(data, urls_dict['mass_frac_water'])

    #put data through clwp func together for calculation
    clwp_data = get_clwp(mass_frac,pot_temp,air_press,level_heights,times)
    if coarseGrainOptions:
        # coarse grain clwp data here
        interp_data = []
        for time_slice in clwp_data:
            interp_data.append(interp_targ_data(time_slice,coarseGrainOptions.coarseFactor,coarseGrainOptions.coarseFactor))
        clwp_coarse = np.array(interp_data)
    else:
        clwp_coarse = clwp_data

    df_st2['clwp'] = clwp_coarse.reshape(-1)
    df_st2 = df_st2.drop(columns=['geoTimeGridPoint'])

    df_st_mrg = pd.merge(df_st,df_st2,how="outer",on="id")

    #------------------------------CDNC Calcs-------------------------------------
    gstpFile = urls_dict['cdnc_ctw']
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

    df_st3 = pd.DataFrame()
        
    df_st3["time"] = [t for t in times for n in range(0, len(lat)*len(lon))]
    df_st3["latitude"] = [l for l in lat for n in range(0, len(lon))]*len(times)
    df_st3["longitude"] = [l for l in lon]*len(times)*len(lat)
    df_st3["id"] = datasetId + '_' + round(df_st3["latitude"],3).astype(str) + "_" + round(df_st3["longitude"],3).astype(str) + "_" + df_st3["time"].astype(str).apply(lambda x: x.replace(" ", 'T'))
    df_st3["geoTimeGridPoint"] = df_st3["id"].apply(make_gstp)
    df_st3 = df_st3.drop(columns=["id"])
    
    # add unique id
    df_st3["id"] = datasetId + '_' + this.id + '_' + round(df_st3["latitude"],3).astype(str) + "_" + round(df_st3["longitude"],3).astype(str) + "_" + df_st3["time"].astype(str).apply(lambda x: x.replace(" ", 'T'))

    df_st3 = df_st3.drop(columns=["time", "latitude", "longitude"])

    # get cdnc_ctw data
    data = c3.NetCDFUtil.openFile(urls_dict['cdnc_ctw'])
    cdnc_ctw = data['m01s01i298'][:,:,:]
    c3.NetCDFUtil.closeFile(data, urls_dict['cdnc_ctw'])
    # get cdnc_ctw data
    data = c3.NetCDFUtil.openFile(urls_dict['cdnc_wghts'])
    cdnc_wghts = data['m01s01i299'][:,:,:]
    c3.NetCDFUtil.closeFile(data, urls_dict['cdnc_wghts'])

    # divide two weighted cdnc by the weights to retrieve raw values
    cdnc_raw = cdnc_ctw / cdnc_wghts
    cdnc_raw[cdnc_raw.mask] = 0
    if coarseGrainOptions:
        # coarse grain clwp data here
        interp_data = []
        for time_slice in cdnc_raw:
            interp_data.append(interp_targ_data(time_slice,coarseGrainOptions.coarseFactor,coarseGrainOptions.coarseFactor))
        cdnc_final = np.array(interp_data)
    else:
        cdnc_final = cdnc_raw

    df_st3['cdnc'] = cdnc_final.reshape(-1)
    df_st3 = df_st3.drop(columns=['geoTimeGridPoint'])

    df_st_mrg = pd.merge(df_st_mrg,df_st3,how="outer",on="id")

    #------------------------------SWRF Calcs------------------------------------
    gstpFile = urls_dict['swrf_out']
    sample = c3.NetCDFUtil.openFile(gstpFile)
    lat = sample["latitude"][:]
    lon = [x*(x < 180) + (x - 360)*(x >= 180) for x in sample["longitude"][:]]
    tim = sample["time"][:]
    c3.NetCDFUtil.closeFile(sample,gstpFile)
    
    # construct correct times array
    zero_time = dt.datetime(1970,1,1,0,0)
    times = []
    for t in tim:
        target_time = zero_time + dt.timedelta(hours=t,minutes=20)
        times.append(target_time)

    if coarseGrainOptions:
        # Coarse-graining: Reduce the resolution of the lat-lon grid
        original_lat = lat.copy()
        original_lon = lon.copy()
        lat = interp_coord(lat,coarseGrainOptions.coarseFactor)
        lon = interp_coord(lon,coarseGrainOptions.coarseFactor)

    df_st4 = pd.DataFrame()
        
    df_st4["time"] = [t for t in times for n in range(0, len(lat)*len(lon))]
    df_st4["latitude"] = [l for l in lat for n in range(0, len(lon))]*len(times)
    df_st4["longitude"] = [l for l in lon]*len(times)*len(lat)
    df_st4["id"] = datasetId + '_' + round(df_st4["latitude"],3).astype(str) + "_" + round(df_st4["longitude"],3).astype(str) + "_" + df_st4["time"].astype(str).apply(lambda x: x.replace(" ", 'T'))
    df_st4["geoTimeGridPoint"] = df_st4["id"].apply(make_gstp)
    df_st4 = df_st4.drop(columns=["id"])
    
    # add unique id
    df_st4["id"] = datasetId + '_' + this.id + '_' + round(df_st4["latitude"],3).astype(str) + "_" + round(df_st4["longitude"],3).astype(str) + "_" + df_st4["time"].astype(str).apply(lambda x: x.replace(" ", 'T'))

    df_st4 = df_st4.drop(columns=["time", "latitude", "longitude"])

    # get swrf data
    data = c3.NetCDFUtil.openFile(urls_dict['swrf_out'])
    swrf_out_raw = data['toa_outgoing_shortwave_flux'][:,:,:]
    c3.NetCDFUtil.closeFile(data, urls_dict['swrf_out'])

    # divide two weighted cdnc by the weights to retrieve raw values
    if coarseGrainOptions:
        # coarse grain clwp data here
        interp_data = []
        for time_slice in swrf_out_raw:
            interp_data.append(interp_targ_data(time_slice,coarseGrainOptions.coarseFactor,coarseGrainOptions.coarseFactor))
        swrf_out_final = np.array(interp_data)
    else:
        swrf_out_final = swrf_out_raw

    df_st4['swrfOut'] = swrf_out_final.reshape(-1)
#     df_st4 = df_st4.drop(columns=['geoTimeGridPoint'])

    df_st_mrg_hold = pd.merge(df_st_mrg,df_st4,how="outer",on="id")
    df_st_mrg_hold.drop(columns=['simulationRun','dataset','pseudoLevelIndex'],inplace=True)
    
    aod_hold = df_st_mrg_hold[~np.isnan(df_st_mrg_hold['dust'])].copy()
    swrf_hold  = df_st_mrg_hold[np.isnan(df_st_mrg_hold['dust'])].copy()
    
    swrf_hold['geoTimeGridPoint'] = swrf_hold['geoTimeGridPoint_y']
    swrf_hold.drop(columns = ['geoTimeGridPoint_x','geoTimeGridPoint_y'],inplace = True)
    
    aod_hold['geoTimeGridPoint'] = aod_hold['geoTimeGridPoint_x']
    aod_hold.drop(columns = ['geoTimeGridPoint_x','geoTimeGridPoint_y'],inplace = True)
    
    df_st_final = pd.concat([aod_hold,swrf_hold],axis=0)
    
    # add dataset
    df_st_final["dataset"] = getattr(c3,datasetType)(id=datasetId)
    
    # add simulation
    df_st_final["simulationRun"] = getattr(c3,thisType)(id=this.id)
    
    # add p level
    df_st_final['pseudoLevelIndex'] = pseudoLevelIndex

    #------------------------------Batch Funcs------------------------------------
    # Initialize an index to track batches
    start_index = 0
    end_index = batchSize

    # Number of records
    total_records = len(df_st_final)
    
    # upsert data
    while start_index < total_records:
#         print(f"Upserting Batch {start_index}")
        # Create a smaller DataFrame for the current batch
        batch_df = df_st_final.iloc[start_index:end_index]

        # Convert the batch DataFrame to a list of dictionaries
        batch_records = batch_df.to_dict(orient="records")

        # Upsert the batch
        getattr(c3, simulationOutputType).upsertBatch(objs=batch_records)

        # Update indices for the next batch
        start_index = end_index
        end_index = min(end_index + batchSize, total_records)
        
    return True

#------------------------------Helper Functions------------------------------------
def get_clwp(mass_frac_data,pot_temp_data,press_data,level_heights,times):
    import numpy as np
    # constants
    P_0 = 100000 #Pa
    exp_term = 0.286
    MW_air = 28.96 #g/mol
    R = 8.314472 #m3 Pa / K mol
    
    spatial_dimensions = list(pot_temp_data.shape)[-2:]
    
    # finding temperature
    temp_data = pot_temp_data * (press_data / P_0)**exp_term #K
    
    #finding air density
    rho_air = press_data * MW_air / (R * temp_data) #g/m3
    
    lwc_data = mass_frac_data * rho_air #g/m3
    heights_diff_arr = [level_heights[0]] #m
    for i in range(len(level_heights)-1):
        heights_diff_arr.append(level_heights[i+1]-level_heights[i])
    
    clwp_all_times = []
    
    for i in range(len(times)):
        lwc_this_time = lwc_data[i,:,:,:]
        clwp_data = np.zeros(spatial_dimensions)
        for level in range(len(level_heights)):
            clwp_data += lwc_this_time[level,:,:] * heights_diff_arr[level]
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
###################################################################################
###################################################################################
###################################################################################
#
# Interpolation routines
#
###################################################################################
###################################################################################
###################################################################################

def upsertInterpOutputToFlightTracks(this,datasetId,stashId,flightDate,campaign):
    import pandas as pd
    import datetime as dt
    import hashlib
    
    import os
    import iris
    import time
    import numpy as np
    import xarray as xr

    from glob import glob
    from netCDF4 import Dataset

    def stash2str(stash_in):
        m = 'm' + str(stash_in.model).zfill(2)
        s = 's' + str(stash_in.section).zfill(2)
        i = 'i' + str(stash_in.item).zfill(3)
        return m + s + i

    def find_nearest(array, value):
        value = np.asarray(value)
        error = np.abs(array - value)
        indx = error.argmin()

        if indx == 0: 
            indx1, indx2 = indx, indx+2
        elif indx == (len(array) - 1): 
            indx1, indx2 = indx-1, indx+1
        else: 
            indx1, indx2 = indx-1, indx+2

        return indx1, indx2
    
    ###################################################################################
    #
    #  Interpolation Resample class
    #
    ###################################################################################
    

    class Resample:
        # Class variable containing pseudo levels
        aod_pslevel = [380, 440, 550, 670, 870, 1200]

        def __init__(self, ensemble, stash, date, campaign, pseudo_level=2,local_path='/tmp'):
    #         self.sim_ = sim_
    #         self.track_ = track_
            self.ensemble = ensemble
            self.stash = stash
            self.date = date
            self.campaign = campaign
            self.level_ = pseudo_level
            self.local_path = local_path + '/' + str(self.ensemble) + self.stash + self.date + self.campaign
            self.output_root = local_path

            start_time = time.time()
            print('\n~~~Beginning model data loading~~~')
    #         self.ensemble_data = self.__load_ensemble__()
            self.model_data = self.load_model_data()
            print('Completed model data loading in {} seconds\n'.format(time.time() - start_time))

            start_time = time.time()
            print('\n~~~Beginning flight track data loading~~~')
            self.flight_track = self.load_aircraft_track()
            print('Completed flight track data loading in {} seconds\n'.format(time.time() - start_time))

            # Flight parameters required for interpolating the model data
            self.obs_time = np.asarray(self.flight_track['time'])
            self.alt = np.asarray(self.flight_track['altitude'])
            self.lat = np.asarray(self.flight_track['latitude'])
            self.lon = np.asarray(self.flight_track['longitude'])
            self.mlevel_obs = np.asarray(self.flight_track['model_level_number'])

        def interpolate(self):
            print('~~~Starting the interpolation of PPE simulations~~~')
            if self.level_:
                print('Interpolating optical data for {}nm\n'.format(Resample.aod_pslevel[self.level_]))

                self.level_interp = self.level_

            start = time.time()
            interpolated_data = self.mapper_v2_(self.model_data)

    #         print('Saving cube...\n')
    #         output_path = self.output_root + f'/flight_track_interp_{self.ensemble}_{self.date}.nc'
    #         if os.path.exists(output_path):
    #             os.remove(output_path)
    #         iris.save(interpolated_data, output_path)

            print('Processing time = ~{}minutes'.format(round((time.time()-start)/60)))

            return interpolated_data

        def mapper_v2_(self, model_cube):
    #         cubelist_save = iris.cube.CubeList()
    #         cubelist_save.append(self.interpolate_v2_(model_cube))
    #         return cubelist_save

    #         ensemble, model_cube = model_obj[0], model_obj[1]

            # Initialises the cubelist to save, and creates lat/lon/alt/ml cubes for the specified date
            cubelist_save = iris.cube.CubeList()
            cube_time = iris.coords.AuxCoord(self.obs_time, standard_name='time')
            coord_base_name = ['latitude', 'longitude', 'altitude', 'model_level_number']

            for i, coordinate in enumerate([self.lat, self.lon, self.alt, self.mlevel_obs]):
                coord_cube_temp = iris.cube.Cube(coordinate, aux_coords_and_dims=[(cube_time, 0)])
                coord_cube_temp.long_name = coord_base_name[i]
                cubelist_save.append(coord_cube_temp)

    #         global mlat, mlon, mtime, model_level, name
            self.mlat = model_cube.coord('latitude').points
            self.mlon = model_cube.coord('longitude').points
            self.mtime= model_cube.coord('time').points
            self.model_level = model_cube.coord('model_level_number').points
            name = model_cube.long_name

            try:
                print('Processing ' + name)
            except:
                print('Processing the next variable (unnamed)')

            if not model_cube.coords('pseudo_level'):
                cube_out = self.interpolate_v2_(model_cube)
                cubelist_save.append(cube_out)     
            else:
                psl_pts  = model_cube.coord('pseudo_level')
                cube_slice = model_cube.extract(iris.Constraint(pseudo_level = self.level_interp))
                cube_out = self.interpolate_v2_(cube_slice, pslevel = Resample.aod_pslevel[self.level_interp]) 
                cubelist_save.append(cube_out)

            return cubelist_save

        def interpolate_v2_(self, cube,pslevel=None):
            model_val = []

            for j in range(len(self.lat)):
                x1, x2 = find_nearest(self.mlon, self.lon[j])
                y1, y2 = find_nearest(self.mlat, self.lat[j])
                z1, z2 = find_nearest(self.model_level, self.mlevel_obs[j])
                t1, t2 = find_nearest(self.mtime, self.obs_time[j])

                xarr = [x1, x2]
                yarr = [y1, y2]
                zarr = [z1, z2]
                tarr = [t1, t2]

                xarr = np.sort(xarr)
                yarr = np.sort(yarr)
                zarr = np.sort(zarr)
                tarr = np.sort(tarr)

                new_small_cube = cube[..., t1:t2, z1:z2, y1:y2, x1:x2]
                sample_points = [('time', self.obs_time[j]),
                        ('model_level_number', self.mlevel_obs[j]),
                        ('latitude', self.lat[j]),
                        ('longitude', self.lon[j])]

                model_temp = new_small_cube.interpolate(sample_points, iris.analysis.Linear(extrapolation_mode='nan'))
                model_val = np.append(model_val, model_temp.data)

            print('interpolation over, ', len(self.lat)+1 ,'iterations')

            cube_time = iris.coords.AuxCoord(self.obs_time, standard_name='time')
    #         cube_time = iris.coords.DimCoord(self.obs_time, standard_name='time')
            save_cube = iris.cube.Cube(model_val, aux_coords_and_dims = [(cube_time, 0)])
    #         save_cube = iris.cube.Cube(model_val, dim_coords_and_dims = [(cube_time, 0)])
            save_cube.standard_name = cube.standard_name
            save_cube.long_name = cube.long_name
            save_cube.units = cube.units

            if pslevel is not None:
                pslevel_aux = iris.coords.AuxCoord(pslevel, units='nm', long_name='wavelength')
                save_cube.add_aux_coord(pslevel_aux)
                save_cube.long_name = cube.attributes['um_stash_source'] + '_' + str(pslevel) + 'nm'

            return save_cube

        def load_aircraft_track(self):
            file_url = c3.FlightTrack.fetch(spec={
                "filter": c3.Filter().eq('flightDate',self.date).and_().eq('campaign',self.campaign)
            }).objs[0].locationFile.url

            tmp_path = self.copy_file_to_local(file_url)

    #         track_file_ = self.track_ + "all_points_{}.nc".format(self.date)
            aircraft_track = Dataset(tmp_path, format='NETCDF4')

            self.aircraft_track_path = tmp_path

            return aircraft_track


        def load_model_data(self):
            file_url = c3.SppeSimulationEnsembleOutputFile.fetch(
                spec={
                    "filter": c3.Filter().eq('stashCode', self.stash).and_().eq('simulationRun.simulationNumber', self.ensemble)
                }
            ).objs[0].file.url

            tmp_path = self.copy_file_to_local(file_url)

            self.model_data_path = tmp_path

            try:
                da = xr.open_mfdataset(tmp_path)[self.stash]
                model_cube = xr.DataArray.to_iris(da)
                if os.path.exists(tmp_path):
                    os.remove(tmp_path)
                return model_cube
            finally:
                if os.path.exists(tmp_path):
                    os.remove(tmp_path)


        def copy_file_to_local(self,file_url):
            filename = os.path.basename(file_url)
            tmp_path = os.path.join(self.local_path, filename)
            os.makedirs(self.local_path, exist_ok=True)
            c3.Client.copyFilesToLocalClient(file_url, self.local_path)
            return tmp_path

        def remove_local_files(self):
            if os.path.exists(self.aircraft_track_path):
                os.remove(self.aircraft_track_path)
            if os.path.exists(self.model_data_path):
                os.remove(self.model_data_path)
            if os.path.exists(self.local_path):
                os.rmdir(self.local_path)

    ###################################################################################
    #
    # Local Function and C3 method
    #
    ###################################################################################
    def hash_string(input_string):
        # Create a new SHA256 hash object
        hasher = hashlib.sha256()

        # Convert the input string to bytes and hash it
        hasher.update(input_string.encode('utf-8'))

        # Get the hexadecimal representation of the hash
        hash_string = hasher.hexdigest()

        return hash_string
    
    def make_gstp(objId):
        return getattr(c3,geoTimeGridType)(id=objId)
    
    def iris_cubelist_to_dataframe(cubelist):
        # Initialize an empty list to store Series
        series_list = []

        # Iterate over the CubeList
        for cube in cubelist:
            # Extract data from the cube and flatten it
            data = cube.data.flatten()

            # Create a Series from the data
            s = pd.Series(data, name=cube.name())

            # Add the Series to the list
            series_list.append(s)

        # Concatenate all Series in the list into a DataFrame
        df = pd.concat(series_list, axis=1)
        return df
    
    thisType = this.toJson()['type'] #type of this obj instance
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
 
    ensemble = this.simulationNumber
    flightTrack = c3.FlightTrack(id=flightDate + '_' + campaign)

    sampler = Resample (
        ensemble = ensemble,
        stash = stashId,
        date = flightDate,
        campaign = campaign
    )
    
    cubelist = sampler.interpolate()
    
    sampler.remove_local_files()
    
    df_grid = iris_cubelist_to_dataframe(cubelist)
    
     # Turn time values into datetime and add to dataframe
    tim = cubelist[0].coord('time').points
    zero_time = dt.datetime(1970,1,1,0,0)
    times = []
    for t in tim:
        target_time = zero_time + dt.timedelta(hours=t)
        times.append(target_time)
    df_grid['time'] = times
    
    # Add flight track and dataset
#     df_grid['flightTrack'] = flightTrack
    df_grid['dataset'] = dataset
    
    # Contruct unique id
#     df_grid['id'] = dataset.id +"_"+"_"+flightTrack.id+round(df_grid["latitude"],3).astype(str) + "_" + round(df_grid["longitude"],3).astype(str) + "_" + df_grid["time"].astype(str).apply(lambda x: x.replace(" ", 'T'))
    df_grid['id'] = dataset.id +"_"+round(df_grid["latitude"],3).astype(str) + "_" + round(df_grid["longitude"],3).astype(str) + "_" + df_grid["time"].astype(str).apply(lambda x: x.replace(" ", 'T'))
    df_grid["id"] = df_grid["id"].apply(hash_string)
    
    df_grid.drop(columns = ['m01s02i530_550nm','altitude','model_level_number'],inplace = True)
    
    # Upsert Grid locations
    batch_records = df_grid.to_dict(orient="records")
    getattr(c3, geoTimeGridType).mergeBatch(objs=batch_records)
    
    df_output = iris_cubelist_to_dataframe(cubelist)
    
    df_output['time'] = df_grid['time']
    
#     df_output['id'] = df_grid['id']
    
    df_output['flightTrack'] = flightTrack
    df_output['dataset'] = dataset
    df_output["simulationRun"] = getattr(c3,thisType)(id=this.id)
    # Contruct unique id
    df_output['id'] = dataset.id +"_"+"_"+flightTrack.id+"_"+this.id+"_"+round(df_output["latitude"],3).astype(str) + "_" + round(df_output["longitude"],3).astype(str) + "_" + df_output["time"].astype(str).apply(lambda x: x.replace(" ", 'T'))
    df_output["id"] = df_output["id"].apply(hash_string)
    df_output = df_output.rename(columns={'m01s02i530_550nm': 'exCoeff550'})
    df_output['modelLevelNumber'] = df_output['model_level_number'].astype(int)
    df_output['gridId'] = dataset.id +"_"+round(df_output["latitude"],3).astype(str) + "_" + round(df_output["longitude"],3).astype(str) + "_" + df_output["time"].astype(str).apply(lambda x: x.replace(" ", 'T'))
    df_output["gridId"] = df_output["gridId"].apply(hash_string)
    df_output["geoTimeGridPoint"] = df_output["gridId"].apply(make_gstp)
    df_output.drop(columns = ['time','latitude','longitude','model_level_number','gridId'],inplace = True)
    
    # upsert Interpolated Output
    batch_records = df_output.to_dict(orient="records")
    getattr(c3,simulationOutputType).mergeBatch(objs=batch_records)
    
#     return df_output
    return True