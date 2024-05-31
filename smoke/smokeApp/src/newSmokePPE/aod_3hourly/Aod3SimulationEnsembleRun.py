def upsertSimulationOutput(this, datasetId, pseudoLevelIndex, batchSize=80276):
    import pandas as pd
    import datetime as dt
    import numpy as np
    
    thisType = this.toJson()['type'] #type of this obj instance
    outputFileType = getattr(c3,thisType).mixins[1].genericVarBindings[1].name #Aod3SimulationEnsembleOutputFile
    datasetObj = c3.SimulationEnsembleDataset.fetch(
        spec = {
            "filter":c3.Filter.inst().eq('id',datasetId),
            "include": "id"
        }
    ).objs[0]
    datasetType = datasetObj.toJson()['type'] #Aod3FullSimulationEnsembleDataset
    geoTimeGridType = getattr(c3,datasetType).mixins[2].genericVarBindings[1].name #Aod3FullGeoTimeGrid
    simulationOutputType = getattr(c3,datasetType).mixins[2].genericVarBindings[2].name #Aod3FullSimulationOutput
    thisDataset = getattr(c3,datasetType).get(datasetId)
    coarseGrainOptions = thisDataset.coarseGrainOptions

    def make_gstp(objId):
        return getattr(c3,geoTimeGridType)(id=objId)

    # var name dicts
    aod_var_names = {
            "solubleAitkenMode" : "atmosphere_optical_thickness_due_to_soluble_aitken_mode_ambient_aerosol",
            "solubleAccumulationMode" : "atmosphere_optical_thickness_due_to_soluble_accumulation_mode_ambient_aerosol",
            "solubleCoarseMode" : "atmosphere_optical_thickness_due_to_soluble_coarse_mode_ambient_aerosol",
            "insolubleAitkenMode" : "atmosphere_optical_thickness_due_to_insoluble_aitken_mode_ambient_aerosol",
            "insolubleAccumulationMode" : "atmosphere_optical_thickness_due_to_insoluble_accumulation_mode_ambient_aerosol",
            "insolubleCoarseMode" : "atmosphere_optical_thickness_due_to_insoluble_coarse_mode_ambient_aerosol"
    }
    aod_vars = [
        'atmosphere_optical_thickness_due_to_soluble_aitken_mode_ambient_aerosol',
        'atmosphere_optical_thickness_due_to_soluble_accumulation_mode_ambient_aerosol',
        'atmosphere_optical_thickness_due_to_soluble_coarse_mode_ambient_aerosol',
        'atmosphere_optical_thickness_due_to_insoluble_aitken_mode_ambient_aerosol',
        'atmosphere_optical_thickness_due_to_insoluble_accumulation_mode_ambient_aerosol',
        'atmosphere_optical_thickness_due_to_insoluble_coarse_mode_ambient_aerosol'
    ]
    aod_var_names_inv = {v: k for k, v in aod_var_names.items()}

    # grab the relevant files
    files = getattr(c3,outputFileType).fetch({
        "filter": c3.Filter().eq("simulationRun.id", this.id)
    }).objs
    
    urls = [file.file.url for file in files]
    #------------------------------AOD Calcs------------------------------------
    df_st_final = pd.DataFrame()
    for url in urls:
        # create GSTP objects
        sample = c3.NetCDFUtil.openFile(url)
        lat = sample["latitude"][:]
        lon = [x*(x < 180) + (x - 360)*(x >= 180) for x in sample["longitude"][:]]
        tim = sample["time"][:]
        
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
        df_st["hour"] = df_st.get('time').apply(lambda x: x.hour)
        df_st["id"] = datasetId + '_' + round(df_st["latitude"],3).astype(str) + "_" + round(df_st["longitude"],3).astype(str) + "_" + df_st["time"].astype(str).apply(lambda x: x.replace(" ", 'T'))
        df_st["geoTimeGridPoint"] = df_st["id"].apply(make_gstp)
        df_st = df_st.drop(columns=["id"])

        # add dataset
        df_st["dataset"] = getattr(c3,datasetType)(id=datasetId)
        
        # add simulation
        df_st["simulationRun"] = getattr(c3,thisType)(id=this.id)
        
        # add pseudoLevelIndex
        # typically use plevel = 2
        df_st["pseudoLevelIndex"] = pseudoLevelIndex
        
        # add unique id
        df_st["id"] = datasetId + '_' + this.id + '_' + round(df_st["latitude"],3).astype(str) + "_" + round(df_st["longitude"],3).astype(str) + "_" + df_st["time"].astype(str).apply(lambda x: x.replace(" ", 'T'))


        
        for aod_type in aod_vars:
            tensor = sample[aod_type]

            # Extracting the 3D tensor for the pseudoLevelIndex
            tensor_3d = np.array(tensor[pseudoLevelIndex, :, :, :])  # shape: (time, lat, lon)

            if coarseGrainOptions:
                interpolated_data = []
                for time_slice in tensor_3d:
                    interp_data_time_slice = interp_targ_data(time_slice,coarseGrainOptions.coarseFactor,coarseGrainOptions.coarseFactor)
                    interpolated_data.append(interp_data_time_slice)

                # Convert the list of interpolated slices into a 3D numpy array
                tensor_3d = np.array(interpolated_data)

            # Flatten the tensor for adding to DataFrame
            df_st[aod_var_names_inv[aod_type]] = tensor_3d.reshape(-1)
        df_st = df_st[(df_st['hour'] == 9) | (df_st['hour'] == 12)]
        df_st = df_st[(df_st['latitude'] >= -29.375) & (df_st['latitude'] <= 9.375)]
        df_st = df_st[(df_st['longitude'] >= -44.0625) & (df_st['longitude'] <= 38.4375)]
        df_st = df_st.drop(columns=["time", "latitude", "longitude"])
        c3.NetCDFUtil.closeFile(sample,url)
        df_st_final = pd.concat([df_st_final,df_st],axis=0,ignore_index=True)


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
    NotImplementedError
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

            # Check for NAN values in the flight track data and truncate all arrays is nessaary
            nan_indices = np.argwhere(np.isnan(self.obs_time))
            if len(nan_indices) > 0:
                self.obs_time = np.delete(self.obs_time, nan_indices)
                self.alt = np.delete(self.alt, nan_indices)
                self.lat = np.delete(self.lat, nan_indices)
                self.lon = np.delete(self.lon, nan_indices)
                self.mlevel_obs = np.delete(self.mlevel_obs, nan_indices)

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
    # batch_records = df_grid.to_dict(orient="records")
    # getattr(c3, geoTimeGridType).mergeBatch(objs=batch_records)
    
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
    # df_output.drop(columns = ['time','latitude','longitude','model_level_number','gridId'],inplace = True)
    df_output.drop(columns = ['model_level_number','gridId'],inplace = True)
    
    # upsert Interpolated Output
    batch_records = df_output.to_dict(orient="records")
    getattr(c3,simulationOutputType).mergeBatch(objs=batch_records)
    
#     return df_output
    return True