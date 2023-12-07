def upsertSimulationOutput(this, datasetId, pseudoLevelIndex, batchSize=80276):
    """
    import pandas as pd
    import datetime as dt
    import numpy as np
    """
    import iris
    import iris.analysis
    from iris.coords import DimCoord
    from iris.cube import Cube
    from iris.analysis import Linear
    
    thisType = this.toJson()['type']
    outputFileType = getattr(c3,thisType).mixins[1].genericVarBindings[1].name
    datasetObj = c3.SimulationEnsembleDataset.fetch(
        spec = {
            "filter":c3.Filter.inst().eq('id',datasetId),
            "include": "id"
        }
    ).objs[0]
    datasetType = datasetObj.toJson()['type']
    geoTimeGridType = getattr(c3,datasetType).mixins[2].genericVarBindings[1].name
    simulationOutputType = getattr(c3,datasetType).mixins[2].genericVarBindings[2].name
    thisDataset = getattr(c3,datasetType).get(datasetId)
    coarseGrainOptions = thisDataset.coarseGrainOptions

    def make_gstp(objId):
        return getattr(c3,geoTimeGridType)(id=objId)

    var_names_ppe = {
            "dust" : "atmosphere_optical_thickness_due_to_dust_ambient_aerosol",
            "solubleAitkenMode" : "atmosphere_optical_thickness_due_to_soluble_aitken_mode_sulphate_aerosol",
            "solubleAccumulationMode" : "atmosphere_optical_thickness_due_to_soluble_accumulation_mode_sulphate_aerosol",
            "solubleCoarseMode" : "atmosphere_optical_thickness_due_to_soluble_coarse_mode_sulphate_aerosol",
            "insolubleAitkenMode" : "atmosphere_optical_thickness_due_to_insoluble_aitken_mode_sulphate_aerosol"
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
        lat = lat[::coarseGrainOptions.coarseFactor]
        lon = lon[::coarseGrainOptions.coarseFactor]

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
            # Define new grid points for coarse interpolation
            new_lat_points = DimCoord(lat, standard_name='latitude', units='degrees')
            new_lon_points = DimCoord(lon, standard_name='longitude', units='degrees')

            # Initialize an empty list to store interpolated data
            interpolated_data = []

            # Iterate over the time dimension and interpolate each time slice
            cnt=0
            for time_slice in tensor_3d:
                # Create DimCoords for the time slice's lat-lon grid
                slice_lat_coord = DimCoord(original_lat, standard_name='latitude', units='degrees')
                slice_lon_coord = DimCoord(original_lon, standard_name='longitude', units='degrees')

                # Create the Iris cube for the time slice
                cube = Cube(time_slice, dim_coords_and_dims=[(slice_lat_coord, 0), (slice_lon_coord, 1)])

                # Perform interpolation
                sample_points = [('latitude', new_lat_points.points), ('longitude', new_lon_points.points)]
                interpolated_cube = cube.interpolate(sample_points, Linear())
                interpolated_data.append(interpolated_cube.data)
                cnt = cnt + 1

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
