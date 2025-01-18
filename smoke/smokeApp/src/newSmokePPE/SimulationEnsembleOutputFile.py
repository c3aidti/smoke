def upsertGridData(this,datasetType,geoTimeGridType,datasetId,coarseGrainOptions=None,batchSize=160552):
    """
    Upsert Geo-spatial plus time data to provided types
    """
    import pandas as pd
    import datetime as dt
    import numpy as np
    
    def createGeoPoint(long,lat):
        """
        Create a GeoPoint object from long, lat.
        """
        return c3.GeoPoint(long=long, lat=lat)
    
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


    dataset = getattr(c3,datasetType).get(datasetId)
    if not ('atmosphere_optical_thickness' in this.file.url):
        raise Exception("Use an AOT url")
    sample = c3.NetCDFUtil.openFile(this.file.url)
    lat = sample["latitude"][:]
    lon = [x*(x < 180) + (x - 360)*(x >= 180) for x in sample["longitude"][:]]
    tim = sample["time"][:]
    c3.NetCDFUtil.closeFile(sample,this.file.url)
    
    zero_time = dt.datetime(1970,1,1,0,0)
    times = []
    for t in tim:
        target_time = zero_time + dt.timedelta(hours=t)
        times.append(target_time)
    
    if coarseGrainOptions:
        # Coarse-graining: Reduce the resolution of the lat-lon grid
        # lat = lat[::coarseGrainOptions.coarseFactor]
        # lon = lon[::coarseGrainOptions.coarseFactor]
        lat = interp_coord(lat,coarseGrainOptions.coarseFactor)
        lon = interp_coord(lon,coarseGrainOptions.coarseFactor)

    # Create space-time dataframe
    df_st = pd.DataFrame()
    df_st["latitude"] = [l for l in lat for n in range(0, len(lon))]*len(times)
    df_st["longitude"] = [l for l in lon]*len(times)*len(lat)
    df_st["time"] = [t for t in times for n in range(0, len(lat)*len(lon))]
    df_st['geo'] = df_st.apply(lambda row: createGeoPoint(row['longitude'], row['latitude']), axis=1)
    df_st["dataset"] = dataset

    # Upsert gstp data to gstpType
    df_st["id"] = dataset.id +"_"+round(df_st["latitude"],3).astype(str) + "_" + round(df_st["longitude"],3).astype(str) + "_" + df_st["time"].astype(str).apply(lambda x: x.replace(" ", 'T'))
    st_records = df_st.to_dict(orient="records")

    start_index = 0
    end_index = batchSize
    total_records = len(df_st)
    while start_index < total_records:
        # print(f"Upserting Batch {start_index}")
        # Create a smaller DataFrame for the current batch
        batch_df = df_st.iloc[start_index:end_index]

        # Convert the batch DataFrame to a list of dictionaries
        batch_records = batch_df.to_dict(orient="records")

        # Upsert the batch
        getattr(c3,geoTimeGridType).upsertBatch(batch_records)

        # Update indices for the next batch
        start_index = end_index
        end_index = min(end_index + batchSize, total_records)
    
    # Create geo dataframe
    # print("geo grid")
    df_geo = pd.DataFrame()
    df_geo["lat"] = [l for l in lat for n in range(0, len(lon))]
    df_geo["long"] = [l for l in lon]*len(lat)
    df_geo['geo'] = df_geo.apply(lambda row: createGeoPoint(row['long'], row['lat']), axis=1)
    df_geo["dataset"] = dataset
    df_geo["id"] = dataset.id +"_"+round(df_geo["lat"],3).astype(str) + "_" + round(df_geo["long"],3).astype(str)

    # Upsert geo coords to geoGridType
    geo_records = df_geo.to_dict(orient="records")
    c3.DatasetGeoGrid.upsertBatch(objs=geo_records)

    # Create time dataframe
    # print("time grid")
    df_time = pd.DataFrame()
    df_time["time"] = [t for t in times]
    df_time["dataset"] = dataset
    df_time["id"] = dataset.id +"_"+df_time["time"].astype(str).apply(lambda x: x.replace(" ", 'T'))
    
    # Upsert times to timeGridType
    time_records = df_time.to_dict(orient="records")
    c3.DatasetTimeSeries.upsertBatch(objs=time_records)
    
    return True