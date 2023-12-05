def upsertGridData(this,datasetType,geoTimeGridType,datasetId):
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

    dataset = getattr(c3,datasetType)(id=datasetId).merge()

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
    getattr(c3,geoTimeGridType).upsertBatch(st_records)
    
    # Create geo dataframe
    # df_geo = pd.DataFrame()
    # df_geo["lat"] = [l for l in lat for n in range(0, len(lon))]
    # df_geo["long"] = [l for l in lon]*len(lat)
    # df_geo['geo'] = df_geo.apply(lambda row: createGeoPoint(row['long'], row['lat']), axis=1)

    # # Upsert geo coords to geoGridType
    # df_geo["id"] = round(df_geo["lat"],3).astype(str) + "_" + round(df_geo["long"],3).astype(str)
    # geo_records = df_geo.to_dict(orient="records")
    # getattr(c3,geoGridType).upsertBatch(objs=geo_records)

    # Create time dataframe
    # df_time = pd.DataFrame()
    # df_time["time"] = [t for t in times]
    
    # # Upsert times to timeGridType
    # df_time["id"] = df_time["time"].astype(str).apply(lambda x: x.replace(" ", 'T'))
    # time_records = df_time.to_dict(orient="records")
    # getattr(c3,timeGridType).upsertBatch(objs=time_records)
    
    return True