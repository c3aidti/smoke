def upsertDataAfterHeadersCreated(this, pseudoLevelIndex):
    """
    Upsert data to Cassandra after the partition keys are created.
    """
    import pandas as pd
    import datetime as dt
    import numpy as np

    def make_gstp(objId):
        return c3.GeoSurfaceTimePoint(id=objId)

    var_names_ppe = {
            "dust" : "atmosphere_optical_thickness_due_to_dust_ambient_aerosol",
            "solubleAitkenMode" : "atmosphere_optical_thickness_due_to_soluble_aitken_mode_sulphate_aerosol",
            "solubleAccumulationMode" : "atmosphere_optical_thickness_due_to_soluble_accumulation_mode_sulphate_aerosol",
            "solubleCoarseMode" : "atmosphere_optical_thickness_due_to_soluble_coarse_mode_sulphate_aerosol",
            "insolubleAitkenMode" : "atmosphere_optical_thickness_due_to_insoluble_aitken_mode_sulphate_aerosol"
    }
    var_names_ppe_inv = {v: k for k, v in var_names_ppe.items()}

    # grab the relevant files
    files = c3.SmokePPESimulationOutputFile.fetch({
        "filter": c3.Filter().eq("simulationSample.id", this.id)
    }).objs

    # create GSTP objects
    gstpFile = files[0]
    sample = c3.NetCDFUtil.openFile(gstpFile.file.url)
    df_st = pd.DataFrame()
    lat = sample["latitude"][:]
    lon = [x*(x < 180) + (x - 360)*(x >= 180) for x in sample["longitude"][:]]
    tim = sample["time"][:]
    zero_time = dt.datetime(1970,1,1,0,0)
    times = []
    for t in tim:
        target_time = zero_time + dt.timedelta(hours=t)
        times.append(target_time)
            
    df_st["time"] = [t for t in times for n in range(0, len(lat)*len(lon))]
    df_st["latitude"] = [l for l in lat for n in range(0, len(lon))]*len(times)
    df_st["longitude"] = [l for l in lon]*len(times)*len(lat)
    df_st["id"] = round(df_st["latitude"],3).astype(str) + "_" + round(df_st["longitude"],3).astype(str) + "_" + df_st["time"].astype(str).apply(lambda x: x.replace(" ", 'T'))
    df_st["geoSurfaceTimePoint"] = df_st["id"].apply(make_gstp)
    df_st = df_st.drop(columns=["time", "latitude", "longitude", "id"])

    # add simulation
    df_st["simulationSample"] = this

    # go over files and add data to dataframe
    for file in files:
        var_name = file.file.url.split('glm_')[-1].split('_m01')[0]
        if var_name in var_names_ppe_inv.keys():
            data = c3.NetCDFUtil.openFile(file.file.url)
            tensor = data[var_name]
            tensor = np.array(tensor[:,pseudoLevelIndex,:,:]).flatten()
            df_st[var_names_ppe_inv[var_name]] = tensor
            c3.NetCDFUtil.closeFile(data, file.file.url)
            meta = c3.MetaFileProcessing(
                lastAction="upsert-data",
                lastProcessAttempt=dt.datetime.now(),
                lastAttemptFailed=False,
                returnCode=0
            )
            c3.SmokePPESimulationOutputFile(
                id=file.id, 
                processed=True, 
                processMeta=meta
            ).merge()

    # add pseudoLevelIndex
    df_st["pseudoLevelIndex"] = pseudoLevelIndex

    # upsert data
    output_records = df_st.to_dict(orient="records")
    c3.SmokePPESimulationOutput.upsertBatch(objs=output_records)

    return True

def upsertToSmokePPEGstpSimulationOutput(this, pseudoLevelIndex, batchSize=40138):
    """
    Upsert data to Cassandra after the partition keys are created.
    """
    import pandas as pd
    import datetime as dt
    import numpy as np

    def make_gstp(objId):
        return c3.SmokePPEGeoSurfaceTimePoint(id=objId)

    var_names_ppe = {
            "dust" : "atmosphere_optical_thickness_due_to_dust_ambient_aerosol",
            "solubleAitkenMode" : "atmosphere_optical_thickness_due_to_soluble_aitken_mode_sulphate_aerosol",
            "solubleAccumulationMode" : "atmosphere_optical_thickness_due_to_soluble_accumulation_mode_sulphate_aerosol",
            "solubleCoarseMode" : "atmosphere_optical_thickness_due_to_soluble_coarse_mode_sulphate_aerosol",
            "insolubleAitkenMode" : "atmosphere_optical_thickness_due_to_insoluble_aitken_mode_sulphate_aerosol"
    }
    var_names_ppe_inv = {v: k for k, v in var_names_ppe.items()}

    # grab the relevant files
    files = c3.SmokePPESimulationOutputFile.fetch({
        "filter": c3.Filter().eq("simulationSample.id", this.id)
    }).objs

    # create GSTP objects
    gstpFile = files[0]
    sample = c3.NetCDFUtil.openFile(gstpFile.file.url)
    df_st = pd.DataFrame()
    lat = sample["latitude"][:]
    lon = [x*(x < 180) + (x - 360)*(x >= 180) for x in sample["longitude"][:]]
    tim = sample["time"][:]
    zero_time = dt.datetime(1970,1,1,0,0)
    times = []
    for t in tim:
        target_time = zero_time + dt.timedelta(hours=t)
        times.append(target_time)
            
    df_st["time"] = [t for t in times for n in range(0, len(lat)*len(lon))]
    df_st["latitude"] = [l for l in lat for n in range(0, len(lon))]*len(times)
    df_st["longitude"] = [l for l in lon]*len(times)*len(lat)
    df_st["id"] = round(df_st["latitude"],3).astype(str) + "_" + round(df_st["longitude"],3).astype(str) + "_" + df_st["time"].astype(str).apply(lambda x: x.replace(" ", 'T'))
    df_st["geoSurfaceTimePoint"] = df_st["id"].apply(make_gstp)
    df_st = df_st.drop(columns=["id"])

    # add simulation
    df_st["simulationSample"] = this
    
    # add pseudoLevelIndex
    df_st["pseudoLevelIndex"] = pseudoLevelIndex
    
    # add unique id
    df_st["id"] = this.id + '_' + round(df_st["latitude"],3).astype(str) + "_" + round(df_st["longitude"],3).astype(str) + "_" + df_st["time"].astype(str).apply(lambda x: x.replace(" ", 'T'))

    df_st = df_st.drop(columns=["time", "latitude", "longitude"])
    
    # go over files and add data to dataframe
    for file in files:
        var_name = file.file.url.split('glm_')[-1].split('_m01')[0]
        if var_name in var_names_ppe_inv.keys():
            data = c3.NetCDFUtil.openFile(file.file.url)
            tensor = data[var_name]
            tensor = np.array(tensor[:,pseudoLevelIndex,:,:]).flatten()
            df_st[var_names_ppe_inv[var_name]] = tensor
            c3.NetCDFUtil.closeFile(data, file.file.url)
            meta = c3.MetaFileProcessing(
                lastAction="upsert-data",
                lastProcessAttempt=dt.datetime.now(),
                lastAttemptFailed=False,
                returnCode=0
            )
            c3.SmokePPESimulationOutputFile(
                id=file.id, 
                processed=True, 
                processMeta=meta
            ).merge()

    # Initialize an index to track batches
    start_index = 0
    end_index = batch_size

    # Number of records
    total_records = len(df_st)

    # upsert data
    while start_index < total_records:
        # Create a smaller DataFrame for the current batch
        batch_df = df_st.iloc[start_index:end_index]

        # Convert the batch DataFrame to a list of dictionaries
        batch_records = batch_df.to_dict(orient="records")

        # Upsert the batch
        c3.SmokePPEGstpSimulationOutput.upsertBatch(objs=batch_records)

        # Update indices for the next batch
        start_index = end_index
        end_index = min(end_index + batch_size, total_records)
        
#     output_records = df_st.to_dict(orient="records")
#     c3.SmokePPEGstpSimulationOutput.upsertBatch(objs=output_records)

    return True


# def upsertDataToGisOutputType(this, pseudoLevelIndex):
#     """
#     Upsert data to the PostGIS-based output type.
#     """
#     import pandas as pd
#     import datetime as dt
#     import numpy as np

#     def createGeoPointM(long,lat,meas):
#         """
#         Create a GeoPointWithMeasure object from long, lat, and meas (time).
#         """
#         return c3.GeoPointWithMeasure(long=long, lat=lat, meas=meas)

#     var_names_ppe = {
#             "dust" : "atmosphere_optical_thickness_due_to_dust_ambient_aerosol",
#             "solubleAitkenMode" : "atmosphere_optical_thickness_due_to_soluble_aitken_mode_sulphate_aerosol",
#             "solubleAccumulationMode" : "atmosphere_optical_thickness_due_to_soluble_accumulation_mode_sulphate_aerosol",
#             "solubleCoarseMode" : "atmosphere_optical_thickness_due_to_soluble_coarse_mode_sulphate_aerosol",
#             "insolubleAitkenMode" : "atmosphere_optical_thickness_due_to_insoluble_aitken_mode_sulphate_aerosol"
#     }
#     var_names_ppe_inv = {v: k for k, v in var_names_ppe.items()}

#     # grab the relevant files
#     files = c3.SmokePPESimulationOutputFile.fetch({
#         "filter": c3.Filter().eq("simulationSample.id", this.id)
#     }).objs

#     # create GSTP objects
#     gstpFile = files[0]
#     sample = c3.NetCDFUtil.openFile(gstpFile.file.url)
#     df_st = pd.DataFrame()
#     lat = sample["latitude"][:]
#     lon = [x*(x < 180) + (x - 360)*(x >= 180) for x in sample["longitude"][:]]
#     tim = sample["time"][:]
#     zero_time = dt.datetime(1970,1,1,0,0)
#     times = []
#     for t in tim:
#         target_time = zero_time + dt.timedelta(hours=t)
#         times.append(target_time)
#     # obtain the time as  timestamp from epoch in milliseconds
#     # this is the format that the PostGIS-based output type expects
#     epoch_times = [int((t - zero_time).total_seconds()*1000) for t in times]

#     df_st["time"] = [t for t in times for n in range(0, len(lat)*len(lon))]
#     df_st["epochTime"] = [t for t in epoch_times for n in range(0, len(lat)*len(lon))]
#     df_st["lat"] = [l for l in lat for n in range(0, len(lon))]*len(times)
#     df_st["long"] = [l for l in lon]*len(times)*len(lat)
#     df_st["id"] = round(df_st["lat"],3).astype(str) + "_" + round(df_st["long"],3).astype(str) + "_" + df_st["time"].astype(str).apply(lambda x: x.replace(" ", 'T')) + "_" + this.id
#     # df_st["geoSurfaceTimePoint"] = df_st["id"].apply(make_gstp)
#     df_st['geomTime'] = df_st.apply(lambda row: createGeoPointM(row['long'], row['lat'], row['epochTime']), axis=1)
#     df_st = df_st.drop(columns=["time", "epochTime","lat", "long"])

#     # add simulation
#     df_st["simulationSample"] = this

#     # add simulationNumber for filtering
#     df_st["simulationNumber"] = this.simulationNumber

#     # go over files and add data to dataframe
#     for file in files:
#         var_name = file.file.url.split('glm_')[-1].split('_m01')[0]
#         if var_name in var_names_ppe_inv.keys():
#             data = c3.NetCDFUtil.openFile(file.file.url)
#             tensor = data[var_name]
#             tensor = np.array(tensor[:,pseudoLevelIndex,:,:]).flatten()
#             df_st[var_names_ppe_inv[var_name]] = tensor
#             c3.NetCDFUtil.closeFile(data, file.file.url)
#     #         meta = c3.MetaFileProcessing(
#     #             lastAction="upsert-data",
#     #             lastProcessAttempt=dt.datetime.now(),
#     #             lastAttemptFailed=False,
#     #             returnCode=0
#     #         )
#     #         c3.SmokePPESimulationOutputFile(
#     #             id=file.id, 
#     #             processed=True, 
#     #             processMeta=meta
#     #         ).merge()

#     # add pseudoLevelIndex
#     df_st["pseudoLevelIndex"] = pseudoLevelIndex

#     # upsert data
#     output_records = df_st.to_dict(orient="records")
#     c3.SmokePPEGeoTimeSimulationOutput.upsertBatch(objs=output_records)

#     return True

# def upsertDataToGeoPointPlusTime(this, pseudoLevelIndex):
#     """
#     Upsert data to the PostGIS-based output type.
#     """
#     import pandas as pd
#     import datetime as dt
#     import numpy as np

#     def createGeoPoint(long,lat):
#         """
#         Create a GeoPoint object from long, lat.
#         """
#         return c3.GeoPoint(long=long, lat=lat)

#     var_names_ppe = {
#             "dust" : "atmosphere_optical_thickness_due_to_dust_ambient_aerosol",
#             "solubleAitkenMode" : "atmosphere_optical_thickness_due_to_soluble_aitken_mode_sulphate_aerosol",
#             "solubleAccumulationMode" : "atmosphere_optical_thickness_due_to_soluble_accumulation_mode_sulphate_aerosol",
#             "solubleCoarseMode" : "atmosphere_optical_thickness_due_to_soluble_coarse_mode_sulphate_aerosol",
#             "insolubleAitkenMode" : "atmosphere_optical_thickness_due_to_insoluble_aitken_mode_sulphate_aerosol"
#     }
#     var_names_ppe_inv = {v: k for k, v in var_names_ppe.items()}

#     # grab the relevant files
#     files = c3.SmokePPESimulationOutputFile.fetch({
#         "filter": c3.Filter().eq("simulationSample.id", this.id)
#     }).objs

#     # create GSTP objects
#     gstpFile = files[0]
#     sample = c3.NetCDFUtil.openFile(gstpFile.file.url)
#     df_st = pd.DataFrame()
#     lat = sample["latitude"][:]
#     lon = [x*(x < 180) + (x - 360)*(x >= 180) for x in sample["longitude"][:]]
#     tim = sample["time"][:]
#     zero_time = dt.datetime(1970,1,1,0,0)
#     times = []
#     for t in tim:
#         target_time = zero_time + dt.timedelta(hours=t)
#         times.append(target_time)
#     # obtain the time as  timestamp from epoch in milliseconds
#     # this is the format that the PostGIS-based output type expects
#     # epoch_times = [int((t - zero_time).total_seconds()*1000) for t in times]

#     df_st["lat"] = [l for l in lat for n in range(0, len(lon))]*len(times)
#     df_st["long"] = [l for l in lon]*len(times)*len(lat)
#     df_st['geo'] = df_st.apply(lambda row: createGeoPoint(row['long'], row['lat']), axis=1)

#     # Add geo coords only to geoGrid2d
#     # df_st["id"] = round(df_st["lat"],3).astype(str) + "_" + round(df_st["long"],3).astype(str)
#     # geo_records = df_st.to_dict(orient="records")
#     # # at this point geo_records contains many duplicates, let's remove them
#     # geo_records = [dict(t) for t in {tuple(d.items()) for d in geo_records}]
#     # # upsert geoGrid2d
#     # c3.GeoGrid2D.upsertBatch(objs=geo_records)
#     # df_st = df_st.drop(columns=["id"])

#     # Add time and id for each point
#     df_st["time"] = [t for t in times for n in range(0, len(lat)*len(lon))]
#     # df_st["epochTime"] = [t for t in epoch_times for n in range(0, len(lat)*len(lon))]
#     # df_st["geoSurfaceTimePoint"] = df_st["id"].apply(make_gstp)
#     df_st["id"] = round(df_st["lat"],3).astype(str) + "_" + round(df_st["long"],3).astype(str) + "_" + df_st["time"].astype(str).apply(lambda x: x.replace(" ", 'T')) + "_" + this.id
#     # add simulation
#     df_st["simulationSample"] = this

#     df_st = df_st.drop(columns=["lat", "long"])
    
#     # go over files and add data to dataframe
#     for file in files:
#         var_name = file.file.url.split('glm_')[-1].split('_m01')[0]
#         if var_name in var_names_ppe_inv.keys():
#             data = c3.NetCDFUtil.openFile(file.file.url)
#             tensor = data[var_name]
#             tensor = np.array(tensor[:,pseudoLevelIndex,:,:]).flatten()
#             df_st[var_names_ppe_inv[var_name]] = tensor
#             c3.NetCDFUtil.closeFile(data, file.file.url)

#     # add pseudoLevelIndex
#     df_st["pseudoLevelIndex"] = pseudoLevelIndex

#     # upsert data
#     output_records = df_st.to_dict(orient="records")
#     c3.SmokePPEGeoPointPlusTimeSimulationOutput.upsertBatch(objs=output_records)

#     return True