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