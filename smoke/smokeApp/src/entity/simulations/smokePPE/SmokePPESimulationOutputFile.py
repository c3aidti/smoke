def createCassandraHeaders(this):
    """
    Function to Open files in the SmokePPESimulationOutputFile table GeoSurfaceTimePoint data.
    
    - Arguments:
        -this: an instance of SimulationOutputFile
    - Returns:
        -bool: True if file was processed, false if file has already been processed or if container type does not match.
    Return codes:
        0: All good!
        1: Failed to open NetCDFFile
        2: Failed to create DataFrame
        3: Failed to upsert data to PostGres table
    """
    import pandas as pd
    import numpy as np
    import datetime as dt

    # open file
    try:
        sample = c3.NetCDFUtil.openFile(this.file.url)
    except:
        meta = c3.MetaFileProcessing(
            lastAction="create-headers",
            lastProcessAttempt=dt.datetime.now(),
            lastAttemptFailed=True,
            returnCode=1)
        c3.SmokePPESimulationOutputFile(
            id=this.id, 
            processed=True, 
            processMeta=meta).merge()
        return False

    # create data frame
    try:
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
    except:
        meta = c3.MetaFileProcessing(
            lastAction="create-headers",
            lastProcessAttempt=dt.datetime.now(),
            lastAttemptFailed=True,
            returnCode=2)
        c3.SmokePPESimulationOutputFile(
            id=this.id, 
            processed=True, 
            processMeta=meta).merge()
        c3.NetCDFUtil.closeFile(sample, this.file.url)
        return False

    # upsert data
    try:
        output_records = df_st.to_dict(orient="records")
        c3.GeoSurfaceTimePoint.upsertBatch(objs=output_records)
    except:
        meta = c3.MetaFileProcessing(
            lastAction="create-headers",
            lastProcessAttempt=dt.datetime.now(),
            lastAttemptFailed=True,
            returnCode=3)
        c3.SmokePPESimulationOutputFile(
            id=this.id, 
            processed=True, 
            processMeta=meta).merge()
        c3.NetCDFUtil.closeFile(sample, this.file.url)
        return False

    # success
    meta = c3.MetaFileProcessing(
        lastAction="create-headers",
        lastProcessAttempt=dt.datetime.now(),
        lastAttemptFailed=False,
        returnCode=0)
    c3.SmokePPESimulationOutputFile(
        id=this.id, 
        processed=True, 
        processMeta=meta).merge()
    c3.NetCDFUtil.closeFile(sample, this.file.url)
    return True