##
# Copyright (c) 2022, C3 AI DTI, Development Operations Team
# All rights reserved. License: https://github.com/c3aidti/.github
##
def upsertAcureAircraftData(this):
    """
    Function to Open files in the SimulationOutputFile table with acure-aircraft container and then populate SimulationModelOutput and SimulationModelOutputSeries data.
    
    - Arguments:
        -this: an instance of SimulationOutputFile
    - Returns:
        -bool: True if file was processed, false if file has already been processed or if container type does not match.
    Return codes:
        0: All good!
        1: Wrong container
    """
    from datetime import datetime, timedelta
    import pandas as pd

    # verify file container
    if(this.container == 'acure-aircraft'):
        #open file
        sample = c3.NetCDFUtil.openFile(this.file.url)
    
        # cast it to dataframe
        df = pd.DataFrame()
        df['time'] = sample.variables['time'][:]
        df['longitude'] = sample.variables['longitude'][:]
        df['latitude'] = sample.variables['latitude'][:]
        df['altitude'] = sample.variables['altitude'][:]
        df['model_level_number'] = sample.variables['model_level_number'][:]
        df['air_potential_temperature']= sample.variables['air_potential_temperature'][:]
        df['air_pressure'] = sample.variables['air_pressure'][:]
        df['cloud_flag'] = sample.variables['m01s38i478'][:]
        df['cdnc_x_cloud_flag'] = sample.variables['m01s38i479'][:]
        df['ambient_extinction_550'] = sample.variables['m01s02i530_550nm'][:]
        df['ambient_scattering_550'] = sample.variables['m01s02i532_550nm'][:]
        df['num_nuc'] = sample.variables['number_of_particles_per_air_molecule_of_soluble_nucleation_mode_aerosol_in_air'][:]
        df['num_Ait'] = sample.variables['number_of_particles_per_air_molecule_of_soluble_aitken_mode_aerosol_in_air'][:]
        df['num_acc'] = sample.variables['number_of_particles_per_air_molecule_of_soluble_accumulation_mode_aerosol_in_air'][:]
        df['num_cor'] = sample.variables['number_of_particles_per_air_molecule_of_soluble_coarse_mode_aerosol_in_air'][:]
        df['num_Aitins'] = sample.variables['number_of_particles_per_air_molecule_of_insoluble_aitken_mode_aerosol_in_air'][:]
        df['mass_SU_Ait'] = sample.variables['mass_fraction_of_sulfuric_acid_in_soluble_aitken_mode_dry_aerosol_in_air'][:] 
        df['mass_SU_acc'] = sample.variables['mass_fraction_of_sulfuric_acid_in_soluble_accumulation_mode_dry_aerosol_in_air'][:] 
        df['mass_SU_cor'] = sample.variables['mass_fraction_of_sulfuric_acid_in_soluble_coarse_mode_dry_aerosol_in_air'][:] 
        df['mass_BC_Ait'] = sample.variables['mass_fraction_of_black_carbon_in_soluble_aitken_mode_dry_aerosol_in_air'][:] 
        df['mass_BC_acc'] = sample.variables['mass_fraction_of_black_carbon_in_soluble_accumulation_mode_dry_aerosol_in_air'][:]
        df['mass_BC_cor'] = sample.variables['mass_fraction_of_black_carbon_in_soluble_coarse_mode_dry_aerosol_in_air'][:] 
        df['mass_BC_Aitins'] = sample.variables['mass_fraction_of_black_carbon_in_insoluble_aitken_mode_dry_aerosol_in_air'][:]
        df['mass_OC_Ait'] = sample.variables['mass_fraction_of_particulate_organic_matter_in_soluble_aitken_mode_dry_aerosol_in_air'][:]  
        df['mass_OC_acc'] = sample.variables['mass_fraction_of_particulate_organic_matter_in_soluble_accumulation_mode_dry_aerosol_in_air'][:]
        df['mass_OC_cor'] = sample.variables['mass_fraction_of_particulate_organic_matter_in_soluble_coarse_mode_dry_aerosol_in_air'][:]
        df['mass_OC_Aitins'] = sample.variables['mass_fraction_of_particulate_organic_matter_in_insoluble_aitken_mode_dry_aerosol_in_air'][:] 
        df['mass_SS_acc'] = sample.variables['mass_fraction_of_seasalt_in_soluble_accumulation_mode_dry_aerosol_in_air'][:]
        df['mass_SS_cor'] = sample.variables['mass_fraction_of_seasalt_in_soluble_coarse_mode_dry_aerosol_in_air'][:] 
        # a little gymnastic to get Datetime objs
        zero_time = datetime(1970,1,1,0,0)
        transformed_times = []
        for time in df['time']:
            target_time = zero_time + timedelta(hours=time)
            transformed_times.append(target_time)
        df['start'] = transformed_times
        df.drop(columns=['time'], inplace=True)

        parent_id = "SMOS_" + this.simulationSample.id
        df['parent'] = parent_id

        now_time = datetime.now()
        diff_time = (now_time - zero_time)
        versionTag= -1 * diff_time.total_seconds()
        df['dataVersion'] = versionTag

        output_records = df.to_dict(orient="records")

        # upsert this batch
        c3.SimulationAcureAircraftOutput.upsertBatch(objs=output_records)

        meta = c3.MetaFileProcessing(
            lastAction="upsert-data",
            lastProcessAttempt=datetime.now(),
            lastAttemptFailed=False,
            returnCode=0)
        c3.SimulationOutputFile(
            id=this.id, 
            processed=True, 
            processMeta=meta).merge()
        return True

    else:
        meta = c3.MetaFileProcessing(
            lastAction="upsert-data",
            lastProcessAttempt=datetime.now(),
            lastAttemptFailed=True,
            returnCode=1)
        c3.SimulationOutputFile(
            id=this.id, 
            processed=False, 
            processMeta=meta).merge()
        return False


def createAODDataCassandraHeaders(this):
    """
    Function to Open files in the SimulationOutputFile table with aod-3hourly container and populate GeoSurfaceTimePoint data.
    
    - Arguments:
        -this: an instance of SimulationOutputFile
    - Returns:
        -bool: True if file was processed, false if file has already been processed or if container type does not match.
    Return codes:
        0: All good!
        1: Failed to open NetCDFFile
        2: Failed to create DataFrame
        3: Failed to upsert data to PostGres table
        4: File container is not aod-3hourly
    """
    import pandas as pd
    import numpy as np
    import datetime as dt

    if(this.container == 'aod-3hourly' or (this.container == 'smoke-ppe' and "soluble_accumulation_mode" in this.url)):
        # open file
        try:
            sample = c3.NetCDFUtil.openFile(this.file.url)
        except:
            meta = c3.MetaFileProcessing(
                lastAction="create-headers",
                lastProcessAttempt=dt.datetime.now(),
                lastAttemptFailed=True,
                returnCode=1)
            c3.SimulationOutputFile(
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
            c3.SimulationOutputFile(
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
            c3.SimulationOutputFile(
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
        c3.SimulationOutputFile(
            id=this.id, 
            processed=True, 
            processMeta=meta).merge()
        c3.NetCDFUtil.closeFile(sample, this.file.url)
        return True
    else:
        meta = c3.MetaFileProcessing(
            lastAction="create-headers",
            lastProcessAttempt=dt.datetime.now(),
            lastAttemptFailed=False,
            returnCode=4)
        c3.SimulationOutputFile(
            id=this.id, 
            processed=True, 
            processMeta=meta).merge()
        return False


def upsert3HourlyAODDataAfterHeadersCreated(this):
    """
    Function to Open files in the SimulationOutputFile table with aod-3hourly container and populate Simulation3HourlyAODOutput data.
    
    - Arguments:
        -this: an instance of SimulationOutputFile
    - Returns:
        -bool: True if file was processed, false if file has already been processed or if container type does not match.
    Return codes:
        0: All good!
        1: Failed to open NetCDFFile
        2: Failed to create variables dataframe
        3: Failed to create Cassandra partition keys
        4: Failed to upsert data to Cassandra
        5: File container is not aod-3hourly
    """
    import pandas as pd
    import numpy as np
    import datetime as dt

    variable_names = {
            "dust" : "atmosphere_optical_thickness_due_to_dust_ambient_aerosol",
            "solubleAitkenMode" : "atmosphere_optical_thickness_due_to_soluble_aitken_mode_ambient_aerosol",
            "solubleAccumulationMode" : "atmosphere_optical_thickness_due_to_soluble_accumulation_mode_ambient_aerosol",
            "solubleCoarseMode" : "atmosphere_optical_thickness_due_to_soluble_coarse_mode_ambient_aerosol",
            "insolubleAitkenMode" : "atmosphere_optical_thickness_due_to_insoluble_aitken_mode_ambient_aerosol"
    }

    def make_gstp(objId):
        return c3.GeoSurfaceTimePoint(id=objId)

    if (this.container == 'aod-3hourly'):
        # open file
        try:
            sample = c3.NetCDFUtil.openFile(this.file.url)
        except:
            meta = c3.MetaFileProcessing(
                lastAction="upsert-data",
                lastProcessAttempt=dt.datetime.now(),
                lastAttemptFailed=True,
                returnCode=1)
            c3.SimulationOutputFile(
                id=this.id, 
                processed=True, 
                processMeta=meta).merge()
            return False

        # extract variables
        try:
            df_var = pd.DataFrame()
            for var in variable_names.items():
                tensor = sample[var[1]][:][2,:,:,:]
                tensor = np.array(tensor).flatten()
                df_var[var[0]] = tensor

            df_var["simulationSample"] = this.simulationSample
        except:
            meta = c3.MetaFileProcessing(
                lastAction="upsert-data",
                lastProcessAttempt=dt.datetime.now(),
                lastAttemptFailed=True,
                returnCode=2)
            c3.SimulationOutputFile(
                id=this.id, 
                processed=True, 
                processMeta=meta).merge()
            c3.NetCDFUtil.closeFile(sample, this.file.url)
            return False

        # find partiion keys
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

            ids = round(df_st["latitude"],3).astype(str) + "_" + round(df_st["longitude"],3).astype(str) + "_" + df_st["time"].astype(str).apply(lambda x: x.replace(" ", 'T'))
            objs = ids.apply(make_gstp)
        except:
            meta = c3.MetaFileProcessing(
                lastAction="upsert-data",
                lastProcessAttempt=dt.datetime.now(),
                lastAttemptFailed=True,
                returnCode=3)
            c3.SimulationOutputFile(
                id=this.id, 
                processed=True, 
                processMeta=meta).merge()
            c3.NetCDFUtil.closeFile(sample, this.file.url)
            return False

        # upsert data
        try:
            df_batch = pd.DataFrame(df_var)
            df_batch["geoSurfaceTimePoint"] = objs
            output_records = df_batch.to_dict(orient="records")
            c3.Simulation3HourlyAODOutput.upsertBatch(objs=output_records)
        except:
            meta = c3.MetaFileProcessing(
                lastAction="upsert-data",
                lastProcessAttempt=dt.datetime.now(),
                lastAttemptFailed=True,
                returnCode=4)
            c3.SimulationOutputFile(
                id=this.id, 
                processed=True, 
                processMeta=meta).merge()
            c3.NetCDFUtil.closeFile(sample, this.file.url)
            return False

        # success
        meta = c3.MetaFileProcessing(
            lastAction="upsert-data",
            lastProcessAttempt=dt.datetime.now(),
            lastAttemptFailed=False,
            returnCode=0)
        c3.SimulationOutputFile(
            id=this.id, 
            processed=True, 
            processMeta=meta).merge()
        c3.NetCDFUtil.closeFile(sample, this.file.url)
        return True

    else:
        meta = c3.MetaFileProcessing(
            lastAction="upsert-data",
            lastProcessAttempt=dt.datetime.now(),
            lastAttemptFailed=False,
            returnCode=5)
        c3.SimulationOutputFile(
            id=this.id, 
            processed=True, 
            processMeta=meta).merge()
        return False