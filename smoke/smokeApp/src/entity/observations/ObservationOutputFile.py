def upsertORACLESData(this):
    """
    Function to Open files in the ObservationOutputFile table and then populate ObservationOraclesOutput data.
    
    - Arguments:
        -this: an instance of ObservationOutputFile
    - Returns:
        -bool: True if file was processed, false if file has already been processed
    """
    from datetime import datetime
    import pandas as pd

    obsSet = c3.ObservationSet.get(this.observationSet.id)
    if (obsSet.name == "ATom_60s"):
        return False
        
    class ObsVars:
        nc_variables = ['time', 'Longitude', 'Latitude', 'GPS_Altitude', 
                        'rBC_massConc', 'Static_Air_Temp', 'Static_Pressure','Dew_Point','Lambda_Avg_SSA_Front',
                        'Lambda_Avg_SSA_Rear', 'TSI_Scat530', 'NO3', 'SO4', 'ORG',
                        'CNgt10', 'Chl', 'UHSASdNdlogd']
    
        variables_map = {'time':'start', 
                'Longitude':'longitude', 
                'Latitude':'latitude', 
                'GPS_Altitude':'altitude',
                'rBC_massConc':'total_BC', 
                'Static_Air_Temp':'temperature', 
                'Static_Pressure':'pressure', 
                'Dew_Point':'dewpoint', 
                'Lambda_Avg_SSA_Front':'SSA_front', 
                'Lambda_Avg_SSA_Rear':'SSA_rear', 
                'TSI_Scat530':'scat530', 
                'NO3':'NO3', 
                'SO4':'total_SO4', 
                'ORG':'total_ORG', 
                'CNgt10':'CNgt10', 
                'Chl':'total_Cl', 
                'UHSASdNdlogd':'UHSASdNdlogd'}

        def get_df_from_c3_file(c3file):
            """
            Opens file, grab variables in the variables_map and returns pandas DataFrame
            """
            source = c3.NetCDFUtil.openFile(c3file.file.url)
            df = pd.DataFrame()
    
            for nc_var in ObsVars.nc_variables:
                c3_var = ObsVars.variables_map[nc_var]
                if nc_var == 'time':
                    df[c3_var] = source.variables[nc_var][:]
                    df[c3_var] = pd.to_datetime(df[c3_var],unit='s')
                elif nc_var == 'UHSASdNdlogd':
                    for i in range(0,99):
                        name = c3_var + "_bin" + str(i)
                        try:
                            df[name] = source.variables[nc_var][:,i]
                        except:
                            pass
                else:
                    try:
                        df[c3_var] = source.variables[nc_var][:]
                    except:
                        pass
            c3.NetCDFUtil.closeFile(source, c3file.file.url)
            return df

    df = ObsVars.get_df_from_c3_file(this)
    parent_id = "OOS_SetName_" + obsSet.name + "_Ver_" + obsSet.versionTag
    df['parent'] = parent_id

    zero_time = datetime(1970,1,1,0,0)
    now_time = datetime.now()
    diff_time = (now_time - zero_time)
    versionTag= -1 * diff_time.total_seconds()
    df['dataVersion'] = versionTag

    output_records = df.to_dict(orient="records")

    # upsert this batch
    c3.ObservationOutput.upsertBatch(objs=output_records)

    this.processed = True
    c3.ObservationOutputFile.merge(this)
    return True


def upsertATOMData(this):
    """
    Function to Open files in the ObservationOutputFile table and then populate ObservationOutput data.
    
    - Arguments:
        -this: an instance of ObservationOutputFile
    - Returns:
        -bool: True if file was processed, false if file has already been processed
    """
    from datetime import datetime, timedelta
    import pandas as pd

    obsSet = c3.ObservationSet.get(this.observationSet.id)
    if (obsSet.name != "ATom_60s"):
        return False

    class ObsVars:
        nc_variables = [
            'time',
            'gps_altitude', 'latitude', 'longitude', 
            'ambient_pressure', 'ambient_temperature', 'RHw_DLH', 
            'theta', 'ozone', 'CO', 
            'num_fine', 'sfc_fine', 'vol_fine',
            'num_coarse', 'sfc_coarse', 'vol_coarse', 
            'num_nucl', 'sfc_nucl', 'vol_nucl', 
            'num_Aitken', 'sfc_Aitken', 'vol_Aitken', 
            'num_accum', 'sfc_accum', 'vol_accum', 
            'dndlogd', 'sulfate_calc', 'nitrate_calc', 
            'ammonium_calc', 'chl_calc', 'oa_calc', 
            'ext_Angstrom_dry', 'ext_angstrom_ambRH', 'ext_Angstrom_dry_uv_vis', 
            'ext_angstrom_ambRH_uv_vis', 'ext_Angstrom_dry_vis_ir', 'ext_angstrom_ambRH_vis_ir',
            'abs_Angstrom_UV_Vis', 'abs_Angstrom_Vis_IR', 'CCN_005', 
            'CCN_010', 'CCN_020', 'CCN_050', 
            'CCN_100', 'f_rh_85', 'f_rh_85_fit', 
            'kappa_ext', 'kappa_ams'
        ]

        variables_map = {
            'time': 'start',
            'gps_altitude':'altitude',
            'latitude':'latitude',
            'longitude':'longitude',
            'ambient_pressure':'ambientPressure',
            'ambient_temperature':'ambientTemperature',
            'RHw_DLH':'RHwDLH',
            'theta':'theta',
            'ozone':'ozone',
            'CO':'CO',
            'num_fine':'numFine',
            'sfc_fine':'sfcFine',
            'vol_fine':'volFine',
            'num_coarse':'numCoarse',
            'sfc_coarse':'sfcCoarse',
            'vol_coarse':'volCoarse',
            'num_nucl':'numNucl',
            'sfc_nucl':'sfcNucl',
            'vol_nucl':'volNucl',
            'num_Aitken':'numAitken',
            'sfc_Aitken':'sfcAitken',
            'vol_Aitken':'volAitken',
            'num_accum':'numAccum',
            'sfc_accum':'sfcAccum',
            'vol_accum':'volAccum',
            'dndlogd':'dndlogd',
            'sulfate_calc':'sulfateCalc',
            'nitrate_calc':'nitrateCalc',
            'ammonium_calc':'ammoniumCalc',
            'chl_calc':'chlCalc',
            'oa_calc':'oaCalc',
            'ext_Angstrom_dry':'extAngstromDry',
            'ext_angstrom_ambRH':'extAngstromAmbRH',
            'ext_Angstrom_dry_uv_vis':'extAngstromDryUVVis',
            'ext_angstrom_ambRH_uv_vis':'extAngstromAmbRHUVVis',
            'ext_Angstrom_dry_vis_ir':'extAngstromDryVisIR',
            'ext_angstrom_ambRH_vis_ir':'extAngstromAmbRHVisIR',
            'abs_Angstrom_UV_Vis':'absAngstromUVVis',
            'abs_Angstrom_Vis_IR':'absAngstromVisIR',
            'CCN_005':'CCN005',
            'CCN_010':'CCN010',
            'CCN_020':'CCN020',
            'CCN_050':'CCN050',
            'CCN_100':'CCN100',
            'f_rh_85':'fRh85',
            'f_rh_85_fit':'fRh85Fit',
            'kappa_ext':'kappaExt',
            'kappa_ams':'kappaAms'
        }

        def get_df_from_c3_file(c3file):
            """
            Opens file, grab variables in the variables_map and returns pandas DataFrame
            """
            source = c3.NetCDFUtil.openFileLegacy(c3file.file.url)
            df = pd.DataFrame()
    
            def get_time_stamps(shift):
                zero_time = datetime(1904,1,1,0,0)
                time_stamp = zero_time + timedelta(seconds=shift)
                return time_stamp

            for nc_var in ObsVars.nc_variables:
                c3_var = ObsVars.variables_map[nc_var]
                if nc_var == 'time':
                    df[c3_var] = source.variables[nc_var][:]
                    df[c3_var] = df[c3_var].apply(get_time_stamps)
                elif nc_var == 'dndlogd':
                    for i in range(0,70):
                        name = c3_var + "_bin" + str(i)
                        try:
                            df[name] = source.variables[nc_var][:,i]
                        except:
                            pass
                else:
                    try:
                        df[c3_var] = source.variables[nc_var][:]
                    except:
                        pass
            c3.NetCDFUtil.closeFileLegacy(source, c3file.file.url)

            filename = c3.FileSystem.inst().rootUrl() + "start.csv"
            df["start"].to_csv(filename)
            return df
    

    df = ObsVars.get_df_from_c3_file(this)
    parent_id = "OOS_SetName_" + obsSet.name + "_Ver_" + obsSet.versionTag
    df['parent'] = parent_id

    output_records = df.to_dict(orient="records")

    # upsert this batch
    c3.ObservationAtomOutput.upsertBatch(objs=output_records)

    c3.ObservationOutputFile(
                id=this.id, 
                processed=True
    ).merge()

    return True