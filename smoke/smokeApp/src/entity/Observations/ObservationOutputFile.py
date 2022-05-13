def upsertORACLESData(this):
    """
    Function to Open files in the ObservationOutputFile table and then populate ObservationOutput data.
    
    - Arguments:
        -this: an instance of ObservationOutputFile

    - Returns:
        -bool: True if file was processed, false if file has already been processed
    """
    from datetime import datetime
    import pandas as pd

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
            return df

    df = ObsVars.get_df_from_c3_file(this)
    obsSet = c3.ObservationSet.get(this.observationSet.id)
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