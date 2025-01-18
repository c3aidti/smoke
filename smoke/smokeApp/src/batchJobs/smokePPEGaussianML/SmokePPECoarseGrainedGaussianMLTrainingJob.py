def doStart(self, job, options):
    import pandas as pd
    # set hardware profile
    job.setHardwareProfile(options.hardwareProfileId)

    # grab all gstps
    gstpFilter = c3.Filter().ge("latitude", options.minLat).and_().lt("latitude", options.maxLat).and_().ge("longitude", options.minLon).and_().lt("longitude", options.maxLon).and_().ge("time", options.minTime).and_().lt("time", options.maxTime)

    allGstps = c3.GeoSurfaceTimePoint.fetch({
        "filter": gstpFilter,
        "limit": -1
    }).toPandas()

    # find all unique time stamps
    times = allGstps["time"].unique()

    batch = []
    # loop over each unique time stamp
    for time in times:
        # get all lat-lon poiunts for that time stamp
        gstpsForTime = allGstps[allGstps["time"] == time]
        # loop between minLat, maxLat with latStep
        n_lat_steps = (options.maxLat - options.minLat) / options.latStep
        n_lon_steps = (options.maxLon - options.minLon) / options.lonStep
        for i in range(n_lat_steps):
            lat_down = options.minLat + i * options.latStep
            lat_up = lat_down + options.latStep
            # loop between minLon, maxLon with lonStep
            for j in range(n_lon_steps):
                lon_left = options.minLon + j * options.lonStep
                lon_right = lon_left + options.lonStep
                # get all gstps in that lat-lon box
                gstpsInBox = gstpsForTime[gstpsForTime["latitude"] >= lat_down and gstpsForTime["latitude"] < lat_up and gstpsForTime["longitude"] >= lon_left and gstpsForTime["longitude"] < lon_right]
                # loop over each gstp
                targets = []
                for gstp in gstpsInBox:
                    targetFilter = c3.Filter().eq("geoSurfaceTimePoint.id", gstp["id"])
                    target = c3.SmokePPESimulationOutput.fetch({
                        "filter": targetFilter
                        "limit": -1
                    })
                    targets.append(target)
                # average over list of targets

                batch.append(above_list_of_targets)
                if len(batch) >= options.batchSize:
                    batchSpec = c3.SmokePPECoarseGrainedGaussianMLTrainingJobBatch.make({"values": batch})
                    job.scheduleBatch(batchSpec)
                    batch = []


