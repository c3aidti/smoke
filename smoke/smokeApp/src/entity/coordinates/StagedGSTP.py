def directStage(gstpFilter):
    """
    Stage directly from the current node.
    """
    # grab elements from GSTP
    gstps = c3.GeoSurfaceTimePoint.fetch({
        "filter": gstpFilter,
        "limit": -1,
        "include": "id"
    }).objs

    stagedObjs = []
    for gstp in gstps:
        o = c3.StagedGSTP(geoSurfaceTimePoint=gstp)
        stagedObjs.append(o)

    # upsert to staging area
    c3.StagedGSTP.upsertBatch(stagedObjs)

    return 0