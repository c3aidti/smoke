def directStage(gstpFilter):
    """
    Stage directly from the current node.
    """
    # grab elements from GSTP
    gstps = c3.GeoSurfaceTimePoint.fetch({
        "filter": gstpFilter,
        "limit": -1,
        "include": "id"
    })

    # upsert to staging area
    c3.StagedGSTP.upsertBatch(gstps)

    return 0