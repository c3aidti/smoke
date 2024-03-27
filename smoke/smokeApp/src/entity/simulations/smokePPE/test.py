def migratePoints(startOffset, endlimit, batchSize):
    # retrive batchSize rows starting at startOffset
    # if batchSize goes beyong endlimit, adjust the limit for that query
    # and exit the loop
    typeName = "GeoSurfaceTimePoint"
    has_more = True
    offset = startOffset
    while(has_more):
        print(f"migrating {offset} to {offset+batchSize}")
        # adjust limit if necessary, this has_more should overrise the
        # has_more in the while loop below
        if offset + batchSize > endlimit:
            batchSize = endlimit - offset
            has_more = False
        spec = {"limit":batchSize,"offset":offset}
        result = getattr(c3,typeName).fetch(spec)
        if has_more:
            has_more = result.hasMore
        offset += batchSize
        # get the objs
        objs = result.objs
        # exit if no objects found
        if objs is None or len(objs) == 0: 
            print(f"No objs to migrate for {type}.")
            break
