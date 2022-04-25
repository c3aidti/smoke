def openFile(url, localPath='/tmp'):
    """
    Opens a NetCDF file from an external storage path (e.g. Azure blob)

    Arguments:
        -url (str): URL to NetCDF file
        -localPath (str): Path to the local file
    Returns:
        -netCDF4.Dataset: A netCDF4 Dataset object
    """
    import netCDF4 as nc
    import os
    filename = os.path.basename(url)
    tmp_path = localPath + '/' + filename
    c3.Client.copyFilesToLocalClient(url, '/tmp')

    return nc.Dataset(tmp_path)


def closeFile(ds, url, localPath='/tmp'):
    """
    Closes a NetCDF file.

    Arguments:
        -ds (netCDF4.Dataset): A netCDF4 Dataset object
        -url (str): URL to a NetCDF file
        -localPath (str): Path to the local file
    Returns:
        -integer
    """
    import os
    ds.close()
    filename = os.path.basename(url)
    tmp_path = localPath + '/' + filename
    os.remove(tmp_path)

    return 1