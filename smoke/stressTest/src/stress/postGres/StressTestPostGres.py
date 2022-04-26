def run():
    """
    Starts a single instance of StressTestPostGres
    """
    import numpy as np
    import pandas as pd
    import string
    import datetime
    import random

    nrows = 2**20

    np.random.seed(1)
    bools = np.random.randint(0, 2, nrows, dtype=bool)

    np.random.seed(2)
    ints = np.random.randint(0, nrows, nrows, dtype=int)

    np.random.seed(3)
    floats = nrows*np.random.rand(nrows)
    floats = floats.astype(dtype=np.float32)

    np.random.seed(4)
    doubles = nrows*np.random.rand(nrows)
    doubles = doubles.astype(dtype=np.float64)

    np.random.seed(5)
    strings = [''.join(random.choice(string.ascii_lowercase + string.digits) for _ in range(10)) for i in range(nrows)]

    np.random.seed(6)
    x = np.random.rand(nrows)
    start = datetime.datetime(1970,1,1,0,0)
    end = start + datetime.timedelta(days=365 * 100)
    dates = start + (end-start)*x

    df = pd.DataFrame()
    df['field0'] = bools
    df['field1'] = ints
    df['field2'] = floats
    df['field3'] = doubles
    df['field4'] = dates
    df['field5'] = strings

    output_records = df.to_dict(orient="records")
    c3.PostGresType.upsertBatch(objs=output_records)
    c3.PostGresType.removeAll(disableAsyncProcessing=True)

    return 0