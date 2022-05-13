def run():
    """
    Starts a single instance of StressTestCassandra
    """
    import numpy as np
    import pandas as pd
    import string
    import datetime
    import random

    # generate cassandra keys
    nEntries = 2**17
    nParts = 2**10

    np.random.seed(1)
    random.seed(1)
    key_doubles = nParts*np.random.rand(nParts)
    key_strings = [''.join(random.choice(string.ascii_lowercase + string.digits) for _ in range(10)) for i in range(nParts)]

    # big dataframe that will store all data
    df = pd.DataFrame(columns=['field0', 'field1', 'field2', 'field3', 'partitionKey'])

    # create sub dataframes for each partition
    n = int(nEntries / nParts)
    start = datetime.datetime(1970,1,1,0,0)
    end = start + datetime.timedelta(days=365 * 100)
    for i in range(nParts):
        subdf = pd.DataFrame()
        partition = c3.CassandraKeyType(**{
            "id": key_strings[i] + str(round(key_doubles[i], 3)),
            "keyString": key_strings[i],
            "keyDouble": key_doubles[i]
        })
        ints = np.random.randint(0, n, n, dtype=int)
        doubles = n*np.random.rand(n)
        x = np.random.rand(n)
        dates = start + (end-start)*x
        strings = [''.join(random.choice(string.ascii_lowercase + string.digits) for _ in range(10)) for i in range(n)]
        subdf['field0'] = ints
        subdf['field1'] = doubles
        subdf['field2'] = dates
        subdf['field3'] = strings
        subdf['partitionKey'] = partition
        df = df.append(subdf)

    # batch upsert this and clean
    output_records = df.to_dict(orient="records")
    batch = c3.CassandraType.upsertBatch(objs=output_records)
    c3.CassandraType.removeBatch(objs=batch.objs)

    return 0