"""
Utility methods, and a demonstration of using a custom library from a model.
"""
import mmlibrary
import json
import io
import pickle


def load_pickled_model_binary():
    with open(mmlibrary.getModel(), 'rb') as f:
        return pickle.load(f)


def read_table_from_file(name: str):
    """
    Read tabular data into a pandas DataFrame.
    """
    import pandas
    data = mmlibrary.getBinaryFromResource(name)
    sample = data[:200]
    if isinstance(sample, bytes):
        sample = sample.decode("utf-8")
    if sample.startswith("{") and "\t" not in sample:
        # json ({col1=[v, ...], ...})
        return pandas.DataFrame(data=json.loads(data))
    else:
        # csv
        return pandas.read_csv(
            io.BytesIO(data) if isinstance(data, bytes) else io.StringIO(data),
            sep=None, engine='python'
        )


def write_table_to_file(name: str, data):
    """
    Store tabular data from a pandas DataFrame.
    """
    sbuf = io.StringIO()
    data.to_csv(sbuf, sep='\t', index=False)
    bbuf = io.BytesIO(sbuf.getvalue().encode("utf-8"))
    mmlibrary.saveBinaryToResource(name, bbuf)


def read_from_db(db_connection_name: str, sql: str):
    import pandas
    cursor = mmlibrary.getDBConnection(db_connection_name).cursor()
    rows = list(cursor.execute(sql))
    return pandas.DataFrame(data=rows, columns=[v[0] for v in cursor.description])
