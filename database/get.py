"""
Handles general query events for the task. These are more abstract than the 'set' functions, as there is a larger
variety of possibilities here.
"""


def table_exists(conn, tableName):
    """
    Checks if a table exists.

    :param conn: The HBase connection object.
    :param tableName: The name of the table to check for existence.
    :return: True if table exists, false otherwise.
    """
    return tableName in conn.tables()


def table_has_row(table, rowKey):
    """
    Determines if a table has a defined row key or not.

    :param table: A HappyBase table object.
    :param rowKey: The desired row key, as a string.
    :return: True if key exists, false otherwise.
    """
    scan = table.scan(row_start=rowKey, filter='KeyOnlyFilter() AND FirstKeyOnlyFilter()', limit=1)
    return next(scan, None) is not None