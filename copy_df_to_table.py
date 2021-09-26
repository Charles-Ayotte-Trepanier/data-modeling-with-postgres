# Code taken from
# https://naysan.ca/2020/06/21/pandas-to-postgresql-using-psycopg2-copy_from/
import psycopg2
from io import StringIO


def copy_from_stringio(cursor, df, table):
    """
    Here we are going save the dataframe in memory
    and use copy_from() to copy it to the table
    """
    # save dataframe to an in memory buffer
    buffer = StringIO()
    df.to_csv(buffer, index=False, header=False, sep=';')
    buffer.seek(0)

    try:
        cursor.copy_from(buffer, table, sep=";")
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        cursor.close()
        return 1
    print("copy_from_stringio() done")
