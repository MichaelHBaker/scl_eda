
import enum
import io
import pandas as pd
import psycopg2
from io import BytesIO
from io import StringIO
import json


from enum import Enum, auto
class load_type (Enum):
    OVERWRITE = auto()
    UPDATE = auto()

panda_to_postgre_types = {
    'int64':'integer',
    'object':'text',
    'float64':'text',
    'datetime64[ns]':'text'
}

def SQL_load_to_postgre(
    file_obj,
    sheet_name,
    header_row: int,
    field_type_row: int,
    field_names: dict,
    field_types: dict,
    primary_key_fields: list,
    join_fields: list,
    index_fields: list,
    date_fields: list,
    null_values: dict,
    table_name, 
    conn_settings: dict, 
    table_action: load_type):
    """
    Load the contents of dataframe to a table in a postgresql database. 
    
    args
        file_obj file-like-object - Must have a write() method. Contains the data to be loaded.
            ex. sharefile api item
        sheet_name string - name of sheet to read from the excel file.
        header_row [see def] - Row containing field names. -1 indicates there is no row with column names.
        field_type_row [see def] - row containing field types. Must be less than header_row. -1 indicates
            there is no row with field types. Must be -1 if there are field_types
        field_names [see def] - Field names in file_obj, paired with desired field name in table_name. 
            Fields that are not included retain their file_obj name.
        field_types [see def] - Field names in file_obj, paired with desired data type in table_name. 
            Fields that are not included retain the type set by the Panda .read_csv().
        primary_key_fields [see def] - Column names in table_name, comprising the primary key. if load_type
            is CREATE or OVERWRITE and pri_key_fields is empty, an auto increment primary key field is added to the table.
        join_fields [see def] - Column names used to update table_name. Join_fields must be
            one or more of the primary_key_fields of table_name. If load_type is UPDATE and join_fields is empty,
            rows will be added only if table_name has an auto increment primary key.
        index_fields [see def] - Fields defining an index for the table. If an index exits, it is replaced.
        null_values [see def] - Field names in the file_obj, paired with a NaN value that is additional to 
            the psycopg2 standard list of NaN values.
        date_fields [see def] - Field names in the file_obj that contain data values to be parsed as datetime.
        table_name string - name of database table including the schema, ex. schema.tablename. A table_name without a
            period is treated as a temporary table.
        load_type [see def] - chose one of the values enumerated in load_type class. If load_type is UPDATE Nothing is done
            under three conditions:
                table exists and load_type = CREATE
                table exists, load_type = UPDATE and primary_key_fields are listed
                table does not exist and load_type = UPDATE
                table does not exist and load_type = OVERWRITE
        conn_settings [see def] - Value required to connect to postgresql database. Expected
            keys are
            {
                'host': 'hostname',
                'port': 1234,
                'user': 'xxxx',
                'password': 'xxxxx',
                'db': 'xxx',
            }   
    return
        True if successful. If not, a error message describing what failed.
    """

    # force table names to lower case with no embedded blanks
    table_name = table_name.replace('"','')
    table_name = table_name.replace(' ','')
    table_name = table_name.lower()

    # does table_name refer to a temporary table or is it unclear
    num_of_periods = table_name.count('.')
    if num_of_periods == 1:
        temp_table = False
        schema = table_name.split('.')[0]
        table_name = table_name.split('.')[1]
        full_table_name = schema + '.' + table_name
    elif num_of_periods > 1:
        return f'table_name = {table_name} has more than one period, schema ambiguous'
    else:
        temp_table=True
        full_table_name = table_name
    
    # connect to db
    try:
        db_conn = psycopg2.connect(
            host=conn_settings['host'], 
            port=conn_settings['port'], 
            dbname=conn_settings['db'], 
            user=conn_settings['user'], 
            password=conn_settings['password'],)
        db_cursor = db_conn.cursor()
    except Exception as e:
        return f'connection error {e}'
  
    sql = f"SELECT Exists (SELECT table_name FROM information_schema.tables WHERE " \
          f"table_name = '{table_name}');"
    db_cursor.execute(sql)
    if db_cursor.fetchone()[0] == False:
        table_exists = False
    else:
        table_exists = True
    
    if len(schema) > 0:
        sql =   f"SELECT Exists (SELECT table_name FROM information_schema.tables WHERE " \
                f"table_schema = '{schema}' AND table_name = '{table_name}');"
        db_cursor.execute(sql)
        if db_cursor.fetchone()[0] == False:
            schema_exists = False
        else:
            schema_exists = True

    if table_exists==True and table_action==load_type.OVERWRITE:
        sql = f'DROP TABLE {full_table_name}'
        try:
            db_cursor.execute(sql)
            db_conn.commit()
        except Exception as e:
            return f'sql delete failed {e}'
    
    # try:
    #     file_obj.seek(0)
    # except Exception as e:
    #     return f'Something wrong with the file_obj {e}'

    if header_row < -1 or field_type_row < -1 or header_row <= field_type_row:
        return f'header_row [{header_row}] or field_type_row [{field_type_row}] is < -1 or header_row <= datatype_row, so nothing was done'

    if field_type_row >= 0 and field_types:
        return f'field_type_row [{field_type_row}] is >= 0 and field_types is not empty, so nothing was done'

    if header_row == -1:
        nrows = field_type_row
    else:
        nrows = header_row

    df = pd.read_excel(file_obj, sheet_name=sheet_name)
    
    # force all column names to lowercase
    # df.columns = map(str.lower, df.columns)
    
    df.rename(columns=str.lower)
    df.rename(columns=lambda x: str.replace(x,"(",""))
    df.rename(columns=lambda x: str.replace(x,")",""))

    # rename columns
    df.rename(columns = field_names, inplace=True)
    
    # create dict with field names as keys data types as values
    file_obj_field_types= dict(df.dtypes)

    # create clause for primary keys
    pk_clause=''
    for pk in primary_key_fields:
        pk_clause = pk_clause + pk + ','
    pk_clause = pk_clause[0:len(pk_clause)-1]

    # create string with sql command to create table
    sql = f'CREATE TABLE {full_table_name} ('
    for k, v in file_obj_field_types.items():
        pgtype = panda_to_postgre_types[str(v)]
        sql = sql + f'{k} {pgtype},'
    sql = sql[0:len(sql)-1] + ', PRIMARY KEY (' + pk_clause + '))'
    
    try:
        db_cursor.execute(sql)
        db_conn.commit()
    except Exception as e:
            return f'create table failed {e}'

    # write dataframe to file like object
    file_obj_csv = io.StringIO()
    df.to_csv(file_obj_csv,index=False,sep='\a', header=False)
    

    try:
        file_obj_csv.seek(0)
        db_cursor.copy_from(file_obj_csv, full_table_name,sep='\a')
        db_conn.commit()
    except Exception as e:
            return f'copy to table failed {e}'

    try:
        db_cursor.close()
    except Exception as e:
            return f'table close failed {e}'

    

