import load
    
path = r"Z:\Favorites\SCL17 (Small Business Energy Solutions)\6 Data Exchange\SCL\Data Examples"
filename = r'\SCL Customer List_250kW SMB.xlsx'
filepath = path + filename
# sfitem = open(filepath)

connsettings = {
                'host': 'localhost',
                'port': 5432,
                'user': 'postgres',
                'password': 'RangeBreak99.',
                'db': 'scl_eda',
                }   

return_value = load.SQL_load_to_postgre(
    file_obj=filepath,
    sheet_name='Unique',
    header_row=1,
    field_type_row=-1,
    field_names={},
    field_types={},
    primary_key_fields=[''],
    join_fields=[],
    index_fields=[''], 
    date_fields=[],
    null_values={},
    table_name = 'scl_eda.accounts', 
    conn_settings = connsettings, 
    table_action=load.load_type.OVERWRITE)

print(return_value)
