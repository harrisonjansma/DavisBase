import os
import struct
import sys
from datetime import datetime, time
import sqlparse
import re
import pdb

############################################################################

def check_input(command):
    if len(command)==0:
        pass
    elif command[-1]!=";":
        return command

    elif command == "help;":
        help()

    elif command == "show tables;":
        show_tables()

    elif command[0:len("create table ")] == "create table ":
        create_table(command)

    elif command[0:len("drop table ")] == "drop table ":
        drop_table(command)

    elif command[0:len("create index ")] == "create index ":
        create_index(command)

    elif command[0:len("insert ")] == "insert ":
        insert_into(command)

    elif command[0:len("delete ")] == "delete ":
        delete_from(command)

    elif command[0:len("update ")] == "update ":
        update(command)

    elif command[0:len("select ")] == "select ":
        query(command)

    elif command == "exit;":
        return True


    else:
        print("Command \"{}\" not recognized".format(command))

####################################################################
# COMPLETED FUNCTIONS



def init():
    if os.path.exists('davisbase_columns.tbl'):
        pass
    else:
        initialize_file('davisbase_columns', True)
        file_name = "davisbase_columns.tbl"
        davisbase_columns_schema = ['TEXT', 'TEXT', 'TEXT', 'TINYINT', 'TEXT', 'TEXT', 'TEXT']

        davisbase_columns_cells = [["davisbase_tables", "rowid", "INT", 1, "NO", 'NO', 'NO' ],
                ["davisbase_tables", "table_name", "TEXT", 2, "NO", 'NO', 'NO' ],
                 ["davisbase_columns", "rowid", "INT", 1, "NO", 'NO', 'NO' ],
                ["davisbase_columns", "table_name", "TEXT", 2, "NO", 'NO', 'NO' ],
                ["davisbase_columns", "column_name", "TEXT", 3, "NO", 'NO', 'NO' ],
                ["davisbase_columns", "data_type", "TEXT", 4, "NO", 'NO', 'NO' ],
                ["davisbase_columns", "ordinal_position", "TINYINT", 5, "NO", 'NO', 'NO' ],
                ["davisbase_columns", "is_nullable", "TEXT", 6, "NO", 'NO', 'NO' ],
              ["davisbase_columns", "unique", "TEXT", 7, "NO", 'NO', 'NO' ],
              ["davisbase_columns", "primary_key", "TEXT", 8, "NO", 'NO', 'NO' ]]

        for i, cell in enumerate(davisbase_columns_cells):
            cell = table_create_cell(davisbase_columns_schema, cell, False, left_child_page=None,  rowid=i+1)
            try:#cant use insert, because insert requires use of catalog, must do this first one manually
                page_insert_cell(file_name, 0, cell)
            except:
                table_leaf_split_page(file_name, 0, cell)

    if os.path.exists('davisbase_tables.tbl'):
        pass
    else:
        initialize_file('davisbase_tables', True)
        file_name = "davisbase_tables.tbl"
        davisbase_tables_schema = ['TEXT']

        cells = [["davisbase_tables"],
                ["davisbase_columns"]]
        for i, cell in enumerate(cells):
            cell = table_create_cell(davisbase_tables_schema, cell, False, left_child_page=None,  rowid=i+1)
            try:
                page_insert_cell(file_name, 0, cell)
            except:
                print("cell_size:",len(cell))
                file_bytes = load_file(file_name)
                print("Remaining space in page:", page_available_bytes(file_bytes, 0))


def help():
    print("DavisBase supported commands.")
    print("##########################################")
    print("SHOW TABLES;")
    print("CREATE TABLE ...;")
    print("DROP TABLE ...;")
    print("CREATE INDEX ...;")
    print("INSERT INTO ...;")
    print("DELETE FROM ...;")
    print("UPDATE ...")
    print("SELECT ...;")
    print("EXIT;")
    return None




def initialize_file(table_name, is_table, is_interior=False, rchild=0):
    """Creates a file and writes the first, empty page (root)"""
    if is_table:
        file_type = ".tbl"
    else:
        file_type = '.ndx'
    if os.path.exists(table_name+file_type):
        os.remove(table_name+file_type)
    with open(table_name+file_type, 'w+') as f:
        pass
    write_new_page(table_name, is_table, is_interior, rchild, -1)
    return None


def initialize_indexes(column_dictionary):
    """
    dictionary = {
    'table_name':{
        "column1":{
            'data_type':"int",
            'ordinal_position':1,
            'is_nullable':'YES',
            'unique':'NO'
            'primary_key':'YES'
            }
        }
    }
    """
    table = list(column_dictionary.keys())
    table_name = table[0]
    column_names = list(column_dictionary[table_name].keys())
    columns = list(column_dictionary[table_name].values())

    for col in column_names:
        if column_dictionary[table_name][col]['primary_key']=='YES':
            index_name = table_name+'_'+col
            initialize_file(index_name, False) #create the empty ndx file for primary key
    return None


def catalog_add_table(column_dictionary):
    """
    dictionary = {
    'table_name':{
        "column1":{
            'data_type':"int",
            'ordinal_position':1,
            'is_nullable':'YES',
            'unique':'NO'
            'primary_key':'YES'
            }
        }
    }
    """
    table = list(column_dictionary.keys())
    assert(len(table)==1)
    table_name = table[0]
    columns =  column_dictionary[table_name]
    column_names = list(column_dictionary[table_name].keys())
    table_insert("davisbase_tables", [table_name])
    table_insert("davisbase_columns",[table_name, "rowid", "INT", 1, "NO", 'NO', 'NO' ] )
    for col in column_names:
        values=[table_name, col, columns[col]['data_type'].upper(), columns[col]['ordinal_position']+1, columns[col]['is_nullable'].upper(), columns[col]['unique'].upper(), columns[col]['primary_key'].upper()]
        table_insert("davisbase_columns", values)



def write_new_page(table_name, is_table, is_interior, rsibling_rchild, parent):
    """Writes a empty page to the end of the file with an appropriate header for the kind of table/index"""
    assert(type(is_table)==bool)
    assert(type(is_interior)==bool)
    assert(type(rsibling_rchild)==int)
    assert(type(parent)==int)
    is_leaf = not is_interior
    is_index = not is_table
    if is_table:
        file_type = ".tbl"
    else:
        file_type = '.ndx'

    file_size = os.path.getsize(table_name + file_type)
    with open(table_name + file_type, 'ab') as f:
        newpage = bytearray(PAGE_SIZE*b'\x00')
        #first byte says what kind of page it is
        if is_table and is_interior:
            newpage[0:1] = b'\x05'
        elif is_table and is_leaf:
            newpage[0:1] = b'\x0d'
        elif is_index and is_interior:
            newpage[0:1] = b'\x02'
        elif is_index and is_leaf:
            newpage[0:1] = b'\x0a'
        else:
             raise ValueError("Page must be table/index")
        newpage[2:16] = struct.pack(endian+'hhii2x', 0, PAGE_SIZE, rsibling_rchild, parent)
        f.write(newpage)
        assert(file_size%PAGE_SIZE==0)
        return int(file_size/PAGE_SIZE)



def dtype_to_int(dtype):
    """based on the documentation, each dtype has a single-digit integer encoding"""
    dtype = dtype.lower()
    mapping = {"null":0,"tinyint":1, "smallint":2, "int":3, "bigint":4, "long":4, 'float':5, "double":6, "year":8, "time":9, "datetime":10, "date":11, "text":12}
    return mapping[dtype]


def int_to_fstring(key):
    """format string for use in struct.pack/struct.unpack"""
    int2packstring={
    2:'h', 3:'i', 4:'q', 5:'f', 6:'d',
    9:'i', 10:'Q', 11:'Q' }
    return int2packstring[key]


def schema_to_int(schema, values):
    """given a list of data types ex [int, year] ,convert to single-digit integer appropriate."""
    dtypes = [dtype_to_int(dt) for dt in schema]
    for i, val in enumerate(values):
        if val==None: #regardless of the col dtype, if null-> dt = 0
            dtypes[i]=0
            continue
        elif dtypes[i]==12: #add the len of the string to dtype
            dtypes[i]+=len(val)
    return dtypes


def get_dt_size(dt):
    """given the single-digit encoding for data type return the number of bytes this data takes"""
    if dt==0:
        return 0
    if dt in [1,8]:
        return 1
    elif dt in [2]:
        return 2
    elif dt in [3,5,9]:
        return 4
    elif dt in [4,6,10,11]:
        return 8
    elif dt>=12:
        return dt-12
    else:
        raise ValueError("what happened????")


def date_to_bytes(date, time=False):
    if not time:
        return struct.pack(">q", int(round(date.timestamp() * 1000)))
    else:
        return struct.pack(">i", int(round(date.timestamp() * 1000)))


def bytes_to_dates(bt, time=False):
    if not time:
        return datetime.fromtimestamp((struct.unpack(">q", bt)[0])/1000)
    else:
        return datetime.fromtimestamp((struct.unpack(">i", bt)[0])/1000)


def time_to_byte(t):
    d =  datetime(1970,1,2,t.hour,t.minute, t.microsecond)
    return date_to_bytes(d, time=True)


def byte_to_time(bt):
    return bytes_to_dates(bt, time=True).time()


def val_dtype_to_byte(val, dt):
    """given a value and a single-digit dtype rep, covert to binary string"""
    if val == None: #NULL
        return b''
    if dt==1: #one byte int
        return val.to_bytes(1, byteorder=sys.byteorder, signed=True)
    if dt==8: #one byte year relative to 2000
        return (val-2000).to_bytes(1, byteorder=sys.byteorder, signed=True)
    if dt in [2,3,4,5,6]: #alldtypes i can convert with struct object
        return struct.pack(int_to_fstring(dt), val)
    if dt in [10,11]: #datetime, date objects
        return date_to_bytes(val)
    if dt==9: #time object
        return time_to_byte(val)
    elif dt>=12:  #look for text
        return val.encode('ascii')


def dtype_byte_to_val(dt, byte_str):
    """Given the single-digit dtype encoding and byte string of approp size, returns Python value"""
    if dt==0:  #null type
        return None
    elif dt==1: #onebyteint
        return int.from_bytes(byte_str, byteorder=sys.byteorder, signed=False)
    elif dt==8: #one byte year
        return int.from_bytes(byte_str, byteorder=sys.byteorder, signed=False)+2000
    elif dt in [2,3,4,5,6]: #alldtypes i can convert with struct object
        return struct.unpack(int_to_fstring(dt), byte_str)[0]
    if dt in [10,11]: #datetime, dateobjects
        return bytes_to_dates(byte_str)
    if dt==9:#time
        return byte_to_time(byte_str)
    elif dt>=12:  #text
        return byte_str.decode("utf-8")
    else:
         raise ValueError("dtype_byte_to_val????")


def table_values_to_payload(schema, value_list):
    """given a list of database string formatted datatypes ['int'] and an assoc
    list of values with NULL=None

    returns a bytestring of all elements in value_list and a single-digit repr of the data types"""
    dtypes = schema_to_int(schema, value_list)
    byte_string = b''
    for val, dt in zip(value_list, dtypes):
        byte_val = val_dtype_to_byte(val, dt)

        byte_string += byte_val
    return byte_string, dtypes


def table_payload_to_values(payload):
    """
    Takes the entire bitstring payload and outputs the values in a list (None=Null)
    """
    num_columns = payload[0]
    temp = payload[1:]
    dtypes =  temp[:num_columns]
    temp = temp[num_columns:]
    i = 0
    values = []
    for dt in dtypes:
        element_size = get_dt_size(dt)
        byte_str = temp[i:i+element_size]
        values.append(dtype_byte_to_val(dt, byte_str))
        i+=element_size
    assert(i==len(temp))
    return values


def index_dtype_value_rowids_to_payload(index_dtype, index_value, rowid_list):
    """
    given list of database string dtype reps ['int'] single value of index, and list of integers

    returns the bytestring payload for an index cell
    """
    dt = schema_to_int([index_dtype], [index_value])
    bin_num_assoc_rowids = bytes([len(rowid_list)])
    bin_indx_dtype = bytes(dt)
    bin_index_val = val_dtype_to_byte(index_value, *dt)
    bin_rowids = struct.pack(endian+str(len(rowid_list))+'i', *rowid_list)
    payload = bin_num_assoc_rowids + bin_indx_dtype + bin_index_val+bin_rowids
    return payload


def index_payload_to_values(payload):
    """import bytestring payload from index cell outputs the index value and list of rowids"""
    assoc_row_ids = payload[0]
    indx_dtype = payload[1]

    element_size = get_dt_size(indx_dtype)
    indx_byte_str = payload[2:2+element_size]
    indx_value = dtype_byte_to_val(indx_dtype, indx_byte_str)

    bin_rowid_list  = payload[2+element_size:]

    i=0
    j = len(bin_rowid_list)
    rowid_values = []
    while(i<j):
        rowid_values.append(struct.unpack(endian+'i', bin_rowid_list[i:i+4])[0])
        i+=4

    return indx_value, rowid_values

def table_create_cell(schema, value_list, is_interior, left_child_page=None,  rowid=None):
    """
    Used to create a cell (binary string representation) that can be inserted into the tbl file

    Parameters:
    schema (list of strings):  ex. ['int', 'date', 'year']
    value_list (list of python values):  ex. [10, '2016-03-23_00:00:00',2004]
    is_interior (bool):  is the cell igoing into an interior or leaf page
    left_child_page (int):  page_no of left child (only if cell is in interior page).
    rowid (int):  rowid of the current cell (only if the cell is going in a leaf page)

    Returns:
    cell (byte-string): ex. b'\x00\x00\x00\x00\x00\x00\x00\x00'
    """
    assert(len(value_list)==len(schema))
    assert(type(schema)==list)
    assert(type(value_list)==list)
    assert(type(is_interior)==bool)

    if  is_interior:
        assert(left_child_page != None)
        assert(rowid != None)
        cell = struct.pack(endian+'ii', left_child_page, rowid)

    else:
        assert(rowid != None)
        payload_body, dtypes  = table_values_to_payload(schema, value_list)
        payload_header = bytes([len(dtypes)]) + bytes(dtypes)
        cell_payload = payload_header + payload_body
        cell_header = struct.pack(endian+'hi', len(cell_payload), rowid)
        cell = cell_header + cell_payload

    return cell


def index_create_cell(index_dtype, index_value, rowid_list, is_interior, left_child_page=None):
    """
    Used to create a cell (binary string representation) that can be inserted into the ndx file

    Parameters:
    index_dtype (string): ex"long"
    index_value (val):  ex' 1037843
    rowid_list (list of ints):  [100,22,3214]
    is_interior (bool):
    left_child_page (int):  only if cell is for interior cell


    Returns:
    cell (byte-string): ex. b'\x00\x00\x00\x00\x00\x00\x00\x00'

    """
    assert(type(is_interior)==bool)
    is_leaf = not is_interior

    payload = index_dtype_value_rowids_to_payload(index_dtype, index_value, rowid_list)
    if is_interior:
        assert(left_child_page != None)
        cell_header = struct.pack(endian+'IH', left_child_page, len(payload))
    elif is_leaf:
        cell_header = struct.pack(endian+'H', len(payload))
    else:
         raise ValueError("Page must be either table")

    cell = cell_header + payload
    return cell


def table_read_cell(cell, is_interior):
    """
    Used to read the contents of a cell (byte string)

    Parameters:
    cell (byte-string): ex b'\x00\x00\x00\x00\x00\x00\x00\x00'
    is_interior (bool):

    Returns:
    values (dictionary): ex.
    interior-> {'left_child_rowid': 1, 'rowid': 10, 'cell_size': 8}
    leaf ->{'bytes_in_payload': 61,'num_columns': 10,
            'data': [2, 2, 12,10,10, 1.2999999523162842, None,2020, None,10, 10,'hist'],
            'cell_size': 67}
    """
    is_leaf = not is_interior

    if  is_interior:
        cell_header = struct.unpack(endian+'ii', cell[0:8])
        res = {'left_child_page':cell_header[0],'rowid':cell_header[1]}
    elif is_leaf:
        cell_header = struct.unpack(endian+'hi', cell[0:6])
        payload = cell[6:]
        values = table_payload_to_values(payload)
        res = {'bytes':cell_header[0]+6, 'rowid':cell_header[1],"data":values}
    else:
        print("error in read cell")
    res["cell_size"]=len(cell)
    res['cell_binary'] = cell
    return res


def index_read_cell(cell, is_interior):
    """
    Used to read the contents of a cell (byte string)

    Parameters:
    cell (byte-string): ex b'\x00\x00\x00\x00\x00\x00\x00\x00'
    is_interior (bool):

    Returns:
    values (dictionary):
    interior -> {'lchild': 12,'index_value': 1000,'assoc_rowids': [1, 2, 3, 4],'cell_size': 32}
    leaf-> {'index_value': 1000, 'assoc_rowids': [1, 2, 3, 4], 'cell_size': 28}
    """
    result=dict()
    if  is_interior:
        cell_header = struct.unpack(endian+'ih', cell[0:6])
        result["left_child_page"]=cell_header[0]
        result["bytes"]=cell_header[0]+6
        payload = cell[6:]
    else:
        cell_header = struct.unpack(endian+'h', cell[0:2])
        result["bytes"]=cell_header[0]+6
        payload = cell[2:]

    indx_value, rowid_list = index_payload_to_values(payload)
    result["index_value"]=indx_value
    result["assoc_rowids"]=rowid_list
    result["cell_size"]=len(cell)
    result['cell_binary'] = cell
    return result


def save_page(file_name, page_num, new_page_data):
    """
    Saves the overwrites the page in the file (at loc- page_num) with a byte-string of length PAGE_SIZE

    Parameters:
    file_name (string): ex 'taco.tbl'
    page_num (int): 1
    new_page_data(bytestring): b'\r\x00\x07\x00\n\x01\x00\x00\x00\x00\xff\xff\xff\xff\x00\x00\xe0\x01\xc0\x01\xa4\x01\x80\x01\\\x013\x01\n\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00'

    Returns:
    None
    """
    assert(len(new_page_data)==PAGE_SIZE)
    file_offset = page_num*PAGE_SIZE
    file_offset_end = (page_num+1)*PAGE_SIZE
    file_bytes = load_file(file_name)
    file_bytes = bytearray(file_bytes)
    file_bytes[file_offset:file_offset_end] = new_page_data
    with open(file_name, 'r+b') as f:
        f.seek(0)
        page = f.write(file_bytes)
    return None


def page_available_bytes(file_bytes, page_num):
    page = load_page(file_bytes, page_num)
    num_cells = struct.unpack(endian+'h', page[2:4])[0]
    bytes_from_top = 16+(2*num_cells)
    cell_content_start =struct.unpack(endian+'h', page[4:6])[0]
    return  cell_content_start - bytes_from_top


def shift_page_content(page, top_indx, bot_indx, shift_step, up=True):
    if shift_step==0:
        return page
    copy = page[top_indx:bot_indx]
    if up:
        assert(top_indx-shift_step>=0)
        new_top_indx = top_indx - shift_step
        new_bot_indx = bot_indx - shift_step
        page[new_top_indx:new_bot_indx]=copy
        page[new_bot_indx:bot_indx]=b'\x00'*shift_step
        return page
    else:
        assert(bot_indx+shift_step<=PAGE_SIZE)
        new_top_indx = top_indx + shift_step
        new_bot_indx = bot_indx + shift_step
        page[new_top_indx:new_bot_indx]=copy
        page[top_indx:new_top_indx]=b'\x00'*shift_step
        return page


def update_array_values(page, first_array_loc_to_change, num_cells, shift_step, up=True):
    """When we shift the page content for cells, the pointers in the page header become incorrect."""
    if shift_step==0:
        return page
    if up:
        for i in range(first_array_loc_to_change, num_cells):
            arr_top = 16+2*i
            arr_bot = 16+2*(i+1)
            prev_val = struct.unpack(endian+'h',page[arr_top:arr_bot])[0]
            page[arr_top:arr_bot]=struct.pack(endian+'h', prev_val-shift_step)
    else:
        for i in range(first_array_loc_to_change, num_cells):
            arr_top = 16+2*i
            arr_bot = 16+2*(i+1)
            prev_val = struct.unpack(endian+'h',page[arr_top:arr_bot])[0]
            page[arr_top:arr_bot]=struct.pack(endian+'h', prev_val+shift_step)
    return page


def get_cell_indices(page, cell_indx):
    cell_top_idx = struct.unpack(endian+'h',page[16+2*cell_indx:16+2*(cell_indx+1)])[0]
    if cell_indx==0: #if cell is first on the page (bottom)
        cell_bot_idx = PAGE_SIZE
    else:
        cell_bot_idx = struct.unpack(endian+'h',page[16+2*(cell_indx-1):16+2*(cell_indx)])[0]
    return cell_top_idx, cell_bot_idx



def page_delete_cell(file_name, page_num, cell_indx):
    """
    Deletes a bytestring into a page from a table or index file. Updates the page header. Fails index given is out of bounds (2, when there is only one cell in page)
    Fails if page is empty (no cells). RETURNS IS_EMPTY FLAG (empty after deletion)

    Parameters:
    file_name (string): ex 'taco.tbl'
    page_num (int): 1
    cell (byte-string): ex b'\x00\x00\x00\x00\x00\x00\x00\x00'

    Returns:
    is_empty (bool): False
    """
    file_bytes = load_file(file_name)
    page = load_page(file_bytes, page_num)
    page = bytearray(page)
    num_cells = struct.unpack(endian+'h', page[2:4])[0]
    assert(cell_indx<=num_cells-1)#index starts at 0
    assert(num_cells>=1) #delete CAN empty a page
    assert(cell_indx>=0)

    cell_content_area_start = struct.unpack(endian+'h', page[4:6])[0]
    end_of_array = 16+2*num_cells
    array_idx_top = 16+2*cell_indx
    array_idx_bot = 16+2*(cell_indx+1)

    #if cell is the last cell (but not if theres only one cell left)
    if (cell_indx==num_cells-1) & (cell_indx!=0):
        cell_top_loc, cell_bot_loc = get_cell_indices(page, cell_indx)
        cell_2_delete = page[cell_top_loc:cell_bot_loc]
        dis2replace= len(cell_2_delete)                    #overwrite the cell2delete
        page[cell_top_loc:cell_bot_loc]=b'\x00'*dis2replace  #change the cell_start area in header
        page[4:6] = struct.pack(endian+'h', cell_content_area_start+dis2replace)
        #delete last entri in cell array
        page[16+2*(num_cells-1):16+2*(num_cells)]=b'\x00'*2
        #update the number of cells
        page[2:4] = struct.pack(endian+'h', num_cells-1)

    else:
        cell_top_loc, cell_bot_loc = get_cell_indices(page, cell_indx)
        cell_2_delete = page[cell_top_loc:cell_bot_loc]
        dis2replace= len(cell_2_delete)
        #shift cell content down
        page = shift_page_content(page, cell_content_area_start, cell_top_loc, dis2replace, up=False)
        #since we just shifted every cell, every value in cell_array is off
        page = update_array_values(page, cell_indx, num_cells, dis2replace, up=False)
        #change the cell_start area
        page[4:6] = struct.pack(endian+'h', cell_content_area_start+dis2replace)
        #shift cell array up (deletes entry for deleted cell)
        page = shift_page_content(page, array_idx_bot, end_of_array, 2, up=True)
        #update num of cells
        page[2:4] = struct.pack(endian+'h', num_cells-1)
    save_page(file_name, page_num, page)
    assert(len(page)==PAGE_SIZE) #ensure page is same size
    return (num_cells - 1) == 0


def page_update_cell(file_name, page_num, cell_indx, cell):
    """
    updates a bytestring into a page from a table or index file. Updates the page header. Fails index given is out of bounds (2, when there is only one cell in page)
    Fails if page is empty (no cells). RETURNS IS_EMPTY FLAG

    Need to think about that happens when an upate cell causes the page to be full

    Parameters:
    file_name (string): ex 'taco.tbl'
    page_num (int): 1
    cell_indx (int): 0
    cell (byte-string): ex b'\x00\x00\x00\x00\x00\x00\x00\x00'

    Returns:
    None
    """
    file_bytes = load_file(file_name)
    page = load_page(file_bytes, page_num)
    page = bytearray(page)

    num_cells = struct.unpack(endian+'h', page[2:4])[0]
    assert(cell_indx<=num_cells-1)#index starts at 0
    assert(num_cells!=0) #delete CAN empty a page
    assert(cell_indx>=0)

    cell_content_area_start = struct.unpack(endian+'h', page[4:6])[0]
    end_of_array = 16+2*num_cells
    array_idx_top = 16+2*cell_indx
    array_idx_bot = 16+2*(cell_indx+1)
    available_bytes = page_available_bytes(file_bytes, page_num)
    cell_top_idx, cell_bot_idx = get_cell_indices(page, cell_indx)
    cell_2_update = page[cell_top_idx:cell_bot_idx]
    if len(cell_2_update)==len(cell):
        page[cell_top_idx:cell_bot_idx] = cell
    elif len(cell_2_update)<len(cell): #need to shift cell_content up
        dis2move =  len(cell) - len(cell_2_update)
        assert(dis2move<=available_bytes)   #NEED TO SPLIT
        page = shift_page_content(page, cell_content_area_start, cell_top_idx, dis2move, up=True)
        #since we just shifted every cell, every value in cell_array is off
        page = update_array_values(page, cell_indx, num_cells, dis2move, up=True)
        #change cell content area start
        page[4:6] = struct.pack(endian+'h', cell_content_area_start-dis2move)
        #insert updated cell
        page[cell_top_idx-dis2move:cell_bot_idx] = cell

    else: #need to shift cell_content up
        dis2move =  len(cell_2_update) - len(cell)
        page = shift_page_content(page, cell_content_area_start, cell_top_idx, dis2move, up=False)
        #since we just shifted every cell, every value in cell_array is off
        page = update_array_values(page, cell_indx, num_cells, dis2move, up=True)
        #change cell content area start
        page[4:6] = struct.pack(endian+'h', cell_content_area_start+dis2move)
        page[cell_top_idx+dis2move:cell_bot_idx] = cell

    save_page(file_name, page_num, page)
    assert(len(page)==PAGE_SIZE) #ensure page is same size
    return None



def update_page_header(file_name, page_num, rsibling_rchild=None, is_interior=None, parent=None):
    is_table = file_name[-4:]=='.tbl'
    is_index=not is_table
    is_leaf = not is_interior
    file_bytes = load_file(file_name)
    page = load_page(file_bytes, page_num)
    page = bytearray(page)
    if rsibling_rchild is not None:
        assert(len(file_bytes)/PAGE_SIZE>=rsibling_rchild)
        page[6:10] = struct.pack(endian+'i', rsibling_rchild)
    if is_interior is not None:
        if page[0] in [5,13]:
            is_table = True
        else:
            is_table = False

        if is_table and is_interior:
            page[0:1] = b'\x05'
        elif is_table and is_leaf:
            page[0:1] = b'\x0d'
        elif is_index and is_interior:
            page[0:1] = b'\x02'
        elif is_index and is_leaf:
            page[0:1] = b'\x0a'
    if parent is not None:
        page[10:14] = struct.pack(endian+'i', parent)
    save_page(file_name, page_num, page)
    return None


def update_cell_lpointer(file_name, page_num, cell_indx, lpointer=None, rowid=None):
    file_bytes = load_file(file_name)
    page = load_page(file_bytes, page_num)
    page = bytearray(page)
    cell_top_idx, cell_bot_idx = get_cell_indices(page, cell_indx)
    if lpointer!=None:
        page[cell_top_idx:cell_top_idx+4] = struct.pack(endian+'i', lpointer)
    if rowid!=None:
        page[cell_top_idx+4:cell_top_idx+8] = struct.pack(endian+'i', rowid)
    save_page(file_name, page_num, page)
    return None


def load_file(file_name):
    """loads the table/index file returns the bytestring for the entire file (reduce number of read/writes)

    Parameters:
    file (byte-string): ex 'taco.tbl'
    page_num (int): 1

    Returns:
    page (bytestring):
    """
    with open(file_name, 'rb') as f:
        return f.read()


def load_page(file_bytes, page_num):
    """
    loads the page of from the table/index PAGE NUMBER STARTS AT ZERO, will only load one pa
    Parameters:
    file_name (string): ex 'taco.tbl'
    page_num (int): 1
    Returns:
    page (bytestring):
    """
    file_offset = page_num*PAGE_SIZE
    return file_bytes[file_offset:(page_num+1)*PAGE_SIZE]


def read_cells_in_page(file_bytes, page_num):
    """read all the data from a page, get the file_bytes object with load_file(file_name)"""
    assert(page_num<(len(file_bytes)/PAGE_SIZE))
    page = load_page(file_bytes, page_num)
    num_cells = struct.unpack(endian+'h', page[2:4])[0]
    parent_page = struct.unpack(endian+'i', page[10:14])[0]
    available_bytes = page_available_bytes(file_bytes, page_num)
    if page[0] in [5,13]:
        is_table = True
    else:
        is_table = False

    if page[0] in [2,5]:
        is_interior = True
    else:
        is_interior = False

    i=0
    data = []
    while i<=num_cells-1:
        if i == 0:
            cell_bot_loc = PAGE_SIZE
        else:
            cell_bot_loc = struct.unpack(endian+'h',page[16+2*(i-1):16+2*(i)])[0]
        cell_top_loc = struct.unpack(endian+'h',page[16+2*i:16+2*(i+1)])[0]
        cell = page[cell_top_loc:cell_bot_loc]
        if is_table:
            data.append(table_read_cell(cell, is_interior))
        else:
            data.append(index_read_cell(cell, is_interior))
        i+=1

    result = {
    "page_number":page_num,
    "parent_page":parent_page,
    "is_table": is_table,
    "is_leaf": not is_interior,
    "num_cells":num_cells,
    "available_bytes":available_bytes
    }
    if is_interior:
        result['rightmost_child_page'] = parent_page = struct.unpack(endian+'i', page[6:10])[0]
    else:
        result['right_sibling_page'] = parent_page = struct.unpack(endian+'i', page[6:10])[0]
    result['cells']=data
    if is_table:
        result['rowids'] = [i['rowid'] for i in data]
    else:
        result['index_values'] = [i['index_value'] for i in data]
    return result


def read_all_pages_in_file(file_name):
    """
    Given the file name, loads all data from every page. This is what we will use during inserts updates, deletes

    Parameters:
    file_name (string): ex"davisbase_tables.tbl"

    Returns:
    pages (dict of dicts): ex. b'\x00\x00\x00\x00\x00\x00\x00\x00'

    """
    if file_name[-3:]=='tbl':
        is_table=True
    else:
        is_table = False

    file = load_file(file_name)
    file_size = len(file)
    assert(file_size%PAGE_SIZE==0)
    num_pages = int(file_size/PAGE_SIZE)
    data = []
    for page_num in range(num_pages):
        data.append(read_cells_in_page(file, page_num))
    for page in data:
        if page['is_leaf']:
            if page['right_sibling_page']!=-1:
                if data[page['right_sibling_page']]['parent_page']==page['parent_page']:
                    data[page['right_sibling_page']]['left_sibling_page'] = page['page_number']
        else:
            for i, cell in enumerate(page['cells']):
                child_page = cell['left_child_page']
                if i!=0:
                    data[child_page]['left_sibling_page']=page['cells'][i-1]['left_child_page']
                if i+1!=len(page['cells']):
                    data[child_page]['right_sibling_page']=page['cells'][i+1]['left_child_page']
                else:
                    data[child_page]['right_sibling_page']=page['rightmost_child_page']

            data[page['rightmost_child_page']]['left_sibling_page']=page['cells'][-1]['left_child_page']
            data[page['rightmost_child_page']]['right_sibling_page']=-1


    return data


def get_indexes(table_name):
    """Returns all filenames for indexes of the table"""
    indexes=[]
    for filename in os.listdir():
        if (filename[:len(table_name)]==table_name) and (filename[-4:]=='.ndx'):
            indexes.append(filename)
    return indexes




def get_next_page_rowid(table_name):
    """Finds the next rowid and page for an insert"""
    pages = read_all_pages_in_file(table_name+'.tbl')
    final_page_num = 0
    while not pages[final_page_num]['is_leaf']:
        final_page_num = pages[final_page_num]['rightmost_child_page']

    final_page = pages[final_page_num]
    if len(pages[0]['cells'])==0:#if there are no records in the table
        next_rowid=0
    else:
        rowid_sorted_cells = sorted(final_page['cells'], key=lambda x: x['rowid'])
        next_rowid = rowid_sorted_cells[-1]['rowid']
    return final_page['page_number'], next_rowid + 1


def get_column_names_from_catalog(table_name):
    """Returns the column names for a table in order"""
    schema, catalog_cells = schema_from_catalog(table_name, with_rowid=True)
    col_names = []
    for cell in catalog_cells:
        col_names.append((cell['data'][3],cell['data'][1])) #list of [(ord_pos, col_name)]
    col_names = sorted(col_names, key=lambda x: x[0])
    return  [i[1] for i in col_names]


def schema_from_catalog(table_name, with_rowid=False):
    """Returns the column datatypes and a list of cells from davisbase_tables"""
    data = read_all_pages_in_file('davisbase_columns.tbl')
    all_cells = []
    all_data = []
    for page in data:
        if not page['is_leaf']:
            continue
        for cell in page['cells']:
            col_table = cell['data'][0].lower()
            if col_table==table_name.lower():
                col_name = cell['data'][1].lower()
                if col_name=='rowid' and not with_rowid:
                    continue
                all_cells.append((cell['data'][3],cell['data'][2])) #list of [(ord_pos, dtype)]
                all_data.append(cell)
    all_cells = sorted(all_cells, key=lambda x: x[0])
    schema = [i[1] for i in all_cells]
    return schema, all_data


def index_insert_cell_in_page(file_name, page_num, cell, cell_indx):
    """Inserts a cell into the middle of a page (rather than end)"""
    file_bytes = load_file(file_name)
    page = load_page(file_bytes, page_num)
    page = bytearray(page)

    num_cells = struct.unpack(endian+'h', page[2:4])[0]
    if cell_indx == num_cells: #add to end of page
        page_insert_cell(file_name, page_num, cell)
        return None

    assert(cell_indx<=num_cells-1)#index starts at 0
    assert(cell_indx>=0)
    assert(len(cell)<page_available_bytes(file_bytes, page_num))
    cell_content_area_start = struct.unpack(endian+'h', page[4:6])[0]
    end_of_array = 16+2*num_cells
    array_idx_top = 16+2*cell_indx
    array_idx_bot = 16+2*(cell_indx+1)

    cell_top_loc, cell_bot_loc = get_cell_indices(page, cell_indx)
    dis2move= len(cell)
    #shift cell content down
    page = shift_page_content(page, cell_content_area_start, cell_bot_loc, dis2move, up=True)
    #since we just shifted every cell, every value in cell_array is off
    page = update_array_values(page, cell_indx, num_cells, dis2move, up=True)
    #change the cell_start area
    page[4:6] = struct.pack(endian+'h', cell_content_area_start-dis2move)
    #shift cell array up (deletes entry for deleted cell)
    page = shift_page_content(page, array_idx_top, end_of_array, 2, up=False)
    page[array_idx_top:array_idx_bot] = struct.pack(endian+'h', cell_bot_loc-dis2move)
    page[cell_bot_loc-dis2move:cell_bot_loc] = cell
    #update num of cells
    page[2:4] = struct.pack(endian+'h', num_cells+1)
    assert(len(page)==PAGE_SIZE)
    save_page(file_name, page_num, page)
    #ensure page is same size
    return (num_cells - 1) == 0


def print_it(file_name, page_format=False, limit=None, pages=None):
    """Used for testing, prints all contents of a table/index in page form or as list"""
    if pages ==None:
        pages  =read_all_pages_in_file(file_name)

    print(file_name[:-4])
    if page_format:
        for page in pages:
            if page["is_leaf"]:
                continue
            else:
                print()
                print("page_number: ",page['page_number'])
                print("parent_page: ",page['parent_page'])
                print("right_child_page: ",page['rightmost_child_page'])
                print("bytes remaining:", page['available_bytes'])
                for cell in page["cells"]:
                    if file_name[-4:]=='.tbl':
                        print("rowid: ",cell['rowid'],"left child: ",cell['left_child_page'])
                    else:
                        print("indx_val: ",cell['index_value'],"left child: ",cell['left_child_page'])
        for page in pages:
            if not page["is_leaf"]:
                continue
            else:
                print()
                print("page_number: ",page['page_number'])
                print("parent_page: ",page['parent_page'])
                print("right_sibling_page: ",page['right_sibling_page'])
                print("bytes remaining:", page['available_bytes'])
                rowids = []
                for cell in page["cells"]:
                    if file_name[-4:]=='.tbl':
                        rowids.append(cell['rowid'])
                    else:
                        rowids.append(cell['index_value'])
                print(rowids)
    else:
        rows = []
        for page in pages:
            if not page["is_leaf"]:
                continue
            else:
                for cell in page["cells"]:
                    if file_name[-4:]=='.tbl':
                        rows.append([cell['rowid']]+cell['data'])
                    else:
                        rows.append([cell['index_value'],cell['assoc_rowids']])
        rows = sorted(rows, key=lambda x: x[0])
        i=1
        for row in rows:
            if limit!=None and i>limit:
                break
            print(row)
            i+=1


def add_rowid_to_cell(file_name, page_num, cell_indx, rowid, cell):
    """Used in index insert, adds a rowid to list of associated rowids
    This is done when an indec_value is already present in the index"""
    cell_binary = cell['cell_binary']+struct.pack(endian+'i', rowid)
    try:
        page_update_cell(file_name, page_num, cell_indx, cell_binary)
    except:
        return


def get_all_table_cells(table_name):
    """Grabs all the cells (no order)"""
    pages  =read_all_pages_in_file(table_name+'.tbl')
    cells = []
    for page in pages:
        if not page["is_leaf"]:
            continue
        else:
            for cell in page["cells"]:
                cells.append(cell)
    return cells

###########################################################################





###########################################################################
# DONE, BUT NEEDS CONNECTING
"""NEEDS CONNECTING TO CREATE_TABLE_PARSER"""
def create_table(command):
    """Given the inputs of the command line, creates table, metadata, and indexes"""
    col_catalog_dictionary = parse_create_table(command)
    table_name = list(col_catalog_dictionary.keys())[0]
    initialize_file(table_name, True)
    catalog_add_table(col_catalog_dictionary)
    initialize_indexes(col_catalog_dictionary)
    return None

"""NEEDS CONNECTING TO CREATE_INDEX_PARSER"""
def create_index(command):
    """Given the inputs of the command line, creates index on an existing table"""
    table_name, column_name = parse_create_index(command)
    index_name = table_name+'_'+column_name
    initialize_file(index_name, False) #create the index
    columns = get_column_names_from_catalog(table_name)[1:] #column names minus rowid
    schema, _ = schema_from_catalog(table_name)
    ord_position =  columns.index(column_name) #position of data
    index_dtype = schema[ord_position]
    cells = get_all_table_cells(table_name)
    for cell in cells:
        rowid = cell['rowid']
        index_value = cell['data'][ord_position]
        index_insert(table_name, column_name, index_dtype, index_value, rowid)

"""NEEDS CONNECTING TO CREATE_INDEX_PARSER"""
"""Also need a function to check for uniqueness, not nullness"""
def insert_into(command):
    """values would be a list of length self.columns, NULL represented as None"""
    """Parser needs to return "table_name", [[col1,col2,col3],[col1,col2,col3],[col1,col2,col3]]"""
    table_name, values = parse_insert_into(command)
    violation_flag, violating_row = FUNCTION_TO_CHECK_CONSTRAINTS_THAT_WE_DONT_HAVE_YET(table_name, values)
    if violation_flag: #if violation fail insert
        print("Constraint violated for row {}".format(violating_row))
        return None
    schema, all_col_data = schema_from_catalog(table_name)
    col_names = get_column_names_from_catalog(table_name)[1:]
    indexes = get_indexes(table_name)
    for val in values:
        next_page, next_rowid = get_next_page_rowid(table_name)
        cell = table_create_cell(schema, values, False,  rowid=next_rowid)
        try:
            page_insert_cell(table_name+'.tbl', next_page, cell)
        except:
            table_leaf_split_page(table_name+'.tbl', next_page, cell)
        for filename in indexes:
            index_colname = filename[len(table_name)+1:-4]
            i = col_names.index(index_colname)
            index_dtype= schema[i]
            index_value= val[i] #index by ord position
            index_insert(table_name, index_colname, index_dtype, index_value, next_rowid)


def delete_from(command):
    table_name, condition = parse_delete_from(command)
    rowids = WHERE_FUNCTION(table_name, condition)
    col_names = get_column_names_from_catalog(table_name)[1:]
    indexes = get_indexes(table_name)
    table_delete(table_name, rowids)
    """for filename in indexes:
        index_colname = filename[len(table_name)+1:-4]
        i = col_names.index(index_colname) #get position
        index_value = cell['data'][i] #index by ord position
        index_delete(table_name, index_colname, index_value, cell['rowid'])"""


def update(command):
    """
    dict_new_values = {
    "column1":new_value_to_update_to,
    "column2":new_value_to_update_to,
    "column4":new_value_to_update_to,
    }"""
    table_name, condition, dict_new_values = parse_delete_from(command)
    cells = WHERE_FUNCTION(table_name, condition)
    col_names = get_column_names_from_catalog(table_name)[1:]
    indexes = get_indexes(table_name)
    for cell in cells:
        table_update(table_name, cell, dict_new_values)
        for filename in indexes:
            index_colname = filename[len(table_name)+1:-4]
            if index_colname in dict_new_values:
                i = col_names.index(index_colname) #get position
                index_value = cell['data'][i] #index by ord position
                new_index_value = dict_new_values[index_colname]
                index_update(table_name, index_colname, index_value, cell['rowid'], new_index_value)


def drop_table(command):
    table_name = parse_drop_table(command)
    if os.path.exists(table_name+".tbl"):
        os.remove(table_name+".tbl")
        _, rows = schema_from_catalog(table_name, with_rowid=True)
        rowids = [row['rowid'] for row in rows]
        table_delete('davisbase_columns.tbl', rowids)

        data = read_all_pages_in_file('davisbase_tables.tbl')
        for page in data:
            if not page['is_leaf']:
                continue
            for cell in page['cells']:
                if table_name==cell['data'][0].lower():
                    rowids = [cell['rowid']]
                    break
        table_delete('davisbase_tables.tbl', rowids)
        for index in get_indexes(table_name):
            os.remove(index)
    else:
        print("Table \"{}\" does note exist.".format(table_name))








#########################################################################
# TESTING

def page_insert_cell(file_name, page_num, cell):
    """
    Inserts a bytestring into a page from a table or index file. Updates the page header. Fails if page-full

    Parameters:
    file_name (string): ex 'taco.tbl'
    page_num (int): 1
    cell (byte-string): ex b'\x00\x00\x00\x00\x00\x00\x00\x00'

    Returns:
    None
    """
    file_bytes = load_file(file_name)
    page = load_page(file_bytes, page_num)
    page = bytearray(page)

    if type(cell)==list:
        cells = cell
        for cell in cells:
            assert(len(cell)<page_available_bytes(file_bytes, page_num)) #CHECK IF PAGE FULL
            num_cells = struct.unpack(endian+'h', page[2:4])[0]
            bytes_from_top = 16+(2*num_cells)
            bytes_from_bot =struct.unpack(endian+'h', page[4:6])[0]
            new_start_index = bytes_from_bot - len(cell)
            #insert cell data
            page[new_start_index:bytes_from_bot] = cell
            #add to 2byte cell array
            page[bytes_from_top:bytes_from_top+2] = struct.pack(endian+'h', new_start_index)
            #update start of cell content
            page[4:6] = struct.pack(endian+'h', new_start_index)
            #update num_cells
            page[2:4] = struct.pack(endian+'h', num_cells+1)
            assert(len(page)==PAGE_SIZE)
    else:
        assert(len(cell)<page_available_bytes(file_bytes, page_num)) #CHECK IF PAGE FULL
        num_cells = struct.unpack(endian+'h', page[2:4])[0]
        bytes_from_top = 16+(2*num_cells)
        bytes_from_bot =struct.unpack(endian+'h', page[4:6])[0]
        new_start_index = bytes_from_bot - len(cell)
        #insert cell data
        page[new_start_index:bytes_from_bot] = cell
        #add to 2byte cell array
        page[bytes_from_top:bytes_from_top+2] = struct.pack(endian+'h', new_start_index)
        #update start of cell content
        page[4:6] = struct.pack(endian+'h', new_start_index)
        #update num_cells
        page[2:4] = struct.pack(endian+'h', num_cells+1)
        assert(len(page)==PAGE_SIZE)
    save_page(file_name, page_num, page)
    return None


def page_delete_cells_on_and_after(file_name, page_num, cell_indx):
    """Deletes all cells in page on or after cell_indx (starts w zero)"""
    file_bytes = load_file(file_name)
    page = load_page(file_bytes, page_num)
    page = bytearray(page)
    num_cells = struct.unpack(endian+'h', page[2:4])[0]
    assert(cell_indx<=num_cells-1)#index starts at 0
    assert(num_cells>=1) #delete CAN empty a page
    assert(cell_indx>=0)
    cell_content_area_start = struct.unpack(endian+'h', page[4:6])[0]
    cell_top_loc, cell_bot_loc = get_cell_indices(page, cell_indx)
    dis = cell_bot_loc - cell_content_area_start
    page[cell_content_area_start:cell_bot_loc] = b'\x00'*dis
    page[16+2*cell_indx:16+2*num_cells] = b'\x00'*2*(num_cells-cell_indx)
    #update num of cells
    page[2:4] = struct.pack(endian+'h', cell_indx)
    #change cell start area
    page[4:6] = struct.pack(endian+'h', cell_content_area_start+dis)
    save_page(file_name, page_num, page)
    assert(len(page)==PAGE_SIZE) #ensure page is same size
    return (num_cells - 1) == 0


def table_interior_split_page(file_name, split_page_num, cell2insert, new_rightmost_page):
    pages = read_all_pages_in_file(file_name)
    values = pages[split_page_num]

    table_name = file_name[:-4]
    parent_num = values['parent_page']
    is_interior = not values['is_leaf']
    is_table = values['is_table']
    assert(is_table)
    assert(is_interior)

    num_cells = values['num_cells']
    cells = values['cells']
    middle_cell = int((num_cells+1)//2) #have to add one since we havent actually added the cell


    middle_cell_binary = cells[middle_cell]['cell_binary']
    middle_rowid = cells[middle_cell]['rowid']

    rightmost_child_page_right = new_rightmost_page
    rightmost_child_page_left = cells[middle_cell]['left_child_page']



    if parent_num==-1: #ROOT CONDITION #children will also be interior nodes
        rchild_num = write_new_page(table_name, is_table, is_interior, new_rightmost_page, split_page_num)
        lchild_num = write_new_page(table_name, is_table, is_interior, rightmost_child_page_left, split_page_num)

        cells2copy=[]
        for i in range(middle_cell):
            cells2copy.append(cells[i]['cell_binary'])
            update_page_header(file_name, cells[i]['left_child_page'], parent=lchild_num)
        update_page_header(file_name, rightmost_child_page_left, parent=lchild_num) #update parent of rightmost
        #Copy cells into left child
        page_insert_cell(file_name, lchild_num, cells2copy)

        cells2copy=[]
        for i in range(middle_cell+1, num_cells):#update child to point header to rchild
            cells2copy.append(cells[i]['cell_binary'])
            update_page_header(file_name, cells[i]['left_child_page'], parent=rchild_num)
        update_page_header(file_name, rightmost_child_page_right, parent=rchild_num)
        #Copy cells into right child
        page_insert_cell(file_name, rchild_num, cells2copy)
        page_insert_cell(file_name, rchild_num, cell2insert)
        #Update the pointers in the new, root node, then delete all but middle cell

        page_delete_cells_on_and_after(file_name, split_page_num, 0)
        page_insert_cell(file_name, split_page_num, middle_cell_binary)
        update_cell_lpointer(file_name, split_page_num, 0, lchild_num)
        update_page_header(file_name, split_page_num, rsibling_rchild=rchild_num)
        return rchild_num  #return so we can update headers of pages that couldnt fit in the old page

    else:
        rsibling = write_new_page(table_name, is_table, is_interior, rightmost_child_page_right, parent_num)

        cells2copy=[]
        for i in range(middle_cell+1, num_cells): #Copy cells into right child update child pointers
            cells2copy.append(cells[i]['cell_binary'])
            update_page_header(file_name, cells[i]['left_child_page'], parent=rsibling)
        update_page_header(file_name, rightmost_child_page_right, parent=rsibling)

        page_insert_cell(file_name, rsibling, cells2copy)
        page_insert_cell(file_name, rsibling, cell2insert)
        page_delete_cells_on_and_after(file_name, split_page_num, middle_cell)

        middle_cell_binary = table_create_cell([], [], True, left_child_page=split_page_num,  rowid=middle_rowid)
        update_page_header(file_name, split_page_num, rsibling_rchild=rightmost_child_page_left)

        if pages[parent_num]['rightmost_child_page']==split_page_num:
            update_page_header(file_name, parent_num, rsibling_rchild=rsibling)
        try:
            page_insert_cell(file_name, parent_num, middle_cell_binary)
        except:
            new_parent = table_interior_split_page(file_name, parent_num, middle_cell_binary, rsibling)
            update_page_header(file_name, rsibling, parent = new_parent)
            update_page_header(file_name, split_page_num, parent = new_parent)
        return rsibling


#could put these two together, but I dont care
def table_leaf_split_page(file_name, split_page_num, cell2insert):
    file_bytes = load_file(file_name)
    values = read_cells_in_page(file_bytes, split_page_num)

    table_name = file_name[:-4]
    parent_num = values['parent_page']
    is_interior = not values['is_leaf']
    is_leaf = values['is_leaf']
    is_table = values['is_table']
    assert(is_table)
    assert(is_leaf)

    num_cells = values['num_cells']
    cells = values['cells']
    middle_cell = int((num_cells+1)/2) #have to add one since we havent actually added the cell
    middle_cell_binary = cells[middle_cell]['cell_binary']
    middle_rowid = cells[middle_cell]['rowid']
    right_sibling_page = values['right_sibling_page']

    if parent_num==-1: #IS ROOT ->create two children
        rchild_num = write_new_page(table_name, is_table, False, -1, split_page_num)
        lchild_num = write_new_page(table_name, is_table, False, rchild_num, split_page_num)

        cells2copy = []
        for i in range(middle_cell):   #Copy cells into left child
            cells2copy.append(cells[i]['cell_binary'])
        page_insert_cell(file_name, lchild_num, cells2copy)

        cells2copy = []
        for i in range(middle_cell, num_cells): #Copy cells into right child
            cells2copy.append(cells[i]['cell_binary'])
        page_insert_cell(file_name, rchild_num, cells2copy)
        page_insert_cell(file_name, rchild_num, cell2insert)

        middle_cell_binary = table_create_cell([], [], True, left_child_page=lchild_num,  rowid=middle_rowid)
        page_delete_cells_on_and_after(file_name, split_page_num, 0)
        page_insert_cell(file_name, split_page_num, middle_cell_binary)
        update_page_header(file_name, split_page_num, rsibling_rchild=rchild_num, is_interior=True)


    else: #Non-root ->propagate upward
        rsibling = write_new_page(table_name, is_table, is_interior, right_sibling_page, parent_num)
        update_page_header(file_name, split_page_num, rsibling_rchild=rsibling)

        cells2copy = []
        for i in range(middle_cell, num_cells): #Copy cells into right child
            cells2copy.append(cells[i]['cell_binary'])
        page_insert_cell(file_name, rsibling, cells2copy)
        page_insert_cell(file_name, rsibling, cell2insert)

        page_delete_cells_on_and_after(file_name, split_page_num, middle_cell)

        update_page_header(file_name, parent_num, rsibling_rchild=rsibling)
        middle_cell_binary = table_create_cell([], [], True, left_child_page=split_page_num,  rowid=middle_rowid)
        try:
            page_insert_cell(file_name, parent_num, middle_cell_binary)
        except:
            new_parent = table_interior_split_page(file_name, parent_num, middle_cell_binary, rsibling)
            update_page_header(file_name,rsibling, parent = new_parent)
            update_page_header(file_name, split_page_num, parent = new_parent)




def page_cell_indx_given_key(pages, index_value):
    """Given rowid, returns the page and cell where the cell should go"""
    page_num=0
    if len(pages[page_num]['cells'])==0:
        return page_num, 0
    return get_page_cell_indx(pages, index_value, page_num)



def get_page_cell_indx(pages, value, page_num):
    """Given rowid, returns the page and cell where the cell should go"""
    page = pages[page_num]
    is_table= page['is_table']
    is_leaf = page['is_leaf']
    if not is_table:
        for i, cell in enumerate(page['cells']):
            if cell['index_value']==value: #add to list
                return page_num, i
            elif cell['index_value'] > value: #move down and left or return if leaf
                if not page['is_leaf']:
                    return get_page_cell_indx(pages, value, cell['left_child_page'])
                else:
                    return page_num, i
            else:
                if page['is_leaf'] and i+1==len(page['cells']): #no more cells, move down and right or return final position
                    return page_num, len(page['cells'])
                if not page['is_leaf'] and i+1==len(page['cells']):
                    return get_page_cell_indx(pages, value, page['rightmost_child_page'])
                else:
                    continue
    else:
        for cell_indx, cell in enumerate(page['cells']):
            if (cell['rowid'] == value):
                if is_leaf: #got a match
                    return page_num, cell_indx
                else:
                    continue #next iteration will get it
            elif (cell['rowid'] > value): #same
                if not is_leaf:
                    return get_page_cell_indx(pages, value, cell['left_child_page'])
                else:
                    return page_num, cell_indx

            elif (cell['rowid']<value ):
                if not is_leaf:
                    return get_page_cell_indx(pages, value, page['rightmost_child_page'])
                else:
                    return page_num, len(page['cells'])
            else:
                assert(False)


def table_insert(table_name, values):
    """values would be a list of length self.columns, NULL represented as None"""
    schema, all_col_data = schema_from_catalog(table_name)
    next_page, next_rowid = get_next_page_rowid(table_name)
    cell = table_create_cell(schema, values, False,  rowid=next_rowid)
    try:
        page_insert_cell(table_name+'.tbl', next_page, cell)
    except:
        table_leaf_split_page(table_name+'.tbl', next_page, cell)
    return None



#############################################################################
#IN PROGRESS



def index_insert(table_name, column_name, index_dtype, index_value, rowid):
    """rowid will not be present, but will key value be present?
    if kv present -> append rowid to cell
    if kv not present -> create new cell insert to page

    if finds kv-> if cell has room -> insert
                ->if no room -> create_cell -> insert in left child"""
    file_name = table_name+'_'+column_name+'.ndx'
    pages = read_all_pages_in_file(file_name)
    page_num, cell_indx = page_cell_indx_given_key(pages, index_value)
    page = pages[page_num]
    if len(page['cells'])!=cell_indx:
        cell = page['cells'][cell_indx]
        if cell['index_value']==index_value: #add to the list
            if rowid not in cell['assoc_rowids']:
                add_rowid_to_cell(file_name, page_num, cell_indx, rowid, cell)
                return

    cell = index_create_cell(index_dtype, index_value, [rowid], False, left_child_page=None)
    #running low on space
    if pages[page_num]['available_bytes']/PAGE_SIZE<0.5:
        index_leaf_split_page(file_name, page_num, cell, index_dtype, cell_indx)
        return
    else:
        try:
            index_insert_cell_in_page(file_name, page_num, cell, cell_indx)
        except:
            index_leaf_split_page(file_name, page_num, cell, index_dtype, cell_indx)


def index_interior_split_page(file_name, split_page_num, cell2insert, new_rightmost_page, cell_index):
    pages = read_all_pages_in_file(file_name)
    values = pages[split_page_num]

    table_name = file_name[:-4]
    parent_num = values['parent_page']
    is_interior = not values['is_leaf']
    is_table = values['is_table']
    assert(not is_table)
    assert(is_interior)

    num_cells = values['num_cells']
    cells = values['cells']
    middle_cell = int((num_cells+1)//2) #have to add one since we havent actually added the cell

    cell2insert=index_read_cell(cell2insert, is_interior)
    insert_order = []
    i=0
    copied=False
    while len(insert_order)!=len(cells)+1:
        if cell_index==i and not copied:
            insert_order.append(cell2insert)
            copied = True
        else:
            insert_order.append(cells[i])
            i+=1

    middle_index = insert_order[middle_cell]['index_value']
    rightmost_child_page_right = values['rightmost_child_page']
    rightmost_child_page_left = insert_order[middle_cell]['left_child_page']
    cells2copy = [cell['cell_binary'] for cell in insert_order]
    middle_cell_binary = cells2copy[middle_cell]
    if parent_num==-1: #ROOT CONDITION #children will also be interior nodes
        rchild_num = write_new_page(table_name, is_table, is_interior, new_rightmost_page, split_page_num)
        lchild_num = write_new_page(table_name, is_table, is_interior, rightmost_child_page_left, split_page_num)

        for order_cell in insert_order[:middle_cell+1]: #Copy cells into left child
            update_page_header(file_name, order_cell['left_child_page'], parent=lchild_num) #update child to point header to lchild
        page_insert_cell(file_name, lchild_num, cells2copy[:middle_cell])

        for order_cell in insert_order[middle_cell+1:]: #Copy cells into right child   #splitting the interior nodes,  the middle cell is not redundant in children
            update_page_header(file_name, order_cell['left_child_page'], parent=rchild_num)
        update_page_header(file_name, values['rightmost_child_page'], parent=rchild_num)
        page_insert_cell(file_name, rchild_num, cells2copy[middle_cell+1:])


        page_delete_cells_on_and_after(file_name, split_page_num, 0)
        page_insert_cell(file_name, split_page_num, middle_cell_binary)
        update_cell_lpointer(file_name, split_page_num, 0, lchild_num)
        update_page_header(file_name, split_page_num, rsibling_rchild=rchild_num)
        print_it("table_name_column1.ndx", page_format=True)
        return rchild_num  #return so we can update headers of pages that couldnt fit in the old page

    else:
        rsibling = write_new_page(table_name, is_table, is_interior, new_rightmost_page, parent_num)
        update_page_header(file_name, split_page_num, rsibling_rchild=rightmost_child_page_left)

        #delete all the copied cells from left sibling
        page_delete_cells_on_and_after(file_name, split_page_num, 0)

        for order_cell in insert_order[:middle_cell+1]: #Copy cells into left child
            update_page_header(file_name, order_cell['left_child_page'], parent=split_page_num) #update child to point header to lchild
        page_insert_cell(file_name, split_page_num, cells2copy[:middle_cell])

        for order_cell in insert_order[middle_cell+1:]: #Copy cells into right child   #splitting the interior nodes,  the middle cell is not redundant in children
            update_page_header(file_name, order_cell['left_child_page'], parent=rsibling)
        update_page_header(file_name, values['rightmost_child_page'], parent=rsibling)
        page_insert_cell(file_name, rsibling, cells2copy[middle_cell+1:])

        parent_page = pages[parent_num]
        parent_cells = parent_page['cells']


        for i, cell in enumerate(parent_cells)-1:
            if cell['index_value'] >  middle_index:
                parent_index = i
                update_cell_lpointer(file_name, parent_num, i, rsibling)
                break
            elif i==len(parent_cells):
                parent_index = len(parent_cells)
                update_page_header(file_name, parent_num, rsibling_rchild=rsibling)
        middle_cell_binary = bytearray(middle_cell_binary)
        middle_cell_binary[0:4] = struct.pack(endian+'i', split_page_num)

        if parent_page['available_bytes']/PAGE_SIZE<0.5:
            new_parent = index_interior_split_page(file_name, parent_num, middle_cell_binary, rsibling, parent_index)

        else:
            try:
                index_insert_cell_in_page(file_name, parent_num, middle_cell_binary, parent_index)
            except:
                new_parent = index_interior_split_page(file_name, parent_num, middle_cell_binary, rsibling, parent_index)


        return rsibling


def index_leaf_split_page(file_name, split_page_num, cell2insert, index_dtype, cell_index):
    file_bytes = load_file(file_name)
    values = read_cells_in_page(file_bytes, split_page_num)

    table_name = file_name[:-4]
    parent_num = values['parent_page']
    is_interior = not values['is_leaf']
    is_table = values['is_table']
    assert(not is_table)

    num_cells = values['num_cells']
    cells = values['cells']
    middle_cell = int((num_cells+1)/2) #have to add one since we havent actually added the cell

    cell2insert=index_read_cell(cell2insert, is_interior)
    insert_order = []
    i=0
    copied=False
    while len(insert_order)!=len(cells)+1:
        if cell_index==i and not copied:
            insert_order.append(cell2insert)
            copied = True
        else:
            insert_order.append(cells[i])
            i+=1


    middle_index = insert_order[middle_cell]['index_value']
    cells2copy = [cell['cell_binary'] for cell in insert_order]
    middle_cell_binary = cells2copy[middle_cell]

    if parent_num==-1: #IS ROOT ->create two children
        rchild_num = write_new_page(table_name, is_table, False, -1, split_page_num)
        lchild_num = write_new_page(table_name, is_table, False, rchild_num, split_page_num)
        page_insert_cell(file_name, lchild_num, cells2copy[:middle_cell])
        page_insert_cell(file_name, rchild_num, cells2copy[middle_cell+1:])
        page_delete_cells_on_and_after(file_name, split_page_num, 0)
        middle_cell_binary = struct.pack(endian+'i', lchild_num) + middle_cell_binary
        page_insert_cell(file_name, split_page_num, middle_cell_binary)
        update_page_header(file_name, split_page_num, rsibling_rchild=rchild_num, is_interior=True)



    else: #Non-root ->propagate upward
        right_sibling_page = values['right_sibling_page']
        rsibling = write_new_page(table_name, is_table, is_interior, right_sibling_page, parent_num)

        page_delete_cells_on_and_after(file_name, split_page_num, 0)
        page_insert_cell(file_name, split_page_num, cells2copy[:middle_cell])
        update_page_header(file_name, split_page_num, rsibling_rchild=rsibling)
        page_insert_cell(file_name, rsibling, cells2copy[middle_cell+1:])
        middle_cell_binary = struct.pack(endian+'i', split_page_num) + middle_cell_binary

        parent_page = read_cells_in_page(file_bytes, parent_num)
        parent_cells = parent_page['cells']



        for i, cell in enumerate(parent_cells):
            if cell['index_value'] >  middle_index:
                parent_index = i
                update_cell_lpointer(file_name, parent_num, i, rsibling)
                break
            elif i==len(parent_cells)-1:
                parent_index = len(parent_cells)
                update_page_header(file_name, parent_num, rsibling_rchild=rsibling)

        if parent_page['available_bytes']/PAGE_SIZE<0.5:
            new_parent = index_interior_split_page(file_name, parent_num, middle_cell_binary, rsibling,parent_index)
            return None
        else:
            try:
                index_insert_cell_in_page(file_name, parent_num, middle_cell_binary, parent_index)
            except:
                new_parent = index_interior_split_page(file_name, parent_num, middle_cell_binary, rsibling, parent_index)
        try:
            check_valid(file_name)
        except:
            print("splitting",rsibling, split_page_num)
            assert(False)



def delete(table_name, rowid):
    page_num, cell_indx = page_cell_indx_given_index_value(table_name, rowid)
    if cell_indx is None: #no value found
        return None
    else:
        try:
            page_delete_cell(file_name, page_num, cell_indx)
        except:
            table_leaf_merge_page(table_name+'.tbl', next_page, cell)

        for filename in get_indexes(table_name):
            for col in all_col_data:
                if col['data'][1]==file_name[len(table_name)+1:-4]:
                    index_dtype= col['data'][2]
                    index_value= values[col['data'][3]] #index by ord position
            next_page  = index_get_next_page(index_value)
            try:
                index_page_delete_cell(index_dtype, index_value, next_rowid)
            except:
                index_leaf_merge_page(table_name+'.tbl', next_page, cell)
        return None





#########################################################
#TABLE DELETE FUNCTIONS

def table_delete(file_name, rowids):
    pages = read_all_pages_in_file(file_name)
    for rowid in rowids:
        page_num=0
        table_delete_recursion(pages, page_num, rowid)
    page_dict_to_file(file_name, pages)

def delete_from_page_dict(pages, page_num, indx):
    page = pages[page_num]
    page['available_bytes']+=len(page['cells'][indx]['cell_binary']) #cell_binary?
    page['num_cells']-=1
    del page['rowids'][indx]
    del page['cells'][indx]
    return pages

def insert_to_page_dict(pages, page_num, cell, indx):
    page = pages[page_num]
    page['available_bytes']-=len(cell['cell_binary'])
    page['num_cells']+=1
    if indx==0:
        page['rowids'] = [cell['rowid']] + page['rowids']
        page['cells'] = [cell] + page['cells']
    elif indx+1<page['num_cells']:
        page['rowids'] = page['rowids'][:indx] + [cell['rowid']] + page['rowids'][indx:]
        page['cells'] = page['cells'][:indx] + [cell] + page['cells'][indx:]
    else:
        page['rowids'].append(cell['rowid'])
        page['cells'].append(cell)
    return pages

def update_cell_binary(cell_binary, rowid=None, left_child=None):
    cell_binary = bytearray(cell_binary)
    if left_child!=None:
        cell_binary[:4] = struct.pack(endian+'i', left_child)
    if rowid!=None:
        cell_binary[4:8] = struct.pack(endian+'i', rowid)
    return cell_binary

def fix_parent_pointer(pages, parent_page, id2fix, left=True):
    page = pages[parent_page]
    for i, id in enumerate(page['rowids']):
        if left:
            if id > id2fix:
                page['rowids'][i]=id2fix
                page['cells'][i]['cell_binary']=update_cell_binary(page['cells'][i]['cell_binary'], rowid=id2fix)
                break
        else:
            if id > id2fix:
                page['rowids'][i-1]=id2fix
                page['cells'][i-1]['cell_binary']=update_cell_binary(page['cells'][i-1]['cell_binary'], rowid=id2fix)
                break
    return None

def steal_sibling_cell(pages, page_num, left=True):
    page = pages[page_num]
    if left:
        lsib = pages[page['left_sibling_page']]
        cell = lsib['cells'][-1]
        insert_to_page_dict(pages, page_num, cell, 0)
        delete_from_page_dict(pages, lsib['page_number'], lsib['num_cells']-1)
        return cell['rowid']
    else:
        rsib = pages[page['right_sibling_page']]
        cell = rsib['cells'][0]
        insert_to_page_dict(pages, page_num, cell, page['num_cells'])
        delete_from_page_dict(pages, rsib['page_number'], 0)
        return cell['rowid']

def try_borrowing(pages, borrower_page):
    page = pages[borrower_page]
    parent = page['parent_page']
    if 'left_sibling_page' in page:
        lsib = pages[page['left_sibling_page']]
        steal_size = len(lsib['cells'][-1]['cell_binary'])
        if lsib['num_cells']>2:
            id2fix=steal_sibling_cell(pages, borrower_page, left=True)
            fix_parent_pointer(pages, page['parent_page'], id2fix, left=True)
            return None #done
        else:
            return 'left' #MERGE
    elif page['right_sibling_page']!=-1 and pages[page['right_sibling_page']]['parent_page']==parent:
        rsib = pages[page['right_sibling_page']]
        steal_size = len(rsib['cells'][0]['cell_binary'])
        if rsib['num_cells']>2:
            id2fix=steal_sibling_cell(pages, borrower_page, left=False)
            fix_parent_pointer(pages, page['parent_page'], id2fix+1, left=False)
            return None #done
        else:
            return 'right' #MERGE
    else:
        return 'right'

def delete_dict(pages, page_num, rowid):
    page = pages[page_num]
    if rowid in page['rowids']:
        indx = page['rowids'].index(rowid)
        pages = delete_from_page_dict(pages, page_num, indx)

        if page['num_cells']<2 and page['parent_page']!=-1:
            return try_borrowing(pages, page_num) #returns 'left' pr 'right'
        else:
            return None #dont need to borrow not empty
    else:
        return None

def merge_children(pages, page_num, child_page_num, left=True):
    page = pages[page_num]
    pdb.set_trace()
    page_children = [i['left_child_page'] for i in pages[page_num]['cells']]
    child_page = pages[child_page_num]

    if left:
        lsib = pages[child_page['left_sibling_page']]
        id2del = page['cells'][page_children.index(lsib['page_number'])]['rowid']
        if not lsib['is_leaf']: #update parent of downstream
            for cell in lsib['cells']:
                pages[cell['left_child_page']]['parent_page'] = child_page_num
            pages[lsib['rightmost_child_page']]['parent_page'] = child_page_num
            middle_cell = table_create_cell([], [], True, left_child_page=lsib['rightmost_child_page'],  rowid=id2del)
            lsib['cells'] = lsib['cells']+middle_cell
            lsib['rowids'] = lsib['rowids']+id2del

        child_page['cells'] = lsib['cells']+child_page['cells']
        child_page['rowids'] = lsib['rowids']+child_page['rowids']
        child_page['num_cells'] = lsib['num_cells']+child_page['num_cells']
        child_page['available_bytes'] = lsib['available_bytes']-(PAGE_SIZE - child_page['available_bytes'])
        child_page['right_sibling_page'] = lsib['right_sibling_page']
        pages[child_page['right_sibling_page']]['left_sibling_page'] = child_page_num
        lsib['deleted'] = True

        return id2del

    else:
        rsib = pages[child_page['right_sibling_page']]
        id2del = page['cells'][page_children.index(child_page_num)]['rowid']
        if not rsib['is_leaf']: #update parent of downstream
            for cell in rsib['cells']:
                pages[cell['left_child_page']]['parent_page'] = child_page_num
            pages[rsib['rightmost_child_page']]['parent_page'] = child_page_num
            middle_cell = table_create_cell([], [], True, left_child_page=rsib['rightmost_child_page'],  rowid=id2del)
            child_page['cells'] = child_page['cells']+middle_cell
            child_page['rowids'] = child_page['rowids']+id2del

        child_page['cells'] = child_page['cells'] + rsib['cells']
        child_page['rowids'] = child_page['rowids'] + rsib['rowids']
        child_page['num_cells'] = child_page['num_cells'] + rsib['num_cells']
        child_page['available_bytes'] = rsib['available_bytes']-(PAGE_SIZE - child_page['available_bytes'])
        child_page['left_sibling_page'] = rsib['left_sibling_page']
        pages[child_page['left_sibling_page']]['right_sibling_page'] = child_page_num
        rsib['deleted'] = True
        return id2del

    if page['parent_page']==-1 and page['num_cells']==1:
        child_page['parent_page'] = -1

def table_delete_recursion(pages, page_num, rowid):
    page = pages[page_num]
    if page['is_leaf']:

        return delete_dict(pages, page_num, rowid) #returns 'left' pr 'right' or None
    else:

        for  i, cell in enumerate(page['cells']):
            if cell['rowid'] > rowid:
                child_page = cell['left_child_page']
                merge_child = table_delete_recursion(pages, child_page, rowid)
                break
            elif i+1 == len(page['cells']):
                child_page = page['rightmost_child_page']
                merge_child = table_delete_recursion(pages, child_page, rowid)
                break
            else:
                continue

        if merge_child is None: #all clear no merges necessary
            return None

        elif merge_child=='left': #merge left
            pdb.set_trace()
            if page['parent_page']==-1 and page['num_cells']==1: #root condition
                merge_children(pages, page_num, child_page, left=True)
                page['deleted'] = True #remove the root
                page['parent_page'] = -13 #so ican find the root later
            else:
                id2del = merge_children(pages, page_num, child_page, left=True)
                return delete_dict(pages, page_num, id2del)

        elif merge_child=='right': #parent of a leaf node
            if page['parent_page']==-1 and page['num_cells']==1: #root condition
                merge_children(pages, page_num, child_page, left=False)
                page['deleted'] = True #remove the root
                page['parent_page'] = -13 #so ican find the root later
            else:
                id2del = merge_children(pages, page_num, child_page, left=False)
                return delete_dict(pages, page_num, id2del)
        else:
            assert(False)

def copy_page(file_name, pages, page_number, parent, i=None):
    page = pages[page_number]
    table_name = file_name[:-4]
    is_table = page['is_table']
    is_interior = not page['is_leaf']
    if is_interior:
        rsibling_rchild = page['rightmost_child_page']
    else:
        rsibling_rchild = page['right_sibling_page']
    if i==None: #need to keep track of fileoffset (first page is initialize already)
        i=0

    write_new_page(table_name, is_table, is_interior, rsibling_rchild, parent)

    if page['num_cells']>1:
        cells2insert = [j["cell_binary"] for j in page['cells']]
    else:
        cells2insert = page['cells'][0]['cell_binary']

    page_insert_cell(file_name, i, cells2insert)
    i+=1
    if is_interior:
        cells = sorted(page['cells'], key=lambda x: x['left_child_page'])
        children = [j["left_child_page"] for j in cells]+[page['rightmost_child_page']]
        for child in children:
            if 'deleted' not in pages[child]:
                copy_page(file_name, pages, child, page_number, i=i)
                i+=1
            else:
                continue


def page_dict_to_file(file_name, pages):
    table_name = file_name[:-4]
    if file_name[-4:]=='.tbl':
        is_table = True
    else:
        is_table=False
    for page in pages:
        if page['parent_page']==-1:
            root_node = page['page_number']
            break

    os.remove(file_name)
    with open(file_name, 'w+') as f:
        pass
    copy_page(file_name, pages, root_node, -1)
    return None



MIN_FILL_RATIO = 0.2
MAX_FILL_RATIO = 0.7

###############################################################################################




def get_predecessor(pages, page_num):
    page = pages[page_num]
    while not page['is_leaf']:
        page_num = page['rightmost_child_page']
        page = pages[page_num]
    return page['cells'][-1], page_num









def update(table_name, new_values):
    return None







#############################################################################
#TO DO









def show_tables():
    """
    This can be implemented by querying dabisbase_tables
    """
    print("ALL TABLES")
    return None


def check_values_match_schema(values,schema):
    """Save coding time, assume will be correct"""
    success = True
    return True


##############################################################################



#########################################################################
#CLI FUNCTIONS



#########################################################################
# DDL FUNCTION




def extract_definitions(token_list):
    '''
    Subordinate function for create table to get column names and their definitions
    '''
    # assumes that token_list is a parenthesis
    definitions = []
    tmp = []
    # grab the first token, ignoring whitespace. idx=1 to skip open (
    tidx, token = token_list.token_next(1)
    while token and not token.match(sqlparse.tokens.Punctuation, ')'):
        tmp.append(token)
        # grab the next token, this times including whitespace
        tidx, token = token_list.token_next(tidx, skip_ws=False)
        # split on ",", except when on end of statement
        if token and token.match(sqlparse.tokens.Punctuation, ','):
            definitions.append(tmp)
            tmp = []
            tidx, token = token_list.token_next(tidx)
    if tmp and isinstance(tmp[0], sqlparse.sql.Identifier):
        definitions.append(tmp)
    return definitions


def parse_create_table(SQL):
    """
    Parses the raw, lower-cased input from the CLI controller. Will identify table name,
    column names, data types, and constraints. Will also check for syntax errors.
    Also check that table_name is all characters (no punctuation spaces...)

    Parameters:
    command (string):  lower-case string from CLI.
    (ex. "CREATE TABLE table_name (
             column_name1 data_type1 [NOT NULL][UNIQUE],
             column_name2 data_type2 [NOT NULL][UNIQUE],
            );""  )

    Returns:
    tuple: (table_name, column_list, definition_list)

    table_name: str
    column_list: list of column objects.

    SQL = \"""CREATE TABLE foo (
         id integer primary key,
         title varchar(200) not null,
         description text);\"""
    """

    if re.match("(?i)create (?i)table [a-zA-Z]+\s\(\s?\n?", SQL):
        if SQL.endswith(');'):
            print("Valid statement")
    else:
        print("Invalid statement")

    parsed = sqlparse.parse(SQL)[0]
    table_name = str(parsed[4])
    _, par = parsed.token_next_by(i=sqlparse.sql.Parenthesis)
    columns = extract_definitions(par)
    col_list = []
    definition_list = []
    for column in columns:
        definitions = ''.join(str(t) for t in column).split(',')
        for definition in definitions:
            d = ' '.join(str(t) for t in definition.split())
            col_list.append(definition.split()[0])
            definition_list.append(d)

    d = {}
    d[table_name] = {}
    for col, definition in zip(col_list, definition_list):
        d[table_name][col] = definition

    return d


def parse_drop_table(command):
    """
    Parses the raw, lower-cased input from the CLI controller. Will identify table name,
    Will also check for syntax errors. Throw error if

    Parameters:
    command (string):  lower-case string from CLI. (ex. "drop table table_name;"" )

    Returns:
    """
    ## check if the drop statement is correct or not
    ## statement must compulsarily end with semicolon
    query_match = "(?i)DROP\s+(.*?)\s*(?i)TABLE\s+[a-zA-Z]+\;"
    if re.match(query_match, command):
        stmt = sqlparse.parse(command)[0]
        tablename = str(stmt.tokens[-2])
    else:
        print("Enter correct query")
    return tablename



def create_index(command):
    print("create index \'{}\'".format(command))
    return None


############################################################################
#DML FUNCTIONS

def insert_into(command):
    '''
    Assuming values are being set along the correct order of columns
    '''
    print("Insert into \'{}\'".format(command))
    query_match = "insert into\s+(.*?)\s*((?i)values\s(.*?)\s*)?;"
    if re.match(query_match, command):
        stmt = sqlparse.parse(command)[0]
        table_name = str(stmt.tokens[4])
        values = str(stmt.tokens[-2])
        values = re.sub("\s", "", re.split(';',re.sub("(?i)values","",values))[0])
        print(values,"\t",table_name)
    else:
        print("Enter correct query")

def delete_from(command):
    print("delete from \'{}\'".format(command))
    ## check if the update statement is correct or not
    query_match = "delete\s+(.*?)\s*(?i)from\s+(.*?)\s*((?i)where\s(.*?)\s*)?;"
    if re.match(query_match, command):
        stmt = sqlparse.parse(command)[0]
        where_clause = str(stmt.tokens[-1])
        where_clause = re.sub("\s", "", re.split(';',re.sub("(?i)where","",where_clause))[0])
        where_clause = re.split('=|>|<|>=|<=|\s',where_clause)
        tablename = str(stmt.tokens[-3]).split(",")
        print(where_clause,"\t",tablename)
    else:
        print("Enter correct query")


def update(command):
    print("update \'{}\'".format(command))
    ## check if the update statement is correct or not
    query_match = "(?i)update\s+(.*?)\s*(?i)set\s+(.*?)\s*((?i)where\s(.*?)\s*)?;"
    if re.match(query_match, command):
        stmt = sqlparse.parse(command)[0]
        where_clause = str(stmt.tokens[-1])
        where_clause = re.sub("\s", "", re.split(';',re.sub("(?i)where","",where_clause))[0])
        where_clause = re.split('=|>|<|>=|<=|\s',where_clause)
        set_col = itemgetter(*[0,-1])(re.split('=|\s',str(stmt.tokens[-3])))
        tablename = str(stmt.tokens[2])
        print(where_clause,"\t", tablename,"\t",set_col)
        ## perform select logic
    else:
        print("Enter correct query")

##########################################################################
#DQL FUNCTIONS

def query(command: str):
    '''
    command : Select statement eg. select a.a,b.b,c from a,b where a.a = b.a;
    return : None
    '''
    print("User wants to query {}".format(command))
    ## check if the select statement is correct or not
    query_match = "select\s+(.*?)\s*(?i)from\s+(.*?)\s*((?i)where\s(.*?)\s*)?;"
    if re.match(query_match, command):
        stmt = sqlparse.parse(command)[0]
        where_clause = str(stmt.tokens[-1])
        where_clause = re.sub("\s", "", re.split(';',re.sub("(?i)where","",where_clause))[0])
        where_clause = re.split('=|>|<|>=|<=|\s',where_clause)
        tablename = str(stmt.tokens[-3]).split(",")[0]
        columns = str(stmt.tokens[2]).split(",")
#         print(where_clause,"\t",tablename,"\t",columns)
        return where_clause, tablename, columns
    else:

        return -1,-1,-1


def select_from(SQL):

    where_condition, table_name, columns =  query(SQL)
    print(table_name, where_condition[0], where_condition[1])

#     column_list = get_column_names_from_catalog(table_name)

#     index = column_list.index(where_condition[0])

    if where_condition == -1:
        print("Enter correct query")

    flag = False
    for node in read_all_pages_in_file("davisbase_columns.tbl"):
        if node['is_leaf'] :
            for cell in node['cells']:
                data = cell['data']

                if data[0] == table_name and data[1] == where_condition[0]:
                    if data[2] == 'INT' and data[3] == int(where_condition[1]):
                        print(cell)
                        flag = True
                        break
                    elif data[2] == 'TEXT' and data[3] == str(where_condition[1]):
                        print(cell)
                        flag = True
                        break
        if flag:
            break


def check_valid(file_name, pages=None, page_num=0, is_table=None):
    if page_num==0:
        pages =read_all_pages_in_file(file_name)
        is_table = pages[0]['is_table']
    page = pages[page_num]
    if page['parent_page']!=-1:
        #parent has cell in this
        parent_children = [i['left_child_page'] for i in pages[page['parent_page']]['cells']]
        parent_children.append(pages[page['parent_page']]['rightmost_child_page'])
        try:
            assert(page_num in parent_children)
        except:
            print("parent", pages[page['parent_page']], "does not point to ", page_num)
            assert(False)

    if  not page['is_leaf']:
        if not is_table:
            key = 'index_value'
        else:
            key='rowid'
        for i, cell in enumerate(page['cells']):
            if i==0:
                continue
            else:
                c_max = max([i[key] for i in pages[cell['left_child_page']]['cells']])
                c_min = min([i[key] for i in pages[cell['left_child_page']]['cells']])
                try:
                    assert((page['cells'][i-1][key]<=c_min)and(c_max<cell[key]))
                except:
                    print("page_num incorrect ordering", page_num, 'child',cell['left_child_page'])
                    assert(False)

    if not page['is_leaf']:
        for cell in page['cells']:
            try:
                check_valid(file_name, pages=pages, page_num=cell['left_child_page'], is_table=is_table)
            except:
                assert(False)
        try:
            check_valid(file_name, pages=pages, page_num=page['rightmost_child_page'], is_table=is_table)
        except:
                assert(False)
    else:
        return


#############################################################################
PAGE_SIZE = 512
MIN_FILL_RATIO = 0.2
MAX_FILL_RATIO = 0.7
BYTE_ORDER = sys.byteorder
if BYTE_ORDER=='big':
    endian = '>'
elif BYTE_ORDER=='little':
    endian = '<'

if __name__== "__main__":
    init()
    print("DavisBase version 0.00.1 2019-11-21")
    print("Enter \"help;\" for usage hints.")
    exit_command = False
    command = ''
    while not exit_command:
        line = input("davisbase> ").lower()
        if len(command)==0:
            command+=line
        else:
            command+=" "+line
        out = check_input(command)
        if type(out)==bool:
            exit_command = True
        elif out==None:
            command=''
        else:
            continue
