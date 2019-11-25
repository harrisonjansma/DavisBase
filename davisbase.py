import os
import struct
import sys
from datetime import datetime, time


############################################################################

def check_input(command):
    if len(command)==0:
        pass

    elif command[-1]!=";":
        print("All commands end with semicolon.")

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
        insert_into(command)

    elif command[0:len("update ")] == "update ":
        insert_into(command)

    elif command[0:len("select ")] == "select ":
        query(command)

    elif command == "exit;":
        return True

    elif command == "test;":
        return True

    else:
        print("Command \"{}\" not recognized".format(command))

####################################################################
#TABLE FUNCTIONS for Harrison to complete

def initialize_file(table_name, is_table):
    """Creates a file and writes the first, empty page (root)"""
    if is_table:
        file_type = ".tbl"
    else:
        file_type = '.ndx'

    if os.path.exists(table_name+file_type):
        os.remove(table_name+file_type)

    with open(table_name+file_type, 'w+') as f:
        pass
    write_new_page(table_name, is_table, False, 0, -1)
    return None



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

    with open(table_name + file_type, 'wb') as f:
        f.seek(2,0) #seek end of file
        f.write(struct.pack(str(PAGE_SIZE-2)+'x')) #write PAGE_SIZE placeholder bytes
        #Header
        f.seek(0, file_size)
        #first byte says what kind of page it is
        if is_table and is_interior:
            f.write(b'\x05')
        elif is_table and is_leaf:
            f.write(b'\x0d')
        elif is_index and is_interior:
            f.write(b'\x02')
        elif is_index and is_leaf:
            f.write(b'\x0a')
        else:
             raise ValueError("Page must be table/index")
        f.write(b'\x00') #unused
        f.write(struct.pack(endian+'hhii2x', 0, PAGE_SIZE, rsibling_rchild, parent))

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
    print(d)
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
    print(j)
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
        res = {'left_child_rowid':cell_header[0],'rowid':cell_header[1]}
    elif is_leaf:
        cell_header = struct.unpack(endian+'hi', cell[0:6])
        payload = cell[6:]
        values = table_payload_to_values(payload)
        res = {'bytes_in_payload':cell_header[0], 'num_columns':cell_header[1],"data":values}
    else:
        print("error in read cell")
    res["cell_size"]=len(cell)
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
        result["lchild"]=cell_header[0]
        payload = cell[6:]
    else:
        cell_header = struct.unpack(endian+'h', cell[0:2])
        payload = cell[2:]

    indx_value, rowid_list = index_payload_to_values(payload)
    result["index_value"]=indx_value
    result["assoc_rowids"]=rowid_list
    result["cell_size"]=len(cell)
    return result


def load_file(file_name):
    """
    loads the table/index file returns the bytestring for the entire file (reduce number of read/writes)

    Parameters:
    file (byte-string): ex 'taco.tbl'
    page_num (int): 1

    Returns:
    page (bytestring):

    """
    with open(file_name, 'rb') as f:
        return f.read()


def load_page(file_name, page_num):
    """
    loads the page of from the table/index file PAGE NUMBER STARTS AT ZERO, will only load one pa

    Parameters:
    file_name (string): ex 'taco.tbl'
    page_num (int): 1

    Returns:
    page (bytestring):

    """
    file_offset = page_num*PAGE_SIZE
    with open(file_name, 'rb') as f:
        f.seek(0, file_offset)
        page = f.read(PAGE_SIZE)
    return page





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
    with open(file_name, 'wb') as f:
        f.seek(0, file_offset)
        page = f.write(new_page_data)
    return None


def page_available_bytes(file_name, page_num):
    page = load_page(file_name, page_num)
    num_cells = struct.unpack(endian+'h', page[2:4])[0]
    bytes_from_top = 16+(2*num_cells)
    cell_content_start =struct.unpack(endian+'h', page[4:6])[0]
    return  cell_content_start - bytes_from_top




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
    page = load_page(file_name, page_num)
    assert(len(cell)<page_available_bytes(file_name, page_num)) #CHECK IF PAGE FULL
    num_cells = struct.unpack(endian+'h', page[2:4])[0]
    bytes_from_top = 16+(2*num_cells)
    bytes_from_bot =struct.unpack(endian+'h', page[4:6])[0]
    new_start_index = bytes_from_bot - len(cell)
    new_page_data = bytearray(page)
    #insert cell data
    new_page_data[new_start_index:bytes_from_bot] = cell
    #add to 2byte cell array
    new_page_data[bytes_from_top:bytes_from_top+2] = struct.pack(endian+'h', new_start_index)
    #update start of cell content
    new_page_data[4:6] = struct.pack(endian+'h', new_start_index)
    #update num_cells
    new_page_data[2:4] = struct.pack(endian+'h', num_cells+1)
    save_page(file_name, page_num, new_page_data)
    assert(len(new_page_data)==PAGE_SIZE)
    return None



def shift_page_content(page, top_indx, bot_indx, shift_step, up=True):
    assert(bot_indx+shift_step<=PAGE_SIZE)
    assert(top_indx-shift_step>=0)
    if shift_step==0:
        return page

    copy = page[top_indx:bot_indx]
    if up:
        new_top_indx = top_indx - shift_step
        new_bot_indx = bot_indx - shift_step
        page[new_top_indx:new_bot_indx]=copy
        page[new_bot_indx:bot_indx]=b'\x00'*shift_step
        return page
    else:
        new_top_indx = top_indx + shift_step
        new_bot_indx = bot_indx + shift_step
        page[new_top_indx:new_bot_indx]=copy
        page[top_indx:new_top_indx]=b'\x00'*shift_step
        return page


def update_array_values(page, first_array_loc_to_change, num_cells, shift_step, up=True):
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
    temp_page = load_page(file_name, page_num)
    page = bytearray(temp_page)
    num_cells = struct.unpack(endian+'h', page[2:4])[0]
    assert(cell_indx<=num_cells-1)#index starts at 0
    assert(num_cells>=1) #delete CAN empty a page
    assert(cell_indx>=0)

    cell_content_area_start = struct.unpack(endian+'h', page[4:6])[0]
    end_of_array = 16+2*num_cells
    array_idx_top = 16+2*idx
    array_idx_bot = 16+2*(idx+1)

    #if cell is the last cell (but not if theres only one cell left)
    if (idx==num_cells-1) & (idx!=0):
        cell_top_loc = cell_content_area_start
        cell_bot_loc = struct.unpack(endian+'h',page[16+2*(idx-1):16+2*(idx)])[0]

        cell_2_delete = page[cell_content_area_start:cell_bot_loc]
        dis2replace= len(cell_2_delete)
        #overwrite the cell2delete
        page[cell_top_loc:cell_bot_loc]=b'\x00'*dis2replace
        #change the cell_start area in header
        page[4:6] = struct.pack(endian+'h', cell_content_area_start+dis2replace)
        #delete last entri in cell array
        page[16+2*(num_cells-1):16+2*(num_cells)]=b'\x00'*2
        #update the number of cells
        page[2:4] = struct.pack(endian+'h', num_cells-1)

    else:
        cell_top_loc = struct.unpack(endian+'h',page[array_idx_top:array_idx_bot])[0]
        if idx==0: #if cell is first on the page (bottom)
            cell_bot_loc = PAGE_SIZE
        else:
            cell_bot_loc = struct.unpack(endian+'h',page[array_idx_top-2:array_idx_top])[0]

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
    assert(len(new_page_data)==PAGE_SIZE) #ensure page is same size
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
    temp_page = load_page(file_name, page_num)
    page = bytearray(temp_page)

    num_cells = struct.unpack(endian+'h', page[2:4])[0]
    assert(cell_indx<=num_cells-1)#index starts at 0
    assert(num_cells!=0) #delete CAN empty a page
    assert(cell_indx>=0)

    cell_content_area_start = struct.unpack(endian+'h', page[4:6])[0]
    end_of_array = 16+2*num_cells
    array_idx_top = 16+2*idx
    array_idx_bot = 16+2*(idx+1)
    available_bytes = page_available_bytes(file_name, page_num)
    cell_top_idx = struct.unpack(endian+'h',page[16+2*idx:16+2*(idx+1)])[0]
    if idx==0: #if cell is first on the page (bottom)
        cell_bot_idx = PAGE_SIZE
    else:
        cell_bot_idx = struct.unpack(endian+'h',page[16+2*(idx-1):16+2*(idx)])[0]


    cell_2_update = page[cell_top_idx:cell_bot_idx]
    if len(cell_2_update)==len(cell):
        page[cell_top_idx:cell_bot_idx] = cell

    elif len(cell_2_update)<len(cell): #need to shift cell_content up
        dis2move =  len(cell) - len(cell_2_update)
        assert(dis2move<=available_bytes)   #NEED TO SPLIT

        page = shift_page_content(page, cell_content_area_start, cell_top_idx, dis2move, up=True)
        #since we just shifted every cell, every value in cell_array is off
        page = update_array_values(page, idx, num_cells, dis2move, up=True)
        #change cell content area start
        page[4:6] = struct.pack(endian+'h', cell_content_area_start-dis2move)

        #insert updated cell
        page[cell_top_idx-dis2move:cell_bot_idx] = cell



    else: #need to shift cell_content up
        dis2move =  len(cell_2_update) - len(cell)
        page = shift_page_content(page, cell_content_area_start, cell_top_idx, dis2move, up=False)
        #since we just shifted every cell, every value in cell_array is off
        page = update_array_values(page, idx, num_cells, dis2move, up=True)
        #change cell content area start
        page[4:6] = struct.pack(endian+'h', cell_content_area_start+dis2move)

        page[cell_top_idx+dis2move:cell_bot_idx] = cell

    save_page(file_name, page_num, page)
    assert(len(new_page_data)==PAGE_SIZE) #ensure page is same size
    return None


def split_page():
    return None

def merge_pages():
    return None

def read_cells_in_page():
    return None

def find_value_page():
    return None




#########################################################################################



def insert_cell_index(table_name, page_num, schema, values):
    return

def delete_cell_index(table_name, page_num, schema, values):
    return

def delete_cell_index(table_name, page_num, schema, values):
    return

##############################################################################

class Table:
    def __init__(table_name, create=False):
        if create:
            #do this last
            if catalog.check_table_exists(table_name):
                create_file() #todo
                update_catalogue() #leave these for later
            else:
                printe("error")

        self.table_name = table_name
        self.file_name = table_name+'.tbl'
        self.column_list = catalog.get_columnlist(table_name) #will use this in inserts/update to check constraints
        self.num_pages = 0 #get file size, divide by page_size
        self.root_page = Table_Page(table_name, 0)
        self.indexed_columns = [col if col.primary_key==True else '' for col in self.column_list]
        self.schema =['list of datatypes in ints (from documentation)']
        self.next_available_rowid = 0


    def get_cell_page(order_key):
        return "cell_page_no"

    def insert(values):
        """values would be a list of length self.columns, NULL represented as None"""
        #get dtypes
        #check dtypes match
        #get_next_rowid
        #check constraints
        #create_cell
        #find_page
        #insert_cell2tablepage
        return "success_flag"

    def update(order_key, values):
        """update a single cell"""
        #get_page
        #cell

    def delete(order_key, values):
        """delete a single cell from a page/ (will be used in a loop once we figure out queries)"""



class Table_Page:
    def __init__(table_name, page_num):
        self.page_number = page_num
        self.table_name = table_name
        self.parent = 0
        self.is_leaf = True
        self.sibling = 0
        self.child = 0
        self.num_cells = 0
    def check_pk_indata():
        return False


class Index:
    def __init__(index_name ,assoc_table, create=False):
        if create:
            #do this last
            if catalog.check_table_exists(table_name):
                create_file() #todo
                update_catalogue() #leave these for later
            else:
                printe("error")

        self.assoc_table = assoc_table
        self.file_name = index_name+'.tbl'
        self.num_pages = 0 #get file size, divide by page_size
        self.root_page = Page(index_name, 0)
        self.next_available_rowid = 0


    def get_cell_page(order_key):
        return "cell_page_no"

    def insert(values):
        """values would be a list of length self.columns, NULL represented as None"""
        #get dtypes
        #check dtypes match
        #get_next_rowid
        #check constraints
        #create_cell
        #find_page
        #insert_cell2tablepage
        return "success_flag"

    def update(order_key, values):
        """update a single cell"""
        #get_page
        #cell

    def delete(order_key, values):
        """delete a single cell from a page/ (will be used in a loop once we figure out queries)"""


class Index_Page:
    def __init__(table_name, page_num):
        self.page_number = page_num
        self.table_name = table_name
        self.parent = 0
        self.is_leaf = True
        self.sibling = 0
        self.child = 0
        self.num_cells = 0
    def check_pk_indata():
        return False


class Column:
    #I want to use this object rather than a dict because the code will be cleaner
    #later on, we can just iterate through the columns
    def __init__(column_name, dtype, not_null, unique, primary_key):
        self.column_name = column_name
        self.dtype = dtype
        self.not_null = not_null
        self.unique = unique
        self.primary_key = pk

class Catalog:
    def __init__():
        return


#########################################################################
#CLI FUNCTIONS

def init():
    if os.path.exists('davisbase_tables.tbl'):
        pass
    else:
        initialize_file('davisbase_tables', True)

    if os.path.exists('davisbase_columns.tbl'):
        pass
    else:
        initialize_file('davisbase_columns', True)

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

#########################################################################
# DDL FUNCTION

def show_tables():
    """
    This can be implemented by querying dabisbase_tables
    """
    print("ALL TABLES")
    return None

def create_table(command):
    table_name, column_list = parse_create_table(command)
    with open(table_name+'.db', 'w+') as f:
        pass


    return None



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
            print('NAME: {name} DEFINITION: {definition}'.format(name=definition.split()[0],
                                                                 definition=d))
            col_list.append(definition.split()[0])
            definition_list.append(d)

    ## table name and two lists columns and definitions
    return (table_name,col_list, definition_list)



def drop_table(command):
    table_name = parse_drop_table(command)
    if check_table_exists(table_name):
        success = delete_all_table_data(table_name)
        if not success:
            print("temporary error")
    else:
        print("Table \"{}\" does note exist.".format(table_name))

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

def check_table_exists(table_name):
    """
    Checks if the table exists in davisbase_tables.tbl

    Returns:
    bool: table_exists
    """
    return False


def delete_all_table_data(table_name):
    """
    Deletes table_name.tbl, check if index exists (if so, delete index), update metadata remove all cells related to table_name

    Returns:
    bool: success_flag
    """
    return False


def catalog_add_table(dictionary, rowid):
    """
    dictionary = {
    'table_name':{
        "column1":{
            'data_type':"int",
            'ordinal_position':1,
            'is_nullable':'YES',
            }
        }
    }
    """
    table = list(dictionary.keys())
    assert(len(table)==1)
    table_name = table[0]


    davisbase_tables_schema = ['text']
    davisbase_columns_schema = ['text', 'text', 'text', 'int', 'text']




def create_index(command):
    print("create index \'{}\'".format(command))
    return None

############################################################################
#DML FUNCTIONS

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
        print(where_clause)
        tablename = str(stmt.tokens[-3]).split(",")
        columns = str(stmt.tokens[2]).split(",")
        print(where_clause,"\t",tablename,"\t",columns)
    else:
        print("Enter correct query")



#############################################################################
PAGE_SIZE = 512
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
    while not exit_command:
        command = input("davisbase> ").lower()
        exit_command = check_input(command)
