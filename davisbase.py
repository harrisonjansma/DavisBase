import os
import struct
import sys

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
    if is_table:
        file_type = ".tbl"
    else:
        file_type = '.ndx'

    if os.path.exists(table_name+file_type):
        print("Table {} already exists.".format(table_name))
    else:
        with open(table_name+file_type, 'w+') as f:
            pass
    write_new_page(table_name, is_table, False, 0, 4294967295)
    return None



def write_new_page(table_name, is_table, is_interior, rsibling_rchild, parent):
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
        f.write(struct.pack('x'*PAGE_SIZE)) #write PAGE_SIZE placeholder bytes

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
        f.write(struct.pack(endian+'hhiixx', 0, PAGE_SIZE-1, rsibling_rchild, parent))



def dtype_to_int(dtype):
    dtype = dtype.lower()
    mapping = {
    "null":0,
    "tinyint":1,
    "smallint":2,
    "int":3,
    "bigint":4,
    "long":4,
    'float':5,
    "double":6,
    "year":8,
    "time":9,
    "datetime":10,
    "date":11,
    "text":12}
    return mapping[dtype]

def schema_to_formatstring(schema, value_list):
    int2packstring={
    0:'x',
    1:'b',
    2:'h',
    3:'i',
    4:'q',
    5:'f',
    6:'d',
    8:'b',
    9:'i',
    10:'Q',
    11:'Q'
    }
    dtypes = [dtype_to_int(dt) for dt in schema]
    format_string = ''
    for i, dt in enumerate(dtypes):
        #check for nulls
        if value_list[i] == None:
            dtypes[i] = 0
            dt=0
        if dt in int2packstring:
            format_string+=int2packstring[dt]
        #look for text
        elif dt==12:
            len_text = len(value_list[i])
            dtypes[i] = dt+len_text
            value_list[i] = value_list[i].encode('ascii')
            format_string = format_string + str(len_text) + 's'
    for i in value_list:
        try:
            value_list.remove(None)
        except:
            break
    return dtypes, format_string, value_list


def create_cell_table(schema, value_list, is_interior, left_child_page=None,  rowid=None):
    assert(type(schema)==list)
    assert(type(value_list)==list)
    assert(type(is_interior)==bool)
    is_leaf = not is_interior
    dtypes, format_string, value_list = schema_to_formatstring(schema, value_list)
    payload = bytes([len(dtypes)])+bytes(dtypes)+struct.pack(format_string, *value_list)

    if  is_interior:
        assert(left_child_page != None)
        assert(rowid != None)
        cell_header = struct.pack(endian+'ii', left_child_page, rowid)
    elif is_leaf:
        assert(rowid != None)
        cell_header = struct.pack(endian+'hi', len(payload), rowid)
    else:
         raise ValueError("Error in cell creation")
    return cell_header + payload


def read_cell(cell, is_table, is_interior):
    is_leaf = not is_interior
    is_index = not is_table

    if  is_table and is_interior:
        cell_header = struct.unpack(endian+'ii', cell[0:8])
    elif is_table and is_leaf:
        cell_header = struct.unpack(endian+'hi', cell[0:6])
    elif is_index and is_interior:
        cell_header = struct.unpack(endian+'ih', cell[0:6])
    elif is_index and is_leaf:
        cell_header = struct.unpack(endian+'h', cell[0:2])
    else:
        print("error in read cell")

    if is_table:
        cell_body =


def create_cell_index(schema, value_list, is_table, is_interior, left_child_page=None, bytes_in_payload=None, rowid=None):
    assert(type(is_table)==bool)
    assert(type(is_interior)==bool)
    is_leaf = not is_interior
    is_index = not is_table


    if is_index and is_interior:
        assert(left_child_page != None)
        assert(rowid != None)
        cell_header = struct.pack(endian+'ih', left_child_page, bytes_in_payload)

    elif is_index and is_leaf:
        assert(bytes_in_payload != None)
        cell_header = struct.pack(endian+'h', bytes_in_payload)
    else:
         raise ValueError("Page must be either table")

    if is_table:
        payload = bytes([len(schema)])+[i for i in schema]+struct.pack('', datavalues))
    else:
        payload = bytes([num_assoc_rowids, indx_dtype]+[i for i in column_dtypes]+struct.pack('', indx_values, rowids))

    return cell_header + payload


def update_page_header(table_name, is_table, page_no, cell_size, insert=True):
    return


#########################################################################################

def insert_cell_table(table_name, page_num, schema, values):
    assert(type(table)==bool)
    assert(type(interior)==bool)
    leaf = not interior
    index = not table

    file_offset = page_number * PAGE_SIZE
    with open(table_name+'.tbl', 'wb') as f:

        f.seek(0, file_offset)
        indx_to_write = unknown_function()
        cell = create_cell(schema, value_list)
        cell_size = len(cell)
        f.seek(0, indx_to_write-cell_size) #make sure to check not overwriting array of locs.

        f.write(cell)

        #update the array in the header

    return

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



def parse_create_table(command):
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
    tuple: (table_name, column_list)

    table_name: str
    column_list: list of column objects.
    """
    return None




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
    return "table_name"

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




def create_index(command):
    print("create index \'{}\'".format(command))
    return None

############################################################################
#DML FUNCTIONS

def insert_into(command):
    print("Insert into \'{}\'".format(command))
    return None

def delete_from(command):
    print("delete from \'{}\'".format(command))
    return None

def update(command):
    print("update \'{}\'".format(command))
    return None

##########################################################################
#DQL FUNCTIONS

def query(command):
    print("User wants to query {}".format(command))
    return None



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
