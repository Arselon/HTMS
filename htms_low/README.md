# The low (physical) level of the HTMS
##  Files structure
### &nbsp;
The HTMS database includes:
  * four system files, which we will call the files **htd, af, bf** and **cf** ; and
  * a set of files for tables, which we will call the **multi-attribute file - MAF**

The first characters in the names of all files are the same - this is the symbolic name of the HT (database). The names of system files and the extensions of their file name are the same (for example, in the base database, the htd file will be named base.htd).

### **htd** file

It is the database information core and contains metadata:

  * common descriptor; 
  * attribute descriptors, including their names, data types; 
  * MAF descriptors containing their structure, number of rows; 
  * as well as other information. 

### **af** file

A shared storage for variable length data arrays. Arrays can be: byte strings,
UTF-8 strings, and number sets. The address of each array is formed by a pair
`(offset, dimension)`, where the offset is the position of the first byte from
the beginning of the af file. The existing HTMS implementation uses the long
Python integer format, which physically occupies 8 bytes. Therefore, the
addresses of arrays in the MAF occupy 16 bytes each.

### **bf** file

A shared storage for copies of files of various MIME types. Files are stored as byte string of the appropriate length. The address of each string is formed by a pair `(offset, length)`.

In MAFs, in fields of the file type, in addition to the address of the file body itself in bf, is stored the file descriptor address. The file descriptors are stored in the af file (not bf!) and contains meta-information: the file name, its MIME type, etc. This information is saved when the file is
downloaded and used when downloading it in a standard way for the OS.

Here, there is a difference from how the work with files is organized in ORM. ORM does not save files in the database itself, but saves only the link - the URL of the file on the server. Therefore, extraneous intervention is possible - changing files using the file system without the "participation" of the
DBMS. HTMS saves files as areas in the bf file inside the HT itself and therefore access to them "from the outside" is impossible.

### **cf** file

A shared storage for links. Each RTA value is stored in cf in a structure that contains a "descriptor of the links block" and the "links block" itself. A links block is a set of pairs `(table number, row number)`. 
The descriptor consists of four numbers:
  1. `links block dimension`, i.e. quantity of the links; 
  2. `MAF number`, to which the value of RTA belongs; 
  3. `identifier` (number) of the attribute in the HT; 
  4. `row number` in which the RTA value is located. 

All links blocks in the cf file are separated using a unique combination of bytes, so it is possible to independently scan the cf file to systematically search and edit the necessary links.

### **multi-attribute file (MAF)**

The body of one table without a header, i.e. actually a set of its rows (records) at the first level of storing the contents of the rows. The name of a particular MAF consists of the database name and table number. All MAFs have the same extension - `maf`, for example, the MAF of the Table No. 51 in the
`base` database will be called `base_51.maf`.

### MAF structure

MAF is a sequence of records of fixed length, which contain the same number of fields with attribute values, the length of which is determined by the attribute data type. Fields contain either directly fixed-length data or links (addresses) to sections of af, bf or cf files where variable-length data is
located.

In HTMS, it is accepted that the physical length of all rows in each table is the same, so the row number allows you to simply calculate its offset from the beginning of the MAF. The contents of the fields with data of variable length are stored in separate files - `af`, `bf` and `cf`.

Physical row numbers can obviously change when you delete old or insert (not at the end) new rows in a table.

###  Data Types

|Data type id.|Definition|Field length in MAF (bytes)|The length of a single element (for variable - length types - bytes)| The default value of a single element (Python notation)|
|-------------|----------|----------|----------|----------|   
`byte1` |  one byte | 1 | 1 | b'\x00' |   
`byte4` |  byte array (4) | 4 | 4 | b'\x00'*4 |  
`byte8` |  byte array (8) | 8 | 8 | b'\x00'*8 |   
`int4` |  integer | 4 | 4 | 2147483647(*) |   
`int8` |  doble integer | 8 | 8 | 9223372036854775807(**) |  
`float4` |  float | 4	| 4 | float('nan') | 
`float8` |  double float | 8	| 8 | float('nan') |  
`utf50` |  UTF-8 string (without BOM) - up to 50 characters | 100 | 100 | b'\xFF'*100 |   
`utf100` |  UTF-8 string (without BOM) - up to 100 characters | 200 | 200 | b'\xFF'*200 |  
`time` |  `time.time()` UTC as a float  | 8 | 8 | 0.0 |  
`datetime` |  date and time in user format - UTF-8 string (without BOM) - up to 25 characters | 50 | 50 | b'\xFF'*50 |   
`*link` |  offset and length of the "link's block" in the cf file  | 16 | 8 | (-1,-1) |   
`*byte` |  an array of bytes of variable length (from 0 to 9223372036854775807) | 16 | 1 | b'\x00' |    
`*utf` |  UTF-8 string (without BOM) | 16 | 2 | b'\xFF' |    
`*int4` |  array of integers of variable length | 16 | 4 | 2147483647(*) |    
`*int8` |  array of integers of variable length | 16 | 8 | 9223372036854775807(**) |    
`*float4` |  array of floats of variable length | 16 | 4 | float('nan') (***) |   
`*float8` |  array of floats of variable length | 16 | 8 | float('nan') (****) |   
`file` |  file of any size and MIME type | 32 | 1 | b'\x00' |  
  
Notes :
1. Undefined values for types `byte1`, `byte4`, `byte8`, `time` cannot be specified. By default, a null value is generated. 
2. Undefined values for `utf50`, `utf100`, `datetime` cannot be specified. By default, a UTF-8 string of zero length is formed. 
3. Features of storing undefined values for numbers: 
    * (*) it is the maximum possible integer (in 4 bytes) - `b'\x7F\xFF\xFF\xFF'`. In HTMS, it is accepted as a sign of an undefined value for integer (None); 
    * (**) maximum possible long integer (in 8 bytes) `b \x7F\xFF\xFF\xFF\xFF\xFF\xFF\xFF'`. In HTMS, it is accepted as a sign of an undefined value for long integer (None);
    * (***) sign of indefinite value (None) for a float in 4 bytes `b'\x7f\xc0\x00\x00'`; 
    * (****) sign of indefinite value (None) for a double float in 8 bytes `b'\x7f\xf8\x00\x00\x00\x00\x00\x00'` 
4. For the remaining types, the attribute value is considered **undefined** if the address `(data array, link block, file body)` is `b'\xFF'*16`. If the address contains an array length of 0, then the array is considered **defined and empty** (byte and character strings of zero length, empty tuple of an array of numbers, file of zero length) 
5. If it is critical to have a database logic with the ability to distinguish between undefined and zero values, then instead of types `byte1`, `byte4`, `byte8` must use `*byte`, and instead of `utf50`, `utf100`, `datetime` - `*utf`. 
#### &nbsp;
## Main classes of HTMS low level API
#### &nbsp;
## Class `HT`
### `(server_ip="", ht_root="", ht_name="", cage_name="", ext={}, new=False, jwtoken="", zmq_context=False, from_subclass=False, mode='wm')`
#### &nbsp;
This is the first base class for creating HTs and working with them. When
initializing each object of the HT class using the method weakref.ref(self)
creates a weak link to it to be able to look for all open HTs in the RAM. The
class can be used at the physical layer of HTMS.
### Required parameters
- `server_ip` ( _str_ ) - IP address (or DNS) of the file server with the HT;  
- `ht_root` ( _str_ ) - the path to HT files on the file server;
- `ht_name` ( _str_ ) - the symbolic name of the HT (used to identify files on servers); 
- `jwtoken` ( _object_ ) - JSON Web Token (see https://pyjwt.readthedocs.io/en/latest/). 
It is used in the file servers to identify client applications. It is passed to the Cage remote file subsystem, where it performs an authentication function for security during communication, and also contains information about access rights. It is provided by the admin of the file server.
### Additional parameters
- `mode` ( _str_ ) - HT files access mode 
  * "rm" - readonly with monopoly; 
  * "rs" - readonly in sharing mode;  
  * "wm" - (by default) read and write with monopoly; 
  * "ws" - write with monopoly and read in sharing mode;  
  * "sp" - special mode for administrator; 
- `cage_name` ( _str_ ) - the conditional name of the Cage object used to identify clients on the server side ( _ht_name_ by default); 
- `new` ( _bool_ ): 
  * _True_ \- a new database is created as a set of files; 
  * _False_ \- (by default), the existing database "opens”, that is, its files are opened, and an object instance of this class is created based on the information in them;  
- `ext` (_dict_) - dictionary with file extensions for all HTMS file types (by default `{"maf":".maf", "adt":".htd", "af":".af", "bf":".bf", "cf":".cf", "bak_htd":".htb", "bak_maf":".mab", "tmp":".tmp", "log":".htl",}`);
- `from_subclass` (_bool_):
  * _True_ \- if an instance of a class is created from a derived class (`HTdb(HT)` class in HTMS mid-level API); 
  * _False_ \- (by default);  
- `zmq_context` ( _bool_ or _object_ ) - (by default _False_) Python bindings for ZeroMQ (see https://pyzmq.readthedocs.io/en/latest/api/zmq.html,  which means the ZeroMQ context will be created in the Cage object itself). Used to optimize the system, this parameter can be left _False_. 
### Attributes 
The main data structure with HT attributes is its descriptor except dictionary `relations`. During operation, HTMS maintains compliance between files and a descriptor so that in case of an error dont lose changes.

Attributes, which used only in the instances and not saved in files:

  * `HT.channels` \- mapping dictionary for adt, af, bf, cf files and Cage channels. 
  * `HT.mafs_opened` - a dictionary with MAF instances. Just as an instance of the HT class serves for working with one HT as a whole, MAF instance provides work with one ("opened") MAF. 
  * `HT._instances` \- the set of weak references to class objects 
### &nbsp;
### Methods
#### &nbsp;
### `getinstances` 
#### `()`
Class instances generator.
#### &nbsp;
### `close` 
#### `(Kerr=[])`
Close HT - closes all database files, destroys objects class `Cage` and destroys the "weak" link to the `HT` object. Upon destruction a weak link also destroys the instance itself.
#### R e t u r n s
  * `True` \- success; 
  * `False` \- error. 
In HTMS there is a special common for all levels subsystem for error processing (see section _HTMS error processing_ below). `Kerr` - is list object for error description, which can have not empty value before method activation, and contains added information about new error, if method returns `False`.
#### &nbsp;
### `attribute` 
#### `(Kerr=[], fun= "add", attr_name="", type=None, newname= "", attr_num_p=0)`
Define or change the set of the common attributes of the database - their symbolic names and data types. When initially create a new HT attribute it is allocated a unique numeric id, which does not change during the timelife of the HT. 
Existing HTMS Implementation does not support the removal of the HT attribute at the physical level. 
#### R e c e i v e s
- `fun` \: 
  * "add" \- (by default) add new attribute in HT; 
  * "new_name" \- rename the attribute;
  * "info" \- return information about attribute.
- `attr_name` - symbolic attribute name;
- `new_name` - new attribute name;
- `type` - HTMS data type used when new attribute added (see section _Data types_ above);
- `attr_num_p` - internal ht attribute id number.
#### R e t u r n s
  * `True` \- success; 
  * `False` \- error;
  * `integer` - internal attribute id number if `fun=info` and `attr_name`!=`""`; 
  * `str` - attribute name if `fun=info` and `attr_num_p`!=`0`; 
#### &nbsp;
### `update_attrs` 
#### `(add_attrs = {})`
Serves only to add new attributes into the database. 
#### R e c e i v e s
- `add_attrs`  - dictionary, mapping the names of the attribute being added with its data type 
#### R e t u r n s
  * `0` \- if `add_attrs` parameter is an empty dictionary; 
  * `integer` \- the number of added attributes (may be less than `len(add_attrs)`, if in dictionary keys there is the names, already used for HT attributes.
#### &nbsp;
### `get_maf_num` 
#### `(maf_name = "")`
Returns the MAF number by its conditional name. 
#### R e c e i v e s
- `maf_name` - symbolic MAF name 
#### R e t u r n s
  * `0` \- if in all HT MAF names there is no `maf_name`  
  * `integer` \- the HT unique MAF number.
#### &nbsp;
### `get_attr_num_and_type` 
#### `(attr_name = "")`
Returns the HT unique attribute number and its data type by its conditional name. 
#### R e c e i v e s
- `attr_name` - symbolic attribute name 
#### R e t u r n s
  * `()` \- if in all HT attribute names there is no `attr_name`  
  * (`integer`, `str`) \- the tuple with HT attribute internal number and HTMS data type.
### &nbsp;
### "External" functions (not classes methods) in package `htms-low-api` that apply only to closed HTs
#### &nbsp;
All functions needs use temporary Cage object, because HT must be closed (i.e there is no HT instance in RAM)
#### &nbsp;
### `rename_ht` 
#### `(Kerr=[], server_ip="", ht_name="", ht_root="", new_ht_name="", jwt_temp_cage="",cage_name= "", zmq_context =False)`
Rename HT (rename database files and internal descriptors). 
#### R e t u r n s
  * `True` \- success; 
  * `False` \- error;
#### &nbsp;
### `delete_ht` 
#### `(Kerr=[], server_ip="", ht_name="", ht_root="", jwt_temp_cage="", cage_name= "", zmq_context =False)`
Remove HT (delete all database files). 
#### R e t u r n s
  * `True` \- success; 
  * `False` \- error;
#### &nbsp;
### `deepcopy_ht` 
#### `(Kerr=[], server_ip="", ht_name="", ht_root="", new_ht_name="", new_ht_root ='', jwt_cage="",cage_name= "",zmq_context =False)`
Copy HT (database files) with full creating all attributes, tables and descriptors, that ensures the creation of a new internal numbering of attributes and tables in the copy of old HT and the absence of fragmentation in shared common files (`af`, `bf` and `cf`). 
#### R e t u r n s
  * `True` \- success; 
  * `False` \- error;
#### &nbsp;
### `compress_ht` 
#### `(Kerr=[], server = '', ht_name='', ht_root = '', jwt_temp_cage="",cage_name="", zmq_context =False)`
Makes deepcopy HT (database files) to the new HT, then delete old HT files, and restores all the names of the old table to the new one. 
#### R e t u r n s
  * `True` \- success; 
  * `False` \- error;
#### &nbsp;
## Class `MAF`
### `(ht, maf_num=0, maf_name="", from_subclass=False)`
#### &nbsp;
This is the second main class at the physical layer of HTMS. MAF is a table of the HT. When creating each object (instance) of class MAF using the method `weakref.ref` a weak link is created to it to have the ability to search for all open MAFs in the RAM.
### Required parameters
- `ht` ( _object_ ) - hypertable - an instance of the HT class that defines the membership of the MAF in the HT;  
- either  `maf_name` or `maf_num`
### Additional parameters
- `maf_name` ( _str_ ) - the symbolic name of the multi-attribute file. When creating a new MAF mandatory and should not overlap with the other existing MAF names. 
- `maf_num` ( _str_ ) - internal unique id - MAF number. If not specified, then a number is assigned to the new MAF, one greater than the maximum of the many existing MAF numbers;
- `from_subclass` ( _bool_ ) - internal unique id - MAF number 
  * _True_ \- if an instance of a class is created from a derived class (`Table(MAF)` class in HTMS mid-level API); 
  * _False_ \- (by default);
When initializing an object of class MAF, the system checks for the presence of MAF with the given arguments in HT and either creates a new file, or "opens" the already existing MAF a class instance is created in the RAM.
When creating or changing the structure of an MAF (a set of fields from a variety of HT attributes) the system makes the corresponding changes in the mafs dictionaries and models of the HT descriptor.
#### &nbsp;
### Attributes 
- `ht` \- reference to the object of the HT class database, which includes the MAF 
- `rows`\- the number of rows in the table - essentially a duplicate meanings in the HT mafs dictionary. **In HTMS tables (MAFs) first row has number 1 (not 0)**. 
- `fields` \- a dictionary of table fields in which keys are the attribute names of the HT, and the values are tuples (`attribute number`, `attribute type`) 
- `offsets` \- a tuple with pairs (`offset`, `field length`), each pair of which strictly corresponds to the table field. The `offset` is the position of the first byte of the field relative to the beginning of the row record, `field length` - the number of bytes in the field. 
- `ch` \- MAF channel number (for the remote access system - `Cage`). 
- `rowlen` \- the length of one MAF row record in bytes. This information is redundant, taking into account the one contained in the HT descriptor, however, it speeds up HTMS and makes it easier to use API for programmers. 
- `_instances` \- the set of weak references to class objects 
### &nbsp;
### Methods
#### &nbsp;
### `getinstances` 
#### `()`
Class instances generator.
#### &nbsp;
### `rename` 
#### `(Kerr=[], new_maf_name='')`
Rename MAF
#### R e t u r n s
  * `True` \- success; 
  * `False` \- error.
#### &nbsp;
### `delete` 
#### `(Kerr=[])`
Delete MAF
#### R e t u r n s
  * `True` \- success; 
  * `False` \- error.
#### &nbsp;
### `close` 
#### `(Kerr=[])`
Close MAF - closes the file in the database, destroys the "weak" link to the object. When destroying a weak links also destroy the instance itself. 
#### R e t u r n s
  * `True` \- success; 
  * `False` \- error.
#### &nbsp;
### `wipe` 
#### `(Kerr=[])` 
Zero" the MAF - it closes, then HTMS deletes it, but without making changes to the dictionary HT models, then a new "empty" MAF is created with the same number and name. 
#### R e t u r n s
  * `True` \- success; 
  * `False` \- error.
#### &nbsp;
### `field` 
#### `(Kerr=[], fun='add',  attr_name='', attr_num_f=0)`
Define or change the fields of the MAF by name (`attr_name`) or number (`attr_num_f`). 
#### R e c e i v e s
- `fun` \: 
  * "add" \- (by default) add new MAF column; 
  * "delete" \- delete column.
#### R e t u r n s
  * `True` \- success; 
  * `False` \- error.
#### &nbsp;
### `row` 
#### `(Kerr=[], fun='add', after=-1, number=1, data=b'')`
Operations with rows of the MAF.
#### R e c e i v e s
- `after` \- number of precedent row; 
- `number` \ - row count ;
- `fun` \: 
  * "add" \- (by default) add new MAF column; 
  * "delete" \- delete column.
  * "read" \- read record in the form of a binary array without structuring by fields; 
  * "write" \- write record in the form of a binary array without structuring by fields.
#### R e t u r n s
  * `True` \- success (only for `add`,`delete` and `write` functions); 
  * `False` \- error;
  * `_binary_` - rows as binary string (for `read` function if success). 
#### &nbsp;
### `w_links` 
#### `(Kerr=[], attr_num=0, num_row=0, links=set(), rollback=False)`
Record (write, update) the value in the reference type attribute (RTA field). Links argument is a set of the pairs - tuples (`MAF number`, `row number`) 
#### R e c e i v e s
- `attr_num` \- HT reference type attribute id number; 
- `num_row` \ - MAF row number (if `0` - indicate link to whole maf);
- `links` - The set of pairs (MAF number, row number). If `set()` that is no links (null).
- `rollback` \: 
  * `False` \- (by default) allows writing a new block of references to the place of the previous one in the file `cf` (if it was and if the length of the new block is no longer than the length of the old one); 
  * `True` \- prohibits writing a new block of references to the place of the previous one in the file `cf`.
#### R e t u r n s
  * `True` \- success; 
  * `False` \- error.
#### &nbsp;
### `r_links` 
#### `(Kerr=[], attr_num=0, num_row=0)`
Read the value of the reference type attribute (RTA field). 
#### R e c e i v e s
- `attr_num` \- HT reference type attribute id number; 
- `num_row` \ - MAF row number (greater `0`);
#### R e t u r n s
  * `True` \- success; 
  * `False` \- error.
#### &nbsp;
### `u_links` 
#### `(Kerr=[], attr_num=0, num_row=0, u_link =(), rollback=False)`
Change the value of the reference type attribute (RTA field). 
#### R e c e i v e s
- `attr_num` \- HT reference type attribute id number; 
- `num_row` \ - MAF row number (greater `0`);
- `u_link` \: 
  * `()` - reset (clear) the field value; 
  * `(-nmaf, *)` - remove from the field value all links to MAF with id number `nmaf`; 
  * `(nmaf, -num_row)` - remove the link to the `num_row` row in the MAF `nmaf`; 
  * `(nmaf, num_row)` - add a link to the `num_row` row in the MAF `nmaf`, if it does not exist.
#### R e t u r n s
  * `True` \- success; 
  * `False` \- error.
#### &nbsp;
### `w_elem` 
#### `(Kerr=[], attr_num=0, num_row=0, elem =b'')`
Write the field value in the form of a binary array without structuring (no serialization). Can be used to write byte strings of the fixed length (data types: `byte1`, `byte4`, `byte8`), either at a very low physical level.
#### R e c e i v e s 
- `elem` \- binary string.
#### R e t u r n s
  * `True` \- success; 
  * `False` \- error.
#### &nbsp;
### `r_elem` 
#### `(Kerr=[], attr_num=0, num_row=0)`
Read the field value in the form of a binary array without structuring (no serialization). 
#### R e t u r n s
  * `elem` \- binary string if success;
  * `False` \- error.
#### &nbsp;
### `w_utf8` 
#### `(Kerr=[], attr_num=0, num_row=0, string='')`
Update the value of a field of type string UTF-8 (for `utf50`, `utf100` `and datetime` data types).
#### R e c e i v e s 
- `string` \- UTF-8 string (without BOM).
#### R e t u r n s
  * `True` \- success; 
  * `False` \- error.
#### &nbsp;
### `r_utf8` 
#### `(Kerr=[], attr_num=0, num_row=0)`
Read the value of a field of type string UTF-8.
#### R e t u r n s
  * `string` \- UTF-8 string if success;
  * `False` \- error.
#### &nbsp;
### `w_numbers` 
#### `(Kerr=[], attr_num=0, num_row=0, numbers='')`
Update the value of a field of a numeric type (`int4`, `int8`, `float4`, `float8`, `time`, `*int4`, `*int8`, `*float4` and `*float8`). 
#### R e c e i v e s 
- `numbers` \- can be either a number or a tuple with numerical values.
#### R e t u r n s
  * `True` \- success; 
  * `False` \- error.
#### &nbsp;
### `r_numbers` 
#### `(Kerr=[], attr_num=0, num_row=0)`
Read the field's value of numeric typ. 
#### R e t u r n s
  * `numbers` \- is always a list of numbers of the appropriate type;
  * `False` \- error.
#### &nbsp;
### `w_bytes` 
#### `(Kerr=[], attr_num=0, num_row=0, bytes='')`
Update the value of a field with type an array of bytes of variable length (`*byte`)  
#### R e c e i v e s 
- `bytes` \- array of bytes.
#### R e t u r n s
  * `True` \- success; 
  * `False` \- error.
#### &nbsp;
### `r_bytes` 
#### `(Kerr=[], attr_num=0, num_row=0)`
Read the field's value of numeric type. 
#### R e t u r n s
  * `bytes` \- array of bytes;
  * `False` \- error.
#### &nbsp;
### `w_str` 
#### `(Kerr=[], attr_num=0, num_row=0, string='', rollback=False)`
Update the value of a field of type UTF-8 string of variable length (`*utf`)   
#### R e c e i v e s 
- `string` \- UTF-8 string;
- `rollback` \: 
  * `False` \- (by default) allows writing a new string to the place of the previous one in the file `af` (if it was and if the length of the new string is no longer than the length of the old one); 
  * `True` \- prohibits writing a new string to the place of the previous one in the file `af`.
#### R e t u r n s
  * `True` \- success; 
  * `False` \- error.
#### &nbsp;
### `r_str` 
#### `(Kerr=[], attr_num=0, num_row=0)`
Read the value of a field of type UTF-8 string of variable length (`*utf`). 
#### R e t u r n s
  * `string` \- UTF-8 string;
  * `False` \- error.  
#### &nbsp;
### `upload_file` 
#### `(Kerr=[], attr_num=0, num_row=0, from_path='', real_file_name='', file_e='', content_t='',  file_d={}, rollback=False)`
Upload the file to the database. Loading is done in blocks (chunks), which size is optimized depending on the size pages of buffer memory for objects of class `Cage`. Update the value of a file descriptor which stores in `af` file. 
#### R e c e i v e s 
#### Required parameters
- `from_path` \- path to file on client computer (with file name and extention);
- `real_file_name` \- file name on HT;
- `file_e` \- file extention on HT;
- `content_t` \- file content type (MIME);
#### Additional parameters
- `file_d` \- dictionary with old file descriptor `{'file_name':..., 'file_ext':..., 'content_type':..., 'file_length':...}`;
- `rollback` \: 
  * `False` \- (by default) allows writing a new file to the place of the previous one in the file `bf` (if it was and if the length of the new string is no longer than the length of the old one); 
  * `True` \- prohibits writing a new file to the place of the previous one in the file `bf`.
#### R e t u r n s
  * `True` \- success; 
  * `False` \- error.  
#### &nbsp;
### `download_file` 
#### `(Kerr=[], attr_num=0, num_row=0,  to_path='')`
Upload the file to the database. Loading is done in blocks (chunks), which size is optimized depending on the size pages of buffer memory for objects of class `Cage`. Update the value of a file descriptor. 
#### R e c e i v e s 
#### Required parameters
- `to_path=` \- path to file on client computer (without file name and extention);
#### R e t u r n s
  * `True` \- success; 
  * `False` \- error.
#### &nbsp;
### `r_file_descr` 
#### `(Kerr=[], attr_num=0, num_row=0)`
Reset (clean) file descriptor. This means the logical deletion of the file. Physically, the space in the `bf` file remains occupied. 
#### R e t u r n s
  * `dict` \- dictionary with file descriptor `{'file_name':..., 'file_ext':..., 'content_type':..., 'file_length':...}`; 
  * `False` \- error.   
#### &nbsp;
### `clean_file_descr` 
#### `(Kerr=[], attr_num=0, num_row=0)`
Reset (clean) file descriptor. This means the logical deletion of the file. Physically, the space in the `bf` file remains occupied. 
#### R e t u r n s
  * `True` \- success; 
  * `False` \- error. 
#### &nbsp;
### `attr_type` 
#### `(n_attr=0)`
Return the data type by HT attribute id number 
#### R e c e i v e s 
  * `n_attr` \- HT attribute id number;
#### R e t u r n s
  * `str` \- HT data type; 
### &nbsp;
### "External" function
#### &nbsp;
### `get_maf` 
#### `(ht_name, n_maf)`
Get info about MAF. HT must be opened. 
#### R e c e i v e s 
  * `ht_name` \- HT name; 
  * `n_maf` \- internal MAF id number;
#### R e t u r n s
  * `maf_name` \- MAF name; 
  * `rows` \- rows count;
### &nbsp;
____________________

#### Copyright 2021 [Arslan S. Aliev](http://www.arslan-aliev.com)

##### Licensed under the Apache License, Version 2.0 (the "License"); you may
not use this software except in compliance with the License. You may obtain a
copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless
required by applicable law or agreed to in writing, software distributed under
the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
CONDITIONS OF ANY KIND, either express or implied. See the License for the
specific language governing permissions and limitations under the License.

#####  htms_low API v.2.3.1, readme.md red. 16.07.2021

