
# The object level of the HTMS
### &nbsp;
##  Includes **HT_Obj** class for creating the HT objects, and **Obj_RAM** class for mapping (transforming) objects to/from the rows of tables and for network searching by links.
### &nbsp;
### Tags: _HTMS, mapping, object-level api_
### &nbsp;
##  Functionality:
  * creating an instance of a specific database (HT) in RAM;
  * mapping from the program code objects to rows of the table, and vice versa.
##  Structure
  PyPI package `htms-obj` includes modules:
-    `htorm.py` - contains classes `HT_Obj` and `Obj_RAM`;
-    `htms_par_obj.py` - contains class `HTMS_User_Err` for error processing, derived class from `Exception` system class. 
#### &nbsp;
## Class **`HT_Obj(HTdb)`**
### `(server='', db_root='', db_name='', jwtoken="", mode='wm', cage_name='', new=False, zmq_context=False)`
#### &nbsp;
The `HT_Obj` derives from `HTdb` class (HTMS middle level) and inherits all of its attributes and methods. As well as `HTdb` it is used to create a specific object to model a specific database and is necessary when creating and using instances of the class `Obj_RAM`.
### Required parameters
- `server` ( _str_ ) - IP address (or DNS) of the file server with the HT;  
- `db_root` ( _str_ ) - the path to HT files on the file server;
- `db_name` ( _str_ ) - the symbolic name of the HT (used to identify files on servers); 
- `jwtoken` ( _object_ ) - JSON Web Token (see https://pyjwt.readthedocs.io/en/latest/). 
It is used in the file servers to identify client applications. It is passed to the Cage remote file subsystem, where it performs an authentication function for security during communication, and also contains information about access rights. It is provided by the admin of the file server.
### Additional parameters
- `mode` ( _str_ ) - HT files access mode 
  * "rm" - readonly with monopoly; 
  * "rs" - readonly in sharing mode;  
  * "wm" - (by default) read and write with monopoly; 
  * "ws" - write with monopoly and read in sharing mode;  
  * "sp" - special mode for administrator; 
- `cage_name` ( _str_ ) - the conditional name of the Cage object used to identify clients on the server side ( _db_name_ by default); 
- `new` ( _bool_ ) 
  * if _True_ \- a new database is created as a set of files; 
  * if _False_ \- (by default), the existing database "opens”, that is, its files are opened, and an object instance of this class is created based on the information in them;  
- `zmq_context` ( _bool_ or _object_ ) - (by default _False_) Python bindings for ZeroMQ (see https://pyzmq.readthedocs.io/en/latest/api/zmq.html,  which means the ZeroMQ context will be created in the Cage object itself). Used to optimize the system, this parameter can be left _False_. 
### Attributes
`HT_Obj`attributes inherited from the class `HTdb` (mid-level HTMS). 
### &nbsp;
## Class **`Obj_RAM(TABLE)`** 
### `(table='', only_fields=set())`
#### &nbsp;
The Obj_RAM derives from `Table` class (HTMS middle level) and inherits all of its attributes and methods. It is used for operations on a specific table in a specific database. With Obj_RAM class the objects are "virtually" mapped (transformed) from the program code to rows of the table, and vice versa. Each object, an instance of this class, either corresponds to a row of a certain table with a subset of its fields (in the simplest case with the entire set of the fields), or serves to add a new row to the table.
### Required parameter
- `table` ( _object_ ) - an instance of a specific table that already exists in RAM;   
### Additional parameter
- `only_fields` ( _str_ or _set_ ) - either the name of a table data attribute (not link attribute), or a set of such names. The set of table data columns that will be present in each instance of this class. If the value is an empty set (by default), then the object will be created with all table attributes.
### Attributes
Each `Obj_RAM` has the following attributes:
  * `id` \- physical row number;
  * `fields` \- the dictionary where keys - the symbolical names of attributes, values - the values of row fields in table (byte string, UTF8 string, integer, float, etc.), or file descriptor (- see full description of HTMS data types in low level HTMS docs). 
  * `setted` \- the time of object creation in RAM as the value of the function `time.time()`
  * `updated` \- the time of object change in RAM as the value of the function `time.time()` (initally equal `setted`)
  * `maf_num` \- unique MAF number in HT (- see full description of HTMS multiattribute files - MAF in low level HTMS docs)
  * `table_name` \- symbolic table name
  * `HT_Obj_name` \- symbolic HT name
### &nbsp;
### Main methods
### **`get_from_table`** 
#### `(rows=(), with_fields={},  modality='all', res_num=Types_htms.max_int4, update=False)` 
This is a method for mapping row(s) of a table into objects in RAM, which creates “empty” instances, physically reads data from the table in the database, and fills the fields of instances with these data. The only data type that is not physically read in the RAM is files. Instead of being in the body of the file itself, the file descriptor is written to the object.
#### R e c e i v e s
- `rows` \- the physical number or the tuple of numbers of the desired rows; if it is empty (by default), the selection continues throughout the table; 
- `with_fields` \- a dictionary with search patterns for table rows filtering (search); if it is equal {} (by default), filtering by fields values is not used. Rules for compiling dictionaries for filtering is described in the method _sieve_ for the class _Table_ in the HTMS middle level;
- `modality`  
  * "one" \- the method will return as a result only one object corresponding to the first available row that meets the search conditions; 
  * "all" \- (by default), the method will return as a result all objects meets the filtering conditions.
- `res_num` \- if explicitly setted as a integer number, the method will return as a result a list of objects no longer than the value of this parameter, by default it is equal to the maximum possible number of rows in HTMS tables. It makes sense only if `modality="all"`; 
- `update`  
  * if _True_ \- a check is made for each table row that meets the filtering conditions for the presence of the corresponding instance in RAM (created earlier). If it is present, then updated those attributes of the found instance that correspond to those fields of the row that have changed since the moment of found instance creation. This maintains the integrity of the database and its virtual map in RAM; 
  * if _False_ \- (by default), rows matching filtering conditions are not checked for corresponding instances in RAM.
#### R e t u r n s
- the list of "row" objects that match the input parameters. Each object corresponds to one row of the table. If nothing is found, it returns `[]`. 
#### &nbsp;  
### **`get_from_RAM`** 
#### `(id=0)` 
A method for selecting one (by `id` row number) or all (if `id=0`) objects of the row type from RAM.
#### R e c e i v e s
- `id` \- (0 by default) the table row number.
#### R e t u r n s
- the list with one or set of "row" objects. If nothing is found, it returns `[]`. Objects has attributes as in `get_from_table` method.  
#### &nbsp;
### **`get_clone`** 
#### `()` 
The method creates a row object in RAM with empty attribute values and with `id=None`.
#### R e t u r n s
- the one "row" object.  
#### &nbsp;
### **`get_attr_num_and_type`** 
#### `(attr_name="")` 
#### R e c e i v e s
- `attr_name` \- symbolic attribute name.
#### R e t u r n s
Tuple with two items:
- unique attribute number;
- attribute data type ("byte1", "byte4", "byte8", "utf50", "utf100", "int4", or other - see full description of HTMS data types in low level HTMS docs).
#### &nbsp;
### **`get_table_object`** 
#### `()` 
The method is used to get an object of the `Table` class, from which it was inherited the instance of the class `Obj_RAM` (see description of the `Table` class in middle level HTMS docs.
#### R e t u r n s
`Table` instance in RAM.
#### &nbsp;
### **`get_HT_Obj`** 
#### `()` 
The method is used to get an object of the `HT_Obj` class, which corresponds to the HT, to which the table belongs from which `Obj_RAM` instance was inherited.
#### R e t u r n s
`HT_Obj` instance.
#### &nbsp;
### **`link`** 
#### `(link_field='', to_table_objects=())` 
Method is used to add (update) links between objects in RAM. It updates the field value in the object for the specified `link_field` and adds links to the specified objects (rows) of the tables. 
#### R e c e i v e s
- `link_field` \- the symbolic attribute name; 
- `to_table_objects` \- the list with `Obj_RAM` objects;
#### &nbsp;
### **`unlink`** 
#### `(link_field ='', to_table_objects= ())` 
Method by the specified attribute name (table field with links) deletes links to the specified `Obj_RAM` objects in RAM.
#### R e c e i v e s
- `link_field` \- the symbolic attribute name; 
- `to_table_objects` \- the list with `Obj_RAM` objects;
#### &nbsp;
### **`ref`**
#### `(link_field='', only_fields=set(), with_fields={}, ref_class=None, res_num=Types_htms.max_int4)`  
Using the specified attribute name (link field) get a list of objects referenced by this object. This is used for navigation in the HT.
#### R e c e i v e s
- `link_field` \- the symbolic attribute name; 
- `only_fields` - as for `Obj_RAM` initialization;
- `with_fields` \- as for `get_from_table` method;
- `ref_class` \- restricts the classes of tables from which the list of referenced objects will be formed to one explicitly specified; 
- `res_num` \- if explicitly setted as a integer number, the method will return as a result a list of objects no longer than the value of this parameter, by default it is equal to the maximum possible number of rows in HTMS tables.
#### &nbsp;
### **`source`**
#### `(source_class=None, only_fields=set(), with_fields={}, res_num=Types_htms.max_int4)`  
Get a list of objects that refer to this.
#### R e c e i v e s
- `source_class` \- restricts the classes of tables from which the list of source objects will be formed to one explicitly specified; 
- `only_fields` - as for `Obj_RAM` initialization;
- `with_fields` \- as for `get_from_table` method;
- `res_num` \- as for `link` method.
#### &nbsp;
### **`delete`**
#### `()`  
Delete the row in the table that corresponds with the "row" object. The object in RAM is **not deleted**.
#### &nbsp;
### **`save`**
#### `()`  
Save the object in a table row (update its fields) if the object has correct identifier field `Obj_RAM.id`. If `Obj_RAM.id==0 or None`, then a new row is added to the table (at the end) and the value `Obj_RAM.id` an equal number of table rows plus one is set.
### &nbsp;

____________________
####  Copyright 2018-2021 [Arslan S. Aliev](http://www.arslan-aliev.com)

#####  Software Licensed under the Apache License, Version 2.0 (the "License"); you may not use this software except in compliance with the License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.

#####  htms_obj API v.2.3.0, readme.md red. 30.08.2021


