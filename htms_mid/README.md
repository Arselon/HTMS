
# The middle level of the HTMS
### &nbsp;
##  Includes classes: **HTdb** - inhered class from HT class (from low level), **Table** - inhered class from MAF class (from low level), **Links_array** - "service" class for set of the links in RAM, **Links_tree** - class for creating and managing objects describing graphs of the search waves.
### &nbsp;
### Tags: _HTMS, search wave, mid-level api_
### &nbsp;
##  Functionality:
  * creating or opening database - as an instance of a hypertable in RAM at the logical level;
  * operations at the logical level with tables, with rows in tables, with data in fields;
  * operations with special link objects;
  * forming the search waves. 
##  Structure
  PyPI package `htms-mid-api` includes modules:
-    `htdb.py` - contains classes `HT_db`, `Table`, `Links_array` and `Links_tree`;
-    `htms_par_middle.py` - settings for middle level debugging,  and class `HTMS_Mid_Err` for error processing, derived class from `Exception` system class. 
#### &nbsp;
## Class **`HT_db(HT)`**
### `(server = '', db_name='', db_root = '', cage_name='', new = False, jwtoken='', zmq_context = False, from_subclass=False, mode='wm')`
#### &nbsp;
The `HT_db` derives from `HT` class (HTMS low level) and inherits all of its attributes and methods. As well as `HT` it is used to create a specific object to model a specific database and is necessary when creating and using instances of the classes `Table`, `Links_array` and `Links_tree`.
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
- `from_subclass` (_bool_):
  * _True_ \- if an instance of a class is created from a derived class (`HT_Obj(HTdb)` class in HTMS object-level API); 
  * _False_ \- (by default);  
- `zmq_context` ( _bool_ or _object_ ) - (by default _False_) Python bindings for ZeroMQ (see https://pyzmq.readthedocs.io/en/latest/api/zmq.html,  which means the ZeroMQ context will be created in the Cage object itself). Used to optimize the system, this parameter can be left _False_. 
### Attributes
`HTdb`attributes inherited from the class `HT` (low level HTMS). 
### &nbsp;
### Main methods
### &nbsp;
### `relation` 
#### `(dic)` 
Links attributes to semantic categories ("whole", "multipart", "cause") for reference type HT attributes (RTA) in `relation` _dictionary_ in the HT descriptor (see HTMS low lewel).
#### R e c e i v e s
- `dic` - _dictionary_, where the _key_ - is the symbolic names of attributes, _value_ - one semantic category. 
### &nbsp;
### `erase_attribute ` 
#### `(attr=0)` 
Removes the HT attribute at the **logical level** by its name or internal id number. At the same time, no deletion is performed at the physical level, but the element `attribute name`: **"erased"** is added(updated) to the `relation` _dictionary_. (Therefore, the id number of the removed attribute cannot be used for new attributes at the physical level, where the logically removed attribute can be worked on - not recommended!).
#### R e c e i v e s
- `attr`- _int_ or _string_, attribute internal id or name respectively.  
#### R e t u r n s
  * `True` \- success; 
  * `False` \- error.
### &nbsp;
## Class **`Links_array`**
### `(links =())`
#### &nbsp;
This is a utility class for working with array ot the objects containing dynamic links. It is not necessary to use this class at the application programming level, but understanding how the structure of an object with an ordered array of references works is important in HTMS. One of the attributes of objects reflecting wave graphs - a set of nodes, is formalized as an object of the Links_array class.

Class is based on the concept of "dynamic link" (you can use the concept of "reactive link" as an equivalent). The array of dynamic links is constantly checked by HTMS when deleting any row or inserting a new one not at the end of the table, and, if necessary, the corresponding row numbers are adjusted in this array - in the same way as the row numbers in the RTA values ​​in all tables (MAFs) are adjusted.
### Required parameters
- `links` ( _object_ ) - a tuple of references (pairs `(MAF id number, row number)`) for the initial object definition. 
### Attributes
- `dyna_link` - a _dictionary_ with "dynamic" links, where the _key_ is the ordinal number of the link (its index), the _value_ is the link - the pair `(MAF id number, row number)`. This _dictionary_ (indexed array of links) can only be accessed through the methods of this class;
- `_instances` - a set of "weak" references on the class instances (see [Python weak references](https://docs.python.org/3/library/weakref.html)).
### &nbsp;
### Main methods
### &nbsp;
### **`get_dyna_links`** 
#### `()` 
Generator of a set of references from a class instance.
#### Y i e l d s
- the pair `(MAF id number, row number)`
### &nbsp;
### `get_dyna_link` 
#### `(ind)` 
Returns a link by index (empty tuple () if not found)
#### R e c e i v e s
- `ind` - the index of the link in links array
#### R e t u r n s
- the pair `(MAF id number, row number)`
### &nbsp;
### **`get_dyna_index`** 
#### `(link)` 
Returns a index by link 
#### R e c e i v e s
- `link` - `(MAF id number, row number)`
#### R e t u r n s
- `index`:
  * _int_ - the index of link in Links_array instance;
  * `-1` - if link not found/ 
### &nbsp;
### **`add_dyna_link`** 
#### `(link)` 
Add new link to array 
#### R e c e i v e s
- `link` - `(MAF id number, row number)`
#### R e t u r n s
  * `True` \- success; 
  * `False` \- error.
### &nbsp;
### **`update_dyna_link`** 
#### `(link, new)` 
Update the link in the array with the same index id, but raises an exception if the operation fails
#### R e c e i v e s
- `link` - `(MAF id number, row number)` - old array item;
- `new` -  `(MAF id number, row number)` - new item;
#### R a i s e
  * `HTMS_Low_Err` - "205 HTMS_Mid_Err     Link update impossible".
### &nbsp;
### **`remove_dyna_link`** 
#### `(link)` 
Remove link from array logically, if it is, by replacing it with a tuple (0,0).  
#### R e c e i v e s
- `link` - `(MAF id number, row number)`
### &nbsp;
### **`get_dyna_rows`** 
#### `(nmaf)` 
Returns a tuple with row numbers from links to specified MAF, in the order given by the indices of the link array elements.
#### R e c e i v e s
- `nmaf` - MAF id number
#### R e t u r n s
- `rows`:
  * `tuple` \-  with row numbers;
  * `()` - if no links found.
### &nbsp;
### **`get_first_dyna_row`** 
#### `(nmaf)` 
Returns a row number from first link to specified MAF, in accordance with the links array elements indexation.
#### R e c e i v e s
- `nmaf` - MAF id number
#### R e t u r n s
- `row`:
  * `int` - row number;
  * `0` - if no links found.
### &nbsp;
## Class **`Links_tree`**
### `(ht_name, root_link, tree_name='')`
#### &nbsp;
A class for creating and managing objects describing graphs produced from search waves. Nodes of the graph - rows of tables, presented in pairs `(MAF id number, row number)`. Graph edges are directed links between tables rows in common HT space. Note: the edges always belongs to one of the semantic types - "whole", "multipart" or "cause"(by default).
### Required parameters
- `ht_name` ( _str_ ) - HT name;
- `root_link` ( _tuple_ ) - pair `(root MAF id number, root row number)`- wave graph root;
- `tree_name` ( _str_ ) - conditional symbolic name of the wave (graph).
### Attributes
- `name` ( _str_ )- graph conditional name;
- `root_nmaf` ( _int_ ) - root MAF id number;
- `root_nrow` ( _int_ ) - root row number in root MAF;
- `nodes_links` - an ordered set of all nodes of the graph - an object of the `Links_array` class - a _dictionary_ of "dynamic" links, where the _key_ is the ordinal number of the link (its index), the _value_ is the link - the pair `("table number", "row number")`. The "zero" element of `nodes_links` is always a link to the table row for which the wave is calculated - the "root" of the directed graph;
- `links_tree` - a _dictionary_ of all graph edges, the elements of which are: _key_ - id number of the reference type attribute (RTA), _value_ - set of indices of graph nodes from `nodes_links` array. Due to the fact that the nodes of the wave graph are specified through the indices of the ordered array of dynamic links, any significant changes will always be taken into account. A _dictionary_ element with a "null" _key_ is not a set of links, but a _dictionary_ with one element `{'maf_name': "table name"}`. This is done to make the algorithms work faster;
- `DOT_vertexes` - a _dictionary_ for describing the nodes of a graph in the DOT language format, which is used when need to visualize (render) the graph. _Keys_ are names of nodes, _values_ are tuples of the form `("table number", "row number")`. Vertex names are formed as strings: `table name + "#" + str(row number)`. Since the DOT language format is very flexible, and its specification depends on the graph visualization requirements, it may be necessary to override this method at the application level. In the current implementation of the HTMS, the algorithm is adapted to use the **[GraphViz](https://graphviz.org/)**  [DOT language format](https://graphviz.org/doc/info/lang.html).;
- `DOT_edges` - a set of edges of a directed graph in the DOT format, the elements of which are tuples `(name of the parent vertex, name of the child vertex, the RTA id number from which the edge begins)`.
### &nbsp;
### Main methods
### &nbsp;
### **`getinstances`** 
#### `()`
Class object generator 
#### Y i e l d s
- the instance of the `Links_tree` class
### &nbsp;
### **`add_child`** 
#### `(parent=(), n_attr=0, child=(), nodes_discovered=set(), nodes_parents=set())` 
Add the vertex child to the wave graph, with creating an edge from the vertex `parent` to the vertex `child`.
#### R e c e i v e s
- `parent` - `(MAF id number, row number)`;
- `child` - `(MAF id number, row number)`;
- `n_attr` - attribute id number, where the `parent` link should come from;
- `nodes_discovered` - the set of links already included in the wave graph as child nodes;
- `nodes_parents` - a set of links already included in the wave graph as parent nodes, of which there is at least one edge to another node;

The sets `nodes_discovered`and `nodes_parents` are common for all direct waves if the number of RTA in HT is more than one. Therefore, the propagation of waves inward stops if:
 - the added edge indicates a node that has already been taken into account in the set of `nodes_discovered` (that is, the wave from this node has already been distributed earlier);
 - the added edge comes out of a node that has already been taken into account in the set `nodes_parents` (since the wave from this node was already propagated earlier).
 Such stops eliminate the appearance of edges - duplicates and looping.
 Using the `Links_tree` method `add_child` is not necessary at the application programming level, but understanding how the structure of an object with a wave graph works is important for understanding HTMS.
#### R a i s e s
  * `HTMS_Low_Err` - '211 HTMS_Mid_Err    Links_tree_add_child parameters error';
  * `HTMS_Low_Err` - '215 HTMS_Mid_Err    Links_tree_add_child parameters error';
  * `HTMS_Low_Err` - '217 HTMS_Mid_Err    Links_tree add_child error: "parent" not found in tree'.
### &nbsp;
### **`tree_DOT`** 
#### `()` 
Utility for obtaining a graph from a wave in the DOT language format. Each call to this method updates the values of the `DOT_vertexes` and `DOT_edges` attributes of the `Links_tree` instance. The application program can use information from the wave object in the internal HTMS format - an indexed array of nodes of the `Links_tree` graph - `nodes_links` and a _dictionary_ of the edges - `links_tree`.
### &nbsp;
## Class **`Table(MAF)`**
### `(ht_root ='', ht_name ='', t_name='', t_nmaf=0)`
#### &nbsp;
The `Table` derives from `MAF` class (HTMS low level) and inherits all of its attributes and methods. This is the second main class of HTMS at the logical middle level. The structure of the `Table`, in addition to the attributes of the superclass `MAF`, contains everything need to work with links at the logical level. When creating a new object (table), two system columns are always added to the set of its fields - `Back_links` (RTA - is used to store a set of links to those MAFs rows, of which there are links to this row) and `Time_row` (the value is the absolute time of row creation - `time.time()` divided by 10).
### Required parameters
- `ht_root` ( _str_ ) - the path to HT files on the file server;
- `ht_name` ( _str_ ) - the symbolic name of the HT (used to identify files on servers). 
### Additional parameters
- `t_nmaf` ( _int_ ) - id number for MAF instance in HT, from which instance the `Table` instance is derived. If not specified, new instance derived from class `Table` with name `t_name`;
- `t_name` ( _str_ ) - given `if t_nmaf`=`0`. Conditional symbolic name of new table. If not specified, the name of the superclass will be used;
### &nbsp;
### Main methods
### &nbsp;
### **`sieve`** 
#### `(with_fields = {}, modality ='all', res_num = Types_htms.max_int4)`
Returns a set of table rows that match the specified selection criteria.
#### R e c e i v e s
- `with_fields` - a _dictionary_ with search patterns, the _keys_ are the names of the attributes (columns), and the _values_ are tuples specifying patterns `("operation", "value1", "value2" or "none")`. If RTA or "file" DTA are found among the fields, they will be ignored. `value1` is a pattern for searching by **operations 1 - 6**. `value1` and `value2` set two boundaries of the search range (from and to) by **operations 7 and 8** for fields of numeric and string types. The `find` **operation 9** means: a) using the Python built-in function of the same name for byte and character strings - searching for a substring: `Value_in_field.find(value1)`, or b)  checking the occurrence of the pattern value in the multiple value of the field - only for numeric arrays. It cannot be used for simple numeric fields (not arrays). Comparison operations (notation and semantics as in Python):
  1. `==` 
  2. `!=` 
  3. `>`
  4. `<`
  5. `>=`
  6. `<=`
  7. `in`
  8. `not in`
  9. `find` 
- `modality`  
  * "one" \- the method will return as a result only one object corresponding to the first available row that meets the search conditions; 
  * "all" \- (by default), the method will return as a result all objects meets the filtering conditions.
- `res_num` \- if explicitly setted as a integer number, the method will return as a result a list of objects no longer than the value of this parameter, by default it is equal to the maximum possible number of rows in HTMS tables. It makes sense only if `modality="all"`. About `Types_htms.max_int4` (max row count in HTMS) see in low lewel HTMS.  
#### R e t u r n s
- the set of numbers of the rows" or set().
### &nbsp;
### **`update_fields`** 
#### `(add_fields=set(), del_fields=set())`
Change the set of table fields. The names of the HT attributes are indicated as elements of the sets. The method works on the basis of the `MAF` class `field` method, but with additional algorithms that are included to handle deletion of RTA category columns. This ensures that those backlinks that become invalid are removed.
#### R e c e i v e s
- `add_fields` - a set of HT attribute names to add to table;
- `del_fields` - a set of HT attribute names to delete from table;
#### R e t u r n s
  * `True` \- success; 
  * `False` \- error.
### &nbsp;
### **`update_row`** 
#### `(row_num =0, add_data={}, delete_data= set())`
Change the field values of a table row with self-defined data (not RTA). The method works on the basis of the class `MAF` methods: `w_elem`, `r_elem`, `r_utf8`, `w_utf8`, `r_numbers`, `w_numbers`, `r_bytes`, `w_bytes`, `r_str`, `w_str` (see low level HTMS).
#### R e c e i v e s
- `row_num` - row number;
- `add_data` - _dictionary_, in which _values_ setted for the fields specified as _keys_ (by the names of the HT attributes);
- `delete_data` - the set of the names of HT attributes (columns). Specified fields will be set to undefined or to zero (see low level HTMS).
#### R a i s e s
  * `HTMS_Mid_Err`- error.
#### R e t u r n s
  * `True` \- success; 
### &nbsp;
### **`delete_row`** 
#### `(nrow =0,  protect= Links_array( () ) )`
Deleting a row. This operation essentially depends on whether the table contains RTA fields. If all fields are self-defined data, then just apply the `MAF` method `row(fun = 'delete', after = nrow-1, number = 1)`. If there is an RTA in the table, then `delete_row` is executed recursively, correcting backlinks and, most difficult, deleting rows associated with the "multipart" and "whole" RTA categories. The wave of deletions can spread both "down" and "up". In extreme case, the removal of just one row may result in the removal of all the rows in all the tables!. 

Since in the process of physically deleting a row using the `row` method, the physical line numbers that are **after the deleted row** are changed, it is impossible to "protect" the necessary rows from deletion by simply listing the references to them. To handle such situations, HTMS has a special class `Links_array` (see above), which is very important. 
#### R e c e i v e s
#### Required parameters
- `nrow` - row number;
#### Additional parameters
- `protect` - provides the "protection" of the tables rows indicated therein, i.e. pairs `(table id, row number)` from deletion.
#### R a i s e s
  * `HTMS_Mid_Err`- error.
#### R e t u r n s
  * `True` \- success; 
### &nbsp;
### **`update_links`** 
#### `(row_num =0, attr_name='', add_links={}, delete_links= {})`
Universal method for changing the RTA field values of a table row.
#### R e c e i v e s
- `row_num` - row number;
- `attr_name` - HT attribute (RTA) name;
- `add_links` - _dictionary_ of the links to add to field value;
- `delete_links` - _dictionary_ of the links to remove from field value.

The elements of `add_links` and `delete_links` _dictionaries_ are specified as follows:
* _key_ is the name of the **target** table (symboilc MAF name),
* _value_ is either the set of row numbers in the target table, or:
    * for add_links:
      * "all" - hint to create links to all line numbers in the target table;
      * `set()` - hint to do not change references to rows in target table.
    * for delete_links:
      * "all" - remove all references to rows in target  table;
      * `set()` - do not change references to rows in target table.
For `delete_links`, instead of a _dictionary_, you can specify `delete_links="all"`, which will reset the entire value of the RTA field to null.
#### R a i s e s
  * `HTMS_Mid_Err`- error.
#### R e t u r n s
  * `True` \- success; 
### &nbsp;
### **`multi_link`** 
#### `(row_num =0, attr_name='',  to_table ='', to_rows=())`
Most convenient method for updating links. Replaces the use of the `update_links` method in the most common cases.
#### R e c e i v e s
- `row_num` - row number;
- `attr_name` - HT attribute (RTA) name;
- `to_table`:
  * _str_ - table name, or 
  * _object_ - table instance;
- `to_rows` - set of the rows numbers.

If `to_rows=()` then method deletes all links from the field to table `to_table`.
### &nbsp;
### **`copy`** 
#### `(new_table_name='', only_data= True, links_fields='blanc', only_fields=set(), with_fields={})`
Copy a table (create a new one).
#### R e c e i v e s
#### Required parameters
- `new_table_name` - name of the copy;
#### Additional parameters
- `only_fields` ( _set_ ) - the set of the names of the table attributes that will be present in new table. If the value is an empty set (by default), then the new table will be created with all copied table attributes;
- `only_data`:
  * `True` - the copy should contain only data columns, without RTA fields;
  * `False` - otherwise.  
- `links_fields` (can only be applied if `only_data=False`):
  * `"blanc"` - the copy should keep the RTA fields, but with empty values;
  * `"full"` or `"ref"` - links are copied in an unchanged form.  
- `only_fields` ( _str_ or _set_ ) - either the name of a table data attribute (not link attribute), or a set of such names. The set of table data columns that will be present in new table. If the value is an empty set (by default), then the new table will be created with all copied table attributes;
- `with_fields` - a _dictionary_ with search patterns (see `sieve` method above).
#### R a i s e s
  * `HTMS_Mid_Err`- error.
#### R e t u r n s
  * `True` \- success.
### &nbsp;
### **`copy_row`** 
#### `(nrow=-1, after_row=-1, only_data= True, links_fields='blanc', only_fields=set())`
Copy a row (create a new one). **Attention!**  All fields with "file" DTA not copied and its values will be setted to null in the copy of row.
#### R e c e i v e s
#### Required parameters
- `nrow` (_int_)- number of copied row;
#### Additional parameters
- `after_row` (_int_) - after which row to insert a copy. If `after_row=-1` (by default) then copy inserted after last table row;
- `only_fields` ( _set_ ) - the set of the names of the table attributes thats values will be present in new row. Others fields will be setted to zero or null. If `set()` (by default) - all values copied.
- `links_fields`:
  * `"blanc"` - the copy should keep the RTA fields, but with empty values;
  * `"full"` or `"ref"` - links are copied in an unchanged form;  
- `only_data` - **obsolete parameter, now not used in the algorithm**.
#### R a i s e s
  * `HTMS_Mid_Err`- error.
#### R e t u r n s
  * `True` \- success.
### &nbsp;
### **`row_tree`** 
#### `(nrow=-1, levels_source=-1, levels_ref=-1, only_fields=set(), with_fields={})`
Construction (forming) of direct waves and/or reverse waves on the links of `nrow` row. This recursive method replaces the search instruction in relational databases and is used to search or filter information.
#### R e c e i v e s
#### Required parameters
- `nrow` (_int_)- number of row from which the wave(-s) propagates;
#### Additional parameters
- `levels_source` (_int_):
  * `-1` (by default) - backward wave propagation is not limited;
  * `0`  -  backward wave propagation not need;
  * `>0` - maximum graph node level for backward wave (source node level for graph always equal `0` ).
- `levels_ref` (_int_) - maximum graph node level for direct waves:
  * `-1` (by default) - direct waves propagation is not limited;
  * `0`  -  direct waves propagation not need;
  * `>0` - maximum graph node level for each direct wave. 
- `only_fields` ( _set_ ) - the set of the names of RTA, thats will used for direct and backward waves forming. If `set()` (by default) - all RTAs will be used.
- with_fields={} - **obsolete parameter, now not used in the algorithm**.
#### R a i s e s
  * `HTMS_Mid_Err`- error.
#### R e t u r n s
  * `(row_source_tree, row_ref_trees)`- (_tuple_):
    * `row_source_tree` - the instance(-s) of the class `Links_tree` with backward wave  or `None`. Conditional name is the string : `HT_name+' : '+MAF_name+' : '+str(nrow)+' - UP'`;  
    * `row_ref_trees` -The dictionary with _values_ - direct wave(-s) - the instance(-s) of the class `Links_tree` or `None`.  _key_ in dictionary - the name of RTA. Conditional name of the direct wave - is the string : `HT_name+' : '+MAF_name+' : '+str(nrow)+' : '+Attribute_name+' - DOWN'`.
  * `None` - if all trees are empty. 

















### &nbsp;

____________________
####  Copyright 2018-2021 [Arslan S. Aliev](http://www.arslan-aliev.com)

#####  Software Licensed under the Apache License, Version 2.0 (the "License"); you may not use this software except in compliance with the License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.

###### Cageserver v.4.1, Cage package v.2.10.0, readme.md red.30.08.2021


