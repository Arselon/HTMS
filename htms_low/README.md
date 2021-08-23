

## Tabular-network data model

##### _Tags: relational database, tabular-network database, api, object-
oriented software, nosql, python_

### Hypertable as a new generation of databases

We have developed a new concept, methods and practical ways to combine the
table and network database models with the advantages of the ORM (Object-
Relational Mapping (transformation)) idea, so that, to refuse to use the query
language in the application code, which allowed us to create a new model and
database technology.

A "tabular" database refers to a "degenerate" relational model in which:

  * records are grouped into tables, and all records (tuples) in the same table have the same set of fields (in columns) 
  * tables do not have identical rows 
  * field values can be not only atomic, but also sets 

The key new concept is the **hypertable** (HT) - this is a database, as a set
of tables:

  * a hypertable has a common set of attributes for all its tables 
  * tables do not have headings: the set of columns of any table is a certain subset of hypertable's attributes, that is, two concepts of RDB: the database domain and the attribute of a separate relation "merge" into one \- the hypertable attribute 
  * there are two disjoint attribute classes:   
attributes with data whose values, as in the RDB, are field fields with self-
defined data for the corresponding columns of the tables, which we will call
the ADT - "attribute of data type "  attributes with links that we call ALT -
"attribute of link type "

The values of ALT fields in the rows of the table are explicit links to any
rows in any tables in the hypertable.

There is a working experimental prototype - a set of tools in Python \- the
HyperTable Management System (HTMS), which includes the following levels (from
top to bottom):

  * hypertable editor implemented as a website on the Django platform, which can connect to any server regardless of applications (functionally similar to PgAdmin utility for PostgeSQL); 
  * library of utilities and classes at the "logical" level API for creating a database and manipulating data in application code (OTNM); 
  * library of utility functions and classes at the "physical" level, which is the basis for the logical level (this API can be used by experienced system programmers); 

Obviously, the formalization of the data scheme at the logical level in HTMS
and ORM are similar, but there are a number of fundamental differences.

In HTMS, attributes and data types are defined as a single space; in ORM, they
are bound to separate tables.

The whole set of database attributes in ORM is created "additively", as models
are defined, therefore they cannot be changed programmatically, but in HTMS -
for the entire database as a whole, and you can add or remove them in the
application without migration.

The attributes for each individual model in ORM are static, and in HTMS they
are dynamic. Tabular structures in HTMS are defined as projections of a single
attribute set - it is simpler and clearer than in ORM. The site algorithms on
HTMS can provide options for changing the original database structure, for
example, adding new attributes or deleting existing ones, which is impossible
in principle in ORM technology .

Note that HTMS technology, if applied in the Django framework, only expands
its capabilities and does not require the abandonment of the use of ORM. For
example, Django entire excellent authentication system, based on the models
and the **User** class (from the _django.contrib.auth.models_ module), can be
used. Therefore, in reality, a Django site with HTMS will usually be "multi-
model", that is, one part of the overall database will be purely relational,
the other tabular-network.

### htms_low - API for HTMS Physical Layer

####  Files structure

The HTMS database includes:

  * four system files, which we will call the files **htd, af, bf** and **cf** ; and
  * a set of files for tables, which we will call the "multi-attribute file" ( **MAF** hereinafter)

The first characters in the names of all files are the same - this is the
symbolic name of the HT (database). The names of system files and the
extensions of their file name are the same (for example, in the base database,
the htd file will be named base.htd).

##### Htd file

It is the database information core and contains metadata:

  * common descriptor; 
  * attribute descriptors, including their names, data types; 
  * MAF descriptors containing their structure, number of rows; 
  * as well as other information. 

##### af file

A shared storage for variable length data arrays. Arrays can be: byte strings,
UTF-8 strings, and number sets. The address of each array is formed by a pair
(offset, dimension), where the offset is the position of the first byte from
the beginning of the af file. The existing HTMS implementation uses the long
Python integer format, which physically occupies 8 bytes. Therefore, the
addresses of arrays in the MAF occupy 16 bytes each.

##### bf file

A shared storage for copies of files of various MIME types. Files are stored
as byte string of the appropriate length. The address of each string is formed
by a pair (offset, length).

In MAFs, in fields of the file type, in addition to the address of the file
body itself in bf, is stored the file descriptor address. The file descriptors
are stored in the af file (not bf!) and contains meta-information: the file
name, its MIME type, etc. This information is saved when the file is
downloaded and used when downloading it in a standard way for the OS.

Here, there is a difference from how the work with files is organized in ORM.
ORM does not save files in the database itself, but saves only the link - the
URL of the file on the server. Therefore, extraneous intervention is possible
- changing files using the file system without the "participation" of the
DBMS. HTMS saves files as areas in the bf file inside the HT itself and
therefore access to them "from the outside" is impossible.

##### cf file

A shared storage for links. Each ALT value is stored in cf in a structure that
contains a "descriptor of the links block" and the "links block" itself. A
links block is a set of pairs (table number, row number). The descriptor
consists of four numbers:

  1. links block dimension, i.e. quantity of the links; 
  2. MAF number, to which the value of ALT belongs; 
  3. identifier (number) of the attribute in the HT; 
  4. row number in which the ALT value is located. 

All links blocks in the cf file are separated using a unique combination of
bytes, so it is possible to independently scan the cf file to systematically
search and edit the necessary links.

##### Multi-attribute file (MAF)

The body of one table without a header, i.e. actually a set of its rows
(records) at the first level of storing the contents of the rows. The name of
a particular MAF consists of the database name and table number. All MAFs have
the same extension - maf, for example, the MAF of the Table No. 51 in the
"base" database will be called "base_51.maf".

##### MAF structure

MAF is a sequence of records of fixed length, which contain the same number of
fields with attribute values, the length of which is determined by the
attribute data type. Fields contain either directly fixed-length data or links
(addresses) to sections of af, bf or cf files where variable-length data is
located.

In HTMS, it is accepted that the physical length of all rows in each table is
the same, so the row number allows you to simply calculate its offset from the
beginning of the MAF. The contents of the fields with data of variable length
are stored in separate files - af, bf and cf.

Physical row numbers can obviously change when you delete old or insert (not
at the end) new rows in a table.

Links, unlike row numbers, are unique at the logical level.

####  Data Types

Data type id. |  Definition  
---|---  
byte1 |  one byte  
byte4 |  byte array (4)  
byte8 |  byte array (8)  
int4 |  integer  
int8 |  integer  
float4 |  float  
float8 |  float  
utf50 |  UTF-8 string (without BOM) - up to 50 characters  
utf100 |  UTF-8 string (without BOM) - up to 100 characters  
time |  time.time () UTC as a float  
datetime |  date and time in user format \- as a UTF-8 string (without BOM) -
up to 25 characters  
*link |  offset and length of the "link's block" in the cf file   
*byte |  an array of bytes of variable length (from 0 to 9223372036854775807)   
*utf |  UTF-8 string (without BOM)   
*int4 |  array of integers of variable length   
*int8 |  array of integers of variable length   
*float4 |  array of floats of variable length   
*float8 |  array of floats of variable length   
file |  file of anysize and MIME type  
  
Notes :

  1. Undefined values for types byte1, byte4, byte8, time cannot be specified. By default, a null value is generated. 
  2. Undefined values for utf50, utf100, datetime types cannot be specified. By default, a UTF-8 string of zero length is formed. 
  3. Features of storing undefined values for numbers: 
    * it is the maximum possible integer (in 4 bytes) - (b'\x7F\xFF\xFF\xFF'). In HTMS, it is accepted as a sign of an undefined value for integer (None). 
    * maximum possible long integer (in 8 bytes) (b \x7F\xFF\xFF\xFF\xFF\xFF\xFF\xFF'). In HTMS, it is accepted as a sign of an undefined value for long integer (None). 
    * sign of indefinite value (None) for a float in 4 bytes (b'\x7f\xc0\x00\x00') 
    * sign of indefinite value (None) for a double float in 8 bytes (b'\x7f\xf8\x00\x00\x00\x00\x00\x00') 
  4. For the remaining types, the attribute value is considered **undefined** if the address (data array, link block, file body) is (b'\xFF'*16). If the address contains an array length of 0, then the array is considered **defined and empty** (byte and character strings of zero length, empty tuple of an array of numbers, file of zero length) 
  5. If it is critical to have a database logic with the ability to distinguish between undefined and zero values, then instead of types byte1, byte4, byte8 must use * byte, and instead of utf50, utf100, datetime - * utf. 

#### Main classes of HTMS low level API

##### class HT(object)

This is the first base class for creating HTs and working with them. When
initializing each object of the HT class using the method weakref.ref(self)
creates a weak link to it to be able to look for all open HTs in the RAM. The
class can be used at the physical layer of HTMS.

Parameters (__init__() method arguments):

  * **ht_name** \- database name (HT name) - UTF-8 string (default is "htms_test") 
    * new - if True, then the database is created as a set of files and a new instance (object) of the class in the RAM. 
    * \- if False (by default), then the existing database "opens", that is, its files are opened and, based on the information in them, a structure is created in RAM - the instance of the class. 
  * **server** \- "url: port" - the web address of the server with the database (default is "127.0.0.1:3570") 
  * **db_root** \- the folder on the database server with its files (default is "C:/htms") 
  * **cage_name** \- the name of two instances (objects) of the Cage class, which provides remote file access to the server with DB - UTF-8 string. The first object is named cage_name, the second is "2"+cage_name (default is db_name and 2db_name). The second object is created and is used only when performing database compression or its full copy operations. 

Attributes (class slots):

The main data structure with HT attributes is its descriptor except dictionary
"relations". During operation, HTMS maintains compliance between files and a
descriptor so that in case of an error dont lose changes.

Attributes, which used only in the instances and not saved in files:

  * **HT.channels** \- mapping dictionary for adt, af, bf, cf files and Cage channels. 
  * **HT.mafs_opened** [maf_number] - a dictionary with MAF instances. Just as an instance of the HT class serves for working with one HT as a whole, MAF instance provides work with one ("opened") MAF. 
  * **HT._instances** \- the set of weak references to class objects 

Methods (class functions):

  * **HT.getinstances** () - class instances generator 
  * **HT.close** () - "close HT" - closes all database files, destroys objects class Cage and destroys the "weak" link to the HT object. Upon destruction a weak link also destroys the instance itself. 
  * **HT.attribute** (fun = "add", attr_name = "", type = None, newname = "") \- to define and change the set of the common attributes of the database - their symbolic names and data types. When initially create a new HT attribute it is allocated a unique numeric id, which does not change during the timelife of the HT. fun: "add", "new_name", "info". Existing HTMS Implementation does not support the removal of the HT attribute at the physical level. 
  * **HT.update_attrs** (add_attrs = {}) - serves only to add new attributes into the database. add_attrs [attr_name] - dictionary, mapping the names of the attribute being added with its data type. 
  * **HT.get_maf_num** (maf_name = "") - returns the MAF number by its conditional name 
  * **HT.get_attr_num_and_type** (attr_name = "") - returns internal number (id) and data type of the attribute by its conditional name 

"External" functions that apply only to closed HTs:

  * **rename_ht** (db_name = "", db_root = "", new_db_name = "") \- to rename HT (database files) 
  * **delete_ht** (db_name = "", db_root = "") \- to remove HT (all database files) 

#####  class MAF(object)

This is the second main class at the physical layer of HTMS. When creating
each object (instance) of class MAF using the method weakref.ref(self) a weak
link is created to it to have the ability to search for all open MAFs in the
RAM.

Parameters

  * **ht** \- an instance of the HT class that defines the membership of the MAF in the HT. 
  * **maf_num** \- a unique MAF number. If not specified, then a number is assigned to the new MAF, one greater than the maximum of the many existing MAF numbers. 
  * **maf_name** is the symbolic name of the MAF. When creating a new MAF mandatory and should not overlap with the other existing MAF names. 

When initializing an object of class MAF, the system checks for the presence
of MAF with the given arguments in HT and either creates a new file, or
"opens" the already existing MAF a class instance is created in the RAM.

When creating or changing the structure of an MAF (a set of fields from a
variety of GT domains) the system makes the corresponding changes in the mafs
dictionaries and models of the HT descriptor.

Attributes:

  * **MAF.ht** \- reference to the object of the HT class database, which includes the MAF 
  * **MAF.rows** \- the number of rows in the table - essentially a duplicate meanings in the HT mafs dictionary. 
  * **MAF.fields** \- a dictionary of table fields in which keys are the attribute names of the HT, and the values are tuples ("attribute number", "attribute type") 
  * **MAF.offsets** \- a tuple with pairs ("offset", "field length"), each pair of which strictly corresponds to the table field The offset is the position of the first byte of the field relative to the beginning of the line, field length - the number of bytes in it. 
  * **MAF.ch** \- MAF channel number (for the remote access system - Cage files). 
  * **MAF.rowlen** \- the length of one MAF row in bytes. This information is redundant, taking into account the one contained in the HT descriptor, however, it speeds up HTMS and makes it easier to use API for programmers 
  * **MAF._instances** \- The set of weak references to class objects 

Methods:

  * **MAF.getinstances** () - class objects (instances) generator 
  * **MAF.rename** (new_maf_name = '') - rename 
  * **MAF.delete** () - delete 
  * **MAF.close** () - "close" MAF - closes the file in the database, destroys the "weak" link to the object. When destroying a weak links also destroy the object itself. 
  * **MAF.wipe** () - "zero" the MAF - it closes, then HTMS deletes it, but without making changes to the dictionary HT models, then a new "empty" MAF is created with the same number and name 
  * **MAF.field** (fun = 'add', attr_name = '', attr_num_f = 0) \- to define and change the fields of the MAF by name or number. Functions: add, delete. 
  * **MAF.row** (fun = 'add', after = -1, number = 1, data = b '') \- to add and delete rows of the MAF table. Functions: add, delete, read, write. The last two functions provide reading or writing data. into a table row in the form of a binary array without structuring by fields. 
  * **MAF.w_links** (attr_num, num_row, links = set ()) \- record the value in the ALT field. Links argument is a set of the pairs - tuples ("MAF number", "line number") 
  * Hereinafter: 
    * attr_num - field attribute id, 
    * num_row - table row number 
  * **MAF.r_links** (attr_num, num_row) - read the value from the ALT field 
  * **MAF.u_links** (attr_num, num_row, u_link = ()) \- change the value in the ALT field: 
    * u_link = () - reset the field value 
    * u_link = (-nmaf, *) - remove from the field value all links to MAF with number=nmaf 
    * u_link = (nmaf, -num_row) - remove the link to the num_row row in the MAF with number=nmaf 
    * u_link = (nmaf, num_row) - add a link to the num_row line in the MAF with the number=nmaf, if it does not exist. 
  * **MAF.w_elem** (attr_num, num_row, elem = b'') \- write the field value in the form of a binary array without structuring (no serialization). Can be used to write byte strings of the fixed length (data types byte1, byte4, byte8), either at a very low physical level 
  * **MAF.r_elem** (attr_num, num_row) \- read the value of the field in the form of a binary array without structuring (no serialization) 
  * **MAF.r_utf8** (attr_num, num_row) \- read the value of a field of type string UTF-8 (for utf50, utf100, and datetime data types) 
  * **MAF.w_utf8** (attr_num, num_row, string = '') \- update the value of a field of type string UTF-8 (for utf50, utf100, and datetime data types) 
  * **MAF.r_numbers** (attr_num, num_row) - read the field's value of numeric type (int4, int8, float4, float8, time, * int4, * int8, * float4 and * float8). The result is always a list of numbers of the appropriate type. 
  * **MAF.w_numbers** (attr_num, num_row, numbers = 0) \- update the value of a field of a numeric type (int4, int8, float4, float8, time, * int4, * int8, * float4 and * float8). numbers can be either a number or a tuple with numerical values. 
  * **MAF.r_bytes** (attr_num, num_row) \- read the value of a field with type an array of bytes of variable length (* byte) 
  * **MAF.w_bytes** (attr_num, num_row, bytes = b``) \- update the value of a field with type an array of bytes of variable length (* byte) 
  * **MAF.r_str** (attr_num, num_row) - read the value of a field of type UTF-8 string of variable length (* utf) 
  * **MAF.w_str** (attr_num, num_row, string = '') \- update the value of the field string UTF-8 variable length (* utf) 
  * **MAF.download_file** (attr_num, num_row, to_path = '') \- upload a file from the database to the client's computer. Recording is done in blocks (chunks), the size of which optimized according to page size buffer memory of objects of class Cage 
  * **MAF.r_file_descr** (attr_num, num_row) \- read the file descriptor in the database in dictionary format {'file_name': "file name", 'file_ext': "extension", 'content_type': "MIME type", 'file_length': "file length (bytes)"} 
  * **MAF.upload_file** (attr_num, num_row, file_d) \- upload the file to the database in accordance with the descriptor, describing it on a client's computer. Loading is done in blocks (chunks), which size is optimized depending on the size pages of buffer memory for objects of class Cage. 
  * **MAF.clean_file_descr** (attr_num, num_row) \- clear the file descriptor in the database. This means that the file is deleted from the database. 
  * **MAF.attr_type** (n_attr = 0) - find out the data type by attribute (domain) number 

External function:

**get_maf** (ht_name, n_maf) - get for the specified HT by MAF number its
symbolic name and current number of rows.

____________________

#### Copyright 2021 [Arslan S. Aliev](http://www.arslan-aliev.com)

##### Licensed under the Apache License, Version 2.0 (the "License"); you may not use this software except in compliance with the License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.

#####  htms_low API v.2.3.1, readme.md red. 16.07.2021

