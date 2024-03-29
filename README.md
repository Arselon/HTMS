## Tabular network database
## &nbsp;
##### _Tags: tabular network database, hypertable, HT, HTMS, object-oriented software, nosql, python_
## &nbsp;
### Tabular network database (TNDB) or the **hypertable** (HT) - is a new generation of databases.
#### &nbsp;
This project is a approach to building data models and implementing software at the junction of the relational, network and object-relational mapping concepts. The new data model was called a "Tabular Network". Tabular network database may be of interest in subject areas in which existing SQL and NoSQL systems are not always effective: in AI, in big data processing, in adaptive applications with self-modifying database structures, in embedded systems etc.
####
The first functionally complete implementation of the TNDB application programming interface (API) was called the **HyperTable Management System - HTMS**.
####
Simplicity was one of the main goals of creating HTMS. Using the HTMS is no more complicated than the APIs of well-known object-relational mapping (ORM) realizations, for example such as Django ORM. But the tabular network model is much closer to the real world than the relational model, so to design the databases easier.
####
### &nbsp;
The **hypertable** is a set of **cross-referenced tables**:
- records are grouped into tables, and all records (rows) in the same table have the same set of fields (columns); 
- tables do not have headings: the set of columns of any table is a certain subset of common hypertable's attributes, i.e tabular structures in HT are defined as projections of a HT attribute set;
- there are two disjoint attribute classes:   
    * attributes, called **data type attributes - DTA**, define fields with self-defined data values;
    * attributes, called **reference type attributes - RTA**, define fields with values that are interpreted as links to rows in tables.  
- field values can be not only atomic, but also sets; 
- starting from the 3rd version of HTMS there are two kinds of RTA: simple **links** (were in HTMS version 2) and **numbered or "weighted" links**. Simple link is a physical address row in HT: pair tuple (_table number_, _row number_), i.e. the values of RTA fields are explicit links to any rows in any tables in the hypertable. Numbered ("weighted") link - is a simple link plus weight (real number): triple tuple (_table number_, _row number_, _weight_). In what follows, for simplicity, the numbered links will simply be called **weights**.

Full description of the tabular network data model see: **[Tabular network data model series](https://medium.com/@azur06400/tabular-network-data-model-series-f7b8469ed333)**.
### &nbsp;
The HTMS includes the following levels and packages:
- The "**object-level**" of the HTMS includes `HT_Obj` class for creating the HT objects, and `Obj_RAM` class for mapping (transforming) objects to/from the rows of tables. The main functional class at this level is `Obj_RAM`, the methods of which provide the basic operations for manipulating, searching and filtering data, which are similar in results to the `objects` methods in the ORM;
- The "**mid-level**" of the HTMS API is a set of classes and functions that form the basis for object-level: for organizing HT structures in general as well as models of individual tables (an analog of the `models` in Django ORM) and support common operations with the attributes, tables, rows and fields (create, change, delete etc.) and for formation search waves;
- The "**low-level**" HTMS API is a set of classes and utility functions - the basis for the mid-level and object-level. In HTMS low level used the special software from file-level (imported from module `cage_err.py` from `cage-api`) for error processing;
- The "**file-level**" HTMS API is a subsystem `Cage` of remote cached access to database files on servers on the network (description see **[github.com/Arselon/Cage](https://github.com/Arselon/Cage)**).
####
The HTMS user interface created as a universal HT screen editor - `HTed`, implemented as a website. It can connect to any data server regardless of application. `HTed` is used to create, design, and edit database files compatible with the HTMS.
####
For most applications, using _object-level_ and _mid-level_ classes and functions is sufficient. Both of them together are a **logical level** of the HTMS. Packages are available on PyPI: `htms-obj` and `htms-mid-api`. 
####
_Low-level_ and  _file-level_ - for advanced developers if they want to get inside the HTMS technology. Both of them together are a **physical level** of the HTMS.
Packages are available on PyPI: `htms-low-api` and `cage`. 
##  &nbsp;
____________________

#### _Copyright 2019-2022 [Arslan S. Aliev](http://www.arslan-aliev.com)_

_Software licensed under the Apache License, Version 2.0 (the "License"); you may not use this software except in compliance with the License. You may obtain a copy of the License at [www.apache.org](http://www.apache.org/licenses/LICENSE-2.0). Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License._    

#####  HTMS API v.3.1.0, readme.md red. 06.08.2022