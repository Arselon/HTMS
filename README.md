
## Tabular network data model
####
This project is a new, original approach to building data models 
and implementing software at the junction of the concepts of relational DBMS, 
network DBMS and the object-relational mapping (ORM) concept. The new 
data model was called a "Tabular Network". 
####
Tabular Network DBMS may be of interest in subject areas in which existing SQL
and NoSQL systems are not always effective: in big data processing, in adaptive
applications with self-modifying database structures, in embedded systems etc.
The first functionally complete implementation of the API was called 
the HyperTable Management System (HTMS).
####
Simplicity was one of the main goals of creating HTMS. Using the HTMS API 
is no more complicated than the APIs of well-known ORMs such as Django. 
But the tabular network model is much closer to the real world than 
the relational model, so to design the databases easier.
####
Full description of the tabular network data model and HTMS on `medium.com` website:
- **[The tabular network model for database management](
https://medium.com/@azur06400/the-tabular-network-model-for-database-management-af086edad4c)** 
- **[Tabular network data model. Part 1. Conceptual definition](https://medium.com/@azur06400/tabular-network-data-model-part-1-conceptual-definition-49e84104b8aa)** 
- **[Tabular network data model. Part 2. Important features](https://medium.com/@azur06400/tabular-network-data-model-part-2-important-features-99a07f514b4)** 
- **[Hypertable Management System (HTMS) for tabular network databases](
https://medium.com/@azur06400/hypertable-management-system-htms-for-tabular-network-databases-1e9ef617f0ad)**
####
The HTMS includes the following levels and packages (see Fig. 5):
- The "**object-level**" of the HTMS application programming interface (API) includes `HT_Obj` class for creating the HT objects, and `Obj_RAM` class for mapping (transforming) objects to/from the rows of tables. The main functional class at this level is `Obj_RAM`, the methods of which provide the basic operations for manipulating, searching and filtering data, which are similar in results to the objects method in the ORM;
- The "**mid-level**" of the HTMS API is a set of classes and functions that form the basis for object-level: for organizing HT structures in general as well as models of individual tables (an analog of the `models` in Django ORM), for support common operations with the attributes, tables, rows and fields (create, change, delete etc.) and for formation search waves;
- The "**low-level**" HTMS API is a set of classes and utility functions - the basis for the mid-level and object-level;
- The "**file-level**" HTMS API is a subsystem `Cage` of remote cached access to database files on servers on the network (description see **[github.com/Arselon/Cage](https://github.com/Arselon/Cage)**).
####
The HTMS user interface created as a universal HT screen editor - `HTed`, implemented as a website. It can connect to any data server regardless of application. `HTed` is used to create, design, and edit database files compatible with the HTMS.
####
For most applications, using _object-level_ and _mid-level_ classes and functions is sufficient. Both of them together are a **logical level** of the HTMS. Packages are available on PyPI: `htms-obj` and `htms-mid-api`. 
####
_Low-level_ and  _file-level_ - for advanced developers if they want to get inside the HTMS technology. Both of them together are a **physical level** of the HTMS.
Packages are available on PyPI: `htms-low-api` and `cage`. 
## 

#### _Copyright 2018-2021 [Arslan S. Aliev](http://www.arslan-aliev.com)_

_Software licensed under the Apache License, Version 2.0 (the "License"); you may not use this software except in compliance with the License. You may obtain a copy of the License at [www.apache.org](http://www.apache.org/licenses/LICENSE-2.0). Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License._    