
## Tabular-network data model

This project is a new, original approach to building data models 
and implementing software at the junction of the concepts of relational DBMS, 
network DBMS and the object-relational mapping (ORM) concept. The new 
data model was called a "Tabular-Network". 

Tabular-Network DBMS may be of interest in subject areas in which existing SQL
and NoSQL systems are not always effective: in big data processing, in adaptive
applications with self-modifying database structures, in embedded systems etc.
The first functionally complete implementation of the API was called 
the HyperTable Management System (HTMS).

Simplicity was one of the main goals of creating HTMS. Using the HTMS API 
is no more complicated than the APIs of well-known ORMs such as Django. 
But the tabular-network model is much closer to the real world than 
the relational model, so to design the databases easier.

HTMS includes:
- **htms_obj_api** - object-oriented high level API - a library of utilities 
and classes at the logical level for creating a database and 
manipulating data in application code (analogue of ORM);
- **htms_mid_api** - library of middle level utility functions and classes, 
which is the basis for the htms_obj_api, and it can also be used 
by application's programmers; 
- **htms_low_api** - library of low level utility functions and classes, 
which is the basis for htms_mid_api and htms_obj_api, and it can also be used 
by experienced system programmers;
- **HTed** - a universal HyperTable screen editor, implemented as a website 
on the Django framework, which can connect to any server regardless 
of applications (functionally close to the PgAdmin web utility for PostgeSQL);

Full description: 

https://medium.com/@azur06400/the-tabular-network-model-for-database-management-af086edad4c
https://medium.com/@azur06400/tabular-network-data-model-part-1-conceptual-definition-49e84104b8aa
https://medium.com/@azur06400/tabular-network-data-model-part-2-important-features-99a07f514b4
https://medium.com/@azur06400/hypertable-management-system-htms-for-tabular-network-databases-1e9ef617f0ad

#### Copyright 2018-2021 Arslan S. Aliev

Licensed under the Apache License, Version 2.0 (the "License"); 
you may not use this software except in compliance with the License. 
You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0 
Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an 
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. 
See the License for the specific language governing permissions and limitations under the License.

