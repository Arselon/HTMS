# HTMS middle level  v. 3.1.0 (Cage class v. 3.1.0)
# © A.S.Aliev, 2018-2022

#
# HTdb(HT) class
#
#    HTdb(
#       server = '', 
#       db_root = '', 
#       db_name='', 
#       cage_name='', 
#       new = False, 
#       jwtoken='', 
#       zmq_context = False, 
#       from_subclass=False, 
#       mode='wm',
#       local_root='',
#    )   
#        mode in ('rs','ws','wm','rm','sp')  -  corresponds to the parameter mod 
#                       in Cage class, method Cage.open
#                 rm  - open read/close with monopoly for channel owner
#                 wm  - open read/write/close with monopoly for channel owner
#                 rs  - open read/close and only read for other clients
#                 ws  - open read/write/close and only read for other clients
#                 sp  - need special external conditions for open and access
#           
#    @classmethods
#
#       getinstances()
#       removeinstances(obj)
#
#    methods (commonly used)
#
#       relation (dic)
#       erase_attribute (attr=0)
#           return True 
#           or 
#           if error - raise HTMS_Mid_Err : 220-225
#
#       close(Kerr=[])
#           return True 
#           or 
#           if error - raise HTMS_Mid_Err : 230
#
#       
# Links_array class
#       
#   Links_array(
#       links =()     
#   )
#           
#    @classmethods
#
#       getinstances()
#       removeinstances(obj)
#
#    methods (commonly used)
#      
#       add_dyna_link (link)
#           return True 
#           or 
#           if error False
#       get_dyna_links ()
#           yields link
#       get_dyna_link (ind)
#           return link
#           or
#           if not found - ()
#       get_dyna_index (link)
#           return index
#           or 
#           if not found - -1               
#       update_dyna_link (self, link, new)
#           if error - raise HTMS_Mid_Err : 205
#       remove_dyna_link (link)
#       remove_dyna_ind (ind)
#       get_dyna_rows (nmaf)
#           return tuple(rows)
#           or 
#           if not found - ()  
#       get_first_dyna_row (nmaf)
#           return row
#           or 
#           if not found - 0  
#
#       
# Links_tree class
#       
#   Links_tree(
#       ht_name, 
#       root_link, 
#       tree_name=''
#       )
#           
#    @classmethods
#
#       getinstances()
#       removeinstances(obj)
#
#    methods (commonly used)
#
#       add_child (
#           parent=(), 
#           n_attr=0, 
#           child=(), 
#           nodes_discovered=set(), 
#           nodes_parents=set(),
#       )
#           if error - raise HTMS_Mid_Err : 207-209
#       tree_DOT()
#           create attributes for instance - 
#           DOT_vertexes and DOT_edges (in the DOT language format)
#       
#       
# Weights_tree class
#       
#   Weights_tree(
#       ht_name, 
#       root_link, 
#       tree_name=''
#       )
#           
#    @classmethods
#
#       getinstances()
#       removeinstances(obj)
#
#    methods (commonly used)
#
#       add_child (
#           parent=(), 
#           n_attr=0, 
#           child=(), 
#           nodes_discovered=set(), 
#           nodes_parents=set(),
#       )
#           if error - raise HTMS_Mid_Err : 227-229
#       tree_DOT()
#           create attributes for instance - 
#           DOT_vertexes and DOT_edges (in the DOT language format)
#       
#
# Table(MAF) class      
#   
#   Table(
#         ht_root ='', 
#         ht_name ='', 
#         t_name='', 
#         t_nmaf=0,
#         local_root=''
#         )
#           
#    @classmethods
#
#       getinstances()
#       removeinstances(obj)
#
#    methods (commonly used)
#
#       row(fun='add', after =-1, number=1, data= b'')       
#           return True 
#           or 
#           if error - raise HTMS_Mid_Err : 07
#       sieve(with_fields = {}, modality ='all', res_num = Types_htms.max_int4) 
#           parameters
#               with_fields = { ( attr_name|field_name : ( oper, value1, value2|none), ...., 
#                            attr_name|field_name : ( oper, value1, value2|none) }
#               oper:  ==/ != / >= / <=/ in / not in / find ....
#           return set(rows) 
#           or 
#           if error - raise HTMS_Mid_Err : 10-20                  
#       update_fields(add_fields=set(), del_fields=set() )
#           return True 
#           or 
#           if error - raise HTMS_Mid_Err : 21-32
#       update_row (row_num =0, add_data={}, delete_data= set())           
#           return True 
#           or 
#           if error - raise HTMS_Mid_Err : 34-42
#       delete_row (nrow =0, protect_link= Links_array(()), protect_weight= Links_array(())
#           return True 
#           or 
#           if error - raise HTMS_Mid_Err : 44-56
#       update_links (row_num =0, attr_name='', add_links={}, delete_links= {} ) 
#           return True 
#           or 
#           if error - raise HTMS_Mid_Err : 60-90             
#       single_link (row_num =0, attr_name='', to_table ='', to_row=0 )
#       multi_link (row_num =0, attr_name='', to_table ='', to_rows=() )
#       update_weights (row_num =0, attr_name='', update_weights={}, delete_weights= {}) 
#           return True 
#           or 
#           if error - raise HTMS_Mid_Err : 160-200             
#       single_weight (row_num =0, attr_name='', to_table ='', to_row=0 )
#       multi_weight (row_num =0, attr_name='', to_table ='', to_rows=() ) 
#       copy_table(new_table_name='', only_data= True, 
#                  links_fields='blanc', weights_fields='blanc',
#                  only_fields=set(), with_fields={}
#       )
#           return True 
#           or 
#           if error - raise HTMS_Mid_Err : 101-123             
#       copy_row(nrow=-1, after_row=-1, 
#                links_fields='blanc', weights_fields='blanc', 
#                only_fields=set()
#       )
#           return True 
#           or 
#           if error - raise HTMS_Mid_Err : 143-154             
#       read_rows(rows=set(), from_row=1, quantity=1,   
#                 links_fields=False,  weights_fields=False, 
#                 only_fields=set()
#        )
#           return {rows} 
#           or 
#           if error - raise HTMS_Mid_Err : 210-217 
#        row_tree (nrow=-1, levels_source=-1, levels_ref=-1, 
#                  data_type='all', only_fields=set()
#        )   
#           return {rows} 
#           or 
#           if error - raise HTMS_Mid_Err : 300-328 
#


import os
import posixpath
import pickle
import struct  
import weakref
import copy
import time
import os.path as path
import time
  
from cage_api import *

from htms_low_api  import *

from .htms_par_middle   import    *                              


Mod_name = "*" + __name__

#------------------------------------------------------------------------------------------------

class HTdb ( HT):

    _instances = set()

    @classmethod
    def getinstances(cls):
        dead = set()
        for ref in cls._instances:
            obj = ref()
            if obj is not None:
                yield obj
            else:
                dead.add(ref)
        cls._instances -= dead

    # -------------------------------------------------------------------------------------------

    @classmethod
    def removeinstances(cls, obj):
        dead = {weakref.ref(obj)}
        if dead == set():
            pass
        else:
            cls._instances -= dead
            del obj

    # ------------------------------------------------------------------------------------------------

    def __str__(self):
        return self.db_name

    def __setattr__(self, nam, val):        
        if nam == 'can_setattr': 
            self.__dict__[nam] = val
        elif self.can_setattr == False:
            self.__dict__[nam] = val
        #elif nam == 'relations':
           # if type( val ).__name__ == 'dict'  and  len ( val ) > 0:
              #  for l in val:
                 #   self.relations.update ( val )      
        elif nam == 'channels':                                                                       # !!!!!!!!!!!!!!!!!!         01/05/20
            if type( val ).__name__ == 'dict'  and  len ( val ) > 0: 
                for l in val:                                                                                    #
                    self.channels.update ( val )                                                      #
        elif val in Types_htms.types.keys():
            # MAX_GENERIC_ATTR_NUM
            new_attr = {}
            new_attr[ nam ] = val
            self.ht.add_ht_attrs( new_attr )
      
        else:
            self.__dict__[nam] = val

    def __init__(self,  server = '', db_root = '', db_name='', cage_name='', 
                 new = False, jwtoken='', zmq_context = False, from_subclass=False,
                 mode='wm', local_root=""):

        self.can_setattr= False

        if db_name =='':
            self.db_name = type (self ).__name__
        else:
            self.db_name = db_name

        for db  in HT.getinstances():
            if db.closed:
                continue
            if  hasattr(db, 'ht_name') and\
                db.server_ip == server and\
                db.ht_name == self.db_name and\
                db.ht_root == db_root:
                if db.mode == 'rs':
                    if mode == 'rs':
                        pr ('240 HTMS_Mid_Err   W A R N I N G !!!  Data base  "%s" already exist and opened readonly. '
                            % db_name )
                    else:
                        pr ('240 HTMS_Mid_Err     Data base  "%s" already exist and opened, that is incompatible with exclusive status.'% db_name )
                        raise HTMS_Mid_Err('240 HTMS_Mid_Err     Data base  "%s" already exist and opened, that is incompatible with exclusive status.'% db_name  )
                        
                else:
                    pr ('241 HTMS_Mid_Err     Data base  "%s" already exist and opened in incompatible mode "%s". '%
                       (db_name, db.mode) )
                    raise HTMS_Mid_Err('241 HTMS_Mid_Err     Data base  "%s" already exist and opened in incompatible mode "%s". '%
                       (db_name, db.mode) )

        if not from_subclass:
            self.weak= weakref.ref(self)
            self._instances.add(self.weak)
            self.__class__._instances.add(self.weak)   #  self.__class__._instances.add(self.weak) 
        else:
            weak= self._instances
            self.__class__._instances.update(weak)

        self.ht=  super( HTdb, self)      
        self.ht._instances.add(self.weak)          
        
        if new == True:
            try:
 
                self.ht.__init__( ht_name =self.db_name,  server_ip = server, ht_root = db_root, 
                                  cage_name=cage_name, new = True, jwtoken=jwtoken, 
                                  zmq_context = zmq_context, from_subclass=True,
                                  mode=mode, local_root=local_root)                  
                Kerr=[]
                self.relations= {}

                rc = self.ht.attribute(Kerr, fun='add', attr_name='Back_links', type= '*link')
                if rc  == False or is_err( Kerr ) >= 0 :      
                    pr ('242 HTMS_Mid_Err     Error create "Back_links" HT atrribute. ')
                    raise HTMS_Mid_Err('242 HTMS_Mid_Err     Error create "Back_links" HT atrribute. ' ) 
                
                rc = self.ht.attribute(Kerr, fun='add', attr_name='Time_row', type= 'time')                
                if rc  == False or is_err( Kerr ) >= 0 :      
                    pr ('243 HTMS_Mid_Err     Error create "Time_row" HT atrribute. ')
                    raise HTMS_Mid_Err('243 HTMS_Mid_Err     Error create "Time_row" HT atrribute. ' )  

                rc = self.ht.attribute(Kerr, fun='add', attr_name='Back_weights', type= '*link')                 #weight
                if rc  == False or is_err( Kerr ) >= 0 :      
                    pr ('244 HTMS_Mid_Err     Error create "Back_weights" HT atrribute. ')
                    raise HTMS_Mid_Err('244 HTMS_Mid_Err     Error create "Back_weights" HT atrribute. ' )  

                self.ht.save_adt(  Kerr)

            except HTMS_Low_Err as errcode:
                pr ('245 HTMS_Mid_Err     Data base  " %s " create error from low level. Error code: %s'%   \
                    (self.db_name, str(errcode) ) )
                raise HTMS_Mid_Err('245 HTMS_Mid_Err     Data base  " %s " create error from low level. Error code: %s'%    \
                    (self.db_name, str(errcode) ) )
        else:
            try:
                self.ht.__init__( ht_name =self.db_name,  server_ip = server, ht_root = db_root,
                                  cage_name=cage_name,  new = False, jwtoken=jwtoken, 
                                  from_subclass=True,
                                  local_root=local_root,
                                  mode=mode)

            except HTMS_Low_Err as errcode:
                    pr ('246 HTMS_Mid_Err     HTdb " %s " open error from low level. Error code: %s'%    \
                    (self.db_name, str(errcode) )  )
                    raise HTMS_Mid_Err('246 HTMS_Mid_Err     HTdb " %s " open error  from low level. Error code: %s'%   \
                    (self.db_name, str(errcode) ) )
            if not ('relations' in self.__dict__ ):
                self.relations= {}

        self.can_setattr= True

#------------------------------------------------------------------------------------------------

    def close(self, Kerr=[]):
        HTdb.removeinstances(self)
        ht_ =  super( HTdb, self)
        try:
            rc= ht_.close(Kerr=Kerr)
        except:
            pr ('230 HTMS_Mid_Err     HTdb " %s " close error from low level.'% self.db_name )
            raise HTMS_Mid_Err('230 HTMS_Mid_Err     HTdb " %s " close error from low level.'% self.db_name  )
        try:
            del self
        except:
            pass
        return rc

    def __del__(self):
        try:
            self.close()
        except:
            pass

#------------------------------------------------------------------------------------------------

    def relation (self, dic ):
            if type( dic ).__name__ == 'dict'  and  len ( dic ) > 0:
                self.relations.update ( dic )    
                
#----------------------------------------------------------------------------------------------------

    def correct_back_links (self, maf_num = 0, row_num= set(), 
                            back_link  = (),  
                            func = 'add', typ='*link'):
    #  maf_num - is maf needed to correct back link(-s) 
    #  row_num - row or rows set or 'all' - is row(-s) needed to correct back link(-s)
    #  back_link = (self.maf_num, row_num) - element of back_link to add or delete
    #  OR
    #  back_link = (self.maf_num, row_num, weight) - element of back_link to add or delete  #weight
    #  func = 'add' or 'delete'
    #  typ  = '*link'  or '*weight'                                                             #weight
        maf_row_num =0
        if  ( type( maf_num ).__name__ == 'str'  and maf_num == 'all' ) or  \
            type( maf_num ).__name__ == 'set':
            if  not ( row_num == set() or ( type( row_num ).__name__ == 'str'  and row_num == 'all')  ):
                pr ('250 HTMS_Mid_Err     Invalid parameter "row_num". Must be empty set or "all". ')
                raise HTMS_Mid_Err('250 HTMS_Mid_Err     Invalid parameter "row_num". Must be empty set or "all".' )  

            maf_row_num = 'all'
            if  ( type( maf_num ).__name__ == 'str'  and maf_num == 'all' ) :
                nmafs = set (  self.mafs.keys() )
            else:
                nmafs = maf_num
        else:
            nmafs = { maf_num }

        if  func == 'delete':
            if  type ( back_link [ 1 ]  ).__name__ == 'str'  and  back_link [ 1 ] == 'all' :
                correct_link = ( - back_link [ 0 ],  0  ) 
            else:
                correct_link = (   back_link [ 0 ],  - back_link [ 1]  ) 
        else:
            correct_link = back_link 

        for nmaf in nmafs:

            if self.mafs[ nmaf ][ 'opened' ] == False:
                maf = MAF( self, maf_name = self.mafs [ nmaf ][ 'name' ] )
                temp_open_maf = True
            else:
                maf = self.mafs_opened [ nmaf ]
                temp_open_maf = False

            if  (  type( row_num).__name__ == 'str' and  row_num == 'all' ) or \
                maf_row_num == 'all' :
                rows = set ( i for i in range ( 1, self.mafs[ nmaf ][ 'rows' ] +1 ) )
            elif     type ( row_num).__name__ == 'int':
                rows = { row_num }
            elif  type ( row_num).__name__ == 'set':  #  and  row_num != set() :
                rows =  row_num 
            else:
                pr ('251 HTMS_Mid_Err    Error in parameters types. ')
                raise HTMS_Mid_Err('251 HTMS_Mid_Err     Error in parameters types.' )  

            for row in rows:
                Kerr=[]
                if typ  == '*link': 
                    rc = maf.u_links ( Kerr, attr_num=1, num_row=row,  u_link = correct_link )     
                            # u_link = () - clear all element   
                            # u_link =(nmaf, 0) - set row =0 - indicate link to whole maf
                            # u_link =(-nmaf, *) - delete all links to maf
                            # u_link =(nmaf,-num_row,) - delete one link to maf , to  num_row
                            # u_link =(nmaf,num_row,) -  add  link to maf - num_row, if not exist
                    if rc == False  or  is_err( Kerr ) >= 0 : 
                        pr ('252 HTMS_Mid_Err    Error write new back links in Maf : "%s", row = %d.' \
                            % ( self.mafs[ nmaf ][ 'name' ], row ) )
                        raise HTMS_Mid_Err('252 HTMS_Mid_Err     Error write new back links in Maf : "%s", row = %d.' \
                            % ( self.mafs[ nmaf ][ 'name' ], row ) )

                else:                                                             #weight
                    rc = maf.u_links ( Kerr, attr_num=3, num_row=row,  u_link = correct_link )     

                    if rc == False  or  is_err( Kerr ) >= 0 : 
                        pr ('253 HTMS_Mid_Err    Error write new back weights in Maf : "%s", row = %d.' \
                            % ( self.mafs[ nmaf ][ 'name' ], row ) )
                        raise HTMS_Mid_Err('253 HTMS_Mid_Err     Error write new back weights in Maf : "%s", row = %d.' \
                            % ( self.mafs[ nmaf ][ 'name' ], row ) )

            if    temp_open_maf == True:
                maf.close()

        self.updated = time.time()

        return True

#------------------------------------------------------------------------------------------------------------------

    def erase_attribute (self, attr=0):

        if self.mode in ('rs', 'rm' ):
            pr ('220 HTMS_Mid_Err    Mode "%s" incompatible for erase_attribute for HT "%s".'
                    % (self.mode, self.ht_name)
            )
            raise HTMS_Mid_Err('220 HTMS_Mid_Err    Mode "%s" incompatible for erase_attribute for HT "%s".'
                    % (self.mode, self.ht_name)
            )
        if attr==0 or attr=='':
            return True
        nattr=0
        if type(attr).__name__=='int':
            if attr in self.attrs:
               name_attr= self.attrs [attr]['name']
               if name_attr in self.relations and self.relations[ name_attr ] == 'erased' :
                    pr ('221 HTMS_Mid_Err    Attribute already erased. ')
                    raise HTMS_Mid_Err('221 HTMS_Mid_Err     Attribute already erased.' )  
               else:
                   nattr= attr                     
            else:
                pr ('222 HTMS_Mid_Err    Invalid number of erasing attribute.  ')
                raise HTMS_Mid_Err('222 HTMS_Mid_Err      Invalid number of erasing attribute. ' )  

        elif type(attr).__name__=='str':
            for at in self.attrs:
                if attr ==  self.attrs [at]['name']:
                    if attr in self.relations and self.relations[attr ] == 'erased' :
                        pr ('223 HTMS_Mid_Err    Attribute already erased. ')
                        raise HTMS_Mid_Err('223 HTMS_Mid_Err     Attribute already erased.' )  
                    nattr= at
                    name_attr= attr
                    break
            if nattr ==0:
                pr ('224 HTMS_Mid_Err     Invalid name of erasing attribute. ')
                raise HTMS_Mid_Err('224 HTMS_Mid_Err       Invalid name of erasing attribute. ' )  
        else:
            pr ('225 HTMS_Mid_Err     Invalid type of parameter "attr". Must be int or str. ')
            raise HTMS_Mid_Err('225 HTMS_Mid_Err      Invalid type of parameter "attr". Must be int or str. ' )  

        for maf in self.mafs:
            if self.mafs [ maf ]['name'][ : 8] != 'deleted:':
                if self.models [maf ] [ nattr ] :
                    table = Table( ht_root=self.ht_root, ht_name =self.ht_name, 
                                  t_nmaf=maf, local_root= self.local_root )
                    table.update_fields ( del_fields={name_attr,} )

        self.relations[ name_attr ] = 'erased'
        t =  time.time()
        self.attrs[ nattr ]['update'] = t

        self.updated = t

        return True

# -------------------------------------------------------------------------------------------

    def update_RAM (self, fun='', attr_num_p=0, maf_num_p=0,  after_row =-1 , num_rows=0 ):

        if self.mode in ('rs', 'rm' ):
            pr ('270 HTMS_Mid_Err    Mode "%s" incompatible for update_RAM for HT "%s".'
                    % (self.mode, self.ht_name)
            )
            raise HTMS_Mid_Err('270 HTMS_Mid_Err    Mode "%s" incompatible for update_RAM for HT "%s".'
                    % (self.mode, self.ht_name)
            )

        delay_kerr =0

        if DEBUG_UPDATE_RAM:
            pr('\n\n  UPDATE_RAM ---   fun = %s, attr_num_p=%d, maf_num_p=%d,  after_row = %d, num_rows = %d' %  \
                (fun , attr_num_p, maf_num_p,  after_row, num_rows ))     

        atr_remove      =False
        maf_remove      =False
        field_remove    =False
        row_add         =False
        row_delete      =False

        if fun == 'atr_remove':           
            atr_remove      = True
            Kerr=[]
            set_warn_int (Kerr, Mod_name, 'update_RAM '+self.ht_name,  1 , message='atr_remove not supported in this version of HTms.' )
            return False
        if fun == 'maf_remove':         
            maf_remove     = True
        if fun == 'field_remove':         
            field_remove   = True
        if fun == 'row_add':                
            row_add          = True
        if fun == 'row_delete':            
            row_delete      = True

        if  len (self.mafs) >0:
            max_maf_num=  max ( self.mafs.keys() )
        else:
            max_maf_num=  0

        if len ( self.attrs) >0:
            max_attr_num =   max ( self.attrs.keys() )
        else:
            max_attr_num = 0

        if row_add or  row_delete:
            if  num_rows  ==0:
                return True
            else:
                if row_add:
                    old_max_row_num = self.mafs [ maf_num_p ]['rows'] -  num_rows 
                else:
                    old_max_row_num = self.mafs [ maf_num_p ]['rows'] +  num_rows 

        if atr_remove:
            if attr_num_p < 1 or attr_num_p > max_attr_num:
                set_err_int (Kerr, Mod_name, 'update_RAM '+self.ht_name,  2 , message='attr_num_p out of range.' )
                return False
            maf_num_p= 0
            after_row = 0
        else :
            if max_maf_num== 0:
                return True
            elif (maf_num_p < 0 or maf_num_p> max_maf_num):
                set_err_int (Kerr, Mod_name, 'update_RAM '+self.ht_name,  3 , message='maf_num_p out of range.' )
                return False

            if  maf_remove:
                attr_num_p = 0  # apply to all fields of maf
                after_row = 0         # apply to all rows of maf

            else:
                if field_remove:
                    if (attr_num_p < 1 or attr_num_p > max_attr_num):
                        set_err_int (Kerr, Mod_name, 'update_RAM '+self.ht_name,  4 , message='attr_num_p out of range. ')
                        return False   
                    after_row = 0      # apply to all rows of maf

                if (row_add  or  row_delete) :
                    if (after_row < 0 or after_row >old_max_row_num):   
                        set_err_int (Kerr, Mod_name, 'update_RAM '+self.ht_name,  6 , message='after_row out of range.' )
                        return False   
                    if ( num_rows  < 1):
                        set_err_int (Kerr, Mod_name, 'update_RAM '+self.ht_name,  7 , message=' num_rows  out of range.' )
                        return False   

        # structure of each update_fields in cf - each elem - packed in 4 bytes integer
        #                                   |------------array of links----------------------|
        #  dim-nmaf-attr_num-nrow-{nmaf-nrow nmaf-nrow ....  nmaf-nrow} FFF
        #  |---- descriptor--------|          0                1                (num_rows-1)        | end marker b'\xFFFFFFFFFFFFFFFF'
	    #  |--source of update_fields--|
 
        if (maf_remove or atr_remove or row_delete or row_add or field_remove):

            for link_array in Links_array.getinstances():
                for link in link_array.get_dyna_links():
                    #pr(str(link))
                    #continue
                    maf = link[ 0 ] 
                    row = link[ 1 ] 
                    if len (link) == 3:
                        weight= link[ 2 ]
                    else:
                        weight=None
                    if maf ==0 or row ==0: # link is zero or link to all maf
                        link_array.remove_dyna_link ( (maf,row) )
                        continue
                    if (maf != 0 and maf == maf_num_p) :	# MAF matched
                        if (maf_remove) :	# removed MAF matched                         
                            link_array.remove_dyna_link ( (maf,row) )
                        elif (row_delete ) :
                            if ( row > (after_row +  num_rows ) and row <= old_max_row_num ) :
									            # MAF row after deleted range - 
										        # correct number 
                                if weight==None:
                                    link_array.update_dyna_link ( 
                                        (maf,row), 
                                        (maf, row-num_rows,) )
                                else:
                                    link_array.update_dyna_link ( 
                                        (maf,row), 
                                        (maf, row-num_rows, 
                                         weight) ) 
                            elif (row > after_row and row <= (after_row +  num_rows )) :
                                link_array.remove_dyna_link ( (maf,row) )
                            elif (row > old_max_row_num) :
                                                # found row number greater than last number - clear it
                                link_array.remove_dyna_link ( (maf,row) )
                                delay_kerr = 474
                            else:
                                continue
                        elif (row_add ) :
                            if ( row > after_row  and row <= old_max_row_num ) :  
                                # MAF row in deleted range  and saved- correct number 											    
                                if weight==None:
                                    link_array.update_dyna_link ( 
                                        (maf,row), 
                                        (maf, row+num_rows,) )
                                else:
                                    link_array.update_dyna_link ( 
                                        (maf,row), 
                                        (maf, row+num_rows, 
                                         weight) ) 
                            elif (row > old_max_row_num ) :  
                                # found row number greater than last number before adding new rows -  delete set 
                                link_array.remove_dyna_link ( (maf,row) )
                                delay_kerr = 475
                            else:
                                continue             

            if (delay_kerr !=0) :
                set_warn_int (Kerr, Mod_name, 'update_RAM '+self.ht_name, 8  , message='Link array delayed error No= %d.'%delay_kerr )
                return False     

            if DEBUG_UPDATE_RAM:
                pr('  UPDATE_RAM ---   FINISH' )     

            return True
        else:
            set_err_int (Kerr, Mod_name, 'update_RAM '+self.ht_name, 9  , message='wrong function.' )
        return False 
    
# -------------------------------------------------------------------------------------------

class Links_array ( object ):

    _instances = set()

    def __init__ ( self, links =() ):

        self._instances.add(weakref.ref(self))

        if  not (type( links ).__name__ == 'tuple' or  type( links ).__name__ == 'list'):
            pr ('200 HTMS_Mid_Err      Links_array parameters error' )
            raise HTMS_Low_Err ('200 HTMS_Mid_Err     Links_array parameters error')

        self.dyna_link = {}
            
        if links == () or links == []:
            return

        set_links = set ( links )

        ind=0
        for  li in set_links:
            if li == () or li[ 0 ] == 0 or li[ 1 ] == 0:
                continue
            self.dyna_link [ ind ] =  li
            ind += 1

    def __str__(self):

        links=""
        for ind in  self.dyna_link:
            link =  self.dyna_link[ ind ]
            if  link[ 0 ] > 0 and link [ 1 ] > 0: 
                links+=str(ind)+":("+str(link[0])+", "+str(link[1])
                if len(link)==3:
                    links+=", "+str(link[2])
                links+="), "
        return links

#------------------------------------------------------------------------------------------------

    @classmethod
    def getinstances(cls):
        dead = set()
        for ref in cls._instances:
            obj = ref()
            if obj is not None:
                yield obj
            else:
                dead.add(ref)
        cls._instances -= dead

# -------------------------------------------------------------------------------------------

    @classmethod
    def removeinstances(cls, obj):
        dead = {weakref.ref(obj)}
        if dead == set():
            pass
        else:
            cls._instances -= dead
            del obj

#------------------------------------------------------------------------------------------------

    def get_dyna_links (self):
        for ind in  self.dyna_link:
            link =  self.dyna_link[ ind ]
            if  link[ 0 ] > 0 and link [ 1 ] > 0: 
                yield link
        
# -------------------------------------------------------------------------------------------

    def get_dyna_link (self, ind):
        if ind in  self.dyna_link:
            link =  self.dyna_link[ ind ]
            if  link[ 0 ] > 0 and link [ 1 ] > 0: 
                return link
        return ()
# -------------------------------------------------------------------------------------------

    def get_dyna_index (self,link):

        if link == () or link[ 0 ] == 0 or link[ 1 ] == 0:
            return
                                                            #weight
        if len (  self.dyna_link ) > 0:            
            for ind in  self.dyna_link:
                if link[0:2] == self.dyna_link [ind] [0:2]:
                    return ind    
        return  -1
        
# -------------------------------------------------------------------------------------------

    def add_dyna_link (self, link):

        if link == () or link[ 0 ] == 0 or link[ 1 ] == 0:
            return False

        ind= self.get_dyna_index (link)
        if ind>-1:
            return False
        elif len (  self.dyna_link ) > 0:
            ind = max ( self.dyna_link.keys() )
        
        self.dyna_link [ ind+1 ] =  link
        return True

# -------------------------------------------------------------------------------------------

    def update_dyna_link (self, link, new):

        if new == () or new[ 0 ] == 0 or new[ 1 ] == 0:
            return

        if len ( self.dyna_link ) > 0:            
            for ind in  self.dyna_link:
                if link[0:2] == self.dyna_link [ind] [0:2]:
                    self.dyna_link [ ind ] = new
                    return
        pr ('205 HTMS_Mid_Err     Link update impossible' )
        raise HTMS_Low_Err ('205 HTMS_Mid_Err     Link update impossible')

# -------------------------------------------------------------------------------------------

    def remove_dyna_link (self, link):
        for ind in  self.dyna_link:
            if link[0:2] == self.dyna_link [ind] [0:2]: 
                self.dyna_link [ ind ]= (0,0)

# -------------------------------------------------------------------------------------------

    def remove_dyna_ind (self, ind):
        if ind in  self.dyna_link:
            self.dyna_link [ ind ]= (0,0)

# -------------------------------------------------------------------------------------------

    def get_dyna_rows (self, nmaf ):
        rows=()
        for ind in  self.dyna_link:
            link =  self.dyna_link[ ind ]
            if  link[ 0 ] == nmaf: 
                rows= rows +( link[1], )
        return rows

# -------------------------------------------------------------------------------------------

    def get_first_dyna_row (self, nmaf ):
        row=0
        for ind in  self.dyna_link:
            link =  self.dyna_link[ ind ]
            if  link[ 0 ] == nmaf: 
                row=  link[1]
                break
        return row

# -------------------------------------------------------------------------------------------

class Links_tree ( object ):

    _instances = set()

    def __init__ ( self, ht_name, root_link, tree_name=''):

        self._instances.add(weakref.ref(self))

        self.ht_name= ht_name
        self.name= tree_name        
        self.root_nmaf= root_link[0]
        self.root_nrow=root_link[1]

        self.maf_name, root_maf_rows = get_maf( ht_name, self.root_nmaf )

        if self.name=='':
            self.name=self.ht_name+' '+self.maf_name+' '+str(self.root_nrow)

        if self.maf_name=='' or root_maf_rows ==0 or self.root_nrow>root_maf_rows :
            pr ('206 HTMS_Mid_Err     Links_tree init parameters error' )
            raise HTMS_Low_Err ('206 HTMS_Mid_Err     Links_tree init parameters error')

        self.nodes_links = Links_array()
        self.nodes_links.add_dyna_link(root_link)
        self.links_tree={}
        self.links_tree[0]={'maf_name':self.maf_name}

#------------------------------------------------------------------------------------------------

    @classmethod
    def getinstances(cls):
        dead = set()
        for ref in cls._instances:
            obj = ref()
            if obj is not None:
                yield obj
            else:
                dead.add(ref)
        cls._instances -= dead

# -------------------------------------------------------------------------------------------

    @classmethod
    def removeinstances(cls, obj):
        dead = {weakref.ref(obj)}
        if dead == set():
            pass
        else:
            cls._instances -= dead
            del obj

#------------------------------------------

    def add_child (self, parent=(), n_attr=0, child=(), 
                   nodes_discovered=set(), nodes_parents=set(),
                   ):
        #recursion=False
        if parent == () or parent[ 0 ] == 0 or parent[ 1 ] == 0 or\
            child == () or child[ 0 ] == 0 or child[ 1 ] == 0 or \
            type(n_attr).__name__!='int' or n_attr==0: 
            pr ('207 HTMS_Mid_Err     Links_tree_add_child parameters error' )
            raise HTMS_Low_Err ('207 HTMS_Mid_Err     Links_tree_add_child parameters error')

        child_maf= child[0]
        child_row=child[1]

        child_maf_name, child_maf_rows = get_maf( self.ht_name, child_maf )

        if child_maf_name=='' or child_maf_rows ==0 or child_row>child_maf_rows :
            pr ('208 HTMS_Mid_Err     Links_tree_add_child parameters error' )
            raise HTMS_Low_Err ('208 HTMS_Mid_Err    Links_tree_add_child parameters error')

        parent_found=set()

        root_link = self.nodes_links.get_dyna_link (0)
        if root_link ==parent:
            parent_found.add(0)

        for node in  self.links_tree:

            node_link = self.nodes_links.get_dyna_link (node)
            if node_link == parent :
                parent_found.add(node)

        if not parent_found:
            pr ('209 HTMS_Mid_Err     Links_tree_add child error: "parent" not found in tree' )
            raise HTMS_Low_Err ('209 HTMS_Mid_Err     Links_tree add_child error: "parent" not found in tree' )

        else:
            for node in  parent_found:

                if child not in nodes_discovered and child not in nodes_parents:  
                    self.nodes_links.add_dyna_link(child)
                ind= self.nodes_links.get_dyna_index (child)      

                if self.links_tree!={} and \
                    node in self.links_tree and\
                    n_attr in self.links_tree[node]:

                    existing_nodes=self.links_tree[node][n_attr ]
                    existing_nodes.add(ind)
                    self.links_tree[node].update( 
                        {n_attr : existing_nodes } )
                else:
                    if self.links_tree=={}:
                         self.links_tree[node]={n_attr : { ind, } }
                    else:
                        self.links_tree[node].update( {n_attr : { ind, } })

                if child not in nodes_discovered and child not in nodes_parents :           
                    self.links_tree.update({
                        ind : {'maf_name':child_maf_name} })              

        return

# -------------------------------------------------------------------------------------------

    def tree_DOT (self):

        self.DOT_vertexes={}
        self.DOT_edges=set()

        ht_name= self.ht_name
        table_name=  self.maf_name

        for node in self.links_tree:
                for source_nattr in self.links_tree[node]: 
                    # source_nattr - is the number of link field attribute                    
                    if source_nattr =='maf_name':
                        source_maf_name=self.links_tree[node]['maf_name']
                        #source_n_attrs=set(self.links_tree[node].key()).delete('maf_name')
                        link=self.nodes_links.get_dyna_link (node)
                        source_nmaf = link[0]
                        source_nrow = link[1]                        
                        source_vertex_name= source_maf_name+'#'+ str(source_nrow) 
                        self.DOT_vertexes[source_vertex_name]= \
                            (source_nmaf, source_nrow) # , source_n_attrs)
                            
                    else: #
                        refs_from_source_nattr = self.links_tree[node][source_nattr]
                        for ref in refs_from_source_nattr:
                            ref_maf_name= self.links_tree[ ref ] ['maf_name']
                            #ref_n_attrs=set(self.links_tree[ref].source_nattrs()).discard('maf_name')
                            link=self.nodes_links.get_dyna_link (ref)
                            ref_nmaf = link[0]
                            ref_nrow = link[1]
                            ref_vertex_name= ref_maf_name+'#'+str( ref_nrow  ) 
                            self.DOT_vertexes[ref_vertex_name]= \
                                ( ref_nmaf, ref_nrow)  #, ref_n_attrs)               
                            self.DOT_edges.add ( 
                                ( source_vertex_name, ref_vertex_name, source_nattr ))

        return True

#------------------------------------------------------------------------------------------------

class Weights_tree ( object ):

    _instances = set()

    def __init__ ( self, ht_name, root_link, tree_name=''):

        self._instances.add(weakref.ref(self))

        self.ht_name= ht_name
        self.name= tree_name        
        self.root_nmaf= root_link[0]
        self.root_nrow=root_link[1]
        #self.root_weight=root_link[2]

        self.maf_name, root_maf_rows = get_maf( ht_name, self.root_nmaf )

        if self.name=='':
            self.name=self.ht_name+' '+self.maf_name+' '+str(self.root_nrow)

        if self.maf_name=='' or root_maf_rows ==0 or self.root_nrow>root_maf_rows :
            pr ('226 HTMS_Mid_Err     Weights init parameters error' )
            raise HTMS_Low_Err ('226 HTMS_Mid_Err     Weights init parameters error')

        self.nodes_links = Links_array()
        self.nodes_links.add_dyna_link(root_link)
        self.weights_tree={}
        self.weights_tree[0]={'maf_name':self.maf_name}

#------------------------------------------------------------------------------------------------

    @classmethod
    def getinstances(cls):
        dead = set()
        for ref in cls._instances:
            obj = ref()
            if obj is not None:
                yield obj
            else:
                dead.add(ref)
        cls._instances -= dead

# -------------------------------------------------------------------------------------------

    @classmethod
    def removeinstances(cls, obj):
        dead = {weakref.ref(obj)}
        if dead == set():
            pass
        else:
            cls._instances -= dead
            del obj

#------------------------------------------

    def add_child (self, parent=(), n_attr=0, child=(), weight=None,
                   nodes_discovered=set(), nodes_parents=set()):
        #recursion=False
        if parent == () or parent[ 0 ] == 0 or parent[ 1 ] == 0 or\
            child == () or child[ 0 ] == 0 or child[ 1 ] == 0 or \
            type(n_attr).__name__!='int' or n_attr==0: 
            pr ('227 HTMS_Mid_Err     Weights_tree_add_child parameters error' )
            raise HTMS_Low_Err ('227 HTMS_Mid_Err     Weights_tree_add_child parameters error')

        child_maf= child[0]
        child_row=child[1]

        child_maf_name, child_maf_rows = get_maf( self.ht_name, child_maf )

        if child_maf_name=='' or child_maf_rows ==0 or child_row>child_maf_rows :
            pr ('228 HTMS_Mid_Err     Weights_tree_add_child parameters error' )
            raise HTMS_Low_Err ('228 HTMS_Mid_Err    Weights_tree_add_child parameters error')

        parent_found=set()

        root_link = self.nodes_links.get_dyna_link (0)
        if root_link ==parent:
            parent_found.add(0)

        for node in  self.weights_tree:

            node_link = self.nodes_links.get_dyna_link (node)
            if node_link == parent :
                parent_found.add(node)

        if not parent_found:
            pr ('229 HTMS_Mid_Err     Weights_tree_add child error: "parent" not found in tree' )
            raise HTMS_Low_Err ('229 HTMS_Mid_Err     Weights_tree add_child error: "parent" not found in tree' )

        else:
            for prnt in  parent_found:

                if child not in nodes_discovered and \
                   child not in nodes_parents:  
                    self.nodes_links.add_dyna_link(child)

                ind= self.nodes_links.get_dyna_index (child)      

                if self.weights_tree!={} and \
                    prnt in self.weights_tree and\
                    n_attr in self.weights_tree[prnt]:

                    existing_nodes=self.weights_tree[prnt][n_attr ]
                    existing_nodes.update( {ind:weight} )
                    self.weights_tree[prnt].update( {n_attr : existing_nodes } )

                else:
                    if self.weights_tree=={}:
                         self.weights_tree[prnt]={ n_attr:{ ind:weight } }
                    else:
                        self.weights_tree[prnt].update({ n_attr: {ind:weight} } )

                if child not in nodes_discovered and \
                   child not in nodes_parents :           
                    self.weights_tree.update({
                        ind : {'maf_name':child_maf_name} })              

        return

# -------------------------------------------------------------------------------------------

    def tree_DOT (self):

        self.DOT_vertexes={}                                               
        self.DOT_edges=set()

        ht_name= self.ht_name
        table_name=  self.maf_name

        for node in self.weights_tree:
                for source_nattr in self.weights_tree[node]: 
                    # source_nattr - is the number of link field attribute                    
                    if source_nattr =='maf_name':
                        source_maf_name=self.weights_tree[node]['maf_name']
                        #source_n_attrs=set(self.weights_tree[node].key()).delete('maf_name')
                        link=self.nodes_links.get_dyna_link (node)
                        source_nmaf = link[0]
                        source_nrow = link[1]  

                        source_vertex_name= source_maf_name+'#'+ \
                                str(source_nrow) 
                        self.DOT_vertexes[source_vertex_name]= \
                            (source_nmaf, source_nrow) 
                            
                    else: #
                        refs_from_source_nattr = self.weights_tree[node][source_nattr].keys()
                        for ref in refs_from_source_nattr:
                            ref_maf_name= self.weights_tree[ ref ] ['maf_name']
                            #ref_n_attrs=set(self.weights_tree[ref].source_nattrs()).discard('maf_name')
                            link=self.nodes_links.get_dyna_link (ref)
                            ref_nmaf = link[0]
                            ref_nrow = link[1]
                            source_weight = self.weights_tree[node][source_nattr][ref]
                            ref_vertex_name= ref_maf_name+'#'+str(ref_nrow)

                            self.DOT_vertexes[ref_vertex_name]= \
                                ( ref_nmaf, ref_nrow)  #, ref_n_attrs)               
                            self.DOT_edges.add ( 
                                ( source_vertex_name, ref_vertex_name,source_nattr, source_weight,  ))

        return True

#------------------------------------------------------------------------------------------------

class Table( MAF ):

    def __str__(self):
        return self.maf_name

    def __del__(self):
        try:
            maf_  =  super(Table, self)
            del maf_
        except:
            pass

    def __setattr__(self, nam, val):
        if nam == 'can_setattr': 
            self.__dict__[nam] = val
        elif self.can_setattr == False:
            return  False
        #self.__dict__[nam] = val
        elif nam == 'fields_add':
            self.update_fields ( val )
        elif nam == 'fields_delete':
            self.update_fields ( del_fields=val )  
        else:
            self.__dict__[nam] = val
   
    def __init__(self, ht_root ='', ht_name ='', 
                  t_name='', t_nmaf=0, local_root='',
                ):

        self.can_setattr= True

        for h_t  in HTdb.getinstances():
            if  h_t.ht_name == ht_name and\
                h_t.ht_root == ht_root and \
                local_root == local_root:
                ht_obj = h_t
                break

        if not ( 'ht_obj' in locals() ) :   
            pr ('02 HTMS_Mid_Err     HT " %s " not exist. '% ht_name )
            raise HTMS_Mid_Err('02 HTMS_Mid_Err     HT " %s " not exist. '% ht_name )

        maf_ =  super(Table, self)
        self.weak= weakref.ref(self)
        maf_._instances.add(self.weak) 

        if t_nmaf !=0:
            maf_.__init__( ht = ht_obj, maf_num = t_nmaf, from_subclass=True)
            table_new = False
        else:
            if t_name == '':
                table_name = type (self ).__name__
            else:
                table_name = t_name
            table_new = True
            for maf in ht_obj.mafs:
                if table_name ==  ht_obj.mafs [maf] ['name']:
                    table_new = False
            maf_.__init__( ht = ht_obj, maf_name = table_name, from_subclass=True)
        #self.can_setattr= True
        pass
    
        if table_new:

            if self.ht.mode in ('rs', 'rm' ):
                pr ('03 HTMS_Mid_Err    Mode "%s" incompatible for create new table for HT "%s".'
                        % (self.ht.mode, self.ht.ht_name)
                )
                raise HTMS_Mid_Err('03 HTMS_Mid_Err    Mode "%s" incompatible for create new table for HT "%s".'
                        % (self.ht.mode, self.ht.ht_name)
                )
            f_temp= {'Back_links', 'Time_row', 'Back_weights'}

            if 'fields' in dir(self):
                f_temp = set.union( f_temp, self.fields )

            self.fields ={}

            for f in f_temp:
                    found = False
                    for natr in self.ht.attrs:
                        if f == self.ht.attrs[ natr]['name']:
                            found = True
                            attr_num = natr
                            break
                    if not found:
                        kerr=[]
                        set_err_int (kerr, Mod_name, 'init '+self.ht.ht_name+'-'+  str(self.maf_num), 1 , \
                                message='Attribute "%s" not found during processing fields names from class when opening new MAF file %s .' % ( f, table_name ) )
                        raise HTMS_Mid_Err('04 HTMS_Mid_Err  Program error. ')
                    kerr=[]
                    self.field (kerr, fun='add', attr_num_f = attr_num )
                    if is_err( kerr ) >= 0 :      
                        set_err_int (kerr, Mod_name, 'init '+self.ht.ht_name+'-'+  str(self.maf_num), 2, \
                                message='Error during processing fields names from class when opening new MAF file %s .' % table_name )
                        raise HTMS_Mid_Err('05 HTMS_Mid_Err  Program error. ')
            kerr=[]
            if not self.ht.save_adt(kerr):
                set_err_int (Kerr, Mod_name,   'init '+self.ht.ht_name+'-'+ str(self.maf_num),  3 , \
                                message='Error save HTD.' )
                raise HTMS_Mid_Err('06 HTMS_Mid_Err  Program error. ')

        self.ht.updated = time.time()


# -------------------------------------------------------------------------------------------

    def row(self, Kerr=[], fun='add', after =-1, number=1, data= b''):

        if self.ht.mode in ('rs', 'rm' ) and fun !='info':
            pr ('07 HTMS_Mid_Err    Mode "%s" incompatible for add row operation in HT "%s".'
                    % (self.ht.mode, self.ht.ht_name)
            )
            raise HTMS_Mid_Err('07 HTMS_Mid_Err    Mode "%s" incompatible for row operation in HT "%s".'
                    % (self.ht.mode, self.ht.ht_name)
            )
        maf =  super( Table , self)

        rc =maf.row( Kerr = Kerr , fun=fun,  after =after, number=number, data= data)

        if rc == False:
            return False

        self.ht.updated = time.time()
        self.ht.mafs [ self.maf_num ][ 'updated' ]= self.ht.updated
        return True
# -------------------------------------------------------------------------------------------

    def get_RAM_instances( self ):
        if 'objects_RAM'  in  dir ( self ):
            dead = set()
            for ref in self.objects_RAM:
                obj = ref()
                if obj is not None:
                    yield obj
                else:
                    dead.add(ref)
            self.objects_RAM -= dead
        else:
            return None

#------------------------------------------------------------------------------------------------

    def sieve (self, with_fields = {}, modality ='all', res_num = Types_htms.max_int4) :
            # with_fields = { ( attr_name|field_name : ( oper, value1, value2|none), ...., 
            #                            attr_name|field_name : ( oper, value1, value2|none) }
            # oper:  ==/ != / >= / <=/ in / not in / find ....

            if with_fields =={}:
                pr ('10 HTMS_Mid_Err  The search query is not defined. ')        
                raise HTMS_Mid_Err('10 HTMS_Mid_Err  The search query is not defined. ')
            if  not (modality  in ( 'all' ,  'one') ) :
                pr ('11 HTMS_Mid_Err  The search query modality is not "all" and not "one". ')
                raise HTMS_Mid_Err('11 HTMS_Mid_Err  The search query modality is not "all" and not "one". ')
            found = False
            type_f = {}
            for f in with_fields:
                if f not in self.fields:
                    pr ('14 HTMS_Mid_Err  The field name "%s" in search query is missing in the with_fields\'s list of MAF. '% f)
                    raise HTMS_Mid_Err('14 HTMS_Mid_Err  The field name "%s" in search query is missing in the with_fields\'s list of MAF. '% f)
                else:
                    found = True
            if not found:
                pr ('15 HTMS_Mid_Err  No searchable field\'s with specified names found in the with_fields\'s list of MAF. ')
                raise HTMS_Mid_Err('15 HTMS_Mid_Err  No searchable field\'s with specified names found in the with_fields\'s list of MAF. ')
            
            res_n=0
            result =set()
            for row in range (1, self.rows +1):
                search_row = False
                for f in with_fields:

                        if self.fields [ f ][ 1 ] in ( '*link', 'file', '*weight') :                      #weight
                            continue
                        
                        attr_num =  self.fields[ f ][0]  
                        attr_type =  self.fields[ f ][1]  
                        is_num_array = False
                        is_string_array = False
                        is_bytes_array = False
                        kerr =[]
                        if attr_type[ : 4] == 'byte':
                            elem =  self.r_elem ( kerr, attr_num, row)             #  bytes array from MAF
                            is_bytes_array = True
                        elif attr_type[  : 3] == 'utf' or attr_type == 'datetime':
                            elem =  self.r_utf8 ( kerr, attr_num, row)                 #  utf-8 string from MAF
                            is_string_array = True
                        elif attr_type  in ('int4', 'int8', 'float4', 'float8', 'time'):
                            elem =  self.r_numbers ( kerr, attr_num, row)       # one number!!! from MAF
                        elif attr_type  in ('*int4', '*int8', '*float4', '*float8',):
                            elem =  self.r_numbers ( kerr, attr_num, row)       # tuple with numbers!!! from AF
                            is_num_array = True
                            if elem==None:
                                elem=()
                        elif attr_type  in ('*byte',):
                            elem =  self.r_bytes ( kerr, attr_num, row)       # bytes array from AF
                            is_bytes_array = True
                            if elem==None:
                                elem=b''
                        elif attr_type  in ('*utf',):
                            elem =  self.r_str ( kerr, attr_num, row)       # utf-8 string from AF
                            is_string_array = True
                            if elem==None:
                                elem=''
                        else:  #error
                            pass
                        if is_err( kerr ) >= 0 :      
                             pr ('16 HTMS_Mid_Err    Error read data.  err = %s' % str (kerr)  )
                             raise HTMS_Mid_Err('16 HTMS_Mid_Err    Error correct data.  err = %s' % str (kerr)  )

                        search = match ( with_fields[ f ], is_num_array, is_string_array, is_bytes_array, attr_type, elem)  # test row on actuality

                        if          search == -1:    
                            pr  ('17 HTMS_Mid_Err  Invalid parameters type in function match.')
                            raise HTMS_Mid_Err ('17 HTMS_Mid_Err  Invalid parameters type in function match.')
                        elif       search == -2:    
                            pr ('18 HTMS_Mid_Err  Invalid combination of parameters in function match.')
                            raise HTMS_Mid_Err ('18 HTMS_Mid_Err  Invalid combination of parameters in function match.')
                        elif       search == -3:    
                            pr ('19 HTMS_Mid_Err  Invalid compare function and/or parameters in search field')
                            raise HTMS_Mid_Err('19 HTMS_Mid_Err  Invalid compare function and/or parameters in search field')
                        elif       search == -4:    
                            pr ('20 HTMS_Mid_Err  Invalid sample for oper "in", "not", "find", when attr type - single number (not array).')
                            raise HTMS_Mid_Err('20 HTMS_Mid_Err  Invalid sample for oper "in", "not", "find", when attr type - single number (not array)/')
                        elif    search == 1:
                            if modality == 'one':
                                search_row =True
                                break
                            elif modality == 'all':
                                search_row =True
                                continue
                        # search ==0
                        elif modality == 'all':
                            search_row = False
                            break

                if search_row :
                    res_n += 1
                    if res_n <= res_num:
                        result = result | { row } 
                        if res_n == res_num:
                            return result

            return result

#------------------------------------------------------------------------------------------------------------------

    def update_fields (self, add_fields=set(), del_fields=set() ):
            # add_fields, del_fields= { attr_name|field_name,...., attr_name|field_name } - set

        if self.ht.mode in ('rs', 'rm' ):
            pr ('21 HTMS_Mid_Err    Mode "%s" incompatible for update fields n in HT "%s".'
                    % (self.ht.mode, self.ht.ht_name)
            )
            raise HTMS_Mid_Err('21 HTMS_Mid_Err    Mode "%s" incompatible for update fields in HT "%s".'
                    % (self.ht.mode, self.ht.ht_name)
            )

        num_temp_mafs_opened = 0
        temp_mafs_opened ={}     
        def close_temp_mafs():
                    nonlocal temp_mafs_opened
                    if temp_mafs_opened != {}:
                        for mf in  temp_mafs_opened :
                            temp_mafs_opened[ mf ].close()
                    del temp_mafs_opened
                    temp_mafs_opened ={}  
        def add_temp_mafs( self, temp_maf):
                    nonlocal temp_mafs_opened,  num_temp_mafs_opened
                    if  not ( temp_maf in self.ht.mafs_opened):
                            temp_mafs_opened[ temp_maf ] = Table(
                                ht_root=self.ht.ht_root, ht_name= self.ht.ht_name, 
                                t_nmaf= temp_maf, local_root=self.ht.local_root )
                            num_temp_mafs_opened += 1

        if len(add_fields) == 0 and len(del_fields) == 0:
            return True

        for attr in self.ht.attrs:
            if attr in (1,2,3):
                continue

            attr_name = self.ht.attrs [attr]['name']
            attr_type =   self.ht.attrs[ attr][ 'type' ]

            if attr_name in self.ht.relations and self.ht.relations[ attr_name ] == 'erased':
                continue

            if attr_name in add_fields:
                    add_fields.discard ( attr_name )
                    if  attr_name  in  self.fields:
                        pass
                    else:
                        kerr=[]
                        self.field (kerr, fun='add',  attr_num_f = attr )
                        if is_err( kerr ) >= 0 :      
                            pr ('24 HTMS_Mid_Err    Error create update_fields field. err = %s' % str (kerr) )
                            raise HTMS_Mid_Err('24 HTMS_Mid_Err    Error create update_fields field.  err = %s' % str (kerr)  )

            if attr_name in del_fields and not (attr_name in add_fields):
                del_fields.discard ( attr_name )
                if  attr_name  in  self.fields:
                    kerr=[]

                    if   attr_type == '*link' and  self.ht.mafs[self.maf_num]['rows'] >0:
                        if DEBUG_DELETE_FIELD_1:
                            if attr_name in self.ht.relations:
                                pr ('\n\n  DELETE_LINK FIELD ---- maf_name = %s,  attr_name =%s, link_type=%s' % \
                                    ( self.maf_name, attr_name, self.ht.relations[ attr_name ]  ) )
                            else:
                                pr ('\n\n  DELETE_LINK FIELD ---- maf_name = %s,  attr_name =%s, link_type= UNDEFINED' % \
                                    ( self.maf_name, attr_name ) )
                        row=1
                        self_row= Links_array( () )
                        self_row.add_dyna_link( ( self.maf_num, row) )
                        row_before=0

                        while(True):

                            if  attr_name in self.ht.relations   and \
                                    self.ht.relations[ attr_name ] == 'whole' : 
                                self.delete_row( row )
                                row= self_row.get_first_dyna_row(self.maf_num)
                            else:

                                links = self.r_links ( kerr, attr_num=attr , num_row= row )
                                if is_err( kerr ) >= 0 :      
                                    pr ('25 HTMS_Mid_Err    Error read links from field deleted in row. err = %s' % str (kerr) )
                                    raise HTMS_Mid_Err('25 HTMS_Mid_Err    Error read links from field deleted in row..  err = %s' % str (kerr)  )
                                if DEBUG_DELETE_FIELD_1:
                                    pr ('\n       DELETE_LINK FIELD ----  ROW=%d, links= %s' % \
                                        ( row, str(links) ) )      
                                
                                links_array = Links_array ( links )
                                for li in links_array.get_dyna_links():
                                
                                    linking_maf_n = li [ 0 ]
                                    row_in_linking_maf =  li [ 1 ]
                                    add_temp_mafs( self, linking_maf_n)       
                                    linking_maf =   self.ht.mafs_opened[ linking_maf_n ]  

                                    if  attr_name in self.ht.relations   and \
                                    self.ht.relations[ attr_name ] == 'multipart' : 
 
                                        linking_maf.delete_row( row_in_linking_maf )

                                    else:
                                        rc=self.ht.correct_back_links (
                                                 maf_num = linking_maf_n, 
                                                 row_num= row_in_linking_maf, 
                                                 back_link  =  ( self.maf_num, row ),  
                                                 func = 'delete')
                                            #  maf_num - is maf needed to correct back link(-s) 
                                            #  row_num - row or rows set or 'all' - is row(-s) needed to correct back link(-s)
                                            #  back_link = (self.maf_num, row_num) - element of back_link to add or delete
                                            #  func = 'add' or 'delete'
                                        if rc ==False:      
                                            pr ('26 HTMS_Mid_Err    Error correct back links to field deleted in ref. maf.  err = %s' % str (kerr)  )
                                            raise HTMS_Mid_Err(
                                                '26 HTMS_Mid_Err    Error correct back links to field deleted in ref. maf.  err = %s' % str (kerr)  )

                                row= self_row.get_first_dyna_row(self.maf_num)
                                if row == 0 :
                                    break

                            if row == 0 :
                                if row_before < self.ht.mafs[self.maf_num]['rows']:
                                    row = row_before+1 
                                else:
                                    break
                            else:
                                if row < self.ht.mafs[self.maf_num]['rows']:                                                
                                    row_before=row
                                    row+=1
                                else:
                                    break

                            self_row= Links_array( () )
                            self_row.add_dyna_link( ( self.maf_num, row) )

                        close_temp_mafs()
                    
                    elif   attr_type == '*weight' and  self.ht.mafs[self.maf_num]['rows'] >0:     #weight
                        if DEBUG_DELETE_FIELD_1:
                            if attr_name in self.ht.relations:
                                pr ('\n\n  DELETE_weight FIELD ---- maf_name = %s,  attr_name =%s, link_type=%s' % \
                                    ( self.maf_name, attr_name, self.ht.relations[ attr_name ]  ) )
                            else:
                                pr ('\n\n  DELETE_weight FIELD ---- maf_name = %s,  attr_name =%s, link_type= UNDEFINED' % \
                                    ( self.maf_name, attr_name ) )
                        row=1
                        self_row= Links_array( () )
                        self_row.add_dyna_link( ( self.maf_num, row, 0.0 ) )
                        row_before=0

                        while(True):

                            if  attr_name in self.ht.relations   and \
                                    self.ht.relations[ attr_name ] == 'whole' : 
                                self.delete_row( row )
                                row= self_row.get_first_dyna_row(self.maf_num)
                            else:

                                weights = self.r_weights ( kerr, attr_num=attr , num_row= row )
                                if is_err( kerr ) >= 0 :      
                                    pr ('27 HTMS_Mid_Err    Error read weights from field deleted in row. err = %s' % str (kerr) )
                                    raise HTMS_Mid_Err('27 HTMS_Mid_Err    Error read weight from field deleted in row..  err = %s' % str (kerr)  )
                                if DEBUG_DELETE_FIELD_1:
                                    pr ('\n       DELETE_weight FIELD ----  ROW=%d, weights= %s' % \
                                        ( row, str(weights) ) )      
                                
                                weights_array = Links_array ( weights )
                                for li in weights_array.get_dyna_links():
                                
                                    weighting_maf_n = li [ 0 ]
                                    row_in_weighting_maf =  li [ 1 ]
                                    #weight_in_weighting_maf =  li [ 2 ]

                                    add_temp_mafs( self, weighting_maf_n)       
                                    weighting_maf =   self.ht.mafs_opened[ weighting_maf_n ]  

                                    if  attr_name in self.ht.relations   and \
                                    self.ht.relations[ attr_name ] == 'multipart' : 
 
                                        weighting_maf.delete_row( row_in_weighting_maf )

                                    else:
                                        rc=self.ht.correct_back_links (
                                                 maf_num = weighting_maf_n, 
                                                 row_num= row_in_weighting_maf, 
                                                 back_link  =  ( self.maf_num, row, ),  #weight_in_weighting_maf ),  
                                                 func = 'delete',
                                                 typ= 'weight')
                                            #  maf_num - is maf needed to correct back link(-s) 
                                            #  row_num - row or rows set or 'all' - is row(-s) needed to correct back link(-s)
                                            #  back_link = (self.maf_num, row_num, weight) - element of back_link to add or delete
                                            #  func = 'add' or 'delete'
                                        if rc ==False:      
                                            pr ('28 HTMS_Mid_Err    Error correct back links to field deleted in ref. maf.  err = %s' % str (kerr)  )
                                            raise HTMS_Mid_Err(
                                                '28 HTMS_Mid_Err    Error correct back links to field deleted in ref. maf.  err = %s' % str (kerr)  )

                                row= self_row.get_first_dyna_row(self.maf_num)
                                if row == 0 :
                                    break

                            if row == 0 :
                                if row_before < self.ht.mafs[self.maf_num]['rows']:
                                    row = row_before+1 
                                    #row_before+=1
                                else:
                                    break
                            else:
                                if row < self.ht.mafs[self.maf_num]['rows']:                                                
                                    row_before=row
                                    row+=1
                                else:
                                    break

                            self_row= Links_array( () )
                            self_row.add_dyna_link( ( self.maf_num, row, ) )

                        close_temp_mafs()

                    else:
                        pass

                    self.field (kerr, fun='delete',  attr_num_f = attr )
                    if is_err( kerr ) >= 0 :      
                         pr ('29 HTMS_Mid_Err    Error delete update_fields field.  err = %s' % str (kerr)  )
                         raise HTMS_Mid_Err('29 HTMS_Mid_Err    Error delete update_fields field.  err = %s' % str (kerr)  )
                    rc2=self.ht.update_RAM (fun = 'field_remove', maf_num_p=self.maf_num,  attr_num_p = attr)
                    if rc2 != True :      
                        pr ('30 HTMS_Mid_Err    Error delete update_fields RAM.  err = %s' % str (kerr)  )
                        raise HTMS_Mid_Err('30 HTMS_Mid_Err    Error delete update_fields field.  err = %s' % str (kerr)  )
  
                else:
                    pass

        if len (add_fields) > 0:
                pr ('31 HTMS_Mid_Err    In the HT full attribute\'s list no item with name(-s) : %s  '% str (add_fields ) )
                raise HTMS_Mid_Err('31 HTMS_Mid_Err    In the HT full attribute\'s list no item with name(-s) : %s  '% str (add_fields ) )
        if len (del_fields) > 0:
                pr ('32 HTMS_Mid_Err    In the MAF field\'s list no item with name(-s)s : %s  '% str (del_fields ) )
                raise HTMS_Mid_Err('32 HTMS_Mid_Err    In the MAF field\'s list no item with name(-s) : %s  '% str (del_fields ) )

        self.ht.updated = time.time()

        return True

#------------------------------------------------------------------------------------------------

    def update_row (self, row_num =0, add_data={}, delete_data= set()) :
            # add_data= { attr_name|field_name : value, ...., attr_name|field_name : value } - dict
            # delete_data= { attr_name|field_name,...., attr_name|field_name } - set

            if self.ht.mode in ('rs', 'rm' ):
                pr ('34 HTMS_Mid_Err    Mode "%s" incompatible for update row n in HT "%s".'
                        % (self.ht.mode, self.ht.ht_name)
                )
                raise HTMS_Mid_Err('34 HTMS_Mid_Err    Mode "%s" incompatible for update row in HT "%s".'
                        % (self.ht.mode, self.ht.ht_name)
                )

            #if len ( self.fields ) == 1 and self.fields[ 0 ] == 'Back_links':         #weight
            if len ( self.fields ) <= 3 :
                pr ('35 HTMS_Mid_Err    Table have no data fields. ' )
                raise HTMS_Mid_Err('35 HTMS_Mid_Err    Table have no fields. ' )

            if  (add_data =={} and delete_data == set()) :
                pr ('36 HTMS_Mid_Err  Data is not setted. ' )
                raise HTMS_Mid_Err('36 HTMS_Mid_Err  Data  is not setted. ' )

            if row_num ==0 :
                pr ('37 HTMS_Mid_Err  Row number is not setted. ' )
                raise HTMS_Mid_Err('37 HTMS_Mid_Err  Row number is not setted. ' )

            if row_num > self.rows:
                kerr = []
                after_row = self.rows
                numbers = ( row_num - self.rows )
                self.row(kerr, fun='add',  after =after_row, number=numbers )
                if is_err( kerr ) >= 0 :      
                     pr ('38 HTMS_Mid_Err    Error add new rows.  err = %s' % str (kerr)  )
                     raise HTMS_Mid_Err('38 HTMS_Mid_Err    Error add new rows.  err = %s' % str (kerr)  )

                #rc = self.ht.update_RAM (fun = 'row_add', maf_num_p=self.maf_num,  after_row = after_row , num_rows = numbers)
                #rc != True :      
                   #pr ( '34 HTMS_Mid_Err    Error update RAM after delete row.  err = %s' % str (kerr)  )
                   #raise HTMS_Mid_Err ( '34 HTMS_Mid_Err    Error update RAM after delete row..  err = %s' % str (kerr)  )

            if len (add_data) >0:
                if 'Time_row' in add_data:
                    del add_data[ 'Time_row' ]
                new =True
            else:
                new = False 

            if len (delete_data) >0:
                if 'Time_row' in delete_data:
                    del delete_data[ 'Time_row' ]
                dlt =True
            else:
                dlt = False 

            kerr = []
            for attr in self.fields:
                attr_num = self.fields[ attr ][ 0 ]
                atr_type =  self.fields[ attr ][ 1 ]
                if atr_type in ('*link', 'file', '*weight'):           #weight
                   if  attr  in add_data: 
                       del add_data [ attr ]
                   continue

                if  ( new  and   len ( add_data ) > 0     and   attr in add_data      and    add_data [ attr ] == None )   or \
                    ( dlt    and   len ( delete_data ) > 0  and   attr in delete_data   and   not (attr in add_data)):

                    if atr_type[ : 4] == 'byte':
                        ins = b''
                        self.w_elem ( kerr, attr_num=attr_num, num_row =row_num, elem = ins )
                    elif atr_type[ : 3] == 'utf' or atr_type == 'datetime':
                        ins = ''
                        self.w_utf8 ( kerr, attr_num=attr_num, num_row =row_num, string = ins  )
                    elif atr_type == '*byte':
                        ins = None
                        self.w_elem ( kerr, attr_num=attr_num, num_row =row_num, elem = ins )
                    elif atr_type == '*utf':
                        ins = None
                        self.w_str ( kerr, attr_num=attr_num, num_row =row_num, string = ins  )
                    elif atr_type in ('int4', 'int8', 'float4', 'float8', 'time'):
                        ins =  Types_htms.types[ atr_type ][ 2 ] 
                        self.w_numbers ( kerr, attr_num=attr_num, num_row =row_num, numbers = ins  )
                    elif atr_type in ('*int4', '*int8', '*float4', '*float8'):
                        ins = None
                        self.w_numbers ( kerr, attr_num=attr_num, num_row =row_num, numbers = ins  )

                    if is_err( kerr ) >= 0 :      
                         pr ('39 HTMS_Mid_Err    Error delete data.  err = %s' % str (kerr)  )
                         raise HTMS_Mid_Err('39 HTMS_Mid_Err    Error delete data.  err = %s' % str (kerr)  )
                    if dlt:
                        delete_data.remove ( attr )
                    else: #  if new
                        del add_data [ attr ]

                if new   and   len ( add_data ) > 0   and    attr in add_data:
                    if atr_type[ : 4] == 'byte':
                        self.w_elem ( kerr, attr_num=attr_num, num_row =row_num, elem = add_data [ attr ] )
                    elif atr_type[ : 3] == 'utf' or atr_type == 'datetime':
                        self.w_utf8 ( kerr, attr_num=attr_num, num_row =row_num, string = add_data [ attr ]  )
                    elif atr_type == '*byte':
                        self.w_bytes ( kerr, attr_num=attr_num, num_row =row_num, bytes = add_data [ attr ]  )
                    elif atr_type == '*utf':
                        self.w_str ( kerr, attr_num=attr_num, num_row =row_num, string = add_data [ attr ]  )
                    else: # element is number or array of numbers
                        self.w_numbers ( kerr, attr_num=attr_num, num_row =row_num, numbers = add_data [ attr ]  )
                    if is_err( kerr ) >= 0 :      
                         pr ('40 HTMS_Mid_Err    Error correct data.  err = %s' % str (kerr)  )
                         raise HTMS_Mid_Err('40 HTMS_Mid_Err    Error correct data.  err = %s' % str (kerr)  )
                    del add_data [ attr ]

            if len (add_data) > 0:
                pr ('41 HTMS_Mid_Err    In the table data attribute\'s list no item with name(-s) : %s  '% str ( list(add_data.keys()  ) ) )
                raise HTMS_Mid_Err('41 HTMS_Mid_Err    In the HT full attribute\'s list no item with name(-s) : %s  '% str ( list (add_data.keys()  ) ) )
            if len (delete_data) > 0:
                pr ('42 HTMS_Mid_Err    In the table data attribute\'s list no item with name(-s) : %s  '% str ( delete_data  ) )
                raise HTMS_Mid_Err('42 HTMS_Mid_Err    In the table attribute\'s list no item with name(-s) : %s  '% str (delete_data ) )

            self.ht.updated = time.time()

            return True

#--------------------------------------------------------------------------------------------------

    def delete_row (self, nrow =0, 
                    protect_link= Links_array( () ),
                    protect_weight= Links_array( () )
                    ) :
            if self.ht.mode in ('rs', 'rm' ):
                pr ('44 HTMS_Mid_Err    Mode "%s" incompatible for delete row in HT "%s".'
                        % (self.ht.mode, self.ht.ht_name)
                )
                raise HTMS_Mid_Err('44 HTMS_Mid_Err    Mode "%s" incompatible for delete row in HT "%s".'
                        % (self.ht.mode, self.ht.ht_name)
                )
            row_num= nrow
            if DEBUG_DELETE_ROW_1:
                pr('\n\n                      DELETE_ROW ---- maf_name = %s,  row_num =%d, '% (self.maf_name, row_num))
                pr('\n                 protect_link= %s, protect_weight= %s ' % \
                     (str (protect_link), str (protect_weight) ))
                for tab in self.ht.mafs_opened:
                    links_dump ( self.ht.mafs_opened [ tab ] )

            num_temp_mafs_opened = 0
            temp_mafs_opened ={}         
            def close_temp_mafs():
                    nonlocal temp_mafs_opened
                    if len ( temp_mafs_opened )>0:
                        for mf in  temp_mafs_opened :
                            temp_mafs_opened[ mf ].close()
                    del temp_mafs_opened
                    temp_mafs_opened ={}  
            def add_temp_mafs( self, temp_maf):
                    nonlocal temp_mafs_opened,  num_temp_mafs_opened
                    if  not ( temp_maf in self.ht.mafs_opened):
                            temp_mafs_opened[ temp_maf ] =Table(
                                ht_root=self.ht.ht_root, ht_name= self.ht.ht_name, 
                                t_nmaf= temp_maf, local_root=self.ht.local_root )
                            num_temp_mafs_opened += 1

            if row_num < 1 or row_num > self.rows:
                pr ('45 HTMS_Mid_Err  Row number is not valid. ' )
                raise HTMS_Mid_Err('45 HTMS_Mid_Err  Row number is not valid. ' )
            else:

                kerr =[]
                back_links = self.r_links ( kerr , attr_num= 1 , num_row= row_num )
                if is_err( kerr ) >= 0 :      
                    pr ( '46 HTMS_Mid_Err    Error read back_links during delete row=%s in table=%s, err = %s' % 
                        (str(row_num),self.ht.mafs[self.maf_num]['name'],str (kerr) ) )
                    raise HTMS_Mid_Err ( '46 HTMS_Mid_Err    Error read back_links during delete row=%s in table=%s, err = %s' % 
                        (str(row_num),self.ht.mafs[self.maf_num]['name'],str (kerr) )) 

                weight_links = self.r_links ( kerr , attr_num= 3 , num_row= row_num )             #weight
                if is_err( kerr ) >= 0 :      
                    pr ( '47 HTMS_Mid_Err    Error read back weight links during delete row=%s in table=%s, err = %s' % 
                        (str(row_num),self.ht.mafs[self.maf_num]['name'],str (kerr) ) )
                    raise HTMS_Mid_Err ( '47 HTMS_Mid_Err    Error read weight_links during delete row=%s in table=%s, err = %s' % 
                        (str(row_num),self.ht.mafs[self.maf_num]['name'],str (kerr) ) )

                back_links_array = Links_array( back_links )
                for bli in back_links_array.get_dyna_links():      # 1st  loop:   search the whole in back_links
                    if not ( bli in protect_link.get_dyna_links() ): 
                        linking_maf_n = bli [ 0 ]
                        row_in_linking_maf =  bli [ 1 ]
                        add_temp_mafs( self, linking_maf_n)       
                        linking_maf =   self.ht.mafs_opened[ linking_maf_n ]     

                        for whole_field  in   linking_maf.fields:     # 2nd loop : search "whole"  link's fields 
                                                                                         # in row =    row_in_linking_maf
                            if   linking_maf.fields[ whole_field ][ 0 ] >3  and  \
                                 linking_maf.fields[ whole_field ][ 1 ] == '*link'  and  \
                                 whole_field in self.ht.relations and \
                                 self.ht.relations[ whole_field ] == 'whole' :  
                                kerr=[]
                                whole_links = linking_maf.r_links ( 
                                    kerr , attr_num=  linking_maf.fields[ whole_field ][ 0 ], 
                                    num_row= row_in_linking_maf )
                                if is_err( kerr ) >= 0 :      
                                     pr ( '48 HTMS_Mid_Err    Error read links of parent row during delete row.  err = %s' % str (kerr)  )
                                     raise HTMS_Mid_Err ( '48 HTMS_Mid_Err    Error read links of parent row during delete row.  err = %s' % str (kerr)  )
                                
                                whole_links_array = Links_array( whole_links )                                        
                                for wli in whole_links_array.get_dyna_links():   #  3rd loop : search direct link to self part form whole field
                                    if not ( wli in protect_link.get_dyna_links() ):
                                        if wli[ 0 ] == self.maf_num and wli [ 1 ] == row_num : # found direct link to self
                                            kerr=[]
                                            # delete founded link
                                            rc = linking_maf.u_links ( kerr , 
                                                   attr_num= linking_maf.fields[ whole_field ][ 0 ], 
                                                   num_row= row_in_linking_maf,   
                                                   u_link = ( self.maf_num, - row_num)  )
                                            if not rc or is_err( kerr ) >= 0 :      
                                                pr ( '49 HTMS_Mid_Err    Error delete link during delete row.  err = %s' % str (kerr)  )
                                                raise HTMS_Mid_Err ( '49 HTMS_Mid_Err    Error delete link during delete row.  err = %s' % str (kerr)  )
                                            if  self.ht.relations[ whole_field ] == 'whole' :
                                                
                                                protect_link.add_dyna_link( ( self.maf_num,   row_num) )
                                                self_row= Links_array( () )
                                                self_row.add_dyna_link( ( self.maf_num,   row_num) )

                                                linking_maf.delete_row(  
                                                    row_in_linking_maf,  
                                                    protect_link = protect_link,
                                                    protect_weight = protect_weight,
                                                  )

                                                row_num= self_row.get_first_dyna_row(self.maf_num)
                                                protect_link.remove_dyna_link( ( self.maf_num,   row_num) )
                                                
                                                parent_row_deleted = True 
                                                break
                                del whole_links_array
                del back_links_array

                back_weights_array = Links_array( weight_links  )            
                for bwi in back_weights_array.get_dyna_links():      # 1st  loop:   search the whole in back_weights
                    if not ( bwi in protect_weight.get_dyna_links() ): 
                        linking_maf_n = bwi [ 0 ]
                        row_in_linking_maf =  bwi [ 1 ]
                        add_temp_mafs( self, linking_maf_n)       
                        linking_maf =   self.ht.mafs_opened[ linking_maf_n ]     

                        for whole_field  in   linking_maf.fields:     # 2nd loop : search "whole"  link's fields 
                                                                                         # in row =    row_in_linking_maf
                            if   linking_maf.fields[ whole_field ][ 0 ] >3  and  \
                                 linking_maf.fields[ whole_field ][ 1 ] == '*weight'  and  \
                                 whole_field in self.ht.relations and \
                                 self.ht.relations[ whole_field ] == 'whole' :  
                                kerr=[]
                                whole_weights = linking_maf.r_weights ( 
                                    kerr , attr_num=  linking_maf.fields[ whole_field ][ 0 ] , num_row= row_in_linking_maf )
                                if is_err( kerr ) >= 0 :      
                                     pr ( '50 HTMS_Mid_Err    Error read weights of parent row during delete row.  err = %s' % str (kerr)  )
                                     raise HTMS_Mid_Err ( '50 HTMS_Mid_Err    Error read weights of parent row during delete row.  err = %s' % str (kerr)  )
                                
                                whole_weights_array = Links_array( whole_weights )                                        
                                for wli in whole_weights_array.get_dyna_links():   #  3rd loop : search direct link to self part form whole field
                                    if not ( wli in protect_weight.get_dyna_links() ):
                                        if wli[ 0 ] == self.maf_num and wli [ 1 ] == row_num : # found direct link to self
                                            kerr=[]
                                            # delete founded link
                                            rc = linking_maf.u_weights ( kerr , 
                                                    attr_num= linking_maf.fields[ whole_field ][ 0 ] , 
                                                    num_row= row_in_linking_maf,   
                                                    u_weight = ( self.maf_num, - row_num)  )
                                            if not rc or is_err( kerr ) >= 0 :      
                                                pr ( '51 HTMS_Mid_Err    Error delete link during delete row.  err = %s' % str (kerr)  )
                                                raise HTMS_Mid_Err ( '51 HTMS_Mid_Err    Error delete link during delete row.  err = %s' % str (kerr)  )
                                            if  self.ht.relations[ whole_field ] == 'whole' :
                                                
                                                protect_weight.add_dyna_link( ( self.maf_num,   row_num) )
                                                self_row= Links_array( () )
                                                self_row.add_dyna_link( ( self.maf_num,   row_num) )

                                                linking_maf.delete_row(  
                                                    row_in_linking_maf, 
                                                    protect_link = protect_link,
                                                    protect_weight = protect_weight,
                                                    )

                                                row_num= self_row.get_first_dyna_row(self.maf_num)
                                                protect_weight.remove_dyna_link( ( self.maf_num,   row_num) )
                                                
                                                parent_row_deleted = True 
                                                break
                                del whole_weights_array
                del back_weights_array
                
                for name_attr  in  self.fields:
                    if  self.fields[ name_attr ][ 1 ] == '*link' and  \
                        self.fields[ name_attr ][ 0 ] > 3  and \
                        name_attr  in self.ht.relations:

                        rel =  self.ht.relations[ name_attr ]

                        if   rel == 'cause':
                            continue
                        elif   rel == 'multipart'  or    rel == 'whole':      # delete all  parts

                            kerr =[]
                            links = self.r_links ( kerr , attr_num= self.fields[ name_attr ][ 0 ] , 
                                num_row= row_num )
                            if is_err( kerr ) >= 0 :      
                                pr ( '52 HTMS_Mid_Err    Error read links during delete row.  err = %s' % str (kerr)  )
                                raise HTMS_Mid_Err ( '52 HTMS_Mid_Err    Error read links during delete row.  err = %s' % str (kerr)  )

                            links_array = Links_array( links )  
                            for li in links_array.get_dyna_links():
                                if not ( li  in protect_link.get_dyna_links()  ):
                                    part_maf = li [ 0 ]
                                    row_in_part_maf =  li [ 1 ]
                                    add_temp_mafs( self, part_maf)       
                                    kerr=[]
                                    rc = self.u_links ( kerr , 
                                            attr_num= self.fields[ name_attr ][ 0 ] , 
                                            num_row= row_num,   
                                            u_link = ( part_maf, - row_in_part_maf)  )
                                    if is_err( kerr ) >= 0 :      
                                        pr ( '53 HTMS_Mid_Err    Error delete link during delete row.  err = %s' % str (kerr)  )
                                        raise HTMS_Mid_Err ( '53 HTMS_Mid_Err    Error delete link during delete row.  err = %s' % str (kerr)  )

                                    protect_link.add_dyna_link( ( self.maf_num,   row_num) )
                                    self_row= Links_array( () )
                                    self_row.add_dyna_link( ( self.maf_num,   row_num) )

                                    self.ht.mafs_opened[ part_maf ].delete_row(  
                                        row_in_part_maf, 
                                        protect_link = protect_link,
                                        protect_weight = protect_weight,                              
                                    )
                                    row_num= self_row.get_first_dyna_row(self.maf_num)

                                    protect_link.remove_dyna_link( ( self.maf_num,   row_num) )
                            del links_array  

                for name_attr  in  self.fields:
                    if  self.fields[ name_attr ][ 1 ] == '*weight' and  self.fields[ name_attr ][ 0 ] > 3  and \
                        name_attr  in self.ht.relations:

                        rel =  self.ht.relations[ name_attr ]

                        if   rel == 'cause':
                            continue
                        elif   rel == 'multipart'  or    rel == 'whole':      # delete all  parts

                            kerr =[]
                            weights = self.r_weights ( kerr , attr_num= self.fields[ name_attr ][ 0 ] , 
                                num_row= row_num )
                            if is_err( kerr ) >= 0 :      
                                pr ( '54 HTMS_Mid_Err    Error read weights during delete row.  err = %s' % str (kerr)  )
                                raise HTMS_Mid_Err ( '54 HTMS_Mid_Err    Error read weights during delete row.  err = %s' % str (kerr)  )
                            weights_array = Links_array( weights )  
                            for li in weights_array.get_dyna_links():
                                if not ( li  in protect_weight.get_dyna_links()  ):
                                    part_maf = li [ 0 ]
                                    row_in_part_maf =  li [ 1 ]
                                    add_temp_mafs( self, part_maf)       
                                    kerr=[]
                                    rc = self.u_weights ( kerr , 
                                           attr_num= self.fields[ name_attr ][ 0 ] , 
                                           num_row= row_num,   
                                           u_weight = ( part_maf, - row_in_part_maf)  )
                                    if is_err( kerr ) >= 0 :      
                                        pr ( '55 HTMS_Mid_Err    Error delete link during delete row.  err = %s' % str (kerr)  )
                                        raise HTMS_Mid_Err ( '55 HTMS_Mid_Err    Error delete link during delete row.  err = %s' % str (kerr)  )

                                    protect_weight.add_dyna_link( ( self.maf_num,   row_num) )
                                    self_row= Links_array( () )
                                    self_row.add_dyna_link( ( self.maf_num,   row_num) )

                                    self.ht.mafs_opened[ part_maf ].delete_row(  
                                        row_in_part_maf, 
                                        protect_link = protect_link,
                                        protect_weight = protect_weight,                              
                                    )
                                    row_num= self_row.get_first_dyna_row(self.maf_num)

                                    protect_weight.remove_dyna_link( ( self.maf_num,   row_num) )
                            del weights_array  

                close_temp_mafs()
                kerr = []
                after_row= row_num-1                                       

                if DEBUG_DELETE_ROW_1:
                    print ('\n\n  DELETE_ROW PHYSICALLY ----  maf_name = %s,  after_row =%d ' %
                          (self.maf_name, after_row ) )
                    for tab in self.ht.mafs_opened:
                        links_dump ( self.ht.mafs_opened [ tab ] )

                if not self.row(kerr, fun='delete',  after =after_row, number= 1 ) or \
                   is_err( kerr ) >= 0 :      
                    pr ( '56 HTMS_Mid_Err    Error delete row.  err = %s' % str (kerr)  )
                    if DEBUG_DELETE_ROW_1:
                        print ('\n\n  ROW   NOT   DELETED-')
                    raise HTMS_Mid_Err ( '56 HTMS_Mid_Err    Error delete row PHYSICALLY.  err = %s' % str (kerr)  )

                if DEBUG_DELETE_ROW_1:
                    pr('\n\n  ROW DELETED  ---- maf_name = %s,  row_num =%d, '% 
                       (self.maf_name, row_num))
                    pr('\n                 protect_link= %s, protect_weight= %s ' % \
                         (str (protect_link), str (protect_weight) ))
                    for tab in self.ht.mafs_opened:
                        links_dump ( self.ht.mafs_opened [ tab ] )

                kerr = []

                if 'objects_RAM'  in  dir ( self ) and len ( self.objects_RAM )  >0 :
                    #pr (str (  [ obj  for obj in self.get_RAM_instances() ]  ))
                    obj_RAM_to_delete = \
                        [ obj  for obj in self.get_RAM_instances() if obj.id == row_num ]
                    for oi in  obj_RAM_to_delete:                   
                            oi. remove_instance()
                            del oi
                    del obj_RAM_to_delete
            
                self.ht.updated = time.time()
        
                return True

#--------------------------------------------------------------------------------------------------

    def update_links (self, row_num =0, attr_name='', add_links={}, delete_links={} ) :
            # add_links = { maf_name : { row_num, ...., row_num }, .....   }.
            #       maf_name : 'all'   - indicator of link "to all rows, ie to a whole MAF"
            #       maf_name :  set()  - no to change
            # delete_links = { maf_name : { row_num, ...., row_num }, .....  }.
            #       ='all'               - delete all links to any maf  in field
            #       maf_name :  'all'   - delete all links to maf
            #       maf_name :  set()   - no to change

            num_temp_mafs_opened = 0
            temp_mafs_opened ={}         

            def close_temp_mafs():
                    nonlocal temp_mafs_opened
                    if temp_mafs_opened != {}:
                        for mf in  temp_mafs_opened :
                            temp_mafs_opened[ mf ].close()
                    del temp_mafs_opened
                    temp_mafs_opened ={}  

            def add_temp_mafs( self, temp_maf):
                    nonlocal temp_mafs_opened,  num_temp_mafs_opened
                    if  not ( temp_maf in self.ht.mafs_opened):
                            temp_mafs_opened[ temp_maf ] = Table(
                                ht_root=self.ht.ht_root, ht_name= self.ht.ht_name, 
                                t_nmaf= temp_maf, local_root=self.ht.local_root )
                            num_temp_mafs_opened += 1

            if self.ht.mode in ('rs', 'rm' ):
                pr ('60 HTMS_Mid_Err    Mode "%s" incompatible for update links in in HT "%s".'
                        % (self.ht.mode, self.ht.ht_name)
                )
                raise HTMS_Mid_Err('60 HTMS_Mid_Err    Mode "%s" incompatible for update links in HT "%s".'
                        % (self.ht.mode, self.ht.ht_name)
                )
            #if len ( self.fields ) == 1 and self.fields[ 0 ] == 'Back_links':       #weight
            if len ( self.fields ) <= 3 :
                pr ('61 HTMS_Mid_Err    Table have no fields. ' )
                raise HTMS_Mid_Err('61 HTMS_Mid_Err    Table have no fields. ' )

            if  row_num ==0 or row_num ==set() or row_num =={} or row_num ==() or row_num ==[] or row_num ==None or \
                attr_name ==''  or  (add_links =={} and delete_links == {} ):
                pr ('62 HTMS_Mid_Err  Data is not setted. ' )
                raise HTMS_Mid_Err('62 HTMS_Mid_Err  Data  is not setted. ' )

            if  not (  attr_name in self.fields):
                 pr ('63 HTMS_Mid_Err   In table is not field with attribute "%s".' % attr_name )
                 raise HTMS_Mid_Err('63 HTMS_Mid_Err   In table is not field with attribute "%s".' % attr_name )

            if attr_name in self.ht.relations and self.ht.relations[ attr_name ] == 'erased':
                 pr ('64 HTMS_Mid_Err   Attribute "%s" was deleted from hypertable.' % attr_name )
                 raise HTMS_Mid_Err('64 HTMS_Mid_Err   In table is not field with attribute "%s".' % attr_name )

            if  self.fields[ attr_name ][ 1 ]  !=  '*link' :
                pr ('65 HTMS_Mid_Err  Type of attribute specified if not "*link". ' )
                raise HTMS_Mid_Err('65 HTMS_Mid_Err  Type of attribute specified if not "*link". ' )

            if row_num > self.rows:
                 pr ('66 HTMS_Mid_Err    Row number exceeded max.' )
                 raise HTMS_Mid_Err('66 HTMS_Mid_Err    Row number exceeded max.' )

            if len (add_links) >0  and delete_links == 'all':
                 pr ('67 HTMS_Mid_Err    Parameters incompatible.' )
                 raise HTMS_Mid_Err('67 HTMS_Mid_Err    Parameters incompatible.' )

            if delete_links == 'all':
                kerr =[]
                attr_num = self.fields [ attr_name ][ 0 ]
                deleted_links =   self.r_links( kerr, attr_num = attr_num, num_row= row_num)
                if is_err( kerr ) >= 0 :
                    pr ('70 HTMS_Mid_Err    Error read old link.  err = %s' % str (kerr)  )
                    raise HTMS_Mid_Err('70 HTMS_Mid_Err    Error read old link.  err = %s' % str (kerr)  )   
                if len (deleted_links) >0 :
                    offset, length = self.offsets [attr_num] 
                    rc2 =  self.ht.cage.write( self.ch, (row_num-1)*self.rowlen+ offset, b'\xFF'*16 , Kerr)   
                    if rc2 == False:
                        pr ('71 HTMS_Mid_Err    Error read old link.  err = %s' % str (kerr)  )
                        raise HTMS_Mid_Err('71 HTMS_Mid_Err    Error read old link.  err = %s' % str (kerr))  
                    for li in deleted_links:
                        add_temp_mafs(self, li [ 0 ] )
                        rc=self.ht.correct_back_links (maf_num = li[ 0 ], 
                                                       row_num = li[ 1 ], 
                                                       back_link = (self.maf_num, row_num),  
                                                       func = 'delete')          
                        if rc == False : 
                            close_temp_mafs()
                            pr ('72 HTMS_Mid_Err    Error correct back links    Maf = %d, row =: %d. ' % ( li [ 0 ], li[ 1 ]) )
                            raise HTMS_Mid_Err('72 HTMS_Mid_Err    Error correct back links    Maf = %d, row =: %d. ' % ( li [ 0 ], li[ 1 ]) )
                    close_temp_mafs()
                self.ht.updated = time.time()
                return True


            if len (add_links) >0:
                new =True
            else:
                new = False

            if len (delete_links) >0 :
                dlt =True
            else:
                dlt = False 

            mafs_to_link = set()
            add_li =set ()
            delete_li =set ()

            if new:
                for maf_name  in  add_links:
                    if add_links[maf_name] == set() or add_links[maf_name] == ():              
                        continue
                    maf_num = self.ht.get_maf_num ( maf_name)
                    if maf_num  == 0:
                        close_temp_mafs()
                        pr ('73 HTMS_Mid_Err    HT have no table : %s. ' % maf_name )
                        raise HTMS_Mid_Err('73 HTMS_Mid_Err    HT have no table : %s. ' % maf_name )
                    mafs_to_link.add ( maf_num )
                    rows = add_links [ maf_name ]
                    if maf_num != self.maf_num:
                        add_temp_mafs( self, maf_num)

                    if type ( rows ).__name__ != 'tuple':
                        if     type ( rows ).__name__ == 'int'   or   rows == 'all':
                            rows = ( rows, )
                        elif  type ( rows ).__name__ == 'set':
                            rows = tuple (rows)
                        else:
                            close_temp_mafs()
                            pr ('74 HTMS_Mid_Err    Incorrect data in "add_links" parameter :%s : %s. ' % ( maf_name, str (rows) ) )
                            raise HTMS_Mid_Err('74 HTMS_Mid_Err    Incorrect data in "add_links" parameter :%s : %s. ' % ( maf_name, str (rows) ) )
                    for r in rows: 
                        if r == row_num  and  maf_num == self.maf_num:
                            close_temp_mafs()
                            pr ('75 HTMS_Mid_Err    Link to self row in self table prohibited :%s : %s. ' % ( maf_name, str (rows) ) )
                            raise HTMS_Mid_Err('75 HTMS_Mid_Err    Link to self row in self table prohibited :%s : %s. ' % ( maf_name, str (rows) ) )
                        add_li.add ( ( maf_num, r ) )
                                    
            if dlt:
                for maf_name  in  delete_links:
                    if delete_links[maf_name] == set() or delete_links[maf_name] == ():              
                        continue
                    maf_num = self.ht.get_maf_num ( maf_name)
                    if maf_num  == 0:
                        close_temp_mafs()
                        pr ('77 HTMS_Mid_Err    HT have no table : %s. ' % maf_name )
                        raise HTMS_Mid_Err('77 HTMS_Mid_Err    HT have no table : %s. ' % maf_name )

                    add_temp_mafs( self, maf_num)
                    mafs_to_link.add ( maf_num )
                    rows = delete_links [ maf_name ]

                    if type ( rows ).__name__ != 'tuple':
                        if type ( rows ).__name__ == 'int'   or   rows == 'all':
                            rows = ( rows, )
                        elif  type ( rows ).__name__ == 'set':
                            rows = tuple (rows)
                        else:
                            close_temp_mafs()
                            pr ('78 HTMS_Mid_Err    Incorrect data in "delete_links" parameter :%s : %s. ' % ( maf_name, str (rows) ) )
                            raise HTMS_Mid_Err('78 HTMS_Mid_Err    Incorrect data in "delete_links" parameter :%s : %s. ' % ( maf_name, str (rows) ) )
                    for r in rows: 
                        delete_li.add ( ( maf_num, r ) )
            kerr =[]
            #  CASE 1  - simple  - only one parameter
            if  len (add_li) +len ( delete_li) <= 1:  
                for_u_link = None
                if new:
                    pair = add_li.pop()
                    maf = pair[ 0 ]
                    row = pair[ 1 ]
                    if  row == 'all':
                        for_u_link = ( maf, 0)                                
                    else:
                        for_u_link = ( maf, row)
                    rc = self.ht.correct_back_links (maf_num = maf, row_num =  row, 
                                                     back_link = (self.maf_num, row_num),  
                                                     func = 'add')          
                    if rc == False : 
                        close_temp_mafs()
                        pr ('79 HTMS_Mid_Err    Error correct back links    Maf = %d, row =: %d. ' % ( maf,row) )
                        raise HTMS_Mid_Err('79 HTMS_Mid_Err    Error correct back links    Maf = %d, row =: %d. ' % (maf,row) )                                 
                else:  # dlt
                    pair = delete_li.pop()
                    maf = pair[ 0 ]
                    row = pair[ 1 ]
                    if  row == 'all':                   
                        for_u_link = ( - maf, 0)
                    else:
                        for_u_link = ( maf, - row)
                    rc = self.ht.correct_back_links (maf_num = maf, row_num =  row, back_link = (self.maf_num, row_num),  func = 'delete')          
                    if rc == False : 
                        close_temp_mafs()
                        pr ('80 HTMS_Mid_Err    Error correct back links    Maf = %d, row =: %d. ' % ( maf,row) )
                        raise HTMS_Mid_Err('80 HTMS_Mid_Err    Error correct back links    Maf = %d, row =: %d. ' % ( maf,row) )                                 

                if for_u_link != None:
                    kerr = []
                    rc = self.u_links (kerr, 
                                       attr_num= self.fields[ attr_name ][ 0 ], 
                                       num_row=row_num,  
                                       u_link = for_u_link  )
                        # u_link = () - clear all element   
                        # u_link =(nmaf, 0) - set row =0 - indicate link to whole maf
                        # u_link =(-nmaf, *) - delete all links to maf
                        # u_link =(nmaf,-num_row,) - delete one link to maf , to  num_row
                        # u_link =(nmaf,num_row,) -  add  link to maf - num_row, if not exist
                    if is_err( kerr ) >= 0 :
                        close_temp_mafs()      
                        pr ('81 HTMS_Mid_Err    Error updatel link.  err = %s' % str (kerr)  )
                        raise HTMS_Mid_Err('81 HTMS_Mid_Err    Error updatel link.  err = %s' % str (kerr)  )            

                    close_temp_mafs()

                    self.ht.updated = time.time()

                    return True

            #  CASE  2 - common
            if True:
                kerr = []
                old_links =  self.r_links( kerr, attr_num = self.fields [ attr_name ][ 0 ], num_row= row_num) 
                if is_err( kerr ) >= 0 :
                    close_temp_mafs()      
                    pr ('82 HTMS_Mid_Err    Error read old link.  err = %s' % str (kerr)  )
                    raise HTMS_Mid_Err('82 HTMS_Mid_Err    Error read old link.  err = %s' % str (kerr)  )   
                new_links =  set()
                old_links_to_another_mafs = set (old_links )
                for maf_to_link in mafs_to_link:
                    # link to whole maf
                    if ( maf_to_link, 'all' ) in delete_li :
                        pass     #new_links = new_links | { ( - maf_to_link, 0 ) } 
                        rc = self.ht.correct_back_links (maf_num = maf_to_link, row_num = 'all', back_link = (self.maf_num, row_num),  func = 'delete')          
                        if rc == False  : 
                            close_temp_mafs()
                            pr ('85 HTMS_Mid_Err    Error correct back links    Maf = %d, row =: %s. ' % ( maf_to_link, 'all') )
                            raise HTMS_Mid_Err('85 HTMS_Mid_Err    Error correct back links    Maf = %d, row =: %s. ' % ( maf_to_link, 'all') )     
                        
                        for li in old_links_to_another_mafs.copy():
                            if li [ 0 ] == maf_to_link:
                                old_links_to_another_mafs.remove( li )
                    elif ( maf_to_link, 'all' ) in add_li :
                        new_links = new_links | { ( maf_to_link, 0) } 
                        rc =self.ht.correct_back_links (maf_num = maf_to_link, row_num = 'all', back_link = (self.maf_num, row_num),  func = 'add')          
                        if rc == False : 
                            close_temp_mafs()
                            pr ('86 HTMS_Mid_Err    Error correct back links    Maf = %d, row =: %s. ' % ( maf_to_link, 'all') )
                            raise HTMS_Mid_Err('86 HTMS_Mid_Err    Error correct back links    Maf = %d, row =: %s. ' % ( maf_to_link, 'all') )     
                    else:
                        # links to singular row
                        old_li_regular  = set ( (ma, ro)  for (ma, ro)  in old_links   
                                               if  ma == maf_to_link ) 
                        add_li_regular =  set ( (ma, ro)  for (ma, ro)  in add_li      
                                               if  ma == maf_to_link and \
                                                   type ( ro ).__name__ == 'int' and ro > 0 ) 
                        del_li_regular =  set ( (ma, ro)  for (ma, ro)  in delete_li   
                                               if  ma == maf_to_link and \
                                                   type ( ro ).__name__ == 'int' and ro > 0 ) 
                        rows_add=set()
                        rows_delete=set()
                        for pair in del_li_regular:
                            rows_delete.add( pair [ 1 ] )
                        for pair in add_li_regular:
                            if pair not in rows_delete:
                                rows_add.add( pair [ 1 ] )
                        #rows_add.difference_update ( rows_delete )

                        rc = self.ht.correct_back_links (maf_num = maf_to_link, row_num =  rows_add, back_link = (self.maf_num, row_num),  func = 'add')          
                        if rc == False  :
                            close_temp_mafs() 
                            pr ('87 HTMS_Mid_Err    Error correct back links    Maf = %d, row =: %s. ' % ( maf_to_link, str(rows_add)) )
                            raise HTMS_Mid_Err('87 HTMS_Mid_Err    Error correct back links    Maf = %d, row =: %s. ' % ( maf_to_link, str(rows_add)) )     
                        rc = self.ht.correct_back_links ( maf_num = maf_to_link, row_num = rows_delete, back_link = (self.maf_num, row_num),  func = 'delete')          
                        if rc == False :
                            close_temp_mafs() 
                            pr ('88 HTMS_Mid_Err    Error correct back links    Maf = %d, row =: %s. ' % (maf_to_link, str(rows_add)) )
                            raise HTMS_Mid_Err('88 HTMS_Mid_Err    Error correct back links    Maf = %d, row =: %s. ' % ( maf_to_link, str(rows_delete)) )     

                        old_links_to_another_mafs.difference_update ( old_li_regular )
                        plus = old_li_regular | add_li_regular 
                        plus.difference_update (del_li_regular  )
                        new_links = new_links | plus

                new_links = new_links | old_links_to_another_mafs

                self.w_links ( kerr , attr_num=self.fields [ attr_name ][ 0 ], num_row=row_num, links =new_links, rollback=False)
                    # link =(nmaf, 0) - set row =0 - indicate link to whole maf

                if is_err( kerr ) >= 0 :
                    close_temp_mafs()      
                    pr ('90 HTMS_Mid_Err    Error write new links.  err = %s' % str (kerr)  )
                    raise HTMS_Mid_Err('90 HTMS_Mid_Err    Error write new links.  err = %s' % str (kerr)  )  
                
            close_temp_mafs()
            self.ht.updated = time.time()
            return True

#------------------------------------------------------------------------------------------------

    def single_link (self, row_num =0, attr_name='', to_table ='', to_row=0 ) :
        typ = type(to_table).__name__
        if typ != 'str':
            to_table=  to_table.maf_name
        if to_row < 0:
            self.update_links ( row_num =row_num, attr_name=attr_name, delete_links= { to_table: - to_row } ) 
        elif to_row == 0:
            self.update_links ( row_num =row_num, attr_name=attr_name, delete_links= { to_table: 'all' } ) 
        else:
            self.update_links ( row_num =row_num, attr_name=attr_name, add_links= { to_table:  to_row } ) 
        
#------------------------------------------------------------------------------------------------

    def multi_link (self, row_num =0, attr_name='', to_table ='', to_rows=() ) :
        typ = type(to_table).__name__
        if typ != 'str':
            to_table=  to_table.maf_name
        if to_rows == ():
            self.update_links ( row_num =row_num, attr_name=attr_name, delete_links= { to_table: 'all'  } ) 
        else:
            self.update_links ( row_num =row_num, attr_name=attr_name, add_links= { to_table:  to_rows } ) 
        

#--------------------------------------------------------------------------------------------------


    def update_weights (self, row_num =0, attr_name='', 
                        update_weights={}, delete_weights= {} 
                        ) :                  # add_weights={},

            # update _weights = { maf_name : { row_num: weight, ...., row_num: weight } , ......}
            # delete_weights = {  maf_name : ( row_num, ...., row_num ), ..... { row_num, ...., row_num },}
            #       ='all'                - delete all weights to any maf  in field
            #       maf_name :  'all'    - delete all weights to maf
            #       maf_name :  set()  - no to change

            if self.ht.mode in ('rs', 'rm' ):
                pr ('160 HTMS_Mid_Err    Mode "%s" incompatible for update weights in in HT "%s".'
                        % (self.ht.mode, self.ht.ht_name)
                )
                raise HTMS_Mid_Err('160 HTMS_Mid_Err    Mode "%s" incompatible for update weights in HT "%s".'
                        % (self.ht.mode, self.ht.ht_name)
                )
            #if len ( self.fields ) == 1 and self.fields[ 0 ] == 'Back_weights':                       #  ???????????????????
            if len ( self.fields ) <= 3:
                pr ('161 HTMS_Mid_Err    Table have no fields. ' )
                raise HTMS_Mid_Err('161 HTMS_Mid_Err    Table have no fields. ' )

            if  row_num ==0 or row_num ==set() or row_num =={} or row_num ==() or row_num ==[] or row_num ==None or \
                attr_name ==''  or  \
                (update_weights=={} and delete_weights == {} ):
                pr ('162 HTMS_Mid_Err  Data is not setted. ' )
                raise HTMS_Mid_Err('162 HTMS_Mid_Err  Data  is not setted. ' )

            if  not (  attr_name in self.fields):
                 pr ('163 HTMS_Mid_Err   In table is not field with attribute "%s".' % attr_numattr_name )
                 raise HTMS_Mid_Err('163 HTMS_Mid_Err   In table is not field with attribute "%s".' % attr_name )

            if attr_name in self.ht.relations and self.ht.relations[ attr_name ] == 'erased':
                 pr ('164 HTMS_Mid_Err   Attribute "%s" was deleted from hypertable.' % attr_name )
                 raise HTMS_Mid_Err('164 HTMS_Mid_Err   In table is not field with attribute "%s".' % attr_name )

            if  self.fields[ attr_name ][ 1 ]  !=  '*weight' :
                pr ('165 HTMS_Mid_Err  Type of attribute specified if not "*weight". ' )
                raise HTMS_Mid_Err('161 HTMS_Mid_Err  Type of attribute specified if not "*weight". ' )

            if row_num > self.rows:
                 pr ('166 HTMS_Mid_Err    Row number exceeded max.' )
                 raise HTMS_Mid_Err('167 HTMS_Mid_Err    Row number exceeded max.' )

            attr_num=self.fields [ attr_name ][ 0 ]
            old_weights={}
            add_weights={}
            num_temp_mafs_opened = 0
            temp_mafs_opened ={} 
            mafs_to_weight = set()
            add_we= {}
            delete_we= set()

            def close_temp_mafs():
                nonlocal temp_mafs_opened
                if len ( temp_mafs_opened )>0:
                    for mf in  temp_mafs_opened :
                        temp_mafs_opened[ mf ].close()
                del temp_mafs_opened
                temp_mafs_opened ={}  

            def add_temp_mafs( self, temp_maf):
                nonlocal temp_mafs_opened,  num_temp_mafs_opened
                if  not ( temp_maf in self.ht.mafs_opened):
                        temp_mafs_opened[ temp_maf ] =Table(
                            ht_root=self.ht.ht_root, ht_name= self.ht.ht_name, 
                            t_nmaf= temp_maf, local_root=self.ht.local_root )
                        num_temp_mafs_opened += 1

            if delete_weights == 'all':
                kerr =[]
                attr_num = self.fields [ attr_name ][ 0 ]
                deleted_weights =   self.r_weights( kerr, attr_num = attr_num, num_row= row_num)
                if is_err( kerr ) >= 0 :
                    pr ('170 HTMS_Mid_Err    Error read old link.  err = %s' % str (kerr)  )
                    raise HTMS_Mid_Err('170 HTMS_Mid_Err    Error read old link.  err = %s' % str (kerr)  )   
                if len (deleted_weights) >0 :
                    offset, length = self.offsets [attr_num] 
                    rc2 =  self.ht.cage.write( self.ch, (row_num-1)*self.rowlen+ offset, b'\xFF'*16 , Kerr)   
                    if rc2 == False:
                        pr ('171 HTMS_Mid_Err    Error read old link.  err = %s' % str (kerr)  )
                        raise HTMS_Mid_Err('171 HTMS_Mid_Err    Error read old link.  err = %s' % str (kerr))  
                    for li in deleted_weights:
                        add_temp_mafs(self, li [ 0 ] )
                        rc=self.ht.correct_back_weights (maf_num = li[ 0 ], 
                                                       row_num = li[ 1 ], 
                                                       back_link = (self.maf_num, row_num),  
                                                       func = 'delete')          
                        if rc == False : 
                            close_temp_mafs()
                            pr ('174 HTMS_Mid_Err    Error correct back weights    Maf = %d, row =: %d. ' % ( li [ 0 ], li[ 1 ]) )
                            raise HTMS_Mid_Err('174 HTMS_Mid_Err    Error correct back weights    Maf = %d, row =: %d. ' % ( li [ 0 ], li[ 1 ]) )
                    close_temp_mafs()
                self.ht.updated = time.time()
                return True

            if update_weights !={}:
                kerr = []
                old_weights_tuple= self.r_weights ( kerr, attr_num=attr_num , num_row=row_num )
                for wr in old_weights_tuple:
                    nmaf_w=wr[0]
                    nrow_w=wr[1]
                    weight_w=wr[2]
                    if not nmaf_w in old_weights:
                        old_weights[nmaf_w]={nrow_w:weight_w}
                    else:
                        old_weights[nmaf_w].update({nrow_w:weight_w})

                for maf_name in update_weights:
                    maf_num = self.ht.get_maf_num ( maf_name)
                    if maf_num  == 0:
                        close_temp_mafs()
                        pr ('176 HTMS_Mid_Err    HT have no table : %s. ' % maf_name )
                        raise HTMS_Mid_Err('176 HTMS_Mid_Err    HT have no table : %s. ' % maf_name )

                    if not maf_name in old_weights:       # and  not maf_name in add_weights:
                        add_weights[maf_name]= update_weights[maf_name]
                    
                    else:     # if maf_name in old_weights:
                        nrows_maf_old_weights= list(old_weights[maf_name].keys())
                        for nrow in update_weights[maf_name]:
                            u_weight=update_weights[maf_name][nrow]                                   
                            if nrow in nrows_maf_old_weights:
                                kerr=[]
                                rc=self.u_weights(kerr, 
                                                  attr_num=attr_num , 
                                                  num_row=row_num, 
                                                  u_weight=(maf_num, nrow, u_weight ))
                                if rc == False:
                                    pr ('177 HTMS_Mid_Err  Error in update_weights. Kerr= %d'%kerr+
                                                    'HT: '+self.ht.ht_name+'- Table: '+ self.maf_name)
                                    raise HTMS_Mid_Err('177 HTMS_Mid_Err    Error in update_weights. Kerr= %d'%kerr+
                                                    'HT: '+self.ht.ht_name+'- Table: '+ self.maf_name )

                            else:
                                add_weights[maf_name].update({nrow:u_weight})

            if len (add_weights) >0:
                new =True
            else:
                new = False 

            if len (delete_weights) >0 :
                dlt =True
            else:
                dlt = False 

            if new:
                for maf_name  in  add_weights:
                    if add_weights [ maf_name ]=={}:              
                        continue

                    maf_num = self.ht.get_maf_num ( maf_name)
                    if maf_num  == 0:
                        close_temp_mafs()
                        pr ('178 HTMS_Mid_Err    HT have no table : %s. ' % maf_name )
                        raise HTMS_Mid_Err('178 HTMS_Mid_Err    HT have no table : %s. ' % maf_name )

                    if maf_num != self.maf_num:
                        add_temp_mafs( self, maf_num)

                    mafs_to_weight.add ( maf_num )
                    weights = add_weights [ maf_name ]           
                    
                    if type(weights).__name__ != 'dict':
                        if     type ( weights ).__name__ == 'tuple'  and len(weights)==2:
                            rows = {rows[0]: rows[1]}
                        else:
                            close_temp_mafs()
                            pr ('179 HTMS_Mid_Err    Incorrect data in "add_weights" parameter :%s : %s. ' \
                                    % ( maf_name, str (row_num) ) )
                            raise HTMS_Mid_Err('179 HTMS_Mid_Err    Incorrect data in "add_weights" parameter :%s : %s. ' % 
                                      ( maf_name, str (row_num) ) )
                    for w in weights.keys(): 
                        if w == row_num  and  maf_num == self.maf_num:
                            close_temp_mafs()
                            pr ('180 HTMS_Mid_Err    Link to self row in self table prohibited :%s : %s. ' \
                                    % ( maf_name, str (row_num) ) )
                            raise HTMS_Mid_Err('180 HTMS_Mid_Err    Link to self row in self table prohibited :%s : %s. ' % 
                                      ( maf_name, str (row_num) ) )
                        add_we[( maf_num, w)]= weights[w]
                                    
            if dlt:
                for maf_name  in  delete_weights:
                    if delete_weights [ maf_name ]==set() or delete_weights [ maf_name ]==():              
                        continue
                    maf_num = self.ht.get_maf_num ( maf_name)
                    if maf_num  == 0:
                        close_temp_mafs()
                        pr ('182 HTMS_Mid_Err    HT have no table : %s. ' % maf_name )
                        raise HTMS_Mid_Err('182 HTMS_Mid_Err    HT have no table : %s. ' % maf_name )

                    add_temp_mafs( self, maf_num)
                    mafs_to_weight.add ( maf_num )
                    rows = delete_weights [ maf_name ]

                    if type ( rows ).__name__ != 'tuple':
                        if type ( rows ).__name__ == 'dict':
                            rows =tuple(r for r in rows.keys() if r>0)
                        elif type ( rows ).__name__ == 'int'   or   rows == 'all':
                            rows = ( rows, )
                        elif  type ( rows ).__name__ == 'set':
                            rows = tuple (rows)
                        else:
                            close_temp_mafs()
                            pr ('184 HTMS_Mid_Err    Incorrect data in "delete_weights" parameter :%s : %s. ' % ( maf_name, str (rows) ) )
                            raise HTMS_Mid_Err('184 HTMS_Mid_Err    Incorrect data in "delete_weights" parameter :%s : %s. ' % ( maf_name, str (rows) ) )
                    for r in rows: 
                        delete_we.add( ( maf_num, r) )
            kerr =[]

            if  len (add_we) +len (delete_we) <= 1:     #  CASE 1  - simple  - only one parameter
                for_u_weight = None
                if delete_weights == 'all':
                    kerr =[]
                    deleted_weights_tuple = self.r_weights( kerr, 
                           attr_num = attr_num, num_row= row_num)
                    if is_err( kerr ) >= 0 :
                        close_temp_mafs()      
                        pr ('188 HTMS_Mid_Err    Error read old weight.  err = %s' % str (kerr)  )
                        raise HTMS_Mid_Err('188 HTMS_Mid_Err    Error read old weight.  err = %s' % str (kerr)  )  
                    if len (deleted_weights_tuple) >0 :
                        for_u_weight = ()
                        #deleted_weights_dict={}
                        for wr in deleted_weights_tuple:
                            nmaf_w=wr[0]
                            #nrow_w=wr[1]
                            #weight_w=wr[2]
                            if not nmaf_w in old_weights:
                                #deleted_weights_dict[nmaf_w]={nrow_w:weight_w}
                                add_temp_mafs(self, nmaf_w )
                            #else:
                            #    deleted_weights_dict[nmaf_w].update={nrow_w:weight_w}                       

                    else:
                        close_temp_mafs()
                        self.ht.updated = time.time()
                        return True

                elif new:
                    keys= list(add_we.keys())
                    key= keys[0]
                    maf = key[ 0 ]
                    row = key[ 1 ]
                    weight= add_we[key]
                    for_u_weight = ( maf, row, weight)
                    rc = self.ht.correct_back_links (maf_num = maf, row_num =  row, 
                                                     back_link = (self.maf_num, row_num),  
                                                     func = 'add', typ='*weight')          
                    if rc == False : 
                        close_temp_mafs()
                        pr ('189 HTMS_Mid_Err    Error correct back weights    Maf = %d, row =: %d. ' % ( maf, row ))
                        raise HTMS_Mid_Err('189 HTMS_Mid_Err    Error correct back weights    Maf = %d, row =: %d. ' % ( maf, row ))                                 
                else:  # dlt
                    keys= list(delete_we)
                    key= keys[0]
                    maf = key[ 0 ]
                    row = key[ 1 ]
                    if  row == 'all':             #    ???????????????????      
                        for_u_weight = ( -maf, 0)
                    else:
                        for_u_weight = ( maf, -row)

                    rc = self.ht.correct_back_links (maf_num = maf, row_num =  row, 
                                                       back_link = (self.maf_num, row_num),  
                                                       func = 'delete', typ='*weight')          
                    if rc == False : 
                        close_temp_mafs()
                        pr ('190 HTMS_Mid_Err    Error correct back weights    Maf = %d, row =: %d. ' % ( maf, row ) )
                        raise HTMS_Mid_Err('190 HTMS_Mid_Err    Error correct back weights    Maf = %d, row =: %d. ' % ( maf, row ) )                                 

                if for_u_weight != None:
                    kerr = []
                    rc = self.u_weights (kerr, attr_num= attr_num, 
                                         num_row=row_num,  u_weight = for_u_weight  )

                        # u_weight = () - clear all element   

                        # ????? u_weight =(nmaf, 0) - - indicate weights to all maf's rows with weight=0.0   
                        #   
                        # u_weight =(-nmaf, *) - delete all weights to maf
                        # u_weight =(nmaf,-num_row,) - delete one weight to maf , to  num_row
                        # u_weight =(nmaf,num_row,) -  add  weight to maf - num_row, if not exist
                    if is_err( kerr ) >= 0 :
                        close_temp_mafs()      
                        pr ('194 HTMS_Mid_Err    Error updatel weight.  err = %s' % str (kerr)  )
                        raise HTMS_Mid_Err('194 HTMS_Mid_Err    Error updatel weight.  err = %s' % str (kerr)  )            

                    close_temp_mafs()

                    self.ht.updated = time.time()

                    return True

            #  CASE  2 - common
            if True:
                kerr = []
                old_weights =  self.r_weights( kerr, attr_num = attr_num, num_row= row_num) 
                if is_err( kerr ) >= 0 :
                    close_temp_mafs()      
                    pr ('195 HTMS_Mid_Err    Error read old weight.  err = %s' % str (kerr)  )
                    raise HTMS_Mid_Err('195 HTMS_Mid_Err    Error read old weight.  err = %s' % str (kerr)  )   
                new_weights =  {}
                old_weights_to_another_mafs ={}
                for tup in range(0, len(old_weights)):
                    old_weights_to_another_mafs[
                        ( old_weights[tup][0], old_weights[tup][1] )
                        ]                  = old_weights[tup][2]

                for maf_to_weight in mafs_to_weight:
                    # weight to whole maf
                    if ( maf_to_weight, 'all' ) in delete_we :
                             #new_weights = new_weights | { ( - maf_to_weight, 0 ) } 
                        rc = self.ht.correct_back_links (maf_num = maf_to_weight, row_num = 'all', 
                                                         back_link = (self.maf_num, row_num),  
                                                         func = 'delete', typ='*weight')          
                        if rc == False  : 
                            close_temp_mafs()
                            pr ('196 HTMS_Mid_Err    Error correct back weights    Maf = %d, row =: %s. ' % ( maf_to_weight, 'all' ) )
                            raise HTMS_Mid_Err('196 HTMS_Mid_Err    Error correct back weights    Maf = %d, row =: %s. ' % ( maf_to_weight, 'all' )  )   
                        
                        for wi in old_weights_to_another_mafs.copy():
                            if wi [ 0 ] == maf_to_weight:
                                old_weights_to_another_mafs.remove( wi )

                    elif ( maf_to_weight, 'all' ) in add_we :
                        close_temp_mafs()
                        pr ('197 HTMS_Mid_Err   Add weights to all maf rows unsupported    Maf = %d, row =: %s. ' % (maf_to_weight, 'all' ) )
                        raise HTMS_Mid_Err('197 HTMS_Mid_Err    Add weights to all maf rows unsupported    Maf = %d, row =: %s. ' % ( maf_to_weight, 'all' ) )     
                    else:
                        # weights to singular row

                        old_we_regular={}
                        for tup in range(0, len(old_weights)):
                            if  old_weights[tup][0]== maf_to_weight:
                                old_we_regular[
                                    ( old_weights[tup][0], old_weights[tup][1] )
                                    ]                  = old_weights[tup][2]

                        add_we_regular={}
                        for key in add_we:
                            if  key[0] == maf_to_weight and \
                                type ( key[1] ).__name__ == 'int' and \
                                key[1] >0:
                                add_we_regular[key]= add_we[key] 

                        del_we_regular = set( (ma, ro)  for (ma, ro)  in delete_we   
                                               if  ma == maf_to_weight and \
                                                   type(ro).__name__ == 'int' \
                                                   and  ro > 0      ) 

                        rows_delete=set( link[1] for link in del_we_regular)
                        rows_add=   set( link[1] for link in add_we_regular.keys() \
                                                     if link not in del_we_regular)

                        rc = self.ht.correct_back_links ( maf_num = maf_to_weight, 
                                                          row_num =  rows_add, 
                                                          back_link = (self.maf_num, row_num),  
                                                          func = 'add', typ='*weight')          
                        if rc == False  :
                            close_temp_mafs() 
                            pr ('198 HTMS_Mid_Err    Error correct back weights    Maf = %d, rows =: %s. ' % ( maf_to_weight, str(rows_add) ) )
                            raise HTMS_Mid_Err('198 HTMS_Mid_Err    Error correct back weights    Maf = %d, rows =: %s. ' % (maf_to_weight, str(rows_add) ) )     

                        rc = self.ht.correct_back_links ( maf_num = maf_to_weight, 
                                                          row_num = rows_delete, 
                                                          back_link = (self.maf_num, row_num),  
                                                          func = 'delete', typ='*weight')          
                        if rc == False :
                            close_temp_mafs() 
                            pr ('199 HTMS_Mid_Err    Error correct back weights    Maf = %d, rows =: %s. ' % (maf_to_weight, str(rows_delete) ) )
                            raise HTMS_Mid_Err('199 HTMS_Mid_Err    Error correct back weights    Maf = %d, rows =: %s. ' % ( maf_to_weight, str(rows_delete)) )     


                        for old_w in old_weights_to_another_mafs.copy():
                            if old_w in old_we_regular:
                                old_weights_to_another_mafs.pop(old_w)
                        old_we_regular.update( add_we_regular )
                        for pl in old_we_regular:
                            if pl not in del_we_regular:
                                new_weights.update( {pl:old_we_regular[pl]} )

                new_weights.update( old_weights_to_another_mafs )

                self.w_weights ( kerr, attr_num=attr_num, num_row=row_num, 
                                weights= new_weights, rollback=False)
                    # weight =(nmaf, 0) - set row =0 - indicate weight to whole maf

                if is_err( kerr ) >= 0 :
                    close_temp_mafs()      
                    pr ('200 HTMS_Mid_Err    Error write new weights.  err = %s' % str (kerr)  )
                    raise HTMS_Mid_Err('200 HTMS_Mid_Err    Error write new weights.  err = %s' % str (kerr)  )   
            close_temp_mafs()

            self.ht.updated = time.time()

            return True

#------------------------------------------------------------------------------------------------


    def single_weight (self, row_num =0, attr_name='', to_table ='', to_row=0 ) :
        typ = type(to_table).__name__
        if typ != 'str':
            to_table=  to_table.maf_name
        if to_row < 0:
            self.update_weights ( row_num =row_num, attr_name=attr_name, delete_weights= { to_table: - to_row } ) 
        elif to_row == 0:
            self.update_weights ( row_num =row_num, attr_name=attr_name, delete_weights= { to_table: 'all' } ) 
        else:
            self.update_weights ( row_num =row_num, attr_name=attr_name, update_weights= { to_table:  to_row } ) 
        
#------------------------------------------------------------------------------------------------

    def multi_weight (self, row_num =0, attr_name='', to_table ='', to_rows=() ) :
        typ = type(to_table).__name__
        if typ != 'str':
            to_table=  to_table.maf_name
        if to_rows == ():
            self.update_weights ( row_num =row_num, attr_name=attr_name, delete_weights= { to_table: 'all'  } ) 
        else:
            self.update_weights ( row_num =row_num, attr_name=attr_name, update_weights= { to_table:  to_rows } ) 
        
#------------------------------------------------------------------------------------------------

    def copy_table(self, new_table_name='', only_data= True, 
             links_fields='blanc', weights_fields='blanc',
             only_fields=set(), with_fields={}):

        if self.ht.mode in ('rs', 'rm' ):
            pr ('101 HTMS_Mid_Err    Mode "%s" incompatible for copy tables in HT "%s".'
                    % (self.ht.mode, self.ht.ht_name)
            )
            raise HTMS_Mid_Err('101 HTMS_Mid_Err    Mode "%s" incompatible for copy tables in HT "%s".'
                    % (self.ht.mode, self.ht.ht_name)
            )
        table_obj_attrs = set ( self.fields.keys()  )
        fields_names=set()
        if type (only_fields).__name__ != 'set':
            pr ('102 HTMS_Mid_Err    Error:  only_fields is not the set.')
            raise HTMS_Mid_Err ('102 HTMS_Mid_Err    Error:  only_fields is not the set.'  )
        if type (with_fields).__name__ != 'dict':
            pr ('103 HTMS_Mid_Err    Error:  with_fields is not the dictionary.')
            raise HTMS_Mid_Err ('103 HTMS_Mid_Err    Error:  with_fields is not the dictionary..'  )
        if  only_fields and not only_fields.issubset( table_obj_attrs):
            pr ('104 HTMS_Mid_Err    Error:  only_fields  not belongs to table fields.')
            raise HTMS_Mid_Err ('104 HTMS_Mid_Err    Error: only_fields  not belongs to table fields.')
        if  with_fields and not keys(with_fields).issubset( table_obj_attrs):
            pr ('105 HTMS_Mid_Err    Error:  with_fields  not belongs to table fields.')
            raise HTMS_Mid_Err ('105 HTMS_Mid_Err    Error:  with_fields  not belongs to table fields.')

        if  only_fields:
            fields_names = list ( only_fields ) 
        else :              
            fields_names = list ( table_obj_attrs) 

        nmaf= self.maf_num

        try:
            new_table= Table( 
                ht_root=self.ht.ht_root, ht_name = self.ht.db_name,  
                t_name= new_table_name, local_root=self.ht.local_root)
        except:     
            pr ( '110 HTMS_Mid_Err    Error create copy table:   "%s"' % new_table_name )
            raise HTMS_Mid_Err ( '110 HTMS_Mid_Err    Error create copy table:  "%s"' % new_table_name )

        new_nmaf=  new_table.maf_num

        num_temp_mafs_opened = 0
        temp_mafs_opened ={}         
        def close_temp_mafs():
                    nonlocal temp_mafs_opened
                    if len ( temp_mafs_opened )>0:
                        for mf in  temp_mafs_opened :
                            temp_mafs_opened[ mf ].close()
                    del temp_mafs_opened
                    temp_mafs_opened ={}  

        def add_temp_mafs( self, temp_maf):
                    nonlocal temp_mafs_opened,  num_temp_mafs_opened
                    if  not ( temp_maf in self.ht.mafs_opened):
                            temp_mafs_opened[ temp_maf ] =Table(
                                ht_root=self.ht.ht_root, ht_name= self.ht.ht_name, t_nmaf= temp_maf )
                            num_temp_mafs_opened += 1

        for field_name in  fields_names:
            field=  self.fields[ field_name ]
            if field[0] in (1,2,3):
                continue                                             #weight
            if  only_data and  field[1] in ('*link', '*weight'):
                    continue
            kerr=[]
            new_table.field(kerr, attr_num_f= field[0] )
            if is_err( kerr ) >= 0 :     
                    pr ( '111 HTMS_Mid_Err    Error add new field.  err = %s' % str (kerr)  )
                    raise HTMS_Mid_Err ( '111 HTMS_Mid_Err    Error add new field. .  err = %s' % str (kerr)  )

        if self.rows >0:
            for nrow in range (1,  self.rows+1):
                kerr=[]
                rc=new_table.row(kerr, fun='add',  after =nrow-1, number=1 )
                if rc == False or  is_err( kerr ) >= 0 :    
                    pr ( '112 HTMS_Mid_Err    Error add new row.  err = %s' % str (kerr)  )
                    raise HTMS_Mid_Err ( '112 HTMS_Mid_Err    Error add new ro .  err = %s' % str (kerr)  )
                for field_name in  fields_names:
                    field=  self.fields[ field_name ]
                    kerr=[]
                    nattr= field[0]
                    attr_name = field_name
                    data_type = field[1]

                    if nattr in (1,2,3) and  links_fields=='full':
                        rc=True
                        kerr =[]
                        back_links = self.r_links ( kerr , attr_num= 1 , num_row= nrow )
                        if is_err( kerr ) >= 0 :      
                            pr ( '113 HTMS_Mid_Err    Error read back_links during delete row=%s in table=%s, err = %s' % 
                                (str(nrow),self.ht.mafs[self.maf_num]['name'],str (kerr) ) )
                            raise HTMS_Mid_Err ( '113 HTMS_Mid_Err    Error read back_links during delete row=%s in table=%s, err = %s' % 
                                (str(rnrow),self.ht.mafs[self.maf_num]['name'],str (kerr) ) )
                        if back_links != ():
                            for bli in back_links:      # 1st  loop:   search in back_links
                                linking_maf_n = bli [ 0 ]
                                row_in_linking_maf =  bli [ 1 ]
                                add_temp_mafs( self, linking_maf_n)       
                                linking_maf =   self.ht.mafs_opened[ linking_maf_n ]     
                                for field  in   linking_maf.fields:     # 2nd loop : search   link's fields 
                                                                                         # in row =    row_in_linking_maf
                                    if   linking_maf.fields[ field ][ 0 ] >1  and  \
                                         linking_maf.fields[ field ][ 1 ] == '*link':  
                                        kerr=[]
                                        old_links = linking_maf.r_links ( 
                                            kerr , attr_num=  linking_maf.fields[ field ][ 0 ] , num_row= row_in_linking_maf )
                                        if is_err( kerr ) >= 0 :      
                                            pr ( '114 HTMS_Mid_Err    Error read links of parent row during delete row.  err = %s' % str (kerr)  )
                                            raise HTMS_Mid_Err ( '114 HTMS_Mid_Err    Error read links of parent row during delete row.  err = %s' % str (kerr)  )
                                        if old_links !=():
                                            new_links =()
                                            found=False
                                            for wli in old_links:   #  3rd loop : search direct link to self part form whole field
                                                if wli[ 0 ] == self.maf_num and wli [ 1 ] == nrow : # found direct link to self
                                                    new_links = old_links + ( ( new_nmaf, nrow), )
                                                    found= True
                                                    break
                                            if found:
                                                rc = linking_maf.w_links ( 
                                                    kerr , 
                                                    attr_num=  linking_maf.fields[ field ][ 0 ] , 
                                                    num_row= row_in_linking_maf, 
                                                    links = new_links
                                                )
                            kerr=[]
                            rc = new_table.w_links ( kerr , attr_num= 1 , num_row= nrow, links= back_links)
                            if rc == False or is_err( kerr ) >= 0 :     
                                pr ( '115 HTMS_Mid_Err    Error write back_links field.  err = %s' % str (kerr)  )
                                raise HTMS_Mid_Err ( '115 HTMS_Mid_Err    Error write back_links field.  err = %s' % str (kerr)  )

                    if  nattr >3:

                        if  not data_type in ('*link', '*weight'):
                    
                                rc=True
                                if data_type[:3]  == 'dat':
                                    df=  self.r_utf8(Kerr=kerr, attr_num=nattr, num_row =nrow)
                                    if df == None:
                                        df=' '
                                    rc=  new_table.w_utf8(Kerr=kerr,  attr_num=nattr, num_row =nrow, string=df )
                                elif data_type in ( "int4", "int8","float4","float8","time") :                   
                                    numb= self.r_numbers (Kerr=kerr,  attr_num=nattr, num_row =nrow)
                                    rc= new_table.w_numbers (Kerr=kerr,  attr_num=nattr, num_row =nrow, numbers = numb)
                                elif data_type in ( "*int4", "*int8","*float4","*float8") :
                                    numbers = self.r_numbers (Kerr=kerr,  attr_num=nattr, num_row =nrow)
                                    rc= new_table.w_numbers (Kerr=kerr,  attr_num=nattr, num_row =nrow, numbers = numbers)
                                elif data_type.find( "byte") != -1 :
                                    if data_type[0] == '*':
                                        bytes = self.r_bytes(Kerr=kerr,  attr_num=nattr, num_row =nrow)
                                        rc =new_table.w_bytes(Kerr=kerr,  attr_num=nattr, num_row =nrow, bytes=bytes)
                                    else:
                                        bytes = self.r_elem(Kerr=kerr,  attr_num=nattr, num_row =nrow)
                                        rc = new_table.w_elem(Kerr=kerr,  attr_num=nattr, num_row =nrow, elem=bytes)
                                elif data_type.find("utf") != -1 :
                                    if data_type[0] == '*':
                                        chars = self.r_str (Kerr=kerr,  attr_num=nattr, num_row =nrow)
                                        rc = new_table.w_str (Kerr=kerr,  attr_num=nattr, num_row =nrow, string=chars)
                                    else:
                                        chars = self.r_utf8(Kerr=kerr,  attr_num=nattr, num_row =nrow)
                                        rc = new_table.w_utf8(Kerr=kerr,  attr_num=nattr, num_row =nrow,  string=chars)
                                elif data_type =="file" :
                                    temp_file_name= str(time.time())+'.tmp'
                                    db_root= self.ht.db_root
                                    file_path=  path.join(db_root, temp_file_name)
                                    kerr=[]
                                    file_descr= self.download_file (kerr , 
                                              attr_num= nattr, 
                                              num_row =n_row,
                                              to_path=file_path)
                                    if file_descr == False or is_err( kerr ) >= 0 :     
                                        pr ( '117 HTMS_Mid_Err    Error read file.  err = %s' % str (kerr)  )
                                        raise HTMS_Mid_Err ( '117 HTMS_Mid_Err    Error read file. .  err = %s' % str (kerr)  )
                                    if file_descr == False:
                                        pass
                                    kerr=[]
                                    rc= new_table.upload_file (Kerr = kerr ,
                                        attr_num = nattr,
                                        num_row = nrow,    
                                        from_path=file_path,   
                                        real_file_name=file_descr['file_name'], 
                                        file_e=file_descr['file_ext'], 
                                        content_t=file_descr['content_type'],  
                                        file_d={
                                            'file_length':file_descr['file_length'],
                                            'extra_content_type':file_descr['extra_content_type']
                                            },
                                    ) 
                                    if rc == False or is_err( kerr ) >= 0 :     
                                        pr ( '118 HTMS_Mid_Err    Error write file.  err = %s' % str (kerr)  )
                                        raise HTMS_Mid_Err ( '118 HTMS_Mid_Err    Error write file.  err = %s' % str (kerr)  )
                                    try:
                                        os.remove(file_path)
                                    except  (OSError, IOError):
                                        pass
                                    if rc == False:
                                        pass

                        else:
                            if  links_fields=='blanc' and weights_fields=='blanc' :
                                continue
                            elif  links_fields in ('full','ref'):
                                kerr=[]
                                links = self.r_links(Kerr=kerr,  attr_num=nattr, num_row =nrow)
                                if links== False or is_err( kerr ) >= 0 :     
                                    pr ( '120 HTMS_Mid_Err    Error read links field.  err = %s' % str (kerr)  )
                                    raise HTMS_Mid_Err ( '120 HTMS_Mid_Err    Error read links field.  err = %s' % str (kerr)  )
                                if links != ():
                                    new_links={}
                                    rows=set()
                                    for li in links:
                                        maf_name= self.ht.mafs[ li[0] ]['name']
                                        if maf_name in new_links:
                                            rows= new_links[maf_name] | {li[1],}
                                        else:
                                            rows = {li[1],}
                                        new_links[ maf_name ] = rows
                                    kerr=[]
                                    rc= new_table.update_links (
                                        row_num = nrow, 
                                        attr_name=attr_name, 
                                        add_links=new_links
                                        )
                                    if rc== False or is_err( kerr ) >= 0 :     
                                        pr ( '121 HTMS_Mid_Err    Error update links field.  err = %s' % str (kerr)  )
                                        raise HTMS_Mid_Err ( '121 HTMS_Mid_Err    update links field.  err = %s' % str (kerr)  )

                            elif  weights_fields in ('full','ref'):
                                kerr=[]
                                weights = self.r_weights(Kerr=kerr,  attr_num=nattr, num_row =nrow)
                                if weights== False or is_err( kerr ) >= 0 :     
                                    pr ( '122 HTMS_Mid_Err    Error read weights field.  err = %s' % str (kerr)  )
                                    raise HTMS_Mid_Err ( '122 HTMS_Mid_Err    Error read weights field.  err = %s' % str (kerr)  )
                                if weights != ():
                                    new_weights={}
                                    rows=set()
                                    for wi in weights:
                                        maf_name= self.ht.mafs[ wi[0] ]['name']
                                        if maf_name in new_weights:
                                            rows= new_weights[maf_name] | {wi[1],}
                                        else:
                                            rows = {wi[1],}
                                        new_weights[ maf_name ] = rows
                                    kerr=[]
                                    rc= new_table.update_weights (
                                        row_num = nrow, 
                                        attr_name=attr_name, 
                                        update_weights=new_weights
                                        )
                                    if rc== False or is_err( kerr ) >= 0 :     
                                        pr ( '123 HTMS_Mid_Err    Error update weights field.  err = %s' % str (kerr)  )
                                        raise HTMS_Mid_Err ( '123 HTMS_Mid_Err    update weights field.  err = %s' % str (kerr)  )


            close_temp_mafs()
            new_table.close() 
        return True

#-----------------------------------------------------------------------------------------------

    def copy_row(self, nrow=-1, after_row=-1, 
                 links_fields='blanc', 
                 weights_fields='blanc', 
                 only_fields=set()):

        if self.ht.mode in ('rs', 'rm' ):
            pr ('130 HTMS_Mid_Err    Mode "%s" incompatible for copy row in HT "%s".'
                    % (self.ht.mode, self.ht.ht_name)
            )
            raise HTMS_Mid_Err('130 HTMS_Mid_Err    Mode "%s" incompatible for copy row in HT "%s".'
                    % (self.ht.mode, self.ht.ht_name)
            )
        table_obj_attrs = set ( self.fields.keys()  )
        fields_names=set()
        if type (only_fields).__name__ != 'set':
            pr ('131 HTMS_Mid_Err    Error:  only_fields is not the set.')
            raise HTMS_Mid_Err ('131 HTMS_Mid_Err    Error:  only_fields is not the set.'  )
        if  only_fields and not only_fields.issubset( table_obj_attrs):
            pr ('132 HTMS_Mid_Err    Error:  only_fields  not belongs to table fields.')
            raise HTMS_Mid_Err ('132 HTMS_Mid_Err    Error: only_fields  not belongs to table fields.')
        if  nrow not in range(1, self.rows+1):
            pr ('133 HTMS_Mid_Err    Error:  Invalid nrow.')
            raise HTMS_Mid_Err ('133 HTMS_Mid_Err    Error:  Invalid nrow.')
        if  after_row not in range(-1, self.rows+1):
            pr ('134 HTMS_Mid_Err    Error:  Invalid after_row.')
            raise HTMS_Mid_Err ('134 HTMS_Mid_Err    Error:  Invalid after_row.')

        num_temp_mafs_opened = 0
        temp_mafs_opened ={}         
        def close_temp_mafs():
                    nonlocal temp_mafs_opened
                    if len ( temp_mafs_opened )>0:
                        for mf in  temp_mafs_opened :
                            temp_mafs_opened[ mf ].close()
                    del temp_mafs_opened
                    temp_mafs_opened ={}  

        def add_temp_mafs( self, temp_maf):
                    nonlocal temp_mafs_opened,  num_temp_mafs_opened
                    if  not ( temp_maf in self.ht.mafs_opened):
                            temp_mafs_opened[ temp_maf ] =Table(
                                ht_root=self.ht.ht_root, ht_name= self.ht.ht_name, 
                                t_nmaf= temp_maf, local_root=self.ht.local_root )
                            num_temp_mafs_opened += 1

        if  only_fields:
            fields_names = list ( only_fields ) 
        else :              
            fields_names = list ( table_obj_attrs) 

        if after_row<0 or after_row> self.rows:
            after_row=self.rows

        new_row= after_row+1

        nmaf= self.maf_num

        kerr=[]
        if True:
                rc=self.row(kerr, fun='add',  after =after_row, number=1 )
                if rc == False or  is_err( kerr ) >= 0 :    
                    pr ( '140 HTMS_Mid_Err    Error add new row.  err = %s' % str (kerr)  )
                    raise HTMS_Mid_Err ( '140 HTMS_Mid_Err    Error add new row.  err = %s' % 
                                        str (kerr)  )

                if nrow > after_row:
                    n_source_row= nrow+1
                else:
                    n_source_row= nrow
                    

                for field_name in  fields_names:
                    field=  self.fields[ field_name ]
                    kerr=[]
                    nattr= field[0]
                    attr_name = field_name
                    data_type = field[1]

                    if nattr in (1,2,3) and  links_fields=='full':
                        rc=True
                        kerr =[]
                        back_links = self.r_links ( kerr , attr_num= 1 , num_row= n_source_row )
                        if is_err( kerr ) >= 0 :      
                            pr ( '141 HTMS_Mid_Err    Error read back_links during delete row=%s in table=%s, err = %s' % 
                                (str(n_source_row),self.ht.mafs[self.maf_num]['name'],str (kerr) ) )
                            raise HTMS_Mid_Err ( '141 HTMS_Mid_Err    Error read back_links during delete row=%s in table=%s, err = %s' % 
                                (str(rn_source_row),self.ht.mafs[self.maf_num]['name'],str (kerr) ) )

                        if back_links != ():
                            for bli in back_links:      # 1st  loop:   search in back_links
                                linking_maf_n = bli [ 0 ]
                                row_in_linking_maf =  bli [ 1 ]
                                add_temp_mafs( self, linking_maf_n)       
                                linking_maf =   self.ht.mafs_opened[ linking_maf_n ]     
                                for field  in   linking_maf.fields:     # 2nd loop : search   link's fields 
                                                                                         # in row =    row_in_linking_maf
                                    if   linking_maf.fields[ field ][ 0 ] >3  and  \
                                         linking_maf.fields[ field ][ 1 ] == '*link':  
                                        kerr=[]
                                        old_links = linking_maf.r_links ( 
                                            kerr , 
                                            attr_num=  linking_maf.fields[ field ][ 0 ] , num_row= row_in_linking_maf )
                                        if is_err( kerr ) >= 0 :      
                                            pr ( '142 HTMS_Mid_Err    Error read links of parent row during delete row.  err = %s' % str (kerr)  )
                                            raise HTMS_Mid_Err ( '142 HTMS_Mid_Err    Error read links of parent row during delete row.  err = %s' % str (kerr)  )
                                        if old_links !=():
                                            new_links =()
                                            found=False
                                            for wli in old_links:   #  3rd loop : search direct link to self part form whole field
                                                if wli[ 0 ] == self.maf_num and wli [ 1 ] == n_source_row : # found direct link to self
                                                    new_links = old_links + ( ( nmaf, new_row), )
                                                    found= True
                                                    break
                                            if found:
                                                rc = linking_maf.w_links ( 
                                                    kerr , 
                                                    attr_num=  linking_maf.fields[ field ][ 0 ] , 
                                                    num_row= row_in_linking_maf, 
                                                    links = new_links
                                                )

                            rc = self.w_links ( kerr , attr_num= 1 , 
                                               num_row= new_row, links= back_links)
                            if rc == False or is_err( kerr ) >= 0 :     
                                pr ( '143 HTMS_Mid_Err    Error write back_links field.  err = %s' % str (kerr)  )
                                raise HTMS_Mid_Err ( '143 HTMS_Mid_Err    Error write back_links field.  err = %s' % str (kerr)  )

                    elif  nattr >3:

                        if  not data_type in ('*link', '*weight'):
                    
                                rc=True
                                if data_type[:3]  == 'dat':
                                    df=  self.r_utf8(Kerr=kerr, attr_num=nattr, 
                                                     num_row =n_source_row)
                                    if df == None:
                                        df=' '
                                    rc=  self.w_utf8(Kerr=kerr,  attr_num=nattr, 
                                                     num_row =new_row, string=df )
                                elif data_type in ( "int4", "int8","float4","float8","time") :                   
                                    numb= self.r_numbers (Kerr=kerr,  attr_num=nattr, 
                                                          num_row =n_source_row)
                                    rc= self.w_numbers (Kerr=kerr,  attr_num=nattr, 
                                                        num_row =new_row, numbers = numb)
                                elif data_type in ( "*int4", "*int8","*float4","*float8") :
                                    numbers = self.r_numbers (Kerr=kerr,  attr_num=nattr, 
                                                              num_row =n_source_row)
                                    rc= self.w_numbers (Kerr=kerr,  attr_num=nattr, 
                                                        num_row =new_row, numbers = numbers)
                                elif data_type.find( "byte") != -1 :
                                    if data_type[0] == '*':
                                        bytes = self.r_bytes(Kerr=kerr,  attr_num=nattr, 
                                                             num_row =n_source_row)
                                        rc =self.w_bytes(Kerr=kerr,  attr_num=nattr, 
                                                         num_row =new_row, bytes=bytes)
                                    else:
                                        bytes = self.r_elem(Kerr=kerr,  attr_num=nattr, 
                                                            num_row =n_source_row)
                                        rc = self.w_elem(Kerr=kerr,  attr_num=nattr, 
                                                         num_row =new_row, elem=bytes)
                                elif data_type.find("utf") != -1 :
                                    if data_type[0] == '*':
                                        chars = self.r_str (Kerr=kerr,  attr_num=nattr, 
                                                            num_row =n_source_row)
                                        rc = self.w_str (Kerr=kerr,  attr_num=nattr, 
                                                         num_row =new_row, string=chars)
                                    else:
                                        chars = self.r_utf8(Kerr=kerr,  attr_num=nattr, 
                                                            num_row =n_source_row)
                                        rc = self.w_utf8(Kerr=kerr,  attr_num=nattr, 
                                                         num_row =new_row,  string=chars)
                                elif data_type =="file" :

                                    """
                                    old_maf_elem_offset , old_maf_elem_length = old_maf.offsets [old_nattr] 

                                    old_maf_elem =  ht.cage.read(
                                            old_maf.ch,   
                                            ( n_source_row - 1 ) *old_maf.rowlen+old_maf_elem_offset, 
                                            old_maf_elem_length,
                                            Kerr
                                    )   
                                    if  old_maf_elem != b'\xFF'*32:
                                        old_bf_addr_file , old_file_length =   struct.unpack( '>QQQQ', old_maf_elem) [ 2: ]
                                        old_file_descr= old_maf.r_file_descr (Kerr = kerr , attr_num=old_nattr, num_row =n_source_row,  )

                                    if  old_maf_elem ==  b'\xFF'*32 or \
                                        old_file_descr == False or \
                                        old_file_descr == None or \
                                        old_file_length ==0: 

                                        new_file_descr = None
                                        new_maf_elem = b'\xFF'* 32

                                    else:

                                        new_bf_addr_file=new_ht.b_free

                                        num_chunks = math.ceil(old_file_length/PAGESIZE2)
                                        for chunk in range(num_chunks):
                                            if chunk<num_chunks-1:
                                                size=PAGESIZE2
                                            else:
                                                size= old_file_length - PAGESIZE2*chunk  

                                            data =  ht.cage2.read( 
                                                        ht.channels [ 'bf' ],  
                                                        old_bf_addr_file+PAGESIZE2*chunk,   
                                                        size, 
                                                        Kerr)   
                                            if data == False:
                                                    pr ('66 HTMS_Low_Err     Error read file in HT for copy. ')
                                                    del ht,  new_ht
                                                    raise HTMS_Low_Err ('66 HTMS_Low_Err     Error read file in HT for copy. ')

                                            rc1 =  new_ht.cage2.write( 
                                                        new_ht.channels [ 'bf' ],  
                                                        new_bf_addr_file+PAGESIZE2*chunk,   
                                                        data, 
                                                        Kerr)   
                                            if rc1 == False:
                                                    pr ('67 HTMS_Low_Err     Error write file to HT copy. ')
                                                    del ht,  new_ht
                                                    raise HTMS_Low_Err ('67 HTMS_Low_Err     Error write file to HT copy. ')

                                        new_file_length= old_file_length
                                        new_ht.b_free +=  new_file_length

                                        new_file_descr=old_file_descr

                                        new_af_addr_descr=new_ht.a_free
                                        rc2 =  new_ht.cage.write( 
                                                new_ht.channels [ 'af' ],  
                                                new_af_addr_descr,   
                                                new_file_descr, 
                                                kerr)   
                                        if rc2 == False:
                                                        pr ('68 HTMS_Low_Err     Error write file descriptor to HT copy. ')
                                                        del ht,  new_ht
                                                        raise HTMS_Low_Err ('68 HTMS_Low_Err     Error write file descriptor to HT copy.. ')
                                        
                                        new_ht.a_free +=  len(new_file_descr) 

                                    new_maf_elem =  struct.pack ('>QQQQ',  
                                            new_af_addr_descr,  
                                            len(new_file_descr), 
                                            new_bf_addr_file , 
                                            new_file_length 
                                    )

                                    new_maf_elem_offset , new_maf_elem_length = new_maf.offsets [nattr] 
                                    rc3 =  new_ht.cage.write( 
                                            new_maf.ch,   
                                            ( n_source_row - 1 ) *new_maf.rowlen+new_maf_elem_offset,   
                                            new_maf_elem , 
                                            kerr
                                        )   
                                    if rc3 == False:
                                            del ht,  new_ht
                                            raise HTMS_Low_Err ('69 HTMS_Low_Err   Error write MAF.' )
                                    """
                        else:
                            if  links_fields=='blanc' and weights_fields=='blanc':
                                continue
                            elif  data_type == '*link' and links_fields in ('full','ref'):
                                rc=True
                                links = self.r_links(Kerr=kerr,  attr_num=nattr, num_row =n_source_row)
                                if links== False or is_err( kerr ) >= 0 :     
                                    pr ( '150 HTMS_Mid_Err    Error read links field.  err = %s' % str (kerr)  )
                                    raise HTMS_Mid_Err ( '150 HTMS_Mid_Err    Error read links field.  err = %s' % str (kerr)  )
                                if links != ():
                                    new_links={}
                                    rows=set()
                                    for li in links:
                                        maf_name= self.ht.mafs[ li[0] ]['name']
                                        if maf_name in new_links:
                                            rows= new_links[maf_name] | {li[1],}
                                        else:
                                            rows = {li[1],}
                                        new_links[ maf_name ] = rows
                                    rc= self.update_links (
                                        row_num = new_row, 
                                        attr_name=attr_name, 
                                        add_links=new_links
                                        )
                                    if rc== False or is_err( kerr ) >= 0 :     
                                        pr ( '151 HTMS_Mid_Err    Error update links field.  err = %s' % str (kerr)  )
                                        raise HTMS_Mid_Err ( '151 HTMS_Mid_Err    update links field.  err = %s' % str (kerr)  )

                            elif  data_type == '*weight' and weights_fields in ('full','ref'):
                                rc=True
                                weights = self.r_weights(Kerr=kerr,  attr_num=nattr, num_row =n_source_row)
                                if weights== False or is_err( kerr ) >= 0 :     
                                    pr ( '152 HTMS_Mid_Err    Error read weights field.  err = %s' % str (kerr)  )
                                    raise HTMS_Mid_Err ( '152 HTMS_Mid_Err    Error read weights field.  err = %s' % str (kerr)  )
                                if weights != ():
                                    new_weights={}
                                    rows=set()
                                    for wi in weights:
                                        maf_name= self.ht.mafs[ wi[0] ]['name']
                                        if maf_name in new_weights:
                                            rows= new_weights[maf_name] | {wi[1],}
                                        else:
                                            rows = {wi[1],}
                                        new_weights[ maf_name ] = rows
                                    rc= self.update_weights (
                                        row_num = new_row, 
                                        attr_name=attr_name, 
                                        update_weights=new_weights
                                        )
                                    if rc== False or is_err( kerr ) >= 0 :     
                                        pr ( '154 HTMS_Mid_Err    Error update weights field.  err = %s' % str (kerr)  )
                                        raise HTMS_Mid_Err ( '154 HTMS_Mid_Err    update weights field.  err = %s' % str (kerr)  )

                close_temp_mafs()
        
                self.ht.updated = time.time()

                return True
#-----------------------------------------------------------------------------------------------

    def read_rows(self, rows=set(), from_row=1, quantity=1,   
                  links_fields=False, 
                 weights_fields=False, 
                 only_fields=set()):
        # rows=set() or  rows="all"
        if self.ht.mode in ('rs', 'rm' ):
            pr ('210 HTMS_Mid_Err    Mode "%s" incompatible for copy row in HT "%s".'
                    % (self.ht.mode, self.ht.ht_name)
            )
            raise HTMS_Mid_Err('210 HTMS_Mid_Err    Mode "%s" incompatible for copy row in HT "%s".'
                    % (self.ht.mode, self.ht.ht_name)
            )
        if from_row>self.rows:
            from_row>=self.rows
        if from_row+quantity-1>self.rows:
            quantity= self.rows- from_row
        table_obj_attrs = set ( self.fields.keys()  )
        fields_names=set()
        if type (only_fields).__name__ != 'set':
            pr ('211 HTMS_Mid_Err    Error:  only_fields is not the set.')
            raise HTMS_Mid_Err ('211 HTMS_Mid_Err    Error:  only_fields is not the set.'  )
        if  only_fields and not only_fields.issubset( table_obj_attrs):
            pr ('212 HTMS_Mid_Err    Error:  only_fields  not belongs to table fields.')
            raise HTMS_Mid_Err ('212 HTMS_Mid_Err    Error: only_fields  not belongs to table fields.')
        if  from_row not in range(1, self.rows+1):
            pr ('213 HTMS_Mid_Err    Error:  Invalid from_row.')
            raise HTMS_Mid_Err ('213 HTMS_Mid_Err    Error:  Invalid from_row.')
        """
        if :
            pr ('214 HTMS_Mid_Err    Error:  Invalid after_row.')
            raise HTMS_Mid_Err ('214 HTMS_Mid_Err    Error:  Invalid after_row.')
        """
        if  only_fields:
            fields_names = list ( only_fields ) 
            links_fields=True 
            weights_fields=True 
        else :              
            fields_names = list ( table_obj_attrs) 

        nmaf= self.maf_num

        kerr=[]
        row={}
        only_rows=[]
        if rows=="all":
            only_rows= range(1, self.rows+1)
        elif len(rows)>0:
            only_rows= [nr for nr in range(1, self.rows+1) if nr in rows]
        else: 
            if from_row+quantity> self.rows+1:
                quantity=self.rows+1-from_row 
            only_rows= [nr for nr in range(from_row, from_row+quantity)]            

        for nrow  in only_rows:
            row.update({nrow:{}})
            for field_name in  fields_names:
                field=  self.fields[ field_name ]
                kerr=[]
                nattr= field[0]
                attr_name = field_name
                data_type = field[1]

                if nattr in (1,2,3):                
                    continue
                else:
                    if  not data_type in ('*link', '*weight'):                     
                                rc=True
                                if data_type[:3]  == 'dat':
                                    value=  self.r_utf8(Kerr=kerr, attr_num=nattr, 
                                                     num_row =nrow)
                                elif data_type in ( "int4", "int8","float4","float8","time") :                   
                                    value= self.r_numbers (Kerr=kerr,  attr_num=nattr, 
                                                          num_row =nrow)
                                elif data_type in ( "*int4", "*int8","*float4","*float8") :
                                    value = self.r_numbers (Kerr=kerr,  attr_num=nattr, 
                                                              num_row =nrow)
                                elif data_type.find( "byte") != -1 :
                                    if data_type[0] == '*':
                                        value = self.r_bytes(Kerr=kerr,  attr_num=nattr, 
                                                             num_row =nrow)
                                    else:
                                        value = self.r_elem(Kerr=kerr,  attr_num=nattr, 
                                                            num_row =nrow)
                                elif data_type.find("utf") != -1 :
                                    if data_type[0] == '*':
                                        value = self.r_str (Kerr=kerr,  attr_num=nattr, 
                                                            num_row =nrow)
                                    else:
                                        value = self.r_utf8(Kerr=kerr,  attr_num=nattr, 
                                                            num_row =nrow)
                                elif data_type =="file" :
                                    value='file'
                    else:
                            if  not links_fields and not weights_fields:
                                continue
                            elif  data_type == '*link' and links_fields :
                                rc=True
                                value = self.r_links(Kerr=kerr,  attr_num=nattr, 
                                                     num_row =nrow)
                                if value== False or is_err( kerr ) >= 0 :     
                                    pr ( '215 HTMS_Mid_Err    Error read links field.  err = %s' % str (kerr)  )
                                    raise HTMS_Mid_Err ( '215 HTMS_Mid_Err    Error read links field.  err = %s' % str (kerr)  )
                            elif  data_type == '*weight' and weights_fields:
                                rc=True
                                value = self.r_weights(Kerr=kerr,  attr_num=nattr, 
                                                       num_row =nrow)
                                if value== False or is_err( kerr ) >= 0 :     
                                    pr ( '217 HTMS_Mid_Err    Error read weights field.  err = %s' % str (kerr)  )
                                    raise HTMS_Mid_Err ( '217 HTMS_Mid_Err    Error read weights field.  err = %s' % str (kerr)  )

                row[nrow].update({attr_name:value})

        return row

#-----------------------------------------------------------------------------------------------


    def row_tree (self, nrow=-1, levels_source=-1, levels_ref=-1, 
                  data_type='all', only_fields=set()):
        
        row_source_tree={}
        row_ref_trees={}
        if  nrow not in range(1, self.rows+1):
            pr ('300 HTMS_Mid_Err    Error:  Invalid nrow.')
            raise HTMS_Mid_Err ('300 HTMS_Mid_Err    Error:  Invalid nrow.')
        kerr=[]
        num_temp_mafs_opened = 0
        temp_mafs_opened ={}         
        def close_temp_mafs():
                    nonlocal temp_mafs_opened
                    if len ( temp_mafs_opened )>0:
                        for mf in  temp_mafs_opened :
                            temp_mafs_opened[ mf ].close()
                    del temp_mafs_opened
                    temp_mafs_opened ={}  

        def add_temp_mafs( self, temp_maf):
                    nonlocal temp_mafs_opened,  num_temp_mafs_opened
                    if  not ( temp_maf in self.ht.mafs_opened):
                            temp_mafs_opened[ temp_maf ] =Table(
                                ht_root=self.ht.ht_root, ht_name= self.ht.ht_name, 
                                t_nmaf= temp_maf, local_root=self.ht.local_root )
                            num_temp_mafs_opened += 1

        if levels_source!=0:
            def add_to_source_tree (parent=(), child=(), ):
                nonlocal only_fields
                nonlocal self
                nonlocal data_type

                nonlocal row_source_tree
                nonlocal level_source, nodes_discovered, nodes_parents
                nonlocal levels_source
                nonlocal num_temp_mafs_opened, temp_mafs_opened
                nonlocal add_temp_mafs
                kerr=[]

                if  level_source == levels_source:
                    return
                level_source +=1

                linking_maf_n = child[ 0 ]
                row_in_linking_maf =  child[ 1 ]
                add_temp_mafs( self, linking_maf_n)       
                linking_maf =   self.ht.mafs_opened[ linking_maf_n ]  
                tree_stop=False 

                if data_type=='*link':
                    for source_field  in   linking_maf.fields:     # 1nd loop :  
                        source_attr_num=  linking_maf.fields[ source_field ][ 0 ]
                        if  linking_maf.fields[ source_field ][ 1 ] == '*link' and \
                            source_attr_num != 1  and \
                            source_attr_num != 3  :
                            source_ref_links=linking_maf.r_links ( 
                                kerr , 
                                attr_num=  source_attr_num , 
                                num_row= row_in_linking_maf )
                            if  source_ref_links==False:
                                pr ('301 HTMS_Mid_Err  row_tree - r_links  Error: %s'%
                                    str(kerr))
                                raise HTMS_Mid_Err (
                                    '301 HTMS_Mid_Err   row_tree - r_links  Error: %s'%
                                    str(kerr))
                            if parent in source_ref_links:  # found ref link to child (from source to self)
                                if only_fields!=set():
                                    if source_field not in only_fields:
                                        tree_stop=True
                                break
                    if tree_stop:
                        return

                    row_source_tree.add_child (
                        parent=parent, 
                        n_attr=source_attr_num,
                        child=child, 
                        nodes_discovered=nodes_discovered,
                        )
                    if child in nodes_discovered:
                        level_source -=1
                        return
                    else:
                        nodes_discovered.add(child)

                    back_links = linking_maf.r_links ( 
                                    kerr , 
                                    attr_num= 1 , 
                                    num_row= row_in_linking_maf )
                    if  back_links==False:
                                pr ('302 HTMS_Mid_Err   row_tree - r_links  Error: %s'%
                                    str(kerr))
                                raise HTMS_Mid_Err (
                                    '302 HTMS_Mid_Err   row_tree - r_links  Error: %s'%
                                    str(kerr))
                    if back_links !=():
                        for bli in back_links:
                            #source_field_name= self.ht.mafs[bli[0]]['name']
                            add_to_source_tree (
                                parent= (linking_maf_n, row_in_linking_maf), 
                                child=bli)
                    else:
                        level_source -=1
                        return

                elif data_type=='*weight':
                    for source_field  in   linking_maf.fields:     # 1nd loop :  
                        source_attr_num=  linking_maf.fields[ source_field ][ 0 ]
                        if  linking_maf.fields[ source_field ][ 1 ] == '*weight' :
                            source_ref_weights=linking_maf.r_weights ( 
                                kerr , 
                                attr_num=  source_attr_num , 
                                num_row= row_in_linking_maf 
                                )
                            if  source_ref_weights==False:
                                        pr ('303 HTMS_Mid_Err   row_tree - r_weights  Error: %s'%
                                            str(kerr))
                                        raise HTMS_Mid_Err (
                                            '303 HTMS_Mid_Err   row_tree - r_weights  Error: %s'%
                                            str(kerr))
                            parent_found=False
                            for srw in source_ref_weights: # seek ref weight to child (from source to self)
                                if srw[0]== parent[0] and srw[1]==parent[1] :
                                    parent_found= True     # found
                                    if only_fields!=set():
                                        if source_field not in only_fields:
                                            tree_stop=True
                                    break
                            if parent_found:
                                break
                    if tree_stop:
                        return

                    row_source_tree.add_child (
                        parent=parent, 
                        n_attr=source_attr_num,
                        child=(child[0],child[1]), 
                        weight= child[2],
                        nodes_discovered=nodes_discovered,
                        )
                    if (child[0],child[1]) in nodes_discovered:
                        level_source -=1
                        return
                    else:
                        nodes_discovered.add((child[0],child[1]))

                    back_weights = linking_maf.r_links(
                                        kerr, 
                                        attr_num= 3, 
                                        num_row= row_in_linking_maf )
                    if  back_weights==False:
                                pr ('305 HTMS_Mid_Err   row_tree - r_links  Error: %s'%
                                    str(kerr))
                                raise HTMS_Mid_Err (
                                    '305 HTMS_Mid_Err   row_tree - r_links  Error: %s'%
                                    str(kerr))
                    if back_weights !=() :
                        for bli in back_weights:
                            weight=None
                            num_maf=bli[0]
                            num_row=bli[1]
                            add_temp_mafs( self, num_maf)
                            back_table= self.ht.mafs_opened[ num_maf ]
                            back_table_fields_names = list ( back_table.fields.keys() )
                        
                            for field_name in  back_table_fields_names:                                     
                                field=  back_table.fields[ field_name ]
                                f_nattr=field[0]
                                f_type= field[1]
                                if f_type=="*weight":
                                    weights_from_source= back_table.r_weights (
                                        kerr, 
                                        attr_num=f_nattr, 
                                        num_row=num_row )
                                    if  weights_from_source==False:
                                        pr ('306 HTMS_Mid_Err   row_tree - r_weights  Error: %s'%
                                            str(kerr))
                                        raise HTMS_Mid_Err (
                                            '306 HTMS_Mid_Err   row_tree - r_weights  Error: %s'%
                                            str(kerr))
                                    for w in weights_from_source:
                                        #  linking_maf_n =       child[ 0 ]
                                        #  row_in_linking_maf =  child[ 1 ]
                                        if w[0]== linking_maf_n and \
                                           w[1]== row_in_linking_maf:
                                               weight= w[2]
                                               break
                                    if weight:      # first finded weight will be attribute of the vertex
                                        break
                            if not weight:
                                return False
                            add_to_source_tree (
                                parent= (linking_maf_n, row_in_linking_maf),
                                        child= (bli[0], bli[1], weight) 
                                        )
                    else:
                        level_source -=1
                        return

                elif data_type=='all':

                    for source_field  in   linking_maf.fields:     # 1nd loop :  
                        source_attr_num=  linking_maf.fields[ source_field ][ 0 ]
                        tree_stop_1=False
                        tree_stop_2=False
                        if  linking_maf.fields[ source_field ][ 1 ] == '*link' and \
                            source_attr_num != 1  and source_attr_num != 3  :
                            source_ref_links=linking_maf.r_links ( 
                                kerr , 
                                attr_num=  source_attr_num , 
                                num_row= row_in_linking_maf )
                            if  source_ref_links==False:
                                pr ('307 HTMS_Mid_Err   row_tree - r_links  Error: %s'%
                                    str(kerr))
                                raise HTMS_Mid_Err (
                                    '307 HTMS_Mid_Err   row_tree - r_links  Error: %s'%
                                    str(kerr))
                            if parent in source_ref_links:  # found ref link to child (from source to self)
                                if only_fields!=set():
                                    if source_field not in only_fields:
                                        tree_stop_1=True
                                break
                        elif  linking_maf.fields[ source_field ][ 1 ] == '*weight' :
                            source_ref_weights=linking_maf.r_weights ( 
                                kerr , 
                                attr_num=  source_attr_num , 
                                num_row= row_in_linking_maf 
                                )
                            if  source_ref_weights==False:
                                        pr ('308 HTMS_Mid_Err   row_tree - r_weights  Error: %s'%
                                            str(kerr))
                                        raise HTMS_Mid_Err (
                                            '308 HTMS_Mid_Err   row_tree - r_weights  Error: %s'%
                                            str(kerr))
                            parent_found=False
                            for srw in source_ref_weights: # seek ref weight to child (from source to self)
                                if srw[0]== parent[0] and srw[1]==parent[1] :
                                    parent_found= True     # found
                                    if only_fields!=set():
                                        if source_field not in only_fields:
                                            tree_stop_2=True
                                    break
                            if parent_found:
                                break
                    if tree_stop_1 and tree_stop_2:
                        return

                    row_source_tree.add_child (
                        parent=parent, 
                        n_attr=source_attr_num,
                        child=(child[0],child[1]), 
                        weight= child[2],
                        nodes_discovered=nodes_discovered,
                        )
                    if (child[0],child[1]) in nodes_discovered:
                        level_source -=1
                        return
                    else:
                        nodes_discovered.add((child[0],child[1]))

                    back_weights = linking_maf.r_links(kerr, attr_num= 3, 
                                         num_row= row_in_linking_maf )
                    if  back_weights==False:
                                pr ('309 HTMS_Mid_Err   row_tree - r_weights  Error: %s'%
                                    str(kerr))
                                raise HTMS_Mid_Err (
                                    '309 HTMS_Mid_Err   row_tree - r_weights  Error: %s'%
                                    str(kerr))
                    back_links = linking_maf.r_links ( 
                                    kerr , 
                                    attr_num= 1 , 
                                    num_row= row_in_linking_maf )
                    if  back_links==False:
                                pr ('310 HTMS_Mid_Err   row_tree - r_links  Error: %s'%
                                    str(kerr))
                                raise HTMS_Mid_Err (
                                    '310 HTMS_Mid_Err   row_tree - r_links  Error: %s'%
                                    str(kerr))
                    if back_weights !=() or back_links !=():

                        if back_links !=():
                            for bli in back_links:
                                #source_field_name= self.ht.mafs[bli[0]]['name']
                                add_to_source_tree (
                                    parent= (linking_maf_n, row_in_linking_maf), 
                                    child=bli+(None,))

                        if back_weights !=() :
                            for bli in back_weights:
                                weight=None
                                num_maf=bli[0]
                                num_row=bli[1]
                                add_temp_mafs( self, num_maf)
                                back_table= self.ht.mafs_opened[ num_maf ]
                                back_table_fields_names = list ( back_table.fields.keys() )
                        
                                for field_name in  back_table_fields_names:                                     
                                    field=  back_table.fields[ field_name ]
                                    f_nattr=field[0]
                                    f_type= field[1]
                                    if f_type=="*weight":
                                        weights_from_source= back_table.r_weights (
                                            kerr, 
                                            attr_num=f_nattr, 
                                            num_row=num_row )
                                        if  weights_from_source==False:
                                            pr ('311 HTMS_Mid_Err   row_tree - r_weights  Error: %s'%
                                                str(kerr))
                                            raise HTMS_Mid_Err (
                                                '311 HTMS_Mid_Err   row_tree - r_weights  Error: %s'%
                                                str(kerr))
                                        for w in weights_from_source:
                                            #  linking_maf_n =       child[ 0 ]
                                            #  row_in_linking_maf =  child[ 1 ]
                                            if w[0]== linking_maf_n and \
                                               w[1]== row_in_linking_maf:
                                                   weight= w[2]
                                                   break
                                        if weight:      # first finded weight will be attribute of the vertex
                                            break
                                if not weight:
                                    return False
                                add_to_source_tree (
                                    parent= (linking_maf_n, row_in_linking_maf),
                                            child= (bli[0], bli[1], weight) 
                                            )
                    else:
                        level_source -=1
                        return

            if data_type=="*link":
                back_links = self.r_links ( kerr , attr_num= 1 , num_row= nrow )
                if  back_links==False:
                                pr ('313 HTMS_Mid_Err   row_tree - r_links  Error: %s'%
                                    str(kerr))
                                raise HTMS_Mid_Err (
                                    '313 HTMS_Mid_Err   row_tree - r_links  Error: %s'%
                                    str(kerr))
                if back_links !=() :
                    row_source_tree= Links_tree(
                        self.ht.ht_name, 
                        (self.maf_num, nrow), 
                        self.ht.ht_name+' : '+self.maf_name+' : '+str(nrow)+' - UP')
                    nodes_discovered=set()
                    nodes_parents=set()
                    level_source=0
                    for bli in back_links:
                        add_to_source_tree (
                            parent= (self.maf_num, nrow), 
                            child=bli
                            )

            elif data_type=="*weight":
                back_weights = self.r_links ( kerr , attr_num= 3 , num_row= nrow )
                if  back_weights==False:
                                pr ('315 HTMS_Mid_Err   row_tree - r_weights  Error: %s'%
                                    str(kerr))
                                raise HTMS_Mid_Err (
                                    '315 HTMS_Mid_Err   row_tree - r_weights  Error: %s'%
                                    str(kerr))
                if back_weights !=() :
                    row_source_tree=Weights_tree(
                        self.ht.ht_name, 
                        (self.maf_num, nrow), 
                        self.ht.ht_name+' : '+self.maf_name+' : '+str(nrow)+' - UP')
                    nodes_discovered=set()
                    nodes_parents=set()
                    level_source=0
                    for bli in back_weights:
                        weight=None
                        num_maf=bli[0]
                        num_row=bli[1]
                        add_temp_mafs( self, num_maf)
                        back_table= self.ht.mafs_opened[ num_maf ]
                        back_table_fields_names = list ( back_table.fields.keys() )
                        
                        for field_name in  back_table_fields_names:                                  
                            field=  back_table.fields[ field_name ]     #  self.fields[ field_name ]
                            f_nattr=field[0]
                            f_type= field[1]
                            if f_type=="*weight":
                                weights_from_source= back_table.r_weights (
                                    kerr, 
                                    attr_num=f_nattr, 
                                    num_row=num_row )
                                if  weights_from_source==False:
                                        pr ('316 HTMS_Mid_Err   row_tree - r_weights  Error: %s'%
                                            str(kerr))
                                        raise HTMS_Mid_Err (
                                            '316 HTMS_Mid_Err   row_tree - r_weights  Error: %s'%
                                            str(kerr))
                                for w in weights_from_source:
                                    if w[0]== self.maf_num and \
                                       w[1]== nrow:
                                            weight= w[2]
                                            break
                                if weight:      # first finded weight will be attribute of the vertex
                                    break
                        if weight:
                            add_to_source_tree (
                                parent= (self.maf_num, nrow), 
                                child= (bli[0], bli[1], weight) 
                            )

            elif data_type=="all":
                back_links = self.r_links ( kerr , attr_num= 1 , num_row= nrow )
                if  back_links==False:
                                pr ('317 HTMS_Mid_Err   row_tree - r_links  Error: %s'%
                                    str(kerr))
                                raise HTMS_Mid_Err (
                                    '317 HTMS_Mid_Err   row_tree - r_links  Error: %s'%
                                    str(kerr))
                back_weights = self.r_links ( kerr , attr_num= 3 , num_row= nrow )
                if  back_weights==False:
                                pr ('318 HTMS_Mid_Err   row_tree - r_weights  Error: %s'%
                                    str(kerr))
                                raise HTMS_Mid_Err (
                                    '318 HTMS_Mid_Err   row_tree - r_weights  Error: %s'%
                                    str(kerr))
                if back_weights !=() or back_links!=() :
                    row_source_tree=Weights_tree(
                        self.ht.ht_name, 
                        (self.maf_num, nrow), 
                        self.ht.ht_name+' : '+self.maf_name+' : '+str(nrow)+' - UP')
                    nodes_discovered=set()
                    nodes_parents=set()
                    level_source=0

                    if back_links!=() :
                        for bli in back_links:
                            add_to_source_tree (
                                parent= (self.maf_num, nrow), 
                                child=bli+(None,)
                                )
                    if back_weights !=():
                        for bli in back_weights:
                            weight=None
                            num_maf=bli[0]
                            num_row=bli[1]
                            add_temp_mafs( self, num_maf)
                            back_table= self.ht.mafs_opened[ num_maf ]
                            back_table_fields_names = list ( back_table.fields.keys() )
                        
                            for field_name in  back_table_fields_names:                                  
                                field=  back_table.fields[ field_name ]     #  self.fields[ field_name ]
                                f_nattr=field[0]
                                f_type= field[1]
                                if f_type=="*weight":
                                    weights_from_source= back_table.r_weights (
                                        kerr, 
                                        attr_num=f_nattr, 
                                        num_row=num_row )
                                    if  weights_from_source==False:
                                        pr ('319 HTMS_Mid_Err   row_tree - r_weights  Error: %s'%
                                            str(kerr))
                                        raise HTMS_Mid_Err (
                                            '319 HTMS_Mid_Err   row_tree - r_weights  Error: %s'%
                                            str(kerr))
                                    for w in weights_from_source:
                                        if w[0]== self.maf_num and \
                                           w[1]== nrow:
                                                weight= w[2]
                                                break
                                    if weight:      # first finded weight will be attribute of the vertex
                                        break
                            if weight:
                                add_to_source_tree (
                                    parent= (self.maf_num, nrow), 
                                    child= (bli[0], bli[1], weight) 
                                )

        if levels_ref!=0:
            def add_to_ref_tree (parent=(), attr_num=0, child=()):
                nonlocal only_fields
                nonlocal self
                nonlocal data_type
                nonlocal field, row_ref_trees
                nonlocal level_ref, nodes_discovered, nodes_parents
                nonlocal levels_ref
                nonlocal num_temp_mafs_opened, temp_mafs_opened
                nonlocal add_temp_mafs
                kerr=[]
                if level_ref == levels_ref:
                    return

                if child==() or parent==():
                    return
                 
                level_ref+=1
                
                kerr=[]

                if data_type=='*link':    
                    linking_maf_n = child [ 0 ]
                    row_in_linking_maf =  child[ 1 ]
                    add_temp_mafs( self, linking_maf_n)       
                    linking_maf =   self.ht.mafs_opened[ linking_maf_n ]     

                    row_ref_trees[field].add_child (
                            parent=parent, 
                            n_attr=attr_num,
                            child=child, 
                            nodes_discovered=nodes_discovered,
                            nodes_parents=nodes_parents,
                            )
                    if child in nodes_discovered:
                        level_ref -=1
                        return
                    else:
                        nodes_discovered.add(child)
                    if child in nodes_parents:
                        level_ref-=1
                        return
                    for fi in linking_maf.fields:
                        attr_num= linking_maf.fields[fi][0]
                        if linking_maf.fields[fi][1]== '*link' and \
                             attr_num !=1 and attr_num !=3  and \
                            (only_fields==set() or (only_fields!=set() and fi in only_fields)):

                            kerr=[]
                            links = linking_maf.r_links (
                                        kerr , 
                                        attr_num=attr_num, 
                                        num_row=row_in_linking_maf )
                            if  links==False:
                                pr ('320 HTMS_Mid_Err   row_tree - r_links  Error: %s'%
                                    str(kerr))
                                raise HTMS_Mid_Err (
                                    '320 HTMS_Mid_Err   row_tree - r_links  Error: %s'%
                                    str(kerr))
                            if links !=():
                                for li in links:
                                    nodes_parents.add((linking_maf_n, row_in_linking_maf ) )
                                    add_to_ref_tree(
                                        parent=(linking_maf_n, row_in_linking_maf ) , 
                                        attr_num=attr_num,
                                        child= li)

                    level_ref-=1
                    return

                elif data_type=='*weight':    
                    weighting_maf_n = child [ 0 ]
                    row_in_weighting_maf =  child[ 1 ]
                    add_temp_mafs( self, weighting_maf_n)       
                    weighting_maf =   self.ht.mafs_opened[ weighting_maf_n ]     

                    row_ref_trees[field].add_child (
                            parent=parent, 
                            n_attr=attr_num,
                            child=(child[0],child[1]),
                            weight= child[2],
                            nodes_discovered=nodes_discovered,
                            nodes_parents=nodes_parents,
                            )
                    if (child[0],child[1]) in nodes_discovered:
                        level_ref -=1
                        return
                    else:
                        nodes_discovered.add((child[0],child[1]))
                    if child in nodes_parents:
                        level_ref-=1
                        return
                    for fi in weighting_maf.fields:
                        attr_num= weighting_maf.fields[fi][0]
                        if weighting_maf.fields[fi][1]== '*weight' and \
                           (only_fields==set() or (only_fields!=set() and fi in only_fields)):

                            kerr=[]
                            weights = weighting_maf.r_weights (
                                            kerr , 
                                            attr_num=attr_num, 
                                            num_row=row_in_weighting_maf )
                            if  weights==False:
                                pr ('322 HTMS_Mid_Err   row_tree - r_weights  Error: %s'%
                                    str(kerr))
                                raise HTMS_Mid_Err (
                                    '322 HTMS_Mid_Err   row_tree - r_weights  Error: %s'%
                                    str(kerr))
                            if weights !=():
                                for wi in weights:
                                    nodes_parents.add((weighting_maf_n, row_in_weighting_maf ) )
                                    add_to_ref_tree(
                                        parent=(weighting_maf_n, row_in_weighting_maf ) , 
                                        attr_num=attr_num,
                                        child= wi)

                    level_ref-=1
                    return

                elif data_type=='all':    
                    any_maf_n = child [ 0 ]
                    row_in_any_maf =  child[ 1 ]
                    add_temp_mafs( self, any_maf_n)       
                    any_maf =   self.ht.mafs_opened[ any_maf_n ]     

                    row_ref_trees[field].add_child (
                            parent=parent, 
                            n_attr=attr_num,
                            child=(child[0],child[1]),
                            weight= child[2],
                            nodes_discovered=nodes_discovered,
                            nodes_parents=nodes_parents,
                            )
                    if (child[0],child[1]) in nodes_discovered:
                        level_ref -=1
                        return
                    else:
                        nodes_discovered.add((child[0],child[1]))
                    if child in nodes_parents:
                        level_ref-=1
                        return
                    for fi in any_maf.fields:
                        attr_num= any_maf.fields[fi][0]
                        kerr=[]

                        if any_maf.fields[fi][1]== '*weight' and \
                           (only_fields==set() or (only_fields!=set() and \
                           fi in only_fields)):

                            weights = any_maf.r_weights (
                                        kerr , 
                                        attr_num=attr_num, 
                                        num_row=row_in_any_maf )
                            if  weights==False:
                                pr ('323 HTMS_Mid_Err   row_tree - r_weights  Error: %s'%
                                    str(kerr))
                                raise HTMS_Mid_Err (
                                    '323 HTMS_Mid_Err   row_tree - r_weights  Error: %s'%
                                    str(kerr))
                            if weights !=():
                                for wi in weights:
                                    nodes_parents.add((any_maf_n, row_in_any_maf ) )
                                    add_to_ref_tree(
                                        parent=(any_maf_n, row_in_any_maf ) , 
                                        attr_num=attr_num,
                                        child= wi)
                        elif any_maf.fields[fi][1]== '*link' and \
                                (only_fields==set() or (only_fields!=set() and \
                                fi in only_fields)) and \
                                attr_num != 1 and \
                                attr_num != 3 :
                            links = any_maf.r_links (kerr, attr_num=attr_num, num_row=nrow )
                            if  links==False:
                                pr ('324 HTMS_Mid_Err   row_tree - r_links  Error: %s'%
                                    str(kerr))
                                raise HTMS_Mid_Err (
                                    '324 HTMS_Mid_Err   row_tree - r_links  Error: %s'%
                                    str(kerr))
                            if links !=():
                                for li in links:
                                    nodes_parents.add((any_maf_n, nrow) )
                                    add_to_ref_tree (
                                        parent=(any_maf_n, nrow), 
                                        attr_num= attr_num,
                                        child=li+(None,))
                    level_ref-=1
                    return

            for field in self.fields:
                attr_num= self.fields[field][0]
                if attr_num not in (1,2,3) and \
                   ( only_fields==set() or \
                        (only_fields!=set() and field in only_fields)
                    ):

                    if  data_type=='*link' and \
                        self.fields[field][1] == '*link':

                        row_ref_trees[field] = Links_tree(
                            self.ht.ht_name, 
                            (self.maf_num, nrow), 
                            self.ht.ht_name+' : '+self.maf_name+' : '+str(nrow)+' : '+field+' - DOWN')
                        nodes_discovered=set()
                        nodes_parents=set()
                        level_ref=0                
                        links = self.r_links (
                                    kerr, 
                                    attr_num=attr_num, 
                                    num_row=nrow )
                        if  links==False:
                                pr ('325 HTMS_Mid_Err   row_tree - r_links  Error: %s'%
                                    str(kerr))
                                raise HTMS_Mid_Err (
                                    '325 HTMS_Mid_Err   row_tree - r_links  Error: %s'%
                                    str(kerr))
                        if links !=():
                            for li in links:
                                nodes_parents.add((self.maf_num, nrow) )
                                add_to_ref_tree (
                                    parent=(self.maf_num, nrow), 
                                    attr_num= attr_num,
                                    child=li)

                    elif  data_type=='*weight' and \
                          self.fields[field][1] == '*weight':

                        row_ref_trees[field] = Weights_tree(
                            self.ht.ht_name, 
                            (self.maf_num, nrow), 
                            self.ht.ht_name+' : '+self.maf_name+' : '+str(nrow)+' : '+field+' - DOWN')
                        nodes_discovered=set()
                        nodes_parents=set()
                        level_ref=0                
                        weights = self.r_weights (
                                    kerr, 
                                    attr_num=attr_num, 
                                    num_row=nrow )
                        if  weights==False:
                                pr ('326 HTMS_Mid_Err   row_tree - r_weights  Error: %s'%
                                    str(kerr))
                                raise HTMS_Mid_Err (
                                    '326 HTMS_Mid_Err   row_tree - r_weights  Error: %s'%
                                    str(kerr))
                        if weights !=():
                            for wi in weights:
                                nodes_parents.add((self.maf_num, nrow) )
                                add_to_ref_tree (
                                    parent=(self.maf_num, nrow), 
                                    attr_num= attr_num,
                                    child=wi)

                    elif  data_type=='all' and \
                        self.fields[field][1] in  ('*link','*weight'):

                        row_ref_trees[field] = Weights_tree(
                                self.ht.ht_name, 
                                (self.maf_num, nrow), 
                                self.ht.ht_name+' : '+self.maf_name+' : '+str(nrow)+' : '+field+' - DOWN')
                        nodes_discovered=set()
                        nodes_parents=set()
                        level_ref=0       

                        if  self.fields[field][1] == '*weight':                            
                            weights = self.r_weights (
                                        kerr, 
                                        attr_num=attr_num, 
                                        num_row=nrow )
                            if  weights==False:
                                pr ('327 HTMS_Mid_Err   row_tree - r_weights  Error: %s'%
                                    str(kerr))
                                raise HTMS_Mid_Err (
                                    '327 HTMS_Mid_Err   row_tree - r_weights  Error: %s'%
                                    str(kerr))
                            if weights !=():
                                for wi in weights:
                                    nodes_parents.add((self.maf_num, nrow) )
                                    add_to_ref_tree (
                                        parent=(self.maf_num, nrow), 
                                        attr_num= attr_num,
                                        child=wi)

                        elif self.fields[field][1] == '*link' and \
                            attr_num != 1 and attr_num != 3 :
                            links = self.r_links (
                                        kerr, 
                                        attr_num=attr_num, 
                                        num_row=nrow )
                            if  links==False:
                                pr ('328 HTMS_Mid_Err   row_tree - r_links  Error: %s'%
                                    str(kerr))
                                raise HTMS_Mid_Err (
                                    '328 HTMS_Mid_Err   row_tree - r_links  Error: %s'%
                                    str(kerr))
                            if links !=():
                                for li in links:
                                    nodes_parents.add((self.maf_num, nrow) )
                                    add_to_ref_tree (
                                        parent=(self.maf_num, nrow), 
                                        attr_num= attr_num,
                                        child=li+(None,))
        close_temp_mafs()

        return row_source_tree, row_ref_trees

#-----------------------------------------------------------------------------------------------

    
"""
    def del_childs(self, node):
           node_childs= self.node_links[node]['childs']
           if node_childs==set():
               return
           for child in node_childs:
                self.node_links.remove_dyna_ind(child)
                del_childs(self, child)
                del  self.links_tree[child]

# -------------------------------------------------------------------------------------------

    def del_node(self, node):
        if node in self.links_tree:
            self.del_childs(node)
            del self.links_tree[ node]
        else:
            pr ('104 HTMS_Low_Err     Links_tree del_node error: "node" not found in tree' )
            raise HTMS_Low_Err (
                '104 HTMS_Low_Err     Links_tree_ del_node error: "node" not found in tree' )

# -------------------------------------------------------------------------------------------

    def check_tree (self):

        root_link= self.nodes_links.get_dyna_link(self.links_tree['root'])
        if root_link==():
             self.nodes_links=None
             self.links_tree={}
             return True
        new_tree={}
        for node in self.links_tree:
            if node=='root':
                new_tree.update[ 'root']= self.links_tree['root'] 
                continue
            node_link =  self.node_links.get_dyna_link(node)
            if node_link ==():
                self.del_childs(node)
            else:
                new_tree[node ]= self.links_tree[node]
        self.links_tree= new_tree
        return True

#------------------------------------------------------------------------------------------------

    def serialize_tree_DOT_ref (self):
        digraph='digraph '+self.name+' { '
        for vertex in self.DOT_vertexes:
            digraph+=' '+vertex+'; '
        for edge in self.DOT_edges:
            digraph+=' '+edge[0]+' -> '+edge[1]+' [label="'+edge[2]+'"]; '

#------------------------------------------------------------------------------------------------

    def serialize_tree_DOT_source (self):
        digraph='digraph '+self.name+' { '
        for vertex in self.DOT_vertexes:
            digraph+=' '+vertex+'; '
        for edge in self.DOT_edges:
            digraph+=' '+edge[1]+' -> '+edge[0]+' [label="'+edge[2]+'"]; '
 
#------------------------------------------------------------------------------------------------

"""  