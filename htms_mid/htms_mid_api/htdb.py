# HTMS middle level  v. 2.3 (Cage class v. 2.9)
# © A.S.Aliev, 2018-2021

"""

delete_row (self, nrow =0, protect= Links_array( () ) ) 
update_links (self, row_num =0, attr_name='', add_links={}, delete_links= {} ) 
single_link (self, row_num =0, attr_name='', to_table ='', to_row=0 )
multi_link (self, row_num =0, attr_name='', to_table ='', to_rows=() )
copy(self, new_table_name='', only_data= True, links_fields='blanc', only_fields=set(), with_fields={})
copy_row (self, nrow=-1, after_row=-1, only_data= True, links_fields='blanc', only_fields=set())
row_tree (self, nrow=-1, levels_source=-1, levels_ref=-1, only_fields=set(), with_fields={})
"""


import os
import posixpath
import pickle
import  struct  
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

    def __str__(self):
        return self.db_name

    def __del__(self):
        try:
            self.close()
            ht_ =  super( HTdb, self)
            del ht_
            del self
        except:
            pass

    def close(self, Kerr=[]):
        HTdb.removeinstances(self)
        ht_ =  super( HTdb, self)
        ht_.close()
        del self

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
            if type( val ).__name__ == 'dict'  and  len ( val ) > 0:                     #
                for l in val:                                                                                    #
                    self.channels.update ( val )                                                      #
        elif val in Types_htms.types.keys():
            new_attr = {}
            new_attr[ nam ] = val
            self.ht.update_attrs(  new_attr )
      
        else:
            self.__dict__[nam] = val

    def __init__(self,  server = '', db_name='', db_root = '', cage_name='', 
                 new = False, jwtoken='', zmq_context = False, from_subclass=False,
                 mode='wm'):

        self.can_setattr= False

        if db_name =='':
            self.db_name = type (self ).__name__
        else:
            self.db_name = db_name

        for db  in HT.getinstances():
            if  hasattr(db, 'ht_name') and\
                db.server_ip == server and\
                db.ht_name == self.db_name and\
                db.ht_root == db_root:
                if db.mode == 'rs':
                    if mode == 'rs':
                        pr ('141-0 HTMS_Mid_Err   W A R N I N G !!!  Data base  "%s" already exist and opened readonly. '
                            % db_name )
                    else:
                        pr ('141-1 HTMS_Mid_Err     Data base  "%s" already exist and opened, that is incompatible with exclusive status.'% db_name )
                        raise HTMS_Mid_Err('141-1 HTMS_Mid_Err     Data base  "%s" already exist and opened, that is incompatible with exclusive status.'% db_name  )
                        
                else:
                    pr ('141-2 HTMS_Mid_Err     Data base  "%s" already exist and opened in incompatible mode "%s". '%
                       (db_name, db.mode) )
                    raise HTMS_Mid_Err('141-2 HTMS_Mid_Err     Data base  "%s" already exist and opened in incompatible mode "%s". '%
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
                                  mode=mode)                  
                Kerr=[]
                self.relations= {}
                rc = self.ht.attribute(Kerr, fun='add', attr_name='Back_links', type= '*link')
                if rc  == False or is_err( Kerr ) >= 0 :      
                    pr ('142 HTMS_Mid_Err     Error create "Back_links" HT atrribute. ')
                    raise HTMS_Mid_Err('142 HTMS_Mid_Err     Error create "Back_links" HT atrribute. ' )  

                rc = self.ht.attribute(Kerr, fun='add', attr_name='Time_row', type= 'time')
                if rc  == False or is_err( Kerr ) >= 0 :      
                    pr ('143 HTMS_Mid_Err     Error create "Time_row" HT atrribute. ')
                    raise HTMS_Mid_Err('143 HTMS_Mid_Err     Error create "Time_row" HT atrribute. ' )  

                self.ht.save_adt(  Kerr)

            except HTMS_Low_Err as errcode:
                pr ('144 HTMS_Mid_Err     Data base  " %s " create error from low level. Error code: %s'%   \
                    (self.db_name, str(errcode) ) )
                raise HTMS_Mid_Err('144 HTMS_Mid_Err     Data base  " %s " create error from low level. Error code: %s'%    \
                    (self.db_name, str(errcode) ) )
        else:
            try:
                self.ht.__init__( ht_name =self.db_name,  server_ip = server, ht_root = db_root,
                                  cage_name=cage_name,  new = False, jwtoken=jwtoken, 
                                  from_subclass=True,
                                  mode=mode)

            except HTMS_Low_Err as errcode:
                    pr ('147 HTMS_Mid_Err     Data base  " %s " open error from low level. Error code: %s'%    \
                    (self.db_name, str(errcode) )  )
                    raise HTMS_Mid_Err('147 HTMS_Mid_Err     Data base  " %s " open error  from low level. Error code: %s'%   \
                    (self.db_name, str(errcode) ) )
            if not ('relations' in self.__dict__ ):
                self.relations= {}

        self.can_setattr= True

#------------------------------------------------------------------------------------------------

    def  relation (self, dic ):
            if type( dic ).__name__ == 'dict'  and  len ( dic ) > 0:
                self.relations.update ( dic )    
                
#----------------------------------------------------------------------------------------------------

    def correct_back_links (self, maf_num = 0, row_num= set(), back_link  = (),  func = 'add'):
    #  maf_num - is maf needed to correct back link(-s) 
    #  row_num - row or rows set or 'all' - is row(-s) needed to correct back link(-s)
    #  back_link = (self.maf_num, row_num) - element of back_link to add or delete
    #  func = 'add' or 'delete'
        maf_row_num =0
        if  ( type( maf_num ).__name__ == 'str'  and maf_num == 'all' ) or  \
            type( maf_num ).__name__ == 'set':
            if  not ( row_num == set() or ( type( row_num ).__name__ == 'str'  and row_num == 'all')  ):
                pr ('150 HTMS_Mid_Err     Invalid parameter "row_num". Must be empty set or "all". ')
                raise HTMS_Mid_Err('150 HTMS_Mid_Err     Invalid parameter "row_num". Must be empty set or "all".' )  

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
                pr ('151 HTMS_Mid_Err    Error in parameters types. ')
                raise HTMS_Mid_Err('151 HTMS_Mid_Err     Error in parameters types.' )  

            for row in rows:
                Kerr=[]
                rc = maf.u_links ( Kerr, attr_num=1, num_row=row,  u_link = correct_link )     
                        # u_link = () - clear all element   
                        # u_link =(nmaf, 0) - set row =0 - indicate link to whole maf
                        # u_link =(-nmaf, *) - delete all links to maf
                        # u_link =(nmaf,-num_row,) - delete one link to maf , to  num_row
                        # u_link =(nmaf,num_row,) -  add  link to maf - num_row, if not exist
                if rc == False  or  is_err( Kerr ) >= 0 : 
                    pr ('152 HTMS_Mid_Err    Error write new back links in Maf : "%s", row = %d.' \
                        % ( self.mafs[ nmaf ][ 'name' ], row ) )
                    raise HTMS_Mid_Err('152 HTMS_Mid_Err     Error write new back links in Maf : "%s", row = %d.' \
                        % ( self.mafs[ nmaf ][ 'name' ], row ) )

            if    temp_open_maf == True:
                maf.close()

        self.updated = time.time()

        return True

#------------------------------------------------------------------------------------------------------------------

    def erase_attribute (self, attr=0):

        if self.mode in ('rs', 'rm' ):
            pr ('160 HTMS_Mid_Err    Mode "%s" incompatible for erase_attribute for HT "%s".'
                    % (self.mode, self.ht_name)
            )
            raise HTMS_Mid_Err('160 HTMS_Mid_Err    Mode "%s" incompatible for erase_attribute for HT "%s".'
                    % (self.mode, self.ht_name)
            )
        if attr==0 or attr=='':
            return True
        nattr=0
        if type(attr).__name__=='int':
            if attr in self.attrs:
               name_attr= self.attrs [attr]['name']
               if name_attr in self.relations and self.relations[ name_attr ] == 'erased' :
                    pr ('155 HTMS_Mid_Err    Attribute already erased. ')
                    raise HTMS_Mid_Err('155 HTMS_Mid_Err     Attribute already erased.' )  
               else:
                   nattr= attr                     
            else:
                pr ('156 HTMS_Mid_Err    Invalid number of erasing attribute.  ')
                raise HTMS_Mid_Err('156 HTMS_Mid_Err      Invalid number of erasing attribute. ' )  

        elif type(attr).__name__=='str':
            for at in self.attrs:
                if attr ==  self.attrs [at]['name']:
                    if attr in self.relations and self.relations[attr ] == 'erased' :
                        pr ('157 HTMS_Mid_Err    Attribute already erased. ')
                        raise HTMS_Mid_Err('157 HTMS_Mid_Err     Attribute already erased.' )  
                    nattr= at
                    name_attr= attr
                    break
            if nattr ==0:
                pr ('158 HTMS_Mid_Err     Invalid name of erasing attribute. ')
                raise HTMS_Mid_Err('158 HTMS_Mid_Err       Invalid name of erasing attribute. ' )  
        else:
            pr ('159 HTMS_Mid_Err     Invalid type of parameter "attr". Must be int or str. ')
            raise HTMS_Mid_Err('159 HTMS_Mid_Err      Invalid type of parameter "attr". Must be int or str. ' )  

        for maf in self.mafs:
            if self.mafs [ maf ]['name'][ : 8] != 'deleted:':
                if self.models [maf ] [ nattr ] :
                    table = Table( ht_root=self.ht.ht_root, ht_name =self.ht_name, t_nmaf=maf)

                    table.update_fields ( del_fields={name_attr,} )

        self.relations[ name_attr ] = 'erased'
        t =  time.time()
        self.attrs[ nattr ]['update'] = t

        self.updated = t

        return True

# -------------------------------------------------------------------------------------------

    def update_RAM (self,  fun = '', attr_num_p=0, maf_num_p=0,  after_row =-1 , num_rows=0 ):

        if self.mode in ('rs', 'rm' ):
            pr ('170 HTMS_Mid_Err    Mode "%s" incompatible for update_RAM for HT "%s".'
                    % (self.mode, self.ht_name)
            )
            raise HTMS_Mid_Err('160 HTMS_Mid_Err    Mode "%s" incompatible for update_RAM for HT "%s".'
                    % (self.mode, self.ht_name)
            )

        delay_kerr =0

        if DEBUG_UPDATE_RAM:
            pr('\n\n  UPDATE_RAM ---   fun = %s, attr_num_p=%d, maf_num_p=%d,  after_row = %d, num_rows = %d' %  \
                (fun , attr_num_p, maf_num_p,  after_row, num_rows ))     

        atr_remove      =False
        maf_remove    =False
        field_remove   =False
        row_add          =False
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
                    if maf ==0 or row ==0: # link is zero or link to all maf
                        link_array.remove_dyna_link ( link )
                        continue
                    if (maf != 0 and maf == maf_num_p) :	# MAF matched
                        if (maf_remove) :	# removed MAF matched                         
                            link_array.remove_dyna_link ( link )
                        elif (row_delete ) :
                            if ( row > (after_row +  num_rows ) and row <= old_max_row_num ) :
									            # MAF row after deleted range - 
										        # correct number 
                                link_array.update_dyna_link ( link, ( maf, row -  num_rows ) ) 
                            elif (row > after_row and row <= (after_row +  num_rows )) :
                                link_array.remove_dyna_link ( link )
                            elif (row > old_max_row_num) :
                                                # found row number greater than last number - clear it
                                link_array.remove_dyna_link ( link )
                                delay_kerr = 474
                            else:
                                continue
                        elif (row_add ) :
                            if ( row > after_row  and row <= old_max_row_num ) :  
                                # MAF row in deleted range  and saved- correct number 											    
                                link_array.update_dyna_link ( link, ( maf, row +  num_rows ) )
                            elif (row > old_max_row_num ) :  
                                # found row number greater than last number before adding new rows -  delete set 
                                link_array.remove_dyna_link ( link )
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

        if len (  self.dyna_link ) > 0:            
            for ind in  self.dyna_link:
                if link == self.dyna_link [ ind ]:
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

        """
        if len (  self.dyna_link ) > 0:            
            for ind in  self.dyna_link:
                if link == self.dyna_link [ ind ]:
                    return
            ind = max ( self.dyna_link.keys() )
        else:
            ind = -1    
        self.dyna_link [ ind+1 ] =  link
        return
        """
# -------------------------------------------------------------------------------------------

    def update_dyna_link (self, link, new):

        if new == () or new[ 0 ] == 0 or new[ 1 ] == 0:
            return

        if len (  self.dyna_link ) > 0:            
            for ind in  self.dyna_link:
                if link == self.dyna_link [ ind ]:
                    self.dyna_link [ ind ] = new
                    return
        pr ('205 HTMS_Mid_Err     Link update impossible' )
        raise HTMS_Low_Err ('205 HTMS_Mid_Err     Link update impossible')

# -------------------------------------------------------------------------------------------

    def remove_dyna_link (self, link):
        for ind in  self.dyna_link:
            if link == self.dyna_link [ ind ]: 
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
            pr ('210 HTMS_Mid_Err     Links_tree init parameters error' )
            raise HTMS_Low_Err ('210 HTMS_Mid_Err     Links_tree init parameters error')

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

    def add_child (self, parent=(), n_attr=0, child=(), nodes_discovered=set(), nodes_parents=set()):
        #recursion=False
        if parent == () or parent[ 0 ] == 0 or parent[ 1 ] == 0 or\
            child == () or child[ 0 ] == 0 or child[ 1 ] == 0 or \
            type(n_attr).__name__!='int' or n_attr==0: 
            pr ('211 HTMS_Mid_Err     Links_tree_add_child parameters error' )
            raise HTMS_Low_Err ('211 HTMS_Mid_Err     Links_tree_add_child parameters error')

        child_maf= child[0]
        child_row=child[1]

        child_maf_name, child_maf_rows = get_maf( self.ht_name, child_maf )

        if child_maf_name=='' or child_maf_rows ==0 or child_row>child_maf_rows :
            pr ('215HTMS_Mid_Err     Links_tree_add_child parameters error' )
            raise HTMS_Low_Err ('215 HTMS_Mid_Err    Links_tree_add_child parameters error')

        parent_found=set()

        root_link = self.nodes_links.get_dyna_link (0)
        if root_link ==parent:
            parent_found.add(0)

        for node in  self.links_tree:

            node_link = self.nodes_links.get_dyna_link (node)
            if node_link == parent :
                parent_found.add(node)

        if not parent_found:
            pr ('217 HTMS_Mid_Err     Links_tree_add child error: "parent" not found in tree' )
            raise HTMS_Low_Err ('217 HTMS_Mid_Err     Links_tree add_child error: "parent" not found in tree' )

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
   
    def __init__(self, ht_root ='', ht_name ='', t_name='', t_nmaf=0):

        self.can_setattr= True

        for h_t  in HTdb.getinstances():
            if  h_t.ht_name == ht_name and h_t.ht_root==ht_root:
                ht_obj = h_t
                break
        if not ( 'ht_obj' in locals() ) :   
            pr ('01 HTMS_Mid_Err     HT " %s " not exist. '% ht_name )
            raise HTMS_Mid_Err('01 HTMS_Mid_Err     HT " %s " not exist. '% ht_name )

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
                pr ('04 HTMS_Mid_Err    Mode "%s" incompatible foe create new table for HT "%s".'
                        % (self.ht.mode, self.ht.ht_name)
                )
                raise HTMS_Mid_Err('04 HTMS_Mid_Err    Mode "%s" incompatible for create new table for HT "%s".'
                        % (self.ht.mode, self.ht.ht_name)
                )
            f_temp= {'Back_links', 'Time_row'}

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
                        raise HTMS_Mid_Err('01 HTMS_Mid_Err  Program error. ')
                    kerr=[]
                    self.field (kerr, fun='add',  attr_num_f = attr_num )
                    if is_err( kerr ) >= 0 :      
                        set_err_int (kerr, Mod_name, 'init '+self.ht.ht_name+'-'+  str(self.maf_num), 2, \
                                message='Error during processing fields names from class when opening new MAF file %s .' % table_name )
                        raise HTMS_Mid_Err('02 HTMS_Mid_Err  Program error. ')
            kerr=[]
            if not self.ht.save_adt(kerr):
                set_err_int (Kerr, Mod_name,   'init '+self.ht.ht_name+'-'+ str(self.maf_num),  3 , \
                                message='Error save HTD.' )
                raise HTMS_Mid_Err('03 HTMS_Mid_Err  Program error. ')

        self.ht.updated = time.time()


# -------------------------------------------------------------------------------------------

    def row(self, Kerr = [] , fun='add',  after =-1, number=1, data= b''):

        if self.ht.mode in ('rs', 'rm' ) and fun !='info':
            pr ('05 HTMS_Mid_Err    Mode "%s" incompatible for add row operation in HT "%s".'
                    % (self.ht.mode, self.ht.ht_name)
            )
            raise HTMS_Mid_Err('05 HTMS_Mid_Err    Mode "%s" incompatible for row operation in HT "%s".'
                    % (self.ht.mode, self.ht.ht_name)
            )
        maf =  super( Table , self)

        rc =maf.row( Kerr = Kerr , fun=fun,  after =after, number=number, data= data)

        if rc == False:
            return False

        if fun == 'add':

            tim0= time.time()/10.
            offset = self.offsets [ 2 ] [ 0 ]
        
            for n_row in range (after+1 , after+number+1):
                time_row = struct.pack (  '>d', tim0*10.)
                rc =  self.ht.cage.write( self.ch,  (n_row - 1) *self.rowlen+offset, time_row , Kerr)
                if rc == False:
                    set_err_int (Kerr, Mod_name, 'row '+self.ht.ht_name+'-'+ self.maf_name,  1 , \
                                    message='Error write time stamps to new table rows. ' )
                    return False
                tim1= time.time()/10.
                while (tim1 == tim0):
                    tim1= time.time()/10.
                tim0= time.time()/10.

            self.ht.updated = time.time()

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

    def sieve (self,  with_fields = {}, modality ='all', res_num = Types_htms.max_int4 ) :
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

                        if self.fields [ f ][ 1 ] == '*link' :
                            continue

                        if self.fields [ f ][ 1 ] == 'file' :
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
                        elif attr_type  in ('*byte',):
                            elem =  self.r_bytes ( kerr, attr_num, row)       # bytes array from AF
                            is_bytes_array = True
                        elif attr_type  in ('*utf',):
                            elem =  self.r_str ( kerr, attr_num, row)       # utf-8 string from AF
                            is_string_array = True
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
            pr ('251 HTMS_Mid_Err    Mode "%s" incompatible for update fields n in HT "%s".'
                    % (self.ht.mode, self.ht.ht_name)
            )
            raise HTMS_Mid_Err('251 HTMS_Mid_Err    Mode "%s" incompatible for update fields in HT "%s".'
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
                                ht_root=self.ht.ht_root, ht_name= self.ht.ht_name, t_nmaf= temp_maf )
                            num_temp_mafs_opened += 1

        if len(add_fields) == 0 and len(del_fields) == 0:
            return True

        for attr in self.ht.attrs:
            if attr in (1,2):
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
                        self_row.add_dyna_link( ( self.maf_num,   row) )
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
                                
                                #protect= Links_array( () )
                                links_array = Links_array ( links )
                                for li in links_array.get_dyna_links():
                                
                                    linking_maf_n = li [ 0 ]
                                    row_in_linking_maf =  li [ 1 ]
                                    add_temp_mafs( self, linking_maf_n)       
                                    linking_maf =   self.ht.mafs_opened[ linking_maf_n ]  

                                    if  attr_name in self.ht.relations   and \
                                    self.ht.relations[ attr_name ] == 'multipart' : 
 
                                        linking_maf.delete_row( row_in_linking_maf )
                                        #row= self_row.get_first_dyna_row(self.maf_num)
                                        #protect.remove_dyna_link( ( linking_maf_n,   row_in_linking_maf) )

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

                                        #row= self_row.get_first_dyna_row(self.maf_num)
                                        #protect.remove_dyna_link( ( linking_maf_n,   row_in_linking_maf) )

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
                            self_row.add_dyna_link( ( self.maf_num,   row) )

                        close_temp_mafs()

                    else:
                        pass

                    self.field (kerr, fun='delete',  attr_num_f = attr )
                    if is_err( kerr ) >= 0 :      
                         pr ('27 HTMS_Mid_Err    Error delete update_fields field.  err = %s' % str (kerr)  )
                         raise HTMS_Mid_Err('27 HTMS_Mid_Err    Error delete update_fields field.  err = %s' % str (kerr)  )
                    rc2=self.ht.update_RAM (fun = 'field_remove', maf_num_p=self.maf_num,  attr_num_p = attr)
                    if rc2 != True :      
                        pr ('301 HTMS_Mid_Err    Error delete update_fields RAM.  err = %s' % str (kerr)  )
                        raise HTMS_Mid_Err('30 HTMS_Mid_Err    Error delete update_fields field.  err = %s' % str (kerr)  )
  
                else:
                    pass

        if len (add_fields) > 0:
                pr ('28 HTMS_Mid_Err    In the HT full attribute\'s list no item with name(-s) : %s  '% str (add_fields ) )
                raise HTMS_Mid_Err('28 HTMS_Mid_Err    In the HT full attribute\'s list no item with name(-s) : %s  '% str (add_fields ) )
        if len (del_fields) > 0:
                pr ('29 HTMS_Mid_Err    In the MAF field\'s list no item with name(-s)s : %s  '% str (del_fields ) )
                raise HTMS_Mid_Err('29 HTMS_Mid_Err    In the MAF field\'s list no item with name(-s) : %s  '% str (del_fields ) )

        self.ht.updated = time.time()

        return True

#------------------------------------------------------------------------------------------------

    def update_row (self, row_num =0, add_data={}, delete_data= set()) :
            # add_data= { attr_name|field_name : value, ...., attr_name|field_name : value } - dict
            # delete_data= { attr_name|field_name,...., attr_name|field_name } - set

            if self.ht.mode in ('rs', 'rm' ):
                pr ('302 HTMS_Mid_Err    Mode "%s" incompatible for update row n in HT "%s".'
                        % (self.ht.mode, self.ht.ht_name)
                )
                raise HTMS_Mid_Err('302 HTMS_Mid_Err    Mode "%s" incompatible for update row in HT "%s".'
                        % (self.ht.mode, self.ht.ht_name)
                )
            if len ( self.fields ) == 1 and self.fields[ 0 ] == 'Back_links':
                pr ('30 HTMS_Mid_Err    Table have no data fields. ' )
                raise HTMS_Mid_Err('30 HTMS_Mid_Err    Table have no fields. ' )

            if  (add_data =={} and delete_data == set()) :
                pr ('31 HTMS_Mid_Err  Data is not setted. ' )
                raise HTMS_Mid_Err('31 HTMS_Mid_Err  Data  is not setted. ' )

            if row_num ==0 :
                pr ('32 HTMS_Mid_Err  Row number is not setted. ' )
                raise HTMS_Mid_Err('32 HTMS_Mid_Err  Row number is not setted. ' )

            if row_num > self.rows:
                kerr = []
                after_row = self.rows
                numbers = ( row_num - self.rows )
                self.row(kerr, fun='add',  after =after_row, number=numbers )
                if is_err( kerr ) >= 0 :      
                     pr ('33 HTMS_Mid_Err    Error add new rows.  err = %s' % str (kerr)  )
                     raise HTMS_Mid_Err('33 HTMS_Mid_Err    Error add new rows.  err = %s' % str (kerr)  )

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
                if atr_type == '*link':
                   if  attr  in add_data: 
                       del add_data [ attr ]
                   continue
                elif atr_type == 'file':
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
                         pr ('35 HTMS_Mid_Err    Error delete data.  err = %s' % str (kerr)  )
                         raise HTMS_Mid_Err('35 HTMS_Mid_Err    Error delete data.  err = %s' % str (kerr)  )
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
                         pr ('36 HTMS_Mid_Err    Error correct data.  err = %s' % str (kerr)  )
                         raise HTMS_Mid_Err('36 HTMS_Mid_Err    Error correct data.  err = %s' % str (kerr)  )
                    del add_data [ attr ]

            if len (add_data) > 0:
                pr ('37 HTMS_Mid_Err    In the table data attribute\'s list no item with name(-s) : %s  '% str ( list(add_data.keys()  ) ) )
                raise HTMS_Mid_Err('37 HTMS_Mid_Err    In the HT full attribute\'s list no item with name(-s) : %s  '% str ( list (add_data.keys()  ) ) )
            if len (delete_data) > 0:
                pr ('38 HTMS_Mid_Err    In the table data attribute\'s list no item with name(-s) : %s  '% str ( delete_data  ) )
                raise HTMS_Mid_Err('38 HTMS_Mid_Err    In the table attribute\'s list no item with name(-s) : %s  '% str (delete_data ) )

            self.ht.updated = time.time()

            return True

#--------------------------------------------------------------------------------------------------

    def delete_row (self, nrow =0, protect= Links_array( () ) ) :

            if self.ht.mode in ('rs', 'rm' ):
                pr ('401 HTMS_Mid_Err    Mode "%s" incompatible for delete row in HT "%s".'
                        % (self.ht.mode, self.ht.ht_name)
                )
                raise HTMS_Mid_Err('401 HTMS_Mid_Err    Mode "%s" incompatible for delete row in HT "%s".'
                        % (self.ht.mode, self.ht.ht_name)
                )
            row_num= nrow
            if DEBUG_DELETE_ROW_1:
                pr ('\n\n  DELETE_ROW ---- maf_name = %s,  row_num =%d, protect= %s ' % \
                     ( self.maf_name, row_num, str (protect) ) )
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
                                ht_root=self.ht.ht_root, ht_name= self.ht.ht_name, t_nmaf= temp_maf )
                            num_temp_mafs_opened += 1

            if row_num < 1 or row_num > self.rows:
                pr ('40 HTMS_Mid_Err  Row number is not valid. ' )
                raise HTMS_Mid_Err('40 HTMS_Mid_Err  Row number is not valid. ' )
            else:

                kerr =[]
                back_links = self.r_links ( kerr , attr_num= 1 , num_row= row_num )
                if is_err( kerr ) >= 0 :      
                    pr ( '41 HTMS_Mid_Err    Error read back_links during delete row=%s in table=%s, err = %s' % 
                        (str(row_num),self.ht.mafs[self.maf_num]['name'],str (kerr) ) )
                    raise HTMS_Mid_Err ( '41 HTMS_Mid_Err    Error read back_links during delete row=%s in table=%s, err = %s' % 
                        (str(row_num),self.ht.mafs[self.maf_num]['name'],str (kerr) ) )

                back_links_array = Links_array( back_links )
                for bli in back_links_array.get_dyna_links():      # 1st  loop:   search the whole in back_links
                    if not ( bli in protect.get_dyna_links() ): 
                        linking_maf_n = bli [ 0 ]
                        row_in_linking_maf =  bli [ 1 ]
                        add_temp_mafs( self, linking_maf_n)       
                        linking_maf =   self.ht.mafs_opened[ linking_maf_n ]     

                        for whole_field  in   linking_maf.fields:     # 2nd loop : search "whole"  link's fields 
                                                                                         # in row =    row_in_linking_maf
                            if   linking_maf.fields[ whole_field ][ 0 ] >1  and  \
                                 linking_maf.fields[ whole_field ][ 1 ] == '*link'  and  \
                                 whole_field in self.ht.relations and \
                                 self.ht.relations[ whole_field ] == 'whole' :  
                                kerr=[]
                                whole_links = linking_maf.r_links ( 
                                    kerr , attr_num=  linking_maf.fields[ whole_field ][ 0 ] , num_row= row_in_linking_maf )
                                if is_err( kerr ) >= 0 :      
                                     pr ( '42 HTMS_Mid_Err    Error read links of parent row during delete row.  err = %s' % str (kerr)  )
                                     raise HTMS_Mid_Err ( '42 HTMS_Mid_Err    Error read links of parent row during delete row.  err = %s' % str (kerr)  )
                                
                                whole_links_array = Links_array( whole_links )                                        
                                for wli in whole_links_array.get_dyna_links():   #  3rd loop : search direct link to self part form whole field
                                    if not ( wli in protect.get_dyna_links() ):
                                        if wli[ 0 ] == self.maf_num and wli [ 1 ] == row_num : # found direct link to self
                                            kerr=[]
                                            # delete founded link
                                            rc = linking_maf.u_links ( kerr , attr_num= linking_maf.fields[ whole_field ][ 0 ] , 
                                                            num_row= row_in_linking_maf,   u_link = ( self.maf_num, - row_num)  )
                                            if not rc or is_err( kerr ) >= 0 :      
                                                pr ( '43 HTMS_Mid_Err    Error delete link during delete row.  err = %s' % str (kerr)  )
                                                raise HTMS_Mid_Err ( '43 HTMS_Mid_Err    Error delete link during delete row.  err = %s' % str (kerr)  )
                                            if  self.ht.relations[ whole_field ] == 'whole' :
                                                
                                                protect.add_dyna_link( ( self.maf_num,   row_num) )
                                                self_row= Links_array( () )
                                                self_row.add_dyna_link( ( self.maf_num,   row_num) )

                                                linking_maf.delete_row(  
                                                    row_in_linking_maf, protect = protect  )

                                                row_num= self_row.get_first_dyna_row(self.maf_num)
                                                protect.remove_dyna_link( ( self.maf_num,   row_num) )
                                                
                                                parent_row_deleted = True 
                                                break
                                del whole_links_array
                del back_links_array
                
                for name_attr  in  self.fields:
                    if  self.fields[ name_attr ][ 1 ] == '*link' and  self.fields[ name_attr ][ 0 ] > 1  and \
                        name_attr  in self.ht.relations:

                        rel =  self.ht.relations[ name_attr ]

                        if   rel == 'cause':
                            continue
                        elif   rel == 'multipart'  or    rel == 'whole':      # delete all  parts
                            kerr =[]
                            links = self.r_links ( kerr , attr_num= self.fields[ name_attr ][ 0 ] , 
                                num_row= row_num )
                            if is_err( kerr ) >= 0 :      
                                pr ( '45 HTMS_Mid_Err    Error read links during delete row.  err = %s' % str (kerr)  )
                                raise HTMS_Mid_Err ( '45 HTMS_Mid_Err    Error read links during delete row.  err = %s' % str (kerr)  )
                            links_array = Links_array( links )  
                            for li in links_array.get_dyna_links():
                                if not ( li  in protect.get_dyna_links()  ):
                                    part_maf = li [ 0 ]
                                    row_in_part_maf =  li [ 1 ]
                                    add_temp_mafs( self, part_maf)       
                                    kerr=[]
                                    rc = self.u_links ( kerr , attr_num= self.fields[ name_attr ][ 0 ] , 
                                        num_row= row_num,   u_link = ( part_maf, - row_in_part_maf)  )
                                    if is_err( kerr ) >= 0 :      
                                        pr ( '46 HTMS_Mid_Err    Error delete link during delete row.  err = %s' % str (kerr)  )
                                        raise HTMS_Mid_Err ( '46 HTMS_Mid_Err    Error delete link during delete row.  err = %s' % str (kerr)  )

                                    protect.add_dyna_link( ( self.maf_num,   row_num) )
                                    self_row= Links_array( () )
                                    self_row.add_dyna_link( ( self.maf_num,   row_num) )

                                    self.ht.mafs_opened[ part_maf ].delete_row(  
                                        row_in_part_maf, protect = protect  )

                                    row_num= self_row.get_first_dyna_row(self.maf_num)

                                    protect.remove_dyna_link( ( self.maf_num,   row_num) )
                            del links_array  

                close_temp_mafs()
                kerr = []
                after_row= row_num-1                                       

                if not self.row(kerr, fun='delete',  after =after_row, number= 1 ) or is_err( kerr ) >= 0 :      
                    pr ( '49 HTMS_Mid_Err    Error delete row.  err = %s' % str (kerr)  )
                    if DEBUG_DELETE_ROW_1:
                        print ('\n\n  ROW   NOT   DELETED-')
                    raise HTMS_Mid_Err ( '49 HTMS_Mid_Err    Error delete row PHYSICALLY.  err = %s' % str (kerr)  )

                if DEBUG_DELETE_ROW_1:
                    print ('\n\n  DELETE_ROW PHYSICALLY ---- after_row =%d ' % after_row )
                    for tab in self.ht.mafs_opened:
                        links_dump ( self.ht.mafs_opened [ tab ] )

                kerr = []
                #rc = self.ht.update_RAM (fun = 'row_delete', maf_num_p=self.maf_num,  after_row = after_row , num_rows = 1)
                #rc != True :      
                    #pr ( '50 HTMS_Mid_Err    Error update RAM after delete row.  err = %s' % str (kerr)  )
                    #raise HTMS_Mid_Err ( '50 HTMS_Mid_Err    Error update RAM after delete row..  err = %s' % str (kerr)  )

                if DEBUG_DELETE_ROW_1:
                    print ('\n\n  ROW DELETED---- maf_name = %s,  row_num =%d, protect= %s ' % \
                         ( self.maf_name, row_num, str (protect) ) )
                #pr ( str ( self.__dict__) )
                if 'objects_RAM'  in  dir ( self ) and len ( self.objects_RAM )  >0 :
                    #pr (str (  [ obj  for obj in self.get_RAM_instances() ]  ))
                    obj_RAM_to_delete = [ obj  for obj in self.get_RAM_instances() if obj.id == row_num ]
                    for oi in  obj_RAM_to_delete:                   
                            oi. remove_instance()
                            del oi
                    del obj_RAM_to_delete
            
                self.ht.updated = time.time()
        
                return True

#--------------------------------------------------------------------------------------------------

    def update_links (self, row_num =0, attr_name='', add_links={}, delete_links= {} ) :
            # add_links, delete_links = {  maf_name : { row_num, ...., row_num }| -1, ..... maf_name : { row_num, ...., row_num }|-1  }.
            #  add_links:
            #       maf_name : 'all'       - indicator of link "to all rows, ie to a whole MAF"
            #       maf_name :  set()  - no to change
            #  delete_links:
            #       'all'                        - delete all links to any maf  in field
            #       maf_name :  'all'    - delete all links to maf
            #       maf_name :  set()  - no to change

            if self.ht.mode in ('rs', 'rm' ):
                pr ('5011 HTMS_Mid_Err    Mode "%s" incompatible for update links in in HT "%s".'
                        % (self.ht.mode, self.ht.ht_name)
                )
                raise HTMS_Mid_Err('5011 HTMS_Mid_Err    Mode "%s" incompatible for update links in HT "%s".'
                        % (self.ht.mode, self.ht.ht_name)
                )
            if len ( self.fields ) == 1 and self.fields[ 0 ] == 'Back_links':
                pr ('50 HTMS_Mid_Err    Table have no fields. ' )
                raise HTMS_Mid_Err('50 HTMS_Mid_Err    Table have no fields. ' )

            if  row_num ==0 or row_num ==set() or row_num =={} or row_num ==() or row_num ==[] or row_num ==None or \
                attr_name ==''  or  (add_links =={} and delete_links == {} ):
                pr ('51 HTMS_Mid_Err  Data is not setted. ' )
                raise HTMS_Mid_Err('51 HTMS_Mid_Err  Data  is not setted. ' )

            if  not (  attr_name in self.fields):
                 pr ('52 HTMS_Mid_Err   In table is not field with attribute "%s".' % attr_name )
                 raise HTMS_Mid_Err('52 HTMS_Mid_Err   In table is not field with attribute "%s".' % attr_name )

            if attr_name in self.ht.relations and self.ht.relations[ attr_name ] == 'erased':
                 pr ('71 HTMS_Mid_Err   Attribute "%s" was deleted from hypertable.' % attr_name )
                 raise HTMS_Mid_Err('52 HTMS_Mid_Err   In table is not field with attribute "%s".' % attr_name )

            if  self.fields[ attr_name ][ 1 ]  !=  '*link' :
                pr ('53 HTMS_Mid_Err  Type of attribute specified if not "*link". ' )
                raise HTMS_Mid_Err('53 HTMS_Mid_Err  Type of attribute specified if not "*link". ' )

            if row_num > self.rows:
                 pr ('54 HTMS_Mid_Err    Row number exceeded max.' )
                 raise HTMS_Mid_Err('54 HTMS_Mid_Err    Row number exceeded max.' )

            #if len (add_links) >0  and delete_links == 'all':
               
            if len (add_links) >0:
                new =True
            else:
                new = False 

            if delete_links != 'all'  and  len (delete_links) >0 :
                dlt =True
            else:
                dlt = False 

            num_temp_mafs_opened = 0
            temp_mafs_opened ={} 
            mafs_to_link = set()
            add_li =set ()
            delete_li =set ()

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

            if new:
                for maf_name  in  add_links:
                    maf_num = self.ht.get_maf_num ( maf_name)
                    if maf_num  == 0:
                        close_temp_mafs()
                        pr ('55 HTMS_Mid_Err    HT have no table : %s. ' % maf_name )
                        raise HTMS_Mid_Err('55 HTMS_Mid_Err    HT have no table : %s. ' % maf_name )
                    mafs_to_link.add ( maf_num )
                    rows = add_links [ maf_name ]
                    if maf_num != self.maf_num:
                        add_temp_mafs( self, maf_num)

                    if rows == set():              
                        continue
                    else:
                        if type ( rows ).__name__ != 'tuple':
                            if     type ( rows ).__name__ == 'int'   or   rows == 'all':
                                rows = ( rows, )
                            elif  type ( rows ).__name__ == 'set':
                                rows = tuple (rows)
                            else:
                                close_temp_mafs()
                                pr ('56 HTMS_Mid_Err    Incorrect data in "add_links" parameter :%s : %s. ' % ( maf_name, str (rows) ) )
                                raise HTMS_Mid_Err('56 HTMS_Mid_Err    Incorrect data in "add_links" parameter :%s : %s. ' % ( maf_name, str (rows) ) )
                        for r in rows: 
                            if r == row_num  and  maf_num == self.maf_num:
                                close_temp_mafs()
                                pr ('57 HTMS_Mid_Err    Link to self row in self table prohibited :%s : %s. ' % ( maf_name, str (rows) ) )
                                raise HTMS_Mid_Err('56 HTMS_Mid_Err    Link to self row in self table prohibited :%s : %s. ' % ( maf_name, str (rows) ) )
                            add_li.add ( ( maf_num, r ) )
                                    
            if dlt:
                for maf_name  in  delete_links:
                    maf_num = self.ht.get_maf_num ( maf_name)
                    if maf_num  == 0:
                        close_temp_mafs()
                        pr ('58 HTMS_Mid_Err    HT have no table : %s. ' % maf_name )
                        raise HTMS_Mid_Err('58 HTMS_Mid_Err    HT have no table : %s. ' % maf_name )

                    add_temp_mafs( self, maf_num)
                    mafs_to_link.add ( maf_num )
                    rows = delete_links [ maf_name ]

                    if rows == set():              
                        continue
                    else:
                        if type ( rows ).__name__ != 'tuple':
                            if type ( rows ).__name__ == 'int'   or   rows == 'all':
                                rows = ( rows, )
                            elif  type ( rows ).__name__ == 'set':
                                rows = tuple (rows)
                            else:
                                close_temp_mafs()
                                pr ('59 HTMS_Mid_Err    Incorrect data in "delete_links" parameter :%s : %s. ' % ( maf_name, str (rows) ) )
                                raise HTMS_Mid_Err('59 HTMS_Mid_Err    Incorrect data in "delete_links" parameter :%s : %s. ' % ( maf_name, str (rows) ) )
                        for r in rows: 
                            delete_li.add ( ( maf_num, r ) )
            kerr =[]
            #  CASE 1  - simple  - only one parameter
            if  len (add_li) +len ( delete_li) <= 1:  
                for_u_link = None
                if delete_links == 'all':
                    kerr =[]
                    deleted_links =   self.r_links( kerr, attr_num = self.fields [ attr_name ][ 0 ], num_row= row_num)
                    if is_err( kerr ) >= 0 :
                        close_temp_mafs()      
                        pr ('60 HTMS_Mid_Err    Error read old link.  err = %s' % str (kerr)  )
                        raise HTMS_Mid_Err('60 HTMS_Mid_Err    Error read old link.  err = %s' % str (kerr)  )   
                    if len (deleted_links) >0 :
                        for_u_link = ()
                        for li in deleted_links:
                            add_temp_mafs(self, li [ 0 ] )
                            """
                            rc=self.ht.correct_back_links (maf_num = li [ 0 ], row_num = li[ 1 ], back_link = (self.maf_num, row_num),  func = 'delete')          
                            if rc == False : 
                                pr ('61 HTMS_Mid_Err    Error correct back links    Maf = %d, row =: %d. ' % ( li [ 0 ], li[ 1 ]) )
                                raise HTMS_Mid_Err('61 HTMS_Mid_Err    Error correct back links    Maf = %d, row =: %d. ' % ( li [ 0 ], li[ 1 ]) )
                            """     
                    else:
                        close_temp_mafs()

                        self.ht.updated = time.time()

                        return True

                elif new:
                    pair = add_li.pop()
                    maf = pair[ 0 ]
                    row = pair[ 1 ]
                    if  row == 'all':
                        for_u_link = ( maf, 0)                                
                    else:
                        for_u_link = ( maf, row)
                    rc = self.ht.correct_back_links (maf_num = maf, row_num =  row, back_link = (self.maf_num, row_num),  func = 'add')          
                    if rc == False : 
                        close_temp_mafs()
                        pr ('62 HTMS_Mid_Err    Error correct back links    Maf = %d, row =: %d. ' % ( li [ 0 ], li[ 1 ]) )
                        raise HTMS_Mid_Err('62 HTMS_Mid_Err    Error correct back links    Maf = %d, row =: %d. ' % ( li [ 0 ], li[ 1 ]) )                                 
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
                        pr ('63 HTMS_Mid_Err    Error correct back links    Maf = %d, row =: %d. ' % ( li [ 0 ], li[ 1 ]) )
                        raise HTMS_Mid_Err('63 HTMS_Mid_Err    Error correct back links    Maf = %d, row =: %d. ' % ( li [ 0 ], li[ 1 ]) )                                 

                if for_u_link != None:
                    kerr = []
                    rc = self.u_links (kerr, attr_num= self.fields[ attr_name ][ 0 ], num_row=row_num,  u_link = for_u_link  )
                        # u_link = () - clear all element   
                        # u_link =(nmaf, 0) - set row =0 - indicate link to whole maf
                        # u_link =(-nmaf, *) - delete all links to maf
                        # u_link =(nmaf,-num_row,) - delete one link to maf , to  num_row
                        # u_link =(nmaf,num_row,) -  add  link to maf - num_row, if not exist
                    if is_err( kerr ) >= 0 :
                        close_temp_mafs()      
                        pr ('64 HTMS_Mid_Err    Error updatel link.  err = %s' % str (kerr)  )
                        raise HTMS_Mid_Err('64 HTMS_Mid_Err    Error updatel link.  err = %s' % str (kerr)  )            

                    close_temp_mafs()

                    self.ht.updated = time.time()

                    return True

            #  CASE  2 - common
            if True:
                kerr = []
                old_links =  self.r_links( kerr, attr_num = self.fields [ attr_name ][ 0 ], num_row= row_num) 
                if is_err( kerr ) >= 0 :
                    close_temp_mafs()      
                    pr ('65 HTMS_Mid_Err    Error read old link.  err = %s' % str (kerr)  )
                    raise HTMS_Mid_Err('65 HTMS_Mid_Err    Error read old link.  err = %s' % str (kerr)  )   
                new_links =  set()
                old_links_to_another_mafs = set (old_links )
                for maf_to_link in mafs_to_link:
                    # link to whole maf
                    if ( maf_to_link, 'all' ) in delete_li :
                        pass     #new_links = new_links | { ( - maf_to_link, 0 ) } 
                        rc = self.ht.correct_back_links (maf_num = maf_to_link, row_num = 'all', back_link = (self.maf_num, row_num),  func = 'delete')          
                        if rc == False  : 
                            close_temp_mafs()
                            pr ('66 HTMS_Mid_Err    Error correct back links    Maf = %d, row =: %d. ' % ( li [ 0 ], li[ 1 ]) )
                            raise HTMS_Mid_Err('66 HTMS_Mid_Err    Error correct back links    Maf = %d, row =: %d. ' % ( li [ 0 ], li[ 1 ]) )     
                        
                        for li in old_links_to_another_mafs.copy():
                            if li [ 0 ] == maf_to_link:
                                old_links_to_another_mafs.remove( li )
                    elif ( maf_to_link, 'all' ) in add_li :
                        new_links = new_links | { ( maf_to_link, 0) } 
                        rc =self.ht.correct_back_links (maf_num = maf_to_link, row_num = 'all', back_link = (self.maf_num, row_num),  func = 'add')          
                        if rc == False : 
                            close_temp_mafs()
                            pr ('67 HTMS_Mid_Err    Error correct back links    Maf = %d, row =: %d. ' % ( li [ 0 ], li[ 1 ]) )
                            raise HTMS_Mid_Err('67 HTMS_Mid_Err    Error correct back links    Maf = %d, row =: %d. ' % ( li [ 0 ], li[ 1 ]) )     
                    else:
                        # links to singular row
                        old_li_regular  =  set  (  (ma, ro)   for (ma, ro)     in old_links   if  ma == maf_to_link and ro >0        ) 
                        add_li_regular =  set  (  (ma, ro)   for (ma, ro)     in add_li       if  ma == maf_to_link and type ( ro ).__name__ == 'int'   and  ro > 0      ) 
                        del_li_regular =   set  (  (ma, ro)   for (ma, ro)     in delete_li   if  ma == maf_to_link and type ( ro ).__name__ == 'int'   and  ro > 0      ) 

                        rows_add=set()
                        rows_delete=set()
                        for pair in add_li_regular:
                            rows_add.add( pair [ 1] )
                        for pair in del_li_regular:
                            rows_delete.add( pair [ 1] )
                        rows_add.difference_update (  rows_delete )

                        rc = self.ht.correct_back_links (maf_num = maf_to_link, row_num =  rows_add, back_link = (self.maf_num, row_num),  func = 'add')          
                        if rc == False  :
                            close_temp_mafs() 
                            pr ('68 HTMS_Mid_Err    Error correct back links    Maf = %d, row =: %d. ' % ( li [ 0 ], li[ 1 ]) )
                            raise HTMS_Mid_Err('68 HTMS_Mid_Err    Error correct back links    Maf = %d, row =: %d. ' % ( li [ 0 ], li[ 1 ]) )     
                        rc = self.ht.correct_back_links ( maf_num = maf_to_link, row_num = rows_delete, back_link = (self.maf_num, row_num),  func = 'delete')          
                        if rc == False :
                            close_temp_mafs() 
                            pr ('69 HTMS_Mid_Err    Error correct back links    Maf = %d, row =: %d. ' % ( li [ 0 ], li[ 1 ]) )
                            raise HTMS_Mid_Err('69 HTMS_Mid_Err    Error correct back links    Maf = %d, row =: %d. ' % ( li [ 0 ], li[ 1 ]) )     

                        old_links_to_another_mafs.difference_update (  old_li_regular )
                        plus = old_li_regular | add_li_regular 
                        plus.difference_update (del_li_regular  )
                        new_links = new_links | plus

                new_links = new_links | old_links_to_another_mafs

                self.w_links ( kerr , attr_num=self.fields [ attr_name ][ 0 ], num_row=row_num, links =new_links, rollback=False)
                    # link =(nmaf, 0) - set row =0 - indicate link to whole maf

                if is_err( kerr ) >= 0 :
                    close_temp_mafs()      
                    pr ('70 HTMS_Mid_Err    Error write new links.  err = %s' % str (kerr)  )
                    raise HTMS_Mid_Err('70 HTMS_Mid_Err    Error write new links.  err = %s' % str (kerr)  )   
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
        
#------------------------------------------------------------------------------------------------

    def copy(self, new_table_name='', only_data= True, links_fields='blanc', only_fields=set(), with_fields={}):

        if self.ht.mode in ('rs', 'rm' ):
            pr ('751 HTMS_Mid_Err    Mode "%s" incompatible for copy tables in HT "%s".'
                    % (self.ht.mode, self.ht.ht_name)
            )
            raise HTMS_Mid_Err('751 HTMS_Mid_Err    Mode "%s" incompatible for copy tables in HT "%s".'
                    % (self.ht.mode, self.ht.ht_name)
            )
        table_obj_attrs = set ( self.fields.keys()  )
        fields_names=set()
        if type (only_fields).__name__ != 'set':
            pr ('75 HTMS_Mid_Err    Error:  only_fields is not the set.')
            raise HTMS_Mid_Err ('75 HTMS_Mid_Err    Error:  only_fields is not the set.'  )
        if type (with_fields).__name__ != 'dict':
            pr ('76 HTMS_Mid_Err    Error:  with_fields is not the dictionary.')
            raise HTMS_Mid_Err ('76 HTMS_Mid_Err    Error:  with_fields is not the dictionary..'  )
        if  only_fields and not only_fields.issubset( table_obj_attrs):
            pr ('77 HTMS_Mid_Err    Error:  only_fields  not belongs to table fields.')
            raise HTMS_Mid_Err ('77 HTMS_Mid_Err    Error: only_fields  not belongs to table fields.')
        if  with_fields and not keys(with_fields).issubset( table_obj_attrs):
            pr ('78 HTMS_Mid_Err    Error:  with_fields  not belongs to table fields.')
            raise HTMS_Mid_Err ('78 HTMS_Mid_Err    Error:  with_fields  not belongs to table fields.')

        if  only_fields:
            fields_names = list ( only_fields ) 
        else :              
            fields_names = list ( table_obj_attrs) 

        nmaf= self.maf_num

        try:
            new_table= Table( 
                ht_root=self.ht.ht_root, ht_name = self.ht.db_name,  t_name= new_table_name)
        except:     
            pr ( '80 HTMS_Mid_Err    Error create copy table:   "%s"' % new_table_name )
            raise HTMS_Mid_Err ( '80 HTMS_Mid_Err    Error create copy table:  "%s"' % new_table_name )

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
            if field[0] in (1,2):
                continue
            if  only_data and  field[0] == '*link':
                    continue
            kerr=[]
            new_table.field(kerr, attr_num_f= field[0] )
            if is_err( kerr ) >= 0 :     
                    pr ( '81 HTMS_Mid_Err    Error add new field.  err = %s' % str (kerr)  )
                    raise HTMS_Mid_Err ( '81 HTMS_Mid_Err    Error add new field. .  err = %s' % str (kerr)  )

        if self.rows >0:
            for nrow in range (1,  self.rows+1):
                kerr=[]
                rc=new_table.row(kerr, fun='add',  after =nrow-1, number=1 )
                if rc == False or  is_err( kerr ) >= 0 :    
                    pr ( '82 HTMS_Mid_Err    Error add new row.  err = %s' % str (kerr)  )
                    raise HTMS_Mid_Err ( '82 HTMS_Mid_Err    Error add new ro .  err = %s' % str (kerr)  )
                for field_name in  fields_names:
                    field=  self.fields[ field_name ]
                    kerr=[]
                    nattr= field[0]
                    attr_name = field_name
                    data_type = field[1]
                    if nattr == 1 and  links_fields=='full':
                        rc=True
                        kerr =[]
                        back_links = self.r_links ( kerr , attr_num= 1 , num_row= nrow )
                        if is_err( kerr ) >= 0 :      
                            pr ( '83 HTMS_Mid_Err    Error read back_links during delete row=%s in table=%s, err = %s' % 
                                (str(nrow),self.ht.mafs[self.maf_num]['name'],str (kerr) ) )
                            raise HTMS_Mid_Err ( '83 HTMS_Mid_Err    Error read back_links during delete row=%s in table=%s, err = %s' % 
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
                                            pr ( '84 HTMS_Mid_Err    Error read links of parent row during delete row.  err = %s' % str (kerr)  )
                                            raise HTMS_Mid_Err ( '84 HTMS_Mid_Err    Error read links of parent row during delete row.  err = %s' % str (kerr)  )
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
                                pr ( '85 HTMS_Mid_Err    Error write back_links field.  err = %s' % str (kerr)  )
                                raise HTMS_Mid_Err ( '85 HTMS_Mid_Err    Error write back_links field.  err = %s' % str (kerr)  )

                    if  nattr >2:

                        if  data_type != '*link':
                    
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
                                        pr ( '87 HTMS_Mid_Err    Error read file.  err = %s' % str (kerr)  )
                                        raise HTMS_Mid_Err ( '87 HTMS_Mid_Err    Error read file. .  err = %s' % str (kerr)  )
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
                                        pr ( '88 HTMS_Mid_Err    Error write file.  err = %s' % str (kerr)  )
                                        raise HTMS_Mid_Err ( '88 HTMS_Mid_Err    Error write file.  err = %s' % str (kerr)  )
                                    try:
                                        os.remove(file_path)
                                    except  (OSError, IOError):
                                        pass
                                    if rc == False:
                                        pass

                        else:
                            if  links_fields=='blanc':
                                continue
                            elif  links_fields in ('full','ref'):
                                kerr=[]
                                links = self.r_links(Kerr=kerr,  attr_num=nattr, num_row =nrow)
                                if links== False or is_err( kerr ) >= 0 :     
                                    pr ( '90 HTMS_Mid_Err    Error read links field.  err = %s' % str (kerr)  )
                                    raise HTMS_Mid_Err ( '90 HTMS_Mid_Err    Error read links field.  err = %s' % str (kerr)  )
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
                                        pr ( '91 HTMS_Mid_Err    Error update links field.  err = %s' % str (kerr)  )
                                        raise HTMS_Mid_Err ( '91 HTMS_Mid_Err    update links field.  err = %s' % str (kerr)  )
            close_temp_mafs()
            new_table.close() 
        return True
        

#-----------------------------------------------------------------------------------------------

    def copy_row(self, nrow=-1, after_row=-1, only_data= True, links_fields='blanc', only_fields=set()):

        if self.ht.mode in ('rs', 'rm' ):
            pr ('105 HTMS_Mid_Err    Mode "%s" incompatible for copy row in HT "%s".'
                    % (self.ht.mode, self.ht.ht_name)
            )
            raise HTMS_Mid_Err('105 HTMS_Mid_Err    Mode "%s" incompatible for copy row in HT "%s".'
                    % (self.ht.mode, self.ht.ht_name)
            )
        table_obj_attrs = set ( self.fields.keys()  )
        fields_names=set()
        if type (only_fields).__name__ != 'set':
            pr ('106 HTMS_Mid_Err    Error:  only_fields is not the set.')
            raise HTMS_Mid_Err ('106 HTMS_Mid_Err    Error:  only_fields is not the set.'  )
        if  only_fields and not only_fields.issubset( table_obj_attrs):
            pr ('107 HTMS_Mid_Err    Error:  only_fields  not belongs to table fields.')
            raise HTMS_Mid_Err ('107 HTMS_Mid_Err    Error: only_fields  not belongs to table fields.')
        if  nrow not in range(1, self.rows+1):
            pr ('108 HTMS_Mid_Err    Error:  Invalid nrow.')
            raise HTMS_Mid_Err ('108 HTMS_Mid_Err    Error:  Invalid nrow.')
        if  after_row not in range(-1, self.rows+1):
            pr ('109 HTMS_Mid_Err    Error:  Invalid after_row.')
            raise HTMS_Mid_Err ('109 HTMS_Mid_Err    Error:  Invalid after_row.')

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

        if  only_fields:
            fields_names = list ( only_fields ) 
            only_data=False

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
                    pr ( '82 HTMS_Mid_Err    Error add new row.  err = %s' % str (kerr)  )
                    raise HTMS_Mid_Err ( '82 HTMS_Mid_Err    Error add new row.  err = %s' % 
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
                    if nattr == 1 and  links_fields=='full':
                        rc=True
                        kerr =[]
                        back_links = self.r_links ( kerr , attr_num= 1 , num_row= n_source_row )
                        if is_err( kerr ) >= 0 :      
                            pr ( '112 HTMS_Mid_Err    Error read back_links during delete row=%s in table=%s, err = %s' % 
                                (str(n_source_row),self.ht.mafs[self.maf_num]['name'],str (kerr) ) )
                            raise HTMS_Mid_Err ( '112 HTMS_Mid_Err    Error read back_links during delete row=%s in table=%s, err = %s' % 
                                (str(rn_source_row),self.ht.mafs[self.maf_num]['name'],str (kerr) ) )
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
                                            pr ( '113 HTMS_Mid_Err    Error read links of parent row during delete row.  err = %s' % str (kerr)  )
                                            raise HTMS_Mid_Err ( '113 HTMS_Mid_Err    Error read links of parent row during delete row.  err = %s' % str (kerr)  )
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

                            rc = self.w_links ( kerr , attr_num= 1 , num_row= new_row, links= back_links)
                            if rc == False or is_err( kerr ) >= 0 :     
                                pr ( '115 HTMS_Mid_Err    Error write back_links field.  err = %s' % str (kerr)  )
                                raise HTMS_Mid_Err ( '115 HTMS_Mid_Err    Error write back_links field.  err = %s' % str (kerr)  )

                    if  nattr >2:

                        if  data_type != '*link':
                    
                                rc=True
                                if data_type[:3]  == 'dat':
                                    df=  self.r_utf8(Kerr=kerr, attr_num=nattr, num_row =n_source_row)
                                    if df == None:
                                        df=' '
                                    rc=  self.w_utf8(Kerr=kerr,  attr_num=nattr, num_row =new_row, string=df )
                                elif data_type in ( "int4", "int8","float4","float8","time") :                   
                                    numb= self.r_numbers (Kerr=kerr,  attr_num=nattr, num_row =n_source_row)
                                    rc= self.w_numbers (Kerr=kerr,  attr_num=nattr, num_row =new_row, numbers = numb)
                                elif data_type in ( "*int4", "*int8","*float4","*float8") :
                                    numbers = self.r_numbers (Kerr=kerr,  attr_num=nattr, num_row =n_source_row)
                                    rc= self.w_numbers (Kerr=kerr,  attr_num=nattr, num_row =new_row, numbers = numbers)
                                elif data_type.find( "byte") != -1 :
                                    if data_type[0] == '*':
                                        bytes = self.r_bytes(Kerr=kerr,  attr_num=nattr, num_row =n_source_row)
                                        rc =self.w_bytes(Kerr=kerr,  attr_num=nattr, num_row =new_row, bytes=bytes)
                                    else:
                                        bytes = self.r_elem(Kerr=kerr,  attr_num=nattr, num_row =n_source_row)
                                        rc = self.w_elem(Kerr=kerr,  attr_num=nattr, num_row =new_row, elem=bytes)
                                elif data_type.find("utf") != -1 :
                                    if data_type[0] == '*':
                                        chars = self.r_str (Kerr=kerr,  attr_num=nattr, num_row =n_source_row)
                                        rc = self.w_str (Kerr=kerr,  attr_num=nattr, num_row =new_row, string=chars)
                                    else:
                                        chars = self.r_utf8(Kerr=kerr,  attr_num=nattr, num_row =n_source_row)
                                        rc = self.w_utf8(Kerr=kerr,  attr_num=nattr, num_row =new_row,  string=chars)
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
                            if  links_fields=='blanc':
                                continue
                            elif  links_fields in ('full','ref'):
                                rc=True
                                links = self.r_links(Kerr=kerr,  attr_num=nattr, num_row =n_source_row)
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
                                    rc= self.update_links (
                                        row_num = new_row, 
                                        attr_name=attr_name, 
                                        add_links=new_links
                                        )
                                    if rc== False or is_err( kerr ) >= 0 :     
                                        pr ( '121 HTMS_Mid_Err    Error update links field.  err = %s' % str (kerr)  )
                                        raise HTMS_Mid_Err ( '121 HTMS_Mid_Err    update links field.  err = %s' % str (kerr)  )

                close_temp_mafs()
        
                self.ht.updated = time.time()

                return True

#-----------------------------------------------------------------------------------------------

    def row_tree (self, nrow=-1, levels_source=-1, levels_ref=-1, 
                  only_fields=set(), with_fields={}):
        
        row_source_tree={}
        row_ref_trees={}
        if  nrow not in range(1, self.rows+1):
            pr ('130 HTMS_Mid_Err    Error:  Invalid nrow.')
            raise HTMS_Mid_Err ('130 HTMS_Mid_Err    Error:  Invalid nrow.')
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
                                ht_root=self.ht.ht_root, ht_name= self.ht.ht_name, t_nmaf= temp_maf )
                            num_temp_mafs_opened += 1

        if levels_source!=0:
            def add_to_source_tree (parent=(), child=(), ):
                    nonlocal only_fields
                    nonlocal self
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

                    for source_field  in   linking_maf.fields:     # 1nd loop :  
                        source_attr_num=  linking_maf.fields[ source_field ][ 0 ]
                        if  linking_maf.fields[ source_field ][ 1 ] == '*link' and \
                            source_attr_num != 1  :
                            source_ref_links=linking_maf.r_links ( 
                                kerr , 
                                attr_num=  source_attr_num , 
                                num_row= row_in_linking_maf )
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
                        nodes_discovered=nodes_discovered)
                    if child in nodes_discovered:
                        level_source -=1
                        return
                    else:
                        nodes_discovered.add(child)
                    back_links = linking_maf.r_links ( kerr , attr_num= 1 , num_row= row_in_linking_maf )
                    if back_links !=():
                        for bli in back_links:
                            source_field_name= self.ht.mafs[bli[0]]['name']
                            add_to_source_tree (
                                parent= (linking_maf_n, row_in_linking_maf), 
                                child=bli)
                    else:
                        level_source -=1
                        return

            back_links = self.r_links ( kerr , attr_num= 1 , num_row= nrow )
            if back_links !=() :
                row_source_tree=Links_tree(
                    self.ht.ht_name, 
                    (self.maf_num, nrow), 
                    self.ht.ht_name+' : '+self.maf_name+' : '+str(nrow)+' - UP')
                nodes_discovered=set()
                nodes_parents=set()
                level_source=0
                for bli in back_links:
                    add_to_source_tree (
                        parent= (self.maf_num, nrow), 
                        child=bli)

        if levels_ref!=0:
            def add_to_ref_tree (parent=(), attr_num=0, child=()):
                        nonlocal only_fields
                        nonlocal self
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
                    
                        linking_maf_n = child [ 0 ]
                        row_in_linking_maf =  child[ 1 ]
                        add_temp_mafs( self, linking_maf_n)       
                        linking_maf =   self.ht.mafs_opened[ linking_maf_n ]     

                        row_ref_trees[field].add_child (
                            parent=parent, 
                            n_attr=attr_num,
                            child=child, 
                            nodes_discovered=nodes_discovered,
                            nodes_parents=nodes_parents)
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
                                 attr_num !=1 and\
                                (only_fields==set() or (only_fields!=set() and fi in only_fields)):

                                kerr=[]
                                links = linking_maf.r_links (
                                    Kerr = kerr , attr_num=attr_num, num_row=row_in_linking_maf )
                                if links !=():
                                    for li in links:
                                        nodes_parents.add((linking_maf_n, row_in_linking_maf ) )
                                        add_to_ref_tree(
                                            parent=(linking_maf_n, row_in_linking_maf ) , 
                                            attr_num=attr_num,
                                            child= li)

                        level_ref-=1
                        return

            for field in self.fields:
                attr_num= self.fields[field][0]
                if self.fields[field][1]== '*link' and \
                     attr_num !=1 and\
                    (only_fields==set() or (only_fields!=set() and field in only_fields)):

                    row_ref_trees[field] = Links_tree(
                    self.ht.ht_name, 
                    (self.maf_num, nrow), 
                    self.ht.ht_name+' : '+self.maf_name+' : '+str(nrow)+' : '+field+' - DOWN')
                    nodes_discovered=set()
                    nodes_parents=set()
                    level_ref=0                
                    links = self.r_links (Kerr = kerr , attr_num=attr_num, num_row=nrow )
                    if links !=():
                        for li in links:
                            nodes_parents.add((self.maf_num, nrow) )
                            add_to_ref_tree (
                                parent=(self.maf_num, nrow), 
                                attr_num= attr_num,
                                child=li)

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