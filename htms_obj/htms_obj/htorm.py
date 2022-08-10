# HTMS object level  v. 3.1.0 (Cage class v. 3.1.0)
# © A.S.Aliev, 2018-2022


# HT_Obj(HTdb) class
#
#    HT_Obj(
#       server = '', 
#       db_root = '', 
#       db_name='',
#       cage_name='', 
#       new = False, 
#       jwtoken="",
#       zmq_context = False,
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
#       open_table(t_name='')
#           return table 
#           or 
#           if error - False
#
# Obj_RAM(Table) class
#
#    Obj_RAM(
#       table ='', 
#       only_fields=set() 
#    )
#
#    @classmethods
#
#       getinstances()
#       removeinstances(obj)
#
#    methods (commonly used)
#
#       def get_HT_Obj()
#           return instance HT 
#           or 
#           if error - None
#       get_table_object()
#           return instance MAF 
#           or 
#           if error - None
#       get_attr_num_and_type(attr_name="")
#           return (num,type) 
#           or 
#           if error - ()
#       about()
#           return information about instance Obj_RAM (for debugging)
#       get_clone()
#           return new dummy instance Obj_RAM
#       get_from_RAM(id=0)
#           return [existing instances Obj_RAM]
#           or
#           if not found - []
#       get_from_table(rows =(), with_fields={},  modality ='all', 
#                      res_num = Types_htms.max_int4, update = False 
#       )
#           return [new instances Obj_RAM]
#           or
#           if not found - []
#       link (
#           link_field ='', 
#           to_table_objects= () 
#       )
#           return True 
#           or 
#           if error - raise HTMS_Mid_Err : 40-44
#       unlink ( 
#           link_field ='', 
#           to_table_objects= () 
#       )
#           return True 
#           or 
#           if error - raise HTMS_Mid_Err : 60-64
#       weight ( 
#           weight_field ='', 
#           to_table_objects= () 
#       )
#           return True 
#           or 
#           if error - raise HTMS_Mid_Err : 50-55
#       unweight ( 
#           link_field ='', 
#           to_table_objects= () 
#       )
#           return True 
#           or 
#           if error - raise HTMS_Mid_Err : 70-75
#       weight_change ( 
#           weight_field ='', 
#           to_RAM_objects= {} 
#       )
#           return True 
#           or 
#           if error - raise HTMS_Mid_Err : 100-104
#       link_refs ( link_field ='', only_fields=set(),  with_fields={}, 
#                   ref_class = None, res_num = Types_htms.max_int4 
#       )
#           return [objects_RAM_from_table]
#           or 
#           if error - raise HTMS_Mid_Err : 80-83
#       weight_refs ( weight_field ='', only_fields=set(),  with_fields={}, 
#                     ref_class = None, res_num = Types_htms.max_int4
#       )
#           return [objects_RAM_from_table]
#           or 
#           if error - raise HTMS_Mid_Err : 85-88
#       source ( source_class = None, only_fields=set(), 
#                with_fields={}, res_num = Types_htms.max_int4 )
#           return [objects_RAM_from_table]
#           or 
#           if error - raise HTMS_Mid_Err : 95-98
#       delete ()
#           return True 
#           or 
#           if error - raise HTMS_Mid_Err : 90
#       save ()
#           return True 
#           or 
#           if error - raise HTMS_Mid_Err : 92-94
#


import weakref
import copy
import time
import binascii

from cage_api import *

from htms_low_api  import *

from htms_mid_api  import *

from .htms_par_obj       import *    


Mod_name = "*" + __name__

#------------------------------------------------------------------------------------------------

class HT_Obj ( HTdb):  

    _instances = set()

    def __str__(self):
        return self.db_name

    def __init__(self,  server = '', db_root = '', db_name='',  
                 cage_name='', new = False, jwtoken="",zmq_context = False,
                 mode='wm', local_root=""):

        self.can_setattr= False

        for db  in HTdb.getinstances():
            if  db.db_name == db_name:
                    pr ('10 HTMS_User_Err     Data base  " %s " already exist and opened. '% db_name )
                    raise HTMS_User_Err('10 HTMS_User_Errr     Data base  " %s " already exist and opened. '% db_name ) 

        self.weak= weakref.ref(self)
        self._instances.add(self.weak)
        self.__class__._instances.add(self.weak)  

        self.htdb  =  super( HT_Obj , self)        #   26/2/21 change  self.htdb  to self.htdb 
        self.htdb._instances.add(self.weak)

        self.htdb .__init__( db_name =db_name,  server = server, db_root = db_root,
                          cage_name=cage_name, new=new,  jwtoken=jwtoken, 
                          zmq_context = zmq_context, from_subclass=True,
                          mode=mode, local_root=local_root  )
#------------------------------------------------------------------------------------------------

    def close(self, Kerr=[]):
        HT_Obj.removeinstances(self)
        db_ =  super( HT_Obj, self)
        try:
            rc= db_.close(Kerr=Kerr)
        except:
            pr ('15 HTMS_Obj_Err     HT_Obj " %s " close error from low level.'% self.db_name )
            raise HTMS_Mid_Err('15 HTMS_Mid_Err     HT_Obj " %s " close error from low level.'% self.db_name  )
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

    def update_RAM (self,  fun = '', attr_num_p=0, maf_num_p=0,  after_row =-1 , num_rows=0 ):

        htdb =  super( HT_Obj , self)
        rc= htdb.update_RAM (fun =fun, attr_num_p=attr_num_p, maf_num_p=maf_num_p, 
                               after_row =after_row , num_rows=num_rows )
        if rc == False:
            return False

        delay_kerr =0

        if DEBUG_UPDATE_RAM:
            pr('\n\n  UPDATE_RAM OBJ---   fun = %s, attr_num_p=%d, maf_num_p=%d,  after_row = %d, num_rows = %d' %  \
                (fun , attr_num_p, maf_num_p,  after_row, num_rows ))     

        atr_remove      =False
        maf_remove    =False
        field_remove   =False
        row_add          =False
        row_delete      =False

        if fun == 'atr_remove':           
            atr_remove      = True
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

        if row_add or  row_delete:
            if  num_rows  ==0:
                return True
            else:
                if row_add:
                    old_max_row_num = self.mafs [ maf_num_p ]['rows'] -  num_rows 
                else:
                    old_max_row_num = self.mafs [ maf_num_p ]['rows'] +  num_rows 

        if atr_remove:
            maf_num_p= 0
            after_row = 0
        else :
            if max_maf_num== 0:
                return True

            if  maf_remove:
                attr_num_p = 0  # apply to all fields of maf
                after_row = 0         # apply to all rows of maf

            else:
                if field_remove:
                    after_row = 0      # apply to all rows of maf

        dead = set()

        for object_RAM in Obj_RAM.getinstances():
            if  object_RAM.id != None: 
                row = object_RAM.id
                if DEBUG_UPDATE_RAM:
                    pr ('\n   object_RAM_OBJ  = %s,  id = %d' %  ( str (object_RAM), row ) )
            else:
                row = 0
            maf = object_RAM.maf_num

            if maf ==0: # link is zero or link to all maf
                dead.add ( object_RAM)
                continue
            if (row != 0 and maf == maf_num_p) :	# MAF matched
                if (maf_remove) :	# removed MAF matched                         
                    dead.add ( object_RAM)
                elif (row_delete ) :
                    if ( row > (after_row +  num_rows ) and row <= old_max_row_num ) :
									            # MAF row after deleted range - 
										        # correct number 
                        object_RAM.id = row -  num_rows 
                    elif (row > after_row and row <= (after_row +  num_rows )) :
                        dead.add ( object_RAM)
                    elif (row > old_max_row_num) :
                                                # found row number greater than last number - clear it
                        dead.add ( object_RAM)
                        delay_kerr = 476
                    else:
                        continue
                elif (row_add ) :
                    if ( row > after_row  and row <= old_max_row_num ) :  
                                # MAF row in deleted range  and saved- correct number 											    
                        object_RAM.id = row +  num_rows 
                    elif (row > old_max_row_num ) :  
                                # found row number greater than last number before adding new rows -  delete set 
                        dead.add ( object_RAM)
                        delay_kerr = 477
                    else:
                        continue
                
        q_dead = len (dead)
        for q in range (q_dead):
            dead_obj = dead.pop()
            dead_obj.remove_instance()                    

        if (delay_kerr !=0) :
            set_warn_int (Kerr, Mod_name, 'update_RAM_OBJ '+self.ht_name, 9  , message='Link array delayed error No= %d.'%delay_kerr )
            return False     

        if DEBUG_UPDATE_RAM:
            pr('  UPDATE_RAM_OBJ ---   FINISH' )     

        return True       

# -------------------------------------------------------------------------------------------

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

    def open_table(self, t_name=''):
        if t_name=='':
            return False
        try:
            table=Table( ht_root=self.ht_root, ht_name=self.db_name, t_name=t_name )
        except:
            return False
        else:
            return table

#------------------------------------------------------------------------------------------------


class Obj_RAM( Table):

    _instances = set()

    def __setattr__(self, nam, val):
        if nam == 'can_setattr': 
            self.__dict__[nam] = val
        elif self.can_setattr == False:
            return
        else:
            self.__dict__[nam] = val
            
    def __init__( self, table ='', only_fields=set() ):
        self.can_setattr= False
        weak_ref= weakref.ref( self )
        self._instances.add( weak_ref )
        #table.__init__( ht_name = table.ht.ht_name)
        table_obj_attrs = set ( table.fields.keys()  )
        if only_fields != set():
            if type (only_fields).__name__ == 'str':
                if  not ( only_fields  in  table_obj_attrs ) :
                    pr ('30 HTMS_User_Err    Error:  object_field not belongs to table fields.')
                    raise HTMS_User_Err('30 HTMS_User_Err    Error:  object_field not belongs to table fields.'  )
                fields_names = [ only_fields ]
            else:
                if  not set(only_fields).issubset( table_obj_attrs):
                    pr ('31 HTMS_User_Err    Error:  only_fields not belongs to table fields.')
                    raise HTMS_User_Err('31 HTMS_User_Err    Error:  only_fields not belongs to table fields.'  )
                fields_names = list ( only_fields ) 
        else :              
            fields_names = list ( table_obj_attrs) 

        data_fields = []
        for name in fields_names :
            if  table.ht.get_attr_num_and_type ( name  )[ 1 ] \
                not in ('*link', '*weight'):
                data_fields.append( name )     

        zero_fields =    list ( None for name in data_fields )

        self.can_setattr = True
        self.fields = dict ( zip ( data_fields, zero_fields)  )
        self.id = None  
        t= time.time()
        self.setted =t
        self.updated =t
        self.maf_num= table.maf_num
        self.table_name= table.ht.mafs[ table.maf_num ] [ 'name' ]
        self.HT_Obj_name= table.ht.ht_name
        self.htdb  = table.ht
        """
        if 'objects_RAM'  in  dir ( table ):
            table.objects_RAM.add( weakref.ref( self ) )
        else:
            table.objects_RAM = { weakref.ref( self ) }
        """
#------------------------------------------------------------------------------------------------

    def __del__ (self):
        try:
            self.remove_instance()
        except:
            pass

# -------------------------------------------------------------------------------------------

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

    def remove_instance( self):
        cl= self.__class__
        dead = { weakref.ref( self ) }
        if dead == set():
            pass
        else:
            cl._instances -= dead
        try:
            table =self.get_table_object()
        except:
            del self
            return
        else:
            if table == None:
                del self
                return
            else:
                if 'objects_RAM'  in  dir ( table ):
                    table.objects_RAM -= dead
        del self

#------------------------------------------------------------------------------------------------
   
    def get_HT_Obj(self):

        for h_t  in HT.getinstances():
            if  h_t.ht_name ==  self.HT_Obj_name :
                return h_t
        return None

#------------------------------------------------------------------------------------------------

    def get_table_object(self):
        ht = self.get_HT_Obj()
        if ht  == None:
            return None
        if  self.maf_num in ht.mafs_opened:
            return  ht.mafs_opened[ self.maf_num ]
        else:
            return None

#------------------------------------------------------------------------------------------------

    def get_attr_num_and_type(self, attr_name=""):
        for attr_num in self.htdb .attrs:
            if self.htdb .attrs[attr_num]["name"] == attr_name:
                return (attr_num, self.htdb .attrs[attr_num]["type"])
        return ()

#------------------------------------------------------------------------------------------------

    def  about(self):
        ht = self.get_HT_Obj()
        table_object = self.get_table_object()
        fields =  ' id = %s ' % str( self.id )
        for field in self.fields:
            attr_num, attr_type = ht.get_attr_num_and_type ( attr_name = field ) 
            if attr_type == '*link':
                kerr =[]
                links =table_object.r_links( kerr, attr_num = attr_num, num_row=  self.id )
                if links != ():
                    if attr_num ==1:
                        fields += ( ' back links : %s ' %   str ( links ), )
                    elif attr_num ==3:
                        fields += ( ' back weights : %s ' %   str ( links ), )
                    else:
                        fields += ( ' %s : %s ' % ( field, str ( links ) ), )
                continue
            elif attr_type == '*weight':
                kerr =[]
                weights =table_object.r_weights( kerr, attr_num = attr_num, num_row= self.id )
                if weights != ():
                    fields += ( ' %s : %s ' % ( field, str ( weights ) ), )
                continue
            else:    
                if  self.fields[ field ]== None:
                    fields +=  ', %s = NONE ' %   ( field, )
                elif attr_type.find ( 'byte' ) >=0:
                    fields +=  ', %s = %s ' %  ( field,  binascii.hexlify(self.fields[ field ]) )
                elif attr_type in ( 'time',  'float8', 'float4' ):
                    fields +=  ', %s = %f ' %   ( field,  self.fields[ field ] )
                elif attr_type in ( 'int8', 'int4' ):
                    fields +=  ', %s = %d ' %   ( field,  self.fields[ field ] )
                elif attr_type in ( 'utf50', 'utf100', '*utf', 'datetime' ):
                    fields +=  ', %s = %s ' %   ( field,  self.fields[ field ] )
                elif attr_type[0] =='*':
                    fields += ', %s = NUM ARRAY ' %   ( field )
                elif attr_type == 'file':
                    fields +=  ', %s = FILE ' %   ( field )
                else:
                    fields += ( ' %s = %s ' %   ( field, str ( self.fields[ field ] ) ), )
                    
        return  ' Obj_RAM :  %s  ---  %10s : %s ' % \
            (self.HT_Obj_name, self.table_name, str (fields ).replace( '\'', '' )[ 1 : -1] )

#------------------------------------------------------------------------------------------------

    def __str__(self):
        return  '%s - %d' % (self.table_name, self.maf_num )

#------------------------------------------------------------------------------------------------

    def get_clone( self ):

        obj = copy.copy(self)
        obj.can_setattr = True
        zero_fields =    list ( None for name in self.fields )
        obj.fields = dict ( zip ( self.fields.keys(), zero_fields)  )

        weak_ref = weakref.ref( obj )
        Obj_RAM._instances.add ( weak_ref )

        if 'ht'  in  dir ( self ):
            del  obj.ht
        
        table_object =  self.get_table_object()
        if 'objects_RAM'  in  dir ( self ):
            table_object.objects_RAM.add( weak_ref )
        else:
            table_object.objects_RAM = { weak_ref }

        return obj

#------------------------------------------------------------------------------------------------

    def get_from_RAM(self, id = 0 ):

        table_object =  self.get_table_object()
        if  len (table_object.objects_RAM) ==0:
            return []
        obj = []
        for obj_ins in  table_object.get_RAM_instances():
            if id == 0:
               obj.append ( obj_ins)
               continue
            elif  obj_ins.id == id: 
                if  obj == None:
                    obj =obj_ins
                elif obj_ins.updated > obj.updated:
                    #remove_instance (obj )
                    obj = obj_ins
                else:
                    #remove_instance ( obj_ins)
                    pass
        return obj

#------------------------------------------------------------------------------------------------

    def get_from_table(self, rows =(), with_fields={},  modality ='all', 
                       res_num = Types_htms.max_int4, update = False ):
            # rows= ( row_num,..., row_num) - tuple
            # with_fields = { ( attr_name|field_name : ( oper, value1, value2|none), ...., attr_name|field_name : ( oper, value1, value2|none) }
            # oper:  ==/ != / >= / <=/ in / not in / ....

        table_object =  self.get_table_object()
        if table_object.rows <1:
            return [] 

        if type (rows).__name__ == 'int':
            if rows <= table_object.rows:
                rows = ( rows,)
                modality='one'
            else:
                pr ('35 HTMS_User_Err    Error in input data.  Row with number  %d exceeds table "%s" length.' \
                    % (rows, table_object.maf_name)  )
                raise HTMS_User_Err('35 HTMS_User_Err    Error in input data.  Row with number  %d exceeds table "%s" length.' \
                    % (rows, table_object.maf_name)  )
        elif  rows  == () and  with_fields  == {} and modality =='one' :
            return []
        elif rows ==() :
            rows =(r for r in range (1, table_object.rows+1) )

        if True:
                if  with_fields != {}:
                    find_rows = table_object.sieve( 
                        with_fields = with_fields.copy(), 
                        modality =modality, 
                        res_num = res_num )
                    if find_rows == () : 
                        return  []
                    else:
                        find_rows = set.intersection ( 
                            set( rows ), 
                            set (find_rows)  )              
                else:
                        find_rows =  set.intersection ( 
                            set( rows ),  
                            set (r for r in range (1, table_object.rows+1) ) )         

                obj_list= []
                res_n=0
                for row in find_rows:

                    is_new = True

                    if update:
                        if len (table_object.objects_RAM) >0:
                            for obj_ins in  table_object.get_RAM_instances():
                                if  obj_ins.id == row :
                                    obj = obj_ins
                                    new_fields = only_fields | set( obj_ins.fields.keys() ) 
                                    fields_names =  list ( new_fields  )
                                    del obj.fields
                                    obj.updated = time.time()
                                    is_new = False
                                    break

                    if is_new:
                        obj = copy.copy(self)
                        fields_names =  list ( self.fields.keys() ) 
                        del  obj.fields
                        del  obj.htdb

                    obj.can_setattr= True
                    obj.id= row
                    obj.fields={}

                    for field in fields_names:
                        kerr=[]
                        attr_num = table_object.fields[ field ][ 0 ]
                        atr_type =  table_object.fields[ field ][ 1 ]

                        if atr_type in ('*link', '*weight'):                           
                            continue                        

                        if atr_type[ : 4] == 'byte':
                            data =  table_object.r_elem ( kerr, attr_num=attr_num, num_row =row)
                        elif atr_type[ : 3] == 'utf' or atr_type == 'datetime':
                            data =  table_object.r_utf8 ( kerr, attr_num=attr_num, num_row =row  )
                            if data == '':
                                data =None
                        elif atr_type == '*byte':
                            data =  table_object.r_bytes ( kerr, attr_num=attr_num, num_row =row  )
                            if data == b'':
                                data =None
                        elif atr_type == '*utf':
                            data = table_object.r_str ( kerr, attr_num=attr_num, num_row =row  )
                            if data == '':
                                data =None
                        elif atr_type == 'file':
                            file_descr= table_object.r_file_descr(kerr, attr_num=attr_num, num_row =row  )
                            if file_descr!=None and file_descr !={} :
                                pass
                            else:
                                file_descr=None
                        else: # element is number or array of numbers
                            data =  table_object.r_numbers ( kerr, attr_num=attr_num, num_row =row  )
                            if atr_type in ( "int4", "int8","float4","float8","time") and \
                                data == Types_htms.types[ atr_type ][ 2 ] :
                                    data =None
                            if atr_type in ( "*int4", "*int8","*float4","*float8") and \
                                data == () :
                                    data =None
                
                        if is_err( kerr ) >= 0 :      
                             pr ('36 HTMS_User_Err    Error read data.  err = %s' % str (kerr)  )
                             raise HTMS_User_Err('35 HTMS_User_Err    Error read data.  err = %s' % str (kerr)  )

                        if atr_type == 'file':
                            obj.fields [ field ] = file_descr
                        else:
                            obj.fields [ field ] =data

                    res_n += 1
                    if res_n <= res_num:
                        if is_new:
                            weak_ref = weakref.ref( obj )
                            Obj_RAM._instances.add ( weak_ref )
                            if 'objects_RAM'  in  dir ( table_object ):
                                table_object.objects_RAM.add( weak_ref )
                            else:
                                table_object.objects_RAM = { weak_ref }

                        obj_list.append(  obj )
                        if res_n == res_num:
                            return obj_list
        
        return obj_list

#----------------------------------------------------------------

    def link ( self, link_field ='', to_table_objects= () ):

        if to_table_objects == ():
            return True 
        if self.id ==None:
            pr ('40 HTMS_User_Err    Impossible save links for Obj_RAM instance without an id.'  )
            raise HTMS_User_Err ('40 HTMS_User_Err    Impossible save links for Obj_RAM instance without an id.'  )
        ht = self.get_HT_Obj()
        if ht.mode in ('rs', 'rm' ):
            pr ('41 HTMS_User_Err    Mode "%s" incompatible for weight objects in HT "%s".'
                    % (ht.mode, ht.ht_name)
            )
            raise HTMS_Mid_Err('41 HTMS_User_Err    Mode "%s" incompatible for weight objects in HT "%s".'
                    % (ht.mode, ht.ht_name)
            )
        table_object = self.get_table_object()
        attr_num_type = ht.get_attr_num_and_type ( attr_name = link_field )
        if attr_num_type == ():
            pr ('42 HTMS_User_Err    Field invalid.'  )
            raise HTMS_User_Err ('42 HTMS_User_Err    Field invalid..'  )
        else:
            attr_num, attr_type = attr_num_type 
        if attr_type != '*link'  or attr_num in (1, 3):
            pr ('43 HTMS_User_Err    Type of link field invalid.'  )
            raise HTMS_User_Err ('43 HTMS_User_Err    Type of link field invalid.'  )

        if  to_table_objects.__class__ == Obj_RAM:
            table_object.update_links ( self.id, link_field, 
                add_links= { to_table_objects.table_name : to_table_objects.id } )
        elif  type ( to_table_objects ).__name__  in ( 'tuple', 'list', 'set'):
            add_links = {}
            for i in range ( len (to_table_objects) ):
                to_table =  to_table_objects[ i ].table_name
                to_row = to_table_objects[ i ].id

                if not ( to_table in links ):
                    add_links [ to_table ] = {  to_row, }
                else:
                    add_links [ to_table ].add ( to_row ) 
            table_object.update_links ( self.id, link_field, add_links= add_links ) 
        else:
            pr ('44 HTMS_User_Err    Parameter "to_table_objects" is invalid.'  )
            raise HTMS_User_Err ('44 HTMS_User_Err    Parameter "to_table_objects" is invalid.'  )

        return True

#----------------------------------------------------------------

    def weight ( self, weight_field ='', to_table_objects= () ):

            #  to_table_objects=
            #                    ( Obj_RAM instance, weight)  or
            #                    { (Obj_RAM instance , weight), ....  }

        if to_table_objects == ():
            return True
        if self.id ==None:
            pr ('50 HTMS_User_Err    Impossible save weights for Obj_RAM instance without an id.'  )
            raise HTMS_User_Err ('51 HTMS_User_Err    Impossible save weights for Obj_RAM instance without an id.'  )
        ht = self.get_HT_Obj()
        if ht.mode in ('rs', 'rm' ):
            pr ('51 HTMS_User_Err    Mode "%s" incompatible for weight objects in HT "%s".'
                    % (ht.mode, ht.ht_name)
            )
            raise HTMS_Mid_Err('51 HTMS_User_Err    Mode "%s" incompatible for weight objects in HT "%s".'
                    % (ht.mode, ht.ht_name)
            )
        table_object = self.get_table_object()
        attr_num_type = ht.get_attr_num_and_type ( attr_name = weight_field )
        if attr_num_type == ():
            pr ('52 HTMS_User_Err    Field invalid.'  )
            raise HTMS_User_Err ('52 HTMS_User_Err    Field invalid..'  )
        else:
            attr_num, attr_type = attr_num_type 
        if attr_type != '*weight':
            pr ('53 HTMS_User_Err    Type of weight field invalid.'  )
            raise HTMS_User_Err ('53 HTMS_User_Err    Type of weight field invalid.'  )

        if  type(to_table_objects).__name__ in ( 'tuple', 'list')\
            and len(to_table_objects)==2 and \
            to_table_objects[0].__class__ == Obj_RAM:
            table_object.update_weights ( self.id, weight_field, 
                update_weights= { to_table_objects[0].table_name : 
                                       { to_table_objects[0].id: float(to_table_objects[1]) }} )
        elif  type ( to_table_objects ).__name__  in ('set', 'tuple', 'list'):
            update_weights = {}
            for i in range ( len (to_table_objects) ):
                if type ( to_table_objects[i] ).__name__  == 'tuple' and \
                   len( to_table_objects[i] )==2 and \
                   to_table_objects[i][0].__class__ == Obj_RAM:
                    to_table =  to_table_objects[ i ][0].table_name
                    to_row = to_table_objects[ i ][0].id
                    we= float( to_table_objects[ i ][1] )
                    if not ( to_table in weights ):
                        update_weights [ to_table ] = { to_row: we }
                    else:
                        update_weights [ to_table ].update ( { to_row: we } ) 
                    table_object.update_weights ( self.id, weight_field, update_weights= update_weights )
                else:
                    pr ('54 HTMS_User_Err    Parameter "to_table_objects" is invalid.'  )
                    raise HTMS_User_Err ('54 HTMS_User_Err    Parameter "to_table_objects" is invalid.'  )
            
        else:
            pr ('55 HTMS_User_Err    Parameter "to_table_objects" is invalid.'  )
            raise HTMS_User_Err ('55 HTMS_User_Err    Parameter "to_table_objects" is invalid.'  )

        return True

#----------------------------------------------------------------#----------------------------------------------------------------

    def unlink ( self, link_field ='', to_table_objects= () ):

        if to_table_objects == ():
            return True
        ht = self.get_HT_Obj()
        if ht.mode in ('rs', 'rm' ):
            pr ('60 HTMS_User_Err    Mode "%s" incompatible for weight objects in HT "%s".'
                    % (ht.mode, ht.ht_name)
            )
            raise HTMS_Mid_Err('60 HTMS_User_Err    Mode "%s" incompatible for weight objects in HT "%s".'
                    % (ht.mode, ht.ht_name)
            )
        table_object = self.get_table_object()
        attr_num_type = ht.get_attr_num_and_type ( attr_name = link_field )
        if attr_num_type == ():
            pr ('61 HTMS_User_Err    Field invalid.'  )
            raise HTMS_User_Err ('61 HTMS_User_Err    Field invalid..'  )
        else:
            attr_num, attr_type = attr_num_type 
        if attr_type != '*link'  or attr_num in (1, 3):
            pr ('62 HTMS_User_Err    Type of link field invalid.'  )
            raise HTMS_User_Err ('62 HTMS_User_Err    Type of link field invalid.'  )
        if  type ( to_table_objects ).__name__ == 'str'  and to_table_objects == 'all':
            table_object.update_links ( self.id, link_field, delete_links= 'all' )
        elif  to_table_objects.__class__ == Obj_RAM:
            table_object.update_links ( self.id, link_field, 
                delete_links= { to_table_objects.table_name : to_table_objects.id } )
        elif  type ( to_table_objects ).__name__ in ('tuple', 'set', 'list'):
            delete_links = {}
            for i in range ( len (to_table_objects) ):
                if  to_table_objects.__class__ == Obj_RAM:
                    to_table =  to_table_objects[ i ].table_name
                    if not ( to_table in links ):
                        delete_links [ to_table ] = ( to_table_objects[ i ].id, )
                    else:
                        delete_links [ to_table ] +=( to_table_objects[ i ].id, ) 
                else:
                    pr ('63 HTMS_User_Err    Parameter "to_table_objects" is invalid.'  )
                    raise HTMS_User_Err ('63 HTMS_User_Err    Parameter "to_table_objects" is invalid.'  )

            table_object.update_links ( self.id, link_field, delete_links= delete_links ) 
        else:
            pr ('64 HTMS_User_Err    Parameter "to_table_objects" is invalid.'  )
            raise HTMS_User_Err ('64 HTMS_User_Err    Parameter "to_table_objects" is invalid.'  )

        return True

 #----------------------------------------------------------------

    def unweight ( self, weight_field ='', to_table_objects= () ):

            #  to_table_objects=
            #                    "all",  or
            #                    (Obj_RAM instance ,  ....  }

        if to_table_objects == ():
            return 
        if self.id ==None:
            pr ('70 HTMS_User_Err    Impossible save weights for Obj_RAM instance without an id.'  )
            raise HTMS_User_Err ('70 HTMS_User_Err    Impossible save weights for Obj_RAM instance without an id.'  )
        ht = self.get_HT_Obj()
        if ht.mode in ('rs', 'rm' ):
            pr ('71 HTMS_User_Err    Mode "%s" incompatible for weight objects in HT "%s".'
                    % (ht.mode, ht.ht_name)
            )
            raise HTMS_Mid_Err('71 HTMS_User_Err    Mode "%s" incompatible for weight objects in HT "%s".'
                    % (ht.mode, ht.ht_name)
            )
        table_object = self.get_table_object()
        attr_num_type = ht.get_attr_num_and_type ( attr_name = weight_field )
        if attr_num_type == ():
            pr ('72 HTMS_User_Err    Field invalid.'  )
            raise HTMS_User_Err ('72 HTMS_User_Err    Field invalid.'  )
        else:
            attr_num, attr_type = attr_num_type 
        if attr_type != '*weight':
            pr ('73 HTMS_User_Err    Type of weight field invalid.'  )
            raise HTMS_User_Err ('73 HTMS_User_Err    Type of weight field invalid.'  )

        if  type(to_table_objects).__name__ =='str' and \
            to_table_objects == 'all':
            table_object.update_weights ( self.id, weight_field, delete_weights= 'all' )

        elif  type ( to_table_objects ).__name__  in ('tuple', 'set', 'list'):
            delete_weights = {}
            for i in range ( len (to_table_objects) ):
                if to_table_objects[ i ].__class__ == Obj_RAM:
                    to_table =  to_table_objects[ i ].table_name
                    to_row = to_table_objects[ i ].id
                    if not ( to_table in weights ):
                        delete_weights [ to_table ] = ( to_row, )
                    else:
                        delete_weights [ to_table ] += ( to_row, ) 
                    table_object.update_weights ( self.id, weight_field, 
                                                 delete_weights= delete_weights )
                else:
                    pr ('74 HTMS_User_Err    Parameter "to_table_objects" is invalid!'  )
                    raise HTMS_User_Err ('74 HTMS_User_Err    Parameter "to_table_objects" is invalid!'  )
            
        else:
            pr ('75 HTMS_User_Err    Parameter "to_table_objects" is invalid.'  )
            raise HTMS_User_Err ('75 HTMS_User_Err    Parameter "to_table_objects" is invalid.'  )

        return True

#----------------------------------------------------------------

    def weight_change ( self, weight_field ='', to_RAM_objects= {} ):

            #  to_RAM_objects=
            #                    { Obj_RAM instance:weight ,  ....  }

        if to_RAM_objects == {}:
            return 
        if self.id ==None:
            pr ('100 HTMS_User_Err    Impossible save weights for Obj_RAM instance without an id.'  )
            raise HTMS_User_Err ('100 HTMS_User_Err    Impossible save weights for Obj_RAM instance without an id.'  )
        ht = self.get_HT_Obj()
        if ht.mode in ('rs', 'rm' ):
            pr ('101 HTMS_User_Err    Mode "%s" incompatible for weight objects in HT "%s".'
                    % (ht.mode, ht.ht_name)
            )
            raise HTMS_Mid_Err('101 HTMS_User_Err    Mode "%s" incompatible for weight objects in HT "%s".'
                    % (ht.mode, ht.ht_name)
            )
        table_object = self.get_table_object()
        attr_num_type = ht.get_attr_num_and_type ( attr_name = weight_field )
        if attr_num_type == ():
            pr ('102 HTMS_User_Err    Field invalid.'  )
            raise HTMS_User_Err ('102 HTMS_User_Err    Field invalid.'  )
        else:
            attr_num, attr_type = attr_num_type 
        if attr_type != '*weight':
            pr ('103 HTMS_User_Err    Type of weight field invalid.'  )
            raise HTMS_User_Err ('103 HTMS_User_Err    Type of weight field invalid.'  )

        upd_weights={}
        for to_obj in to_RAM_objects:
            if to_obj.__class__ == Obj_RAM:
                to_table = to_obj.table_name
                to_row = to_obj.id
                weight= to_RAM_objects[to_obj]
                if upd_weights=={} or to_table not in upd_weights:
                    upd_weights[to_table]= { to_row : weight }
                else:
                    upd_weights[to_table].update ( { to_row : to_RAM_objects[to_obj] } )
            else:
                pr ('104 HTMS_User_Err    Parameter "to_RAM_objects" is invalid!'  )
                raise HTMS_User_Err ('104 HTMS_User_Err    Parameter "to_RAM_objects" is invalid!'  )

        table_object.update_weights ( self.id, weight_field, 
                      update_weights= upd_weights )

        return True

#----------------------------------------------------------------  
#  
    def link_refs ( self, link_field ='', only_fields=set(),  with_fields={}, 
             ref_class = None, res_num = Types_htms.max_int4 ):   #'all' \{'field':value}) ):

        ht = self.get_HT_Obj()
        table_object = self.get_table_object()
        attr_num_type = ht.get_attr_num_and_type ( attr_name = link_field )
        if attr_num_type == ():
            pr ('80 HTMS_User_Err    Link field invalid.'  )
            raise HTMS_User_Err ('80 HTMS_User_Err    Link field invalid..'  )
        else:
            attr_num, attr_type = attr_num_type 
        if attr_type != '*link'  or attr_num in (1, 3):
            pr ('81 HTMS_User_Err    Type of link field invalid.'  )
            raise HTMS_User_Err ('81 HTMS_User_Err    Type of link field invalid.'  )
        
        kerr=[]
        links = table_object.r_links( kerr, attr_num, self.id)
        if is_err( kerr ) >= 0 :
            pr ('82 HTMS_User_Err   Error read links.  err = %s' % str (kerr)  )
            raise HTMS__User_Err ('82 HTMS_User_Err   Error read links.  err = %s' % str (kerr)  )

        num_temp_mafs_opened = 0
        temp_mafs_opened ={} 
        
        def close_temp_mafs():
                nonlocal temp_mafs_opened
                if len ( temp_mafs_opened )>0:
                    for mf in  temp_mafs_opened :
                        temp_mafs_opened[ mf ].close()
                del temp_mafs_opened
                temp_mafs_opened ={}  

        def add_temp_mafs( temp_maf):
                nonlocal ht, temp_mafs_opened,  num_temp_mafs_opened
                if  not ( temp_maf in ht.mafs_opened):
                        temp_mafs_opened[ temp_maf ] = Table(ht_name= ht.ht_name, t_nmaf = temp_maf  )
                        num_temp_mafs_opened += 1

        if links == ():
            return []
        if ref_class != None:
            for table in ref_class.getinstances():
                if table.__class__ == ref_class :
                    single_maf = table.maf_num
                    tables_maf_num = [ single_maf ]
                    break
        else:
            single_maf = False
            tables_maf_num = list ( set ( maf for (maf, row) in links  if  not single_maf or  maf == single_maf  ) )
            if tables_maf_num == []:
                return []
                                               
        objects_RAM_from_table = []
        for maf_num in tables_maf_num:
            add_temp_mafs( maf_num )
            rows = tuple ( set (row  for (maf, row) in links  if  maf == maf_num ) )
            tabl_data_fields = set()
            for attr in ht.mafs_opened[ maf_num ].fields:
                if  ht.mafs_opened[ maf_num ].fields [ attr ][ 1 ] in ('*weight','*link'):
                    if attr in only_fields:
                        pr ('83 HTMS_User_Err  Link type attribute "%s" in "only fields" parameter' % str (attr)  )
                        raise HTMS__User_Err ('83 HTMS_User_Err  Link type attribute "%s" in "only fields" parameter' % str (attr)  )
                    continue
                if  only_fields == set() or  attr in only_fields :
                    tabl_data_fields.add ( attr )            
            if  tabl_data_fields == set():
                continue
            temp_record = Obj_RAM ( ht.mafs_opened[ maf_num ], only_fields = tabl_data_fields )
            temp_obj = temp_record.get_from_table( rows, with_fields=with_fields, res_num = res_num )

            for obj in temp_obj:
                weak_ref = weakref.ref( obj )
                Obj_RAM._instances.add ( weak_ref )
                if 'objects_RAM'  in  dir ( table_object ):
                    table_object.objects_RAM.add( weak_ref )
                else:
                    table_object.objects_RAM = { weak_ref }
            
            objects_RAM_from_table += temp_obj  
                       
            if len ( objects_RAM_from_table ) >=  res_num:
                objects_RAM_from_table = objects_RAM_from_table[ : res_num ]
                break

        close_temp_mafs()

        return objects_RAM_from_table

#------------------------------------------------------------------------------

    def weight_refs ( self, weight_field ='', only_fields=set(),  with_fields={}, 
             ref_class = None, res_num = Types_htms.max_int4 ):   #'all' \{'field':value}) ):

        ht = self.get_HT_Obj()
        table_object = self.get_table_object()
        attr_num_type = ht.get_attr_num_and_type ( attr_name = weight_field )
        if attr_num_type == ():
            pr ('85 HTMS_User_Err   Weight field invalid.'  )
            raise HTMS_User_Err ('85 HTMS_User_Err    Weight field invalid..'  )
        else:
            attr_num, attr_type = attr_num_type 
        if attr_type != '*weight':
            pr ('86 HTMS_User_Err    Type of weight field invalid.'  )
            raise HTMS_User_Err ('86 HTMS_User_Err    Type of weight field invalid.'  )
        
        kerr=[]
        weights = table_object.r_weights( kerr, attr_num, self.id)
        if is_err( kerr ) >= 0 :
            pr ('87 HTMS_User_Err   Error read weights.  err = %s' % str (kerr)  )
            raise HTMS__User_Err ('87 HTMS_User_Err   Error read weights.  err = %s' % str (kerr)  )

        num_temp_mafs_opened = 0
        temp_mafs_opened ={} 
        
        def close_temp_mafs():
                nonlocal temp_mafs_opened
                if len ( temp_mafs_opened )>0:
                    for mf in  temp_mafs_opened :
                        temp_mafs_opened[ mf ].close()
                del temp_mafs_opened
                temp_mafs_opened ={}  

        def add_temp_mafs( temp_maf):
                nonlocal ht, temp_mafs_opened,  num_temp_mafs_opened
                if  not ( temp_maf in ht.mafs_opened):
                        temp_mafs_opened[ temp_maf ] = Table(ht_name= ht.ht_name, t_nmaf = temp_maf  )
                        num_temp_mafs_opened += 1

        if weights == ():
            return []

        if ref_class != None:
            for table in ref_class.getinstances():
                if table.__class__ == ref_class :
                    single_maf = table.maf_num
                    tables_maf_num = [ single_maf ]
                    break
        else:
            single_maf = False
            tables_maf_num = list ( set ( maf for (maf, row, we) in weights  
                                         if  not single_maf or  maf == single_maf  ) )
            if tables_maf_num == []:
                return []
                                               
        objects_RAM_from_table = []
        for maf_num in tables_maf_num:
            add_temp_mafs( maf_num )
            rows = tuple ( set (row  for (maf, row, we) in weights  if  maf == maf_num ) )
            tabl_data_fields = set()
            for attr in ht.mafs_opened[ maf_num ].fields:
                if  ht.mafs_opened[ maf_num ].fields [ attr ][ 1 ] in ('*weight','*link'):
                    if attr in only_fields:
                        pr ('88 HTMS_User_Err  weight type attribute "%s" in "only fields" parameter' % str (attr)  )
                        raise HTMS__User_Err ('88 HTMS_User_Err  weight type attribute "%s" in "only fields" parameter' % str (attr)  )
                    continue
                if  only_fields == set() or  attr in only_fields :
                    tabl_data_fields.add ( attr )            
            if  tabl_data_fields == set():
                continue
            temp_record = Obj_RAM ( ht.mafs_opened[ maf_num ], only_fields = tabl_data_fields )
            temp_obj = temp_record.get_from_table( rows, with_fields=with_fields, res_num = res_num )

            for obj in temp_obj:
                obj_row=obj.id
                for w in weights:
                    if w[0]==maf_num  and w[1]==obj_row:
                        obj.weight=w[2]
                weak_ref = weakref.ref( obj )
                Obj_RAM._instances.add ( weak_ref )
                if 'objects_RAM'  in  dir ( table_object ):
                    table_object.objects_RAM.add( weak_ref )
                else:
                    table_object.objects_RAM = { weak_ref }
            
            objects_RAM_from_table += temp_obj  
                       
            if len ( objects_RAM_from_table ) >=  res_num:
                objects_RAM_from_table = objects_RAM_from_table[ : res_num ]
                break

        close_temp_mafs()

        return objects_RAM_from_table

#----------------------------------------------------------------
    
    def source ( self, source_class = None, only_fields=set(), 
                with_fields={}, res_num = Types_htms.max_int4 ):   #'all' \{'field':value}) ):

        ht = self.get_HT_Obj()
        table_object = self.get_table_object()

        num_temp_mafs_opened = 0
        temp_mafs_opened ={} 

        result= ([], [])
        
        def close_temp_mafs():
                nonlocal temp_mafs_opened
                if len ( temp_mafs_opened )>0:
                    for mf in  temp_mafs_opened :
                        temp_mafs_opened[ mf ].close()
                del temp_mafs_opened
                temp_mafs_opened ={}  

        def add_temp_mafs( temp_maf):
                nonlocal ht, temp_mafs_opened,  num_temp_mafs_opened
                if  not ( temp_maf in ht.mafs_opened):
                        temp_mafs_opened[ temp_maf ] = Table(ht_name= ht.ht_name, t_nmaf = temp_maf  )
                        num_temp_mafs_opened += 1

        if  source_class == None:
            return []
        source_maf_num = 0
        for table in Table.getinstances():
            if table.__class__ == source_class :
                source_maf_num = table.maf_num
                break
        if  source_maf_num == 0:
            pr ('96 HTMS_User_Err   Invalid source class. ' )
            raise HTMS__User_Err ('96 HTMS_User_Err   Invalid source class.')

        add_temp_mafs( source_maf_num )

        source_maf =  ht.mafs_opened [ source_maf_num ]

        kerr=[]
        all_back_links =  table_object.r_links ( kerr, attr_num=1 , num_row=self.id )
        if is_err( kerr ) >= 0 :
            pr ('97 HTMS_User_Err   Error read back links.  err = %s' % str (kerr)  )
            raise HTMS__User_Err ('97 HTMS_User_Err   Error read back links.  err = %s' % str (kerr)  )

        kerr=[]
        all_back_weights =  table_object.r_links ( kerr, attr_num=3 , num_row=self.id )
        if is_err( kerr ) >= 0 :
            pr ('98 HTMS_User_Err   Error read back weights.  err = %s' % str (kerr)  )
            raise HTMS__User_Err ('98 HTMS_User_Err   Error read back weights.  err = %s' % str (kerr)  )

        if  all_back_links == () and all_back_weights == ():
            return result

        link_source_rows = []
        for back_link in all_back_links:
            if  back_link[ 0 ] == source_maf_num:
                link_source_rows.append ( back_link [ 1 ] )

        weight_source_rows = []
        for back_weight in all_back_weights:
            if  back_weight[ 0 ] == source_maf_num:
                weight_source_rows.append ( back_weight [ 1 ] )

        if  link_source_rows == [] and weight_source_rows==[]:
            return result

        if  all_back_links != () and link_source_rows !=[]:
            result_0=[]

            tabl_data_fields = set()
            for attr in source_maf.fields:
                if  source_maf.fields [ attr ][ 1 ] in ('*link', '*weight'):
                    continue
                if  only_fields == set() or  attr in only_fields :
                    tabl_data_fields.add ( attr )            
            if  tabl_data_fields != set():
                temp_record = Obj_RAM ( source_maf, only_fields = tabl_data_fields )
                result_0 = temp_record.get_from_table( link_source_rows, 
                                                       with_fields=with_fields, 
                                                       res_num = res_num )
                for obj in result_0:
                    weak_ref = weakref.ref( obj )
                    Obj_RAM._instances.add ( weak_ref )
                    if 'objects_RAM'  in  dir ( table_object ):
                        table_object.objects_RAM.add( weak_ref )
                    else:
                        table_object.objects_RAM = { weak_ref }

        if  all_back_weights != () and weight_source_rows !=[]:
            result_1=[]

            tabl_data_fields = set()
            for attr in source_maf.fields:
                if  source_maf.fields [ attr ][ 1 ] in ('*link', '*weight'):
                    continue
                if  only_fields == set() or  attr in only_fields :
                    tabl_data_fields.add ( attr )            
            if  tabl_data_fields != set():
                temp_record = Obj_RAM ( source_maf, only_fields = tabl_data_fields )
                result_1 = temp_record.get_from_table( weight_source_rows, 
                                                      with_fields=with_fields, res_num = res_num )

                for obj in result_1:
                    weak_ref = weakref.ref( obj )
                    Obj_RAM._instances.add ( weak_ref )
                    if 'objects_RAM'  in  dir ( table_object ):
                        table_object.objects_RAM.add( weak_ref )
                    else:
                        table_object.objects_RAM = { weak_ref }       

        close_temp_mafs()

        return ( result_0, result1 ) 

#----------------------------------------------------------------

    def delete  (self):

        ht = self.get_HT_Obj()
        if ht.mode in ('rs', 'rm' ):
            pr ('90 HTMS_User_Err    Mode "%s" incompatible for weight objects in HT "%s".'
                    % (ht.mode, ht.ht_name)
            )
            raise HTMS_Mid_Err('90 HTMS_User_Err    Mode "%s" incompatible for weight objects in HT "%s".'
                    % (ht.mode, ht.ht_name)
            )
        table_object = self.get_table_object()
        table_object.delete_row ( self.id )
        #del self

        return True   
   
#-------------------.-----------------------------------------------------------------------------
    def save (self):

        ht = self.get_HT_Obj()
        if ht.mode in ('rs', 'rm' ):
            pr ('92 HTMS_User_Err    Mode "%s" incompatible for weight objects in HT "%s".'
                    % (ht.mode, ht.ht_name)
            )
            raise HTMS_Mid_Err('92 HTMS_User_Err    Mode "%s" incompatible for weight objects in HT "%s".'
                    % (ht.mode, ht.ht_name)
            )
        table_object = self.get_table_object()
        table = table_object.ht.mafs_opened[ table_object.maf_num ]
        if  'objects_RAM'  in  dir ( table )  and \
            self in table.get_RAM_instances():
            pass
        else:        
            pr ('93 HTMS_User_Err   Program error.' )
            raise HTMS_User_Err('93 HTMS_User_Err   Program error.' )
        
        if  self.id    == 0 or self.id    == None :
            self.id    = table_object.rows+1

        table_object.update_row ( row_num =self.id, add_data=self.fields.copy() )

        if not ( 'Time_row' in self.fields)  or \
            self.fields [ 'Time_row' ]    == 0 or \
            self.fields [ 'Time_row' ]    == None :
            kerr = []
            self.fields [ 'Time_row' ] = table_object.r_numbers ( kerr, attr_num=2, num_row =self.id  )
            if is_err( kerr ) >= 0 :      
                 pr ('94 HTMS_User_Err    Error read Time_row.  err = %s' % str (kerr)  )
                 raise HTMS_User_Err('94 HTMS_User_Err    Error read Time_row.  err = %s' % str (kerr)  )
     
        return True

