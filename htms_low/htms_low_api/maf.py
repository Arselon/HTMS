# HTMS low level  v. 2.3 (Cage class v. 2.9)
# © A.S.Aliev, 2018-2021

import os
import posixpath
import pickle
import struct  
import weakref
import copy
import time
import math

from cage_api             import  *

from .htms_par_low     import   *
from .data_types          import   *

Mod_name = '*'+__name__
    
# -------------------------------------------------------------------------------------------

class MAF():

    _instances = set()

    def __del__(self):
        try:
            self.close()
           #time.sleep(0.1)
            del self
        except:
            pass

    def __str__(self):
        return self.maf_name

    def __init__(self, 
        ht,
        maf_num=0,
        maf_name = '',
        from_subclass=False
        ):
        """
        try:
            ht_class= ht.__class__
            for h_t  in ht_class.getinstances():
                    if  h_t.ht_name == ht.name:
                        ht_ = h_t
                        break
            if not ( 'ht_' in locals() ) :   
                pr ('08 HTMS_Low_Err     HT " %s " not exist. '% ht.name )
                raise HTMS_Low_Err('08 HTMS_Low_Err     HT " %s " not exist. '% ht.name )
        except:
            pr ('09 HTMS_Low_Err     HT " %s " not exist. '% ht.name )
            raise HTMS_Low_Err('09 HTMS_Low_Err     HT " %s " not exist. '% ht.name )
        """
        self.maf_name =''

        self.ht = ht  
        self.ch = -1
        self.maf_num = 0
        self.offsets = ((0,1),)  
        #self.fields = {}                                         #  !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

        if maf_num >0 and maf_num in self.ht.mafs:
            if maf_name != '' and self.ht.mafs[ maf_num ]['name' ] != maf_name:
                pr ('10 HTMS_Low_Err    MAF names mismatch. MAF  not opened.')
                raise HTMS_Low_Err('10 HTMS_Low_Err    MAF names mismatch. MAF  not opened.')
            self.maf_num=    maf_num  #  MAF exist
            self.maf_name = self.ht.mafs[ maf_num ]['name' ]   
        elif  maf_num >0:
                pr ('11 HTMS_Low_Err   MAF number not found in HT list of mafs. MAF  not opened.')
                raise HTMS_Low_Err('11 HTMS_Low_Err    MAF number not found in HT list of mafs. MAF  not opened.\n')
        else:
            if  maf_name !='':
                found = False
                for maf_num in self.ht.mafs:
                    if maf_name == self.ht.mafs[ maf_num]['name']:   #  MAF exist
                        found = True
                        self.maf_num=  maf_num 
                        self.maf_name = maf_name
                        break
                if not found:   # need to create new MAF
                    self.maf_name = maf_name
            else:
                pr ('12 HTMS_Low_Err   MAF name and number are null values. MAF  not opened.')
                raise HTMS_Low_Err('12 HTMS_Low_Err   MAF name and number are null values. MAF  not opened.')

        if  self.maf_num == 0:
            if not self.create ():
                pr ('13 HTMS_Low_Err   Error creating MAF..')
                raise HTMS_Low_Err('13 HTMS_Low_Err   Error creating MAF.')
           #time.sleep(0.1)

        else:    
            if self.open() == False:
                pr ('14 HTMS_Low_Err   Error opening MAF..')
                raise HTMS_Low_Err('14 HTMS_Low_Err   Error opening  MAF.')

        if not from_subclass:
            self.weak= weakref.ref(self)
            self.__class__._instances.add(self.weak)
          
        self.rows = self.ht.mafs[ self.maf_num]['rows']
        self.setted = self.ht.mafs[ self.maf_num]['setted']
        self.updated = self.ht.mafs[ self.maf_num]['updated']

        self.path = posixpath.join( *(self.ht.ht_root.split(os.path.sep)+[self.ht.ht_name+'_'+str(self.maf_num)+self.ht.ext['maf']]))
        """
        self.table_obj= copy.copy(self)
        del   self.table_obj.ch, self.table_obj.ht, self.table_obj.null_row, self.table_obj.offsets, \
                self.table_obj.setted, self.table_obj.updated, self.table_obj.rowlen, self.table_obj.rows, \
                self.table_obj.maf_num, self.table_obj.path
        self.table_obj.maf_object = self
        """

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
        dead = { weakref.ref(obj) }
        if dead == set():
            pass
        else:
            cls._instances -= dead
            del obj

#------------------------------------------------------------------------------------------------
    def create (self, Kerr= []):

            if self.ht.mode in ('rs', 'rm' ):
                set_err_int (Kerr, Mod_name,   'create '+self.ht.ht_name+'-'+ str(self.maf_num), 8 , \
                                    message='Mode "%s" incompatible for create MAF for HT "%s".'
                    % (self.ht.mode, self.ht.ht_name)
                )
                return False

            t =  time.time()
            if len(self.ht.mafs) == 0:
                    self.ht.mafs[ 1 ] = { 'rows' : 0, 'setted': t, 'updated': t ,  'opened': False, 'name': self.maf_name }  
                    self.maf_num = 1
            else:
                for  maf_num in self.ht.mafs:
                        if  self.maf_name  == self.ht.mafs [maf_num]['name']:
                            set_err_int (Kerr, Mod_name, 'create '+self.ht.ht_name+'-'+  str(self.maf_num), 1, \
                                    message='Argument "maf_name" is wrong - already used.' )
                            return False
                self.maf_num = maf_num +1 
                self.ht.mafs[ self.maf_num ] = { 'rows' : 0, 'setted': t, 'updated': t ,  'opened': False, 'name': self.maf_name }  

            f_maf = posixpath.join( *(self.ht.ht_root.split(os.path.sep)+[self.ht.ht_name+'_'+str(self.maf_num)+self.ht.ext['maf']]))
            
            rc1 = self.ht.cage.file_create( CAGE_SERVER_NAME,  f_maf, Kerr)
           #time.sleep(0.1)
            if  rc1 == False:
                set_err_int (Kerr, Mod_name, 'create '+self.ht.ht_name+'-'+  str(self.maf_num), 2 , \
                        message='Error during new MAF file %s creating.' % f_maf )

                del self.ht.mafs[ self.maf_num ]
                self.maf_num -= 1                    
                return False  

            elif  rc1 == -1  or rc1 == -2:  # file already exist and closed (-1)/ opened(-2)
                    # delete old file because new HT creating
                if len (Kerr) >0:  Kerr.pop()
                if len (Kerr) >0:  Kerr.pop()
                rc11 =  self.ht.cage.file_remove( CAGE_SERVER_NAME,  f_maf, Kerr)
               #time.sleep(0.1)
                if   rc11 == True:
                    rc12 = self.ht.cage.file_create( CAGE_SERVER_NAME,  f_maf, Kerr)
                if   rc11 == False or rc12 == False:
                    set_err_int (Kerr, Mod_name, 'create '+self.ht.ht_name+'-'+  str(self.maf_num), 3 , \
                                message='Error during deleting old and creating new MAF file %s .' % 
                                 f_maf )

                    del self.ht.mafs[ self.maf_num ]
                    self.maf_num -= 1   
                    return False

            #self.ht.mafs[  self.maf_num][ 'path' ] =       f_maf
            rc = self.ht.cage.open( CAGE_SERVER_NAME,  f_maf, Kerr, ) 
           #time.sleep(0.1)
            if rc == False:
                set_err_int (Kerr, Mod_name, 'create '+self.ht.ht_name+'-'+  str(self.maf_num), 4 , \
                                message='Error during opening new MAF file %s .' % f_maf )

                del self.ht.mafs[ self.maf_num ]
                self.maf_num -= 1   
                return False
            self.ht.mafs[  self.maf_num][ 'opened' ] =  True 
            self.ht.mafs_opened [  self.maf_num ] = self
            self.ch =        rc 
            self.rowlen = 1
            self.null_row = b'\x00'
            self.rows = 0

            max_attr_num =  max (  self.ht.attrs.keys() )
            zero_model = ( False  for attr_num in range ( 0, max_attr_num+1 ))
            self.ht.models += ( tuple (zero_model), ) 
            del  zero_model

            if not self.ht.save_adt(Kerr):
                set_err_int (Kerr, Mod_name,   'create '+self.ht.ht_name+'-'+ str(self.maf_num), 7 , \
                                    message='Error save HTD.' )

                del self.ht.mafs[ self.maf_num ]
                self.maf_num -= 1   
                return False

            self.updated = time.time()
            self.ht.updated =self.updated 
            
            return True
 
#------------------------------------------------------------------------------------------------

    def rename (self, Kerr= [], new_maf_name=''):

        if self.ht.mode in ('rs', 'rm' ):
            set_err_int (Kerr, Mod_name,   'rename '+self.ht.ht_name+'-'+ str(self.maf_num), 1 , \
                 message='Mode "%s" incompatible for rename MAF for HT "%s".'
                    % (self.ht.mode, self.ht.ht_name)
            )
            return False
        if  new_maf_name !='':
                found = False
                for maf_num in self.ht.mafs:
                    if new_maf_name == self.ht.mafs[ maf_num]['name']:   #  MAF exist
                        found = True
                        break
                if not found:   # 
                    self.maf_name = new_maf_name
                    self.ht.mafs[self.maf_num]['name']=  new_maf_name

                    self.updated = time.time()
                    self.ht.mafs [ self.maf_num]['updated']= self.updated
                    self.ht.updated =self.updated 

                    return True

        set_err_int (Kerr, Mod_name,   'rename '+self.ht.ht_name+'-'+ str(self.maf_num), 1 , \
                                        message='Error:  new_maf_name is empty or already used.' )
        return False

# -------------------------------------------------------------------------------------------


    def attr_type(self, n_attr=0):
        if len (self.ht.attrs) ==0 or  not self.ht.models [ self.maf_num ] [ n_attr ]:
            return False
        return self.ht.attrs [ n_attr ] [ 'type' ] 

# -------------------------------------------------------------------------------------------


    def delete(self, Kerr=[]):

        if self.ht.mode in ('rs', 'rm' ):
            set_err_int (Kerr, Mod_name,   'delete '+self.ht.ht_name+'-'+ str(maf_num), 5 , \
                               message='Mode "%s" incompatible for delete MAF for HT "%s".'
                    % (self.ht.mode, self.ht.ht_name)
            )
            return False

        max_attr_num =   max (  self.ht.attrs.keys() )
        max_maf_num=  max (  self.ht.mafs.keys() )
        zero_model = ( False  for attr_num in range ( 0, max_attr_num+1 ))
        old_models = self.ht.models

        self.ht.models =((False,),)
        for maf_num in self.ht.mafs:
            if maf_num != self.maf_num:
                self.ht.models += (old_models[maf_num],)
            else:
                self.ht.models += (tuple(zero_model),)
        path=  self.path
        ht= self.ht
        maf_num= self.maf_num
        maf_name = self.maf_name

        rc =   self.close(Kerr)
       #time.sleep(0.1)
        rc1 = ht.cage.file_remove( CAGE_SERVER_NAME, path, Kerr )
       #time.sleep(0.1)
        if not rc or not rc1 :
            set_err_int (Kerr, Mod_name,   'delete '+self.ht.ht_name+'-'+ str(maf_num), 1 , \
                               message='Error delete MAF. rc = %ds'% str(rc) )

            self.ht.models=old_models 
            return False
        rc2 = ht.update_cf (Kerr , fun = 'maf_remove', maf_num_p=  maf_num)
       #time.sleep(0.1)
        if not rc2 :
            set_err_int (Kerr, Mod_name,   'delete '+self.ht.ht_name+'-'+ str(maf_num), 2 , \
                               message='Error delete MAF. rc = %ds'% str(rc) )
            return False
        rc3= True
        if hasattr(ht,"db_name"):
            rc3 = ht.update_RAM (fun = 'maf_remove', maf_num_p= maf_num)
        if not rc3:
            set_err_int (Kerr, Mod_name,   'delete '+self.ht.ht_name+'-'+ str(maf_num), 3 , \
                               message='Error delete MAF. rc = %ds'% str(rc) )
            return False

        ht.mafs [ maf_num]['name']= 'deleted:'+maf_name
        ht.mafs [ maf_num]['updated']= time.time()
       #time.sleep(0.1)
        if not ht.save_adt(Kerr):
            set_err_int (Kerr, Mod_name,   'delete '+ht.ht_name+'-'+ str(maf_num), 4, \
                               message='Error save HTD.' )

            self.ht.models=old_models 
            return False

        ht.updated = time.time()
        del   self
        return True

# -------------------------------------------------------------------------------------------

    def open(self, Kerr=[], mode=''):  

           # if self.ht.mafs[  self.maf_num][ 'opened' ]:
            #    return True 

            f_maf = posixpath.join( *(self.ht.ht_root.split(os.path.sep)+[self.ht.ht_name+'_'+str(self.maf_num)+self.ht.ext['maf']]))   
            rc = self.ht.cage.open( CAGE_SERVER_NAME,  f_maf, Kerr, mod = mode ) 
           #time.sleep(0.1)
            if rc == False:
                set_err_int (Kerr, Mod_name, 'open '+self.ht.ht_name+'-'+  str(self.maf_num), 1 , \
                                message='Error during opening MAF file %s .' % f_maf )
                return False
            else:
                self.ht.mafs[  self.maf_num].update( { 'opened': True })
                self.ht.mafs_opened [  self.maf_num ] = self
                self.ch =    rc 
                self.rowlen = 1
                for attr_num in  self.ht.attrs:
                    if self.ht.models [self.maf_num ] [ attr_num ] :
                        field_len = Types_htms.types [ self.ht.attrs [ attr_num ][ 'type' ] ][0]
                        self.offsets += ( (self.rowlen, field_len ),)             
                        self.rowlen += field_len
                    else:
                        self.offsets += ((-1,0),)  

                self.null_row = b'\x00'
                self.fields ={}

                if  len( self.ht.attrs ) >0: 
                    for natr in self.ht.attrs:
                        if self.ht.models [ self.maf_num] [ natr ] :

                            atr_type =  self.ht.attrs[ natr ][ 'type' ]

                            self.fields [  self.ht.attrs[ natr ][ 'name' ] ] = ( natr,  atr_type )   

                            if atr_type[ : 4] == 'byte':
                                self.null_row += Types_htms.types[ atr_type ][ 2 ]
                            elif atr_type[ : 3] == 'utf' or atr_type == 'datetime':
                                self.null_row += Types_htms.types[ atr_type ][ 2 ]
                            elif atr_type in ('int4', 'int8', 'float4', 'float8', 'time'):
                                self.null_row += struct.pack ( Types_htms.types[ atr_type ][ 3 ], Types_htms.types[ atr_type ][ 2 ] )
                            else: # element is address of data in AF (LB) or CF file (array of elementary data)
                                self.null_row += b'\xFF'* Types_htms.types[ atr_type ][ 0 ] # indicates null address

                return True

# -------------------------------------------------------------------------------------------

    def close(self, Kerr=[]):  

        if not self.ht.mafs[  self.maf_num][ 'opened' ]:
                return True 
                
        if 'objects_RAM' in dir(self):   
            for obj  in  self.get_RAM_instances():
                del obj
                
        self.removeinstances( self )

#        self.ht.cage.stat(Kerr)
   
        rc = self.ht.cage.close( self.ch, Kerr )  
       #time.sleep(0.1)
        if rc == False:
            set_err_int (Kerr, Mod_name, 'close '+self.ht.ht_name+'-'+  str(self.maf_num), 1 , \
                                message='Error during close Cage for MAF  %s .' % self.maf_name )
            return False
        else:
            self.ht.mafs[  self.maf_num][ 'opened' ] = False
            del self.ht.mafs_opened [  self.maf_num ]
            #self.ch = -1
            #self.offsets =((0,1),)  
            #self.fields = {}
            #del self.table_obj
            del self
            return True

# -------------------------------------------------------------------------------------------

    def wipe(self, Kerr =[]):


                if self.ht.mode in ('rs', 'rm' ):
                    set_err_int (Kerr, Mod_name, 'wipe '+self.ht.ht_name+'-'+  str(self.maf_num), 5 , \
                                message='Mode "%s" incompatible for wipe MAF for HT "%s".'
                        % (self.ht.mode, self.ht.ht_name)
                    )
                    return False

                old_mode=self.ht.cage.cage_ch[self.ch][2]      # = (server, kw, mod, path)

                self.close(Kerr)
               #time.sleep(0.1)
                self.ht.update_cf (Kerr , fun = 'maf_remove', maf_num_p= self.maf_num )
                rc3= True
               #time.sleep(0.1)
                if hasattr(self.ht,"db_name"):
                    self.ht.update_RAM (fun = 'maf_remove', maf_num_p= self.maf_num )
                self.ht.mafs[ self.maf_num ] [ 'rows' ] =0
                self.rows = 0
                self.rowlen = 1
                self.null_row = b'\x00'
                self.offsets = ((0,1),) 
                
                max_attr_num =  max (  self.ht.attrs.keys() )
                zero_model = tuple ( False  for attr_num in range ( 0, max_attr_num+1 ))
                new_models=((False,),)
                for nmaf in self.ht.mafs:
                    if nmaf != self.maf_num:
                        new_models +=( self.ht.models [ nmaf] , )
                    else:
                        new_models += ( zero_model , )
                self.ht.models = new_models
                del new_models
                del  zero_model

                self.fields = {}
                f_maf = posixpath.join( *(self.ht.ht_root.split(os.path.sep)+[self.ht.ht_name+'_'+str(self.maf_num)+self.ht.ext['maf']]))
                rc11 =  self.ht.cage.file_remove( CAGE_SERVER_NAME,  f_maf, Kerr)
               #time.sleep(0.1)
                if   rc11 == True:
                    rc12 = self.ht.cage.file_create( CAGE_SERVER_NAME,  f_maf, Kerr)
                   #time.sleep(0.1)
                if   rc11 == False or rc12 == False:
                    set_err_int (Kerr, Mod_name, 'wipe '+self.ht.ht_name+'-'+  str(self.maf_num), 1 , \
                                message='Error during deleting old and creating new MAF file %s .' % f_maf )
                    return False
      
                rc = self.ht.cage.open( CAGE_SERVER_NAME,  f_maf, Kerr, mod = old_mode ) 
               #time.sleep(0.1)
                self.ch =   rc      
                self.ht.mafs[self.maf_num][ 'opened' ] =  True 
                self.ht.mafs_opened [self.maf_num]  = self

                if not self.ht.save_adt(Kerr):
                    set_err_int (Kerr, Mod_name,   'wipe '+self.ht.ht_name+'-'+ str(self.maf_num), 2 , \
                                       message='Error save HTD.' )
                    return False
               #time.sleep(0.1)
                self.updated = time.time()
                self.ht.mafs [ self.maf_num]['updated']= self.updated
                self.ht.updated =self.updated 

                return True

# -------------------------------------------------------------------------------------------

    def field(self, Kerr=[], fun='add',  attr_name='', attr_num_f=0):

        if self.ht.mode in ('rs', 'rm' ):
            set_err_int (Kerr, Mod_name, 'field '+self.ht.ht_name+'-'+   str( self.maf_num ) , 29, \
                        message='Mode "%s" incompatible for fields modify in MAF for HT "%s".'
                    % (self.ht.mode, self.ht.ht_name)
            )
            return False

        old_mode=self.ht.cage.cage_ch[self.ch][2]
            
        if      type( attr_num_f ).__name__ != 'int' or \
                type( attr_name   ).__name__ != 'str':
            set_err_int (Kerr, Mod_name, 'field '+self.ht.ht_name+'-'+   str( self.maf_num ) , 1, \
                        message='Parameters type invalid.')
            return False

        if attr_num_f >0 and attr_num_f in self.ht.attrs:
            if attr_name != '' and self.ht.attrs[ attr_num_f ]['name' ] != attr_name:
                set_err_int (Kerr, Mod_name, 'field '+self.ht.ht_name+'-'+   str( self.maf_num ) , 2, \
                        message='Attribute names mismatch.')
                return False
            attr_name= self.ht.attrs[ attr_num_f ]['name' ]
        elif attr_num_f> 0:
            set_err_int (Kerr, Mod_name, 'field '+self.ht.ht_name+'-'+   str( self.maf_num ) , 3, \
                        message='Attribute number not found in HT list of attrs.')
            return False
        else:
            if  attr_name !='':
                found = False
                for natr in self.ht.attrs:
                    if attr_name == self.ht.attrs[ natr]['name']:
                        found = True
                        attr_num_f = natr
                        break
                if not found:
                    set_err_int (Kerr, Mod_name, 'field '+self.ht.ht_name+'-'+   str( self.maf_num ) , 4, \
                        message='Attribute "%s" not found in HT list of attrs.' % attr_name)
                    return False
            else:
                set_err_int (Kerr, Mod_name, 'field '+self.ht.ht_name+'-'+ str( self.maf_num), 5, \
                        message='Attribute name and number are null values.')
                return False

        if  self.ch == -1:
            set_warn_int (Kerr, Mod_name, 'field '+self.ht.ht_name+'-'+  str( self.maf_num ) ,  6, \
                        message='MAF closed. Need to open it' )
            return False

        atr_type =    self.ht.attrs[ attr_num_f][ 'type' ]
        atr_name =  self.ht.attrs[ attr_num_f][ 'name' ]

        if fun =='delete':

            num_atr =0
            for at in self.ht.models[ self.maf_num]:
                if at:
                    num_atr += 1
                
            if num_atr ==1 : # delete last field  -> delete all MAF rows
                self.wipe(Kerr)
                return True

        if fun in ('add', 'delete'):
            if self.rows >0:
                temp_maf = posixpath.join( *(self.ht.ht_root.split(os.path.sep)+[self.ht.ht_name+'_'+str(self.maf_num)+self.ht.ext['tmp']]))
                rc1 = self.ht.cage.file_create( CAGE_SERVER_NAME,  temp_maf, Kerr)
                if  rc1 == False:
                    set_err_int (Kerr, Mod_name, 'field '+self.ht.ht_name+'-'+  str(self.maf_num), 15 , \
                        message='Error during new TEMP MAF file %s creating.' % temp_maf )
                    return False  
                elif  rc1 == -1 or rc1 == -2:  # file already exist 
                        # delete old file because new HT creating
                    if len (Kerr) >0:  Kerr.pop()
                    if len (Kerr) >0:  Kerr.pop()
                    rc11 =  self.ht.cage.file_remove( CAGE_SERVER_NAME, temp_maf , Kerr)
                   #time.sleep(0.1)
                    if   rc11 == True:
                        rc12 = self.ht.cage.file_create( CAGE_SERVER_NAME,  temp_maf , Kerr)
                    if   rc11 == False or rc12 == False:
                        set_err_int (Kerr, Mod_name, 'field '+self.ht.ht_name+'-'+  str(self.maf_num), 16 , \
                            message='Error during new TEMP MAF file %s creating.' % temp_maf )
                        return False

                ch_temp = self.ht.cage.open( CAGE_SERVER_NAME, temp_maf, Kerr, ) 
               #time.sleep(0.1)
                if ch_temp == False:
                    set_err_int (Kerr, Mod_name, 'field '+self.ht.ht_name+'-'+ str( self.maf_num), 17, \
                        message='Error opening temp mafile.')
                    return False

            new_offsets =((0,1),)  

            pos0, pos1, pos2, pos3 = (0,1,1,1)
            shift = 1
            for natr  in  self.ht.attrs:
                field_len = Types_htms.types [ self.ht.attrs[ natr ]['type'] ] [ 0 ]
 
                if natr  < attr_num_f:
                    if self.ht.models [ self.maf_num ] [natr ] :
                        new_offsets += ( (pos1, field_len) , ) 
                        pos1 += field_len
                    else: 
                        new_offsets += ((-1,0),)

                elif natr  == attr_num_f:
                    if fun =='add':
                        if self.ht.models [ self.maf_num] [  attr_num_f ] :
                            set_warn_int (Kerr, Mod_name, 'field '+self.ht.ht_name+'-'+  str( self.maf_num ) ,  18 , \
                                    message='Attribute just bounded with MAF.' )
                            return False

                        extension= field_len

                        new_models=((False,),)
                        for nmaf in self.ht.mafs:
                            if nmaf != self.maf_num:
                                new_models +=( self.ht.models [ nmaf] , )
                            else:
                               new_models += ( self.ht.models [ nmaf][ : attr_num_f]+( True,) + self.ht.models [ nmaf][ attr_num_f +1 : ] , )
                        self.ht.models = new_models
                        del new_models

                        new_offsets +=  ( (pos1,extension),)
                        pos2, pos3 = (pos1, pos1)
                        shift = pos1 + extension
                        if  not hasattr(self, "fields"):
                            self.fields={}
                        self.fields [  self.ht.attrs[ attr_num_f ][ 'name' ] ] = ( attr_num_f ,   self.ht.attrs[  attr_num_f ][ 'type' ]  )   

                    else: #        fun =='delete'
                        if not self.ht.models [ self.maf_num] [ attr_num_f ] :
                            set_warn_int (Kerr, Mod_name, 'field '+self.ht.ht_name+'-'+  str( self.maf_num ) ,  19 , \
                                    message='Attribute not bounded with MAF.' )
                            return False
                        compression = field_len

                        new_models=((False,),)
                        for nmaf in self.ht.mafs:
                            if nmaf != self.maf_num:
                                new_models +=( self.ht.models [ nmaf] , )
                            else:
                               new_models += ( self.ht.models [ nmaf][ : attr_num_f]+( False,) + self.ht.models [ nmaf][ attr_num_f +1 : ] , )
                        self.ht.models = new_models
                        del new_models

                        new_offsets += ((-1,0),)
                        pos2 = pos1+ compression
                        pos3 = pos1+ compression
                        shift =  pos1
                        del self.fields [  self.ht.attrs[ attr_num_f ][ 'name' ] ] 

                else:    # natr > attr_num_f

                    if self.ht.models [ self.maf_num] [ natr ] :
                        new_offsets += ( (shift, field_len) , )
                        pos3 += field_len
                        shift += field_len
                    else: 
                         new_offsets += ((-1,0),)

            old_rowlen =   self.rowlen   

            if __debug__ and pos3 != old_rowlen:
                set_err_int (Kerr, Mod_name, 'field '+self.ht.ht_name+'-'+  str( self.maf_num ) ,  20 , \
                        message='HT attribute specified not exist.' )
                return False

            if fun =='add':
                self.rowlen = old_rowlen + extension
                if atr_type[ : 4] == 'byte':
                    ins = Types_htms.types[ atr_type ][ 2 ]
                elif atr_type[ : 3] == 'utf' or atr_type == 'datetime':
                    ins = Types_htms.types[ atr_type ][ 2 ]
                elif atr_type in ('int4', 'int8', 'float4', 'float8', 'time'):
                    ins = struct.pack ( Types_htms.types[ atr_type ][ 3 ], Types_htms.types[ atr_type ][ 2 ] )
                else: # element is address of data in AF (LB) or CF file (array of elementary data)
                    ins = b'\xFF'* Types_htms.types[ atr_type ][ 0 ] # indicates null address

                self.null_row= self.null_row [pos0:pos1] + ins + self.null_row[pos2:pos3]
            else:
                self.rowlen = old_rowlen - compression
                self.null_row= self.null_row [pos0:pos1] + self.null_row[pos2:pos3]

            self.offsets = new_offsets
            del new_offsets

            if self.rows >0:
                
                for r in range (self.rows):
                        row = self.ht.cage.read(self.ch,  r * old_rowlen, old_rowlen , Kerr)
                        if   row == False:
                            set_err_int (Kerr, Mod_name, 'field '+self.ht.ht_name+'-'+ str( self.maf_num),  21, \
                                message='Error read old mafile.')
                            return False
                        if fun =='add':
                            row = row [pos0:pos1] + ins + row[pos2:pos3]
                        else:  #        fun =='delete'
                            row = row [pos0:pos1] + row[pos2:pos3]

                        rc = self.ht.cage.write(ch_temp, r * self.rowlen, row, Kerr) 

                        #pr (' fields ---  attr_name = %s, row_len = %d  write row No= %d  rc = %s'%\
                                #(attr_name, len (row),   r,  str(rc ))  )

                if not self.ht.cage.close( ch_temp, Kerr) or not self.ht.cage.close( self.ch, Kerr): 
                    set_err_int (Kerr, Mod_name, 'field '+self.ht.ht_name+'-'+ str( self.maf_num), 22, \
                                    message='Error close temp or old mafile.')
                    return False
               #time.sleep(0.1) 
                old_maf =  posixpath.join( *(self.ht.ht_root.split(os.path.sep)+[self.ht.ht_name+'_'+str(self.maf_num)+self.ht.ext['bak_maf'] ] ) )

                kerr=[]
                rc0 = self.ht.cage.file_remove( CAGE_SERVER_NAME, old_maf, kerr)    
               #time.sleep(0.1)
                kerr=[]
                f_maf = posixpath.join( *(self.ht.ht_root.split(os.path.sep)+[self.ht.ht_name+'_'+str(self.maf_num)+self.ht.ext['maf']]))

                rc1 = self.ht.cage.file_rename( CAGE_SERVER_NAME, f_maf, old_maf, kerr)
               #time.sleep(0.1)
                if  rc1 == False or rc1 == -3:
                    set_err_int (Kerr, Mod_name, 'field '+self.ht.ht_name+'-'+ str( self.maf_num), 23, \
                                message='Error renaming old mafile.')
                    return False
                elif rc1 == -2:   #  -  try to delete old BAK_MAF
                    if len (Kerr) >0:  Kerr.pop()
                    if len (Kerr) >0:  Kerr.pop()
                    rc11 =  self.ht.cage.file_remove( CAGE_SERVER_NAME,  old_maf, kerr)
                   #time.sleep(0.1)
                    if   rc11 == True:
                        rc12 = self.ht.cage.file_rename( CAGE_SERVER_NAME, f_maf, old_maf, kerr)
                       #time.sleep(0.1)
                    if   rc11 == False or rc12 == False:
                        set_err_int (Kerr, Mod_name, 'field '+self.ht.ht_name+'-'+ str( self.maf_num), 24, \
                                message='Error renaming old mafile.')
                        return False

                kerr=[]
                if not self.ht.cage.file_rename( CAGE_SERVER_NAME, temp_maf, f_maf, Kerr):
                    set_err_int (Kerr, Mod_name, 'field '+self.ht.ht_name+'-'+ str( self.maf_num), 25, \
                        message='Error renaming new mafile.')
                    return False                
               #time.sleep(0.1)
                self.ch = self.ht.cage.open( CAGE_SERVER_NAME,  f_maf , Kerr, mod = old_mode ) 
               #time.sleep(0.1)
                if fun =='delete':
                    rc1 = self.ht.update_cf (Kerr , fun = 'field_remove', maf_num_p=self.maf_num,  attr_num_p = attr_num_f)

            self.updated= time.time()
            self.ht.mafs [ self.maf_num ][ 'updated' ]= self.updated        

            if not self.ht.save_adt(Kerr):
                set_err_int (Kerr, Mod_name,   'field '+self.ht.ht_name+'-'+ str(self.maf_num), 26 , \
                                   message='Error save HTD.' )
                return False

            return attr_num_f
        else:
            set_err_int (Kerr, Mod_name,  'field '+self.ht.ht_name+'-'+  str( self.maf_num ) , 27 , \
                        message='Invalid function.' )
            return False

        self.updated = time.time()
        self.ht.mafs [ self.maf_num]['updated']= self.updated
        self.ht.updated =self.updated 

        return True

# -------------------------------------------------------------------------------------------

    def row(self, Kerr = [] , fun='add',  after =-1, number=1, data= b''):

        if self.ht.mode in ('rs', 'rm' ) and fun !='read':
            set_err_int (Kerr, Mod_name, 'row '+self.ht.ht_name+'-'+ self.maf_name,  5 , \
                     message='Mode "%s" incompatible for rows modify in MAF for HT "%s".'
                    % (self.ht.mode, self.ht.ht_name)
            )
            return False

        if after == -1:
            after = self.rows
        if  len(self.ht.attrs) == 0:
            set_warn_int (Kerr, Mod_name, 'row '+self.ht.ht_name+'-'+ self.maf_name,  1 , \
                        message='no attributes in HT.' )
            return False
        if self.rowlen ==0:
            set_warn_int (Kerr, Mod_name, 'row '+self.ht.ht_name+'-'+ self.maf_name,  2 , \
                        message='no fields in MAF.' )
            return False
        if  after> self.rows:
            set_warn_int (Kerr, Mod_name, 'row '+self.ht.ht_name+'-'+ self.maf_name,  3 , \
                        message='Parameter "after" exceeds number of rows. Taken after = number of rows  ' )
            after = self.rows

        if fun =='add':
            if self.rowlen * (self.rows - after) < MAXSTRLEN1 and self.rowlen * number <  MAXSTRLEN1:
                if self.rows - after >0 :
                    moved_rows = self.ht.cage.read(self.ch, (after) *self.rowlen, self.rowlen * (self.rows - after) , Kerr)
                    rc1 = self.ht.cage.write(self.ch,  ( after + number ) *self.rowlen, moved_rows , Kerr)
                rc2 = self.ht.cage.write(self.ch,  ( after ) *self.rowlen, number * self.null_row  , Kerr)
            else:
                for r in range ( self.rows - 1, after -1, -1 ):
                        moved_row = self.ht.cage.read(self.ch, ( r  ) *self.rowlen, self.rowlen , Kerr)
                        rc = self.ht.cage.write(self.ch,  ( r + number ) *self.rowlen, moved_row , Kerr)
                        if not rc:
                            set_err_int (Kerr, Mod_name, 'row '+self.ht.ht_name+'-'+ self.maf_name,  4 , \
                                message='Error write to MAF. ' )
                            return False
                for r in range (after , after+number):
                        rc = self.ht.cage.write(self.ch,  ( r  ) *self.rowlen, self.null_row , Kerr)
                        if not rc:
                            set_err_int (Kerr, Mod_name, 'row '+self.ht.ht_name+'-'+ self.maf_name,  5 , \
                                message='Error write to MAF. ' )
                            return False

            self.rows += number
            self.ht.mafs[self.maf_num]['rows']= self.rows
            """
            tim0= time.time()/10.
            offset = self.offsets [ 2 ] [ 0 ]
            for n_row in range (after+1 , after+number+1):
                time_row = struct.pack (  '>d', tim0*10.)
                rc =  self.ht.cage.write( self.ch,  (n_row - 1) *self.rowlen+offset, time_row , Kerr)
                if rc == False:
                    set_err_int (Kerr, Mod_name, 'row '+self.ht.ht_name+'-'+ self.maf_name,  6 , \
                                message='Error write time stamp to MAF. ' )
                tim1= time.time()/10.
                while (tim1 == tim0):
                    tim1= time.time()/10.
                tim0= time.time()/10.
            """
            self.updated = time.time()     
            self.ht.mafs [ self.maf_num ][ 'updated' ]= self.updated                  
            if after< self.rows- number:           
                rc1 = self.ht.update_cf (Kerr , fun = 'row_add', maf_num_p=self.maf_num,  after_row = after, num_rows = number)
                rc2= True
                if hasattr(self.ht,"db_name"):
                    rc2=self.ht.update_RAM ( fun = 'row_add', maf_num_p=self.maf_num,  after_row = after, num_rows = number)
            """
            if not self.ht.save_adt(Kerr):
                set_err_int (Kerr, Mod_name,   'row '+self.ht.ht_name+'-'+ str(self.maf_num), 7 , \
                                   message='Error save HTD.' )
                return False
            """
            """
            if not self.ht.cage.put_pages ( self.ch, Kerr):
                set_err_int (Kerr, Mod_name,  'row '+self.ht.ht_name+'-'+ str(self.maf_num),  8, \
                            message='Error CF push modified pages.' )   
                return False
            """
            self.updated = time.time()
            self.ht.mafs [ self.maf_num]['updated']= self.updated
            self.ht.updated =self.updated 

            return True

        elif fun =='delete':
            if after +number >  self.rows:
                num =  self.rows - after
            else:
                num = number
            if num >0 :
                if  after +num <  self.rows:
                    if self.rowlen *  num < MAXSTRLEN1: 
                        moved_rows = self.ht.cage.read(self.ch, (after+num ) *self.rowlen, self.rowlen *  (self.rows - ( after + num) ) , Kerr)
                        rc = self.ht.cage.write(self.ch,  ( after) *self.rowlen, moved_rows , Kerr)
                    else:
                        for r in range (after+num, self.rows ):
                            moved_row = self.ht.cage.read(self.ch, ( r ) *self.rowlen, self.rowlen , Kerr)
                            rc = self.ht.cage.write(self.ch,  ( r - num ) *self.rowlen, moved_row , Kerr)
                            if not rc:
                                set_err_int (Kerr, Mod_name, 'row '+self.ht.ht_name+'-'+ self.maf_name,  9 , \
                                    message='Error write to MAF. ' )
                                return False
                self.rows -= num

            self.ht.mafs[self.maf_num]['rows']= self.rows

            self.updated = time.time()     
            self.ht.mafs [ self.maf_num ][ 'updated' ]= self.updated                             
                            
            if num> 0:
                if not self.ht.update_cf (Kerr , fun = 'row_delete', maf_num_p=self.maf_num,  after_row = after , num_rows = num) :
                    set_err_int (Kerr, Mod_name, 'row '+self.ht.ht_name+'-'+ self.maf_name,  10, \
                                    message='Error update links in CF. ' )
                    return False
                if hasattr(self.ht,"db_name"):
                    if not self.ht.update_RAM (fun = 'row_delete', maf_num_p=self.maf_num,  after_row = after , num_rows = num):
                        set_err_int (Kerr, Mod_name, 'row '+self.ht.ht_name+'-'+ self.maf_name,  10, \
                                    message='Error update links in RAM objects. ' )
                        return False
            """
            if not self.ht.save_adt(Kerr):
                set_err_int (Kerr, Mod_name,   'row '+self.ht.ht_name+'-'+ str(self.maf_num), 11 , \
                                   message='Error save HTD.' )
                return False
            """
            """
            if not self.ht.cage.put_pages ( self.ch, Kerr):
                set_err_int (Kerr, Mod_name,  'row '+self.ht.ht_name+'-'+ str(self.maf_num),  12, \
                            message='Error CF push modified pages.' )   
                return False
            """
            self.updated = time.time()
            self.ht.mafs [ self.maf_num]['updated']= self.updated
            self.ht.updated =self.updated 

            return True

        elif fun =='read':
            if after +number >  self.rows:
                num =  self.rows - after
            else:
                num = number

            readed_rows = b''
            if num >0 :
                for r in range (after, after+num):
                    rc  = self.ht.cage.read(self.ch, ( r  ) *self.rowlen, self.rowlen , Kerr)
                    if rc == False:
                        set_err_int (Kerr, Mod_name, 'row '+self.ht.ht_name+'-'+ self.maf_name,  13 , \
                                message='Error read from MAF. ' )
                        return False
                    readed_rows += self.ht.cage.read(self.ch, ( r  ) *self.rowlen, self.rowlen , Kerr)

            return readed_rows
           
        elif fun =='write':
            if after +number >  self.rows:
                num =  self.rows - after
            else:
                num = number
            c =0
            if num >0 :
                for r in range (after,  after+num):
                    row_to_write= data[c: c*self.rowlen]
                    rc  = self.ht.cage.write(self.ch, ( r ) *self.rowlen, row_to_write , Kerr)
                    if rc == False:
                        set_err_int (Kerr, Mod_name, 'row '+self.ht.ht_name+'-'+ self.maf_name,  14 , \
                                message='Error write to MAF. ' )
                        return False
                    c +=1
                self.updated = time.time()     
                self.ht.mafs [ self.maf_num ][ 'updated' ]= self.updated           
            """
            if not self.ht.cage.put_pages ( self.ch, Kerr):
                set_err_int (Kerr, Mod_name,  'row '+self.ht.ht_name+'-'+ str(self.maf_num),  16, \
                            message='Error CF push modified pages.' )   
                return False
            """
            self.updated = time.time()
            self.ht.mafs [ self.maf_num]['updated']= self.updated
            self.ht.updated =self.updated 
        
            return True

        else:
            set_err_int (Kerr, Mod_name,   'row '+self.ht.ht_name+'-'+ self.maf_name,  17 , \
                        message='Invalid function.' )
            return False

# -------------------------------------------------------------------------------------------


    def w_links (self, Kerr = [] , attr_num=0, num_row=0, links =set(), rollback=False):
                    # link =(nmaf, 0) - set row =0 - indicate link to whole maf

        if self.ht.mode in ('rs', 'rm' ): 
            set_err_int (Kerr, Mod_name,  'w_links '+self.ht.ht_name+'-'+ str(self.maf_num),  1 , \
                    message='Mode "%s" incompatible for links modify in MAF for HT "%s".'
                    % (self.ht.mode, self.ht.ht_name)
            )
            return False

        if num_row > self.rows:
            set_err_int (Kerr, Mod_name,  'w_links '+self.ht.ht_name+'-'+ str(self.maf_num),  1 , \
                    message='num_row parameter over the  number of MAF rows.' )
            return False
        if self.attr_type( attr_num ) != "*link":
            set_err_int (Kerr, Mod_name,  'w_links '+self.ht.ht_name+'-'+ str(self.maf_num),  2 , \
                    message='attr_num parameter not corresponds with field data.' )
            return False

        offset , length = self.offsets [attr_num] 

        if  links  == set():
            new_cf_link = b'\xFF'* 16 # indicator of "null" set of links 
        else:
            links_list = list (links)
            new_dim = 0
            new_link_block= b''

            for a in range (0, len ( links_list ) ):
                if not ( links_list [a][ 0 ]   in self.ht.mafs  and self.ht.mafs [  links_list [a][ 0 ] ]['name'][ : 8] != 'deleted:') :
                    set_err_int (Kerr, Mod_name,  'w_links '+self.ht.ht_name+'-'+  str( self.maf_num ) ,  5 , \
                        message='Links contains link to not existed MAF no. = %d' %  links_list [a][ 0 ] ) 
                    return False
                if   links_list [a][ 1 ] < 0 or   links_list [a][ 1 ] > self.ht.mafs [ links_list [a][ 0 ]  ]['rows'] :
                    set_err_int (Kerr, Mod_name,  'w_links '+self.ht.ht_name+'-'+  str( self.maf_num ) ,  6 , \
                        message='Links contains link to not existed row  %d  in MAF no. = %d' % ( links_list [a][1], links_list [a][ 0 ] )  )
                    return False

                new_dim += 1
                try:
                     new_link_block += struct.pack('>Li',  links_list [a][ 0 ],  links_list [a][ 1 ] )
                        #  links_list [a][ 1 ] (num_row) == 0 - indicator of link "to all rows, ie to a whole MAF" 
                except struct.error as err:
                    set_err_int (Kerr, Mod_name, 'w_links '+self.ht.ht_name+'-'+ str(self.maf_num),  7 , \
                        message='cf pack links error = %s.'% err )
                    return False      

            len_new_cf_block = 16+ new_dim*8 + 8
            overwrite = False               # write block on new place

            old_cf_link =  self.ht.cage.read( self.ch, (num_row-1)*self.rowlen+offset, 16 , Kerr)   
            if old_cf_link == False:
                set_err_int (Kerr, Mod_name, 'w_links '+self.ht.ht_name+'-'+ self.maf_name,  3 , \
                            message='Error read CF block address from MAF. ' )
                return False
            if old_cf_link != b'\xFF'*16:
                old_cf_addr, len_old_cf_block =   struct.unpack( '>QQ', old_cf_link )
            else:
                old_cf_addr = -1

            if old_cf_addr != -1  and  not rollback:
                if   len_new_cf_block <= len_old_cf_block :
                    overwrite = True        # write block on old place (with overlap)
            try:
                descr_links= struct.pack('>LLLL', new_dim,  self.maf_num, attr_num, num_row )
            except struct.error as err:
                set_err_int (Kerr, Mod_name,  'w_links '+self.ht.ht_name+'-'+ str(self.maf_num),  4 , \
                    message='cf pack descriptor error = %s.'% err )
                return False      

            new_cf_block = descr_links + new_link_block + b'\xFF'*8  # end marker
           
            if overwrite:
                rc1 =  self.ht.cage.write( self.ht.channels [ 'cf' ],  old_cf_addr,   (new_cf_block + (len_old_cf_block - len_new_cf_block) * b'\xFF'  ), Kerr)   
                if rc1 == False:
                    set_err_int (Kerr, Mod_name, 'w_links '+self.ht.ht_name+'-'+ str(self.maf_num),  8 , \
                        message='Error 1 write CF.' )
                    return False      
                new_cf_link =    struct.pack ('>QQ',  old_cf_addr,   len_new_cf_block )
            else:
                # null maf to old links descriptor
                if old_cf_addr != -1  :
                    maf_descr_zero = struct.pack('>L', 0 )
                    #pr('  W LINK Zero Descr    maf %d -->0,    atr=%d,     row=%d ' % \
                                    #(  self.maf_num, attr_num, num_row) )   
                    rc0 =  self.ht.cage.write( self.ht.channels [ 'cf' ],  old_cf_addr+4, maf_descr_zero,  Kerr)   
                    if rc0 == False:
                        set_err_int (Kerr, Mod_name, 'w_links '+self.ht.ht_name+'-'+ str(self.maf_num),  8 , \
                            message='Error 2 write CF.' )
                        return False      

                rc1 =  self.ht.cage.write( self.ht.channels [ 'cf' ],  self.ht.c_free,  new_cf_block , Kerr)   
                if rc1 == False:
                    set_err_int (Kerr, Mod_name, 'w_links '+self.ht.ht_name+'-'+ str(self.maf_num),  9 , \
                        message='Error write CF.' )
                    return False      
                new_cf_link =                        struct.pack ('>QQ',  self.ht.c_free,  len_new_cf_block )

                self.ht.c_free += len (new_cf_block)     
                """
                if not self.ht.save_adt(Kerr):
                    set_err_int (Kerr, Mod_name,   'w_links '+self.ht.ht_name+'-'+ str(self.maf_num),  10 , \
                                    message='Error save HTD.' )
                    return False
                """
        """
        if not self.ht.cage.put_pages ( self.ht.channels [ 'cf' ], Kerr):
            set_err_int (Kerr, Mod_name,  'w_links '+self.ht.ht_name+'-'+ str(self.maf_num),  11, \
                        message='Error CF push modified pages.' )   
            return False                        
        """
        rc2 =  self.ht.cage.write( self.ch,   (num_row-1)*self.rowlen+offset,   new_cf_link , Kerr)   
        if rc2 == False:
            set_err_int (Kerr, Mod_name, 'w_links '+self.ht.ht_name+'-'+ str(self.maf_num),  12 , \
                        message='Error write MAF.' )
            return False
        """    
        if not self.ht.cage.put_pages ( self.ch, Kerr):
            set_err_int (Kerr, Mod_name,  'w_links '+self.ht.ht_name+'-'+ str(self.maf_num),  13, \
                        message='Error MAF push modified pages.' )
            return False
        """
        self.updated = time.time()
        self.ht.mafs [ self.maf_num]['updated']= self.updated
        self.ht.updated =self.updated 

        return True
      

# -------------------------------------------------------------------------------------------

    def r_links (self, Kerr = [] , attr_num=0 , num_row=0 ):

        if num_row > self.rows:
            set_err_int (Kerr, Mod_name,  'r_links '+self.ht.ht_name+'-'+ str(self.maf_num),  1 , \
                    message='num_row parameter over the  number of MAF rows.' )
            return False
        if self.attr_type( attr_num ) != "*link":
            set_err_int (Kerr, Mod_name,  'r_links '+self.ht.ht_name+'-'+ str(self.maf_num),  2 , \
                    message='attr_num parameter not corresponds with field data.' )
            return False

        links=()
        offset , length = self.offsets [attr_num] 
        cf_link =  self.ht.cage.read( self.ch, (num_row - 1)*self.rowlen+offset, 16 , Kerr)   
        if cf_link == False:
            set_err_int (Kerr, Mod_name,   'r_links '+self.ht.ht_name+'-'+ str(self.maf_num),  3, \
                        message='Error read CF block address in MAF.' )
            return False
        elif cf_link ==  b'\xFF'* 16:
            return  links

        cf_addr, len_cf_block =   struct.unpack( '>QQ', cf_link )

        if len_cf_block < 16  or len_cf_block > self.ht.c_free   or  cf_addr > self.ht.c_free -16 or  cf_addr < 0 :
            set_err_int (Kerr, Mod_name,   'r_links '+self.ht.ht_name+'-'+ str(self.maf_num),  4, \
                        message='Information about CF block is corrupted in MAF. ' )
            return False

        cf_block =  self.ht.cage.read( self.ht.channels [ 'cf' ], cf_addr, len_cf_block , Kerr)
        if cf_block == False:
            set_err_int (Kerr, Mod_name,   'r_links '+self.ht.ht_name+'-'+ str(self.maf_num),  5, \
                        message='Error read CF block.' )
            return False  

        old_dim, maf_num, natr, nrow = struct.unpack ('>LLLL', cf_block[ 0 : 16])

        if old_dim == 0 or maf_num == 0:
            cf_link = b'\xFF'* 16 # indicator of "null" set of links 
            rc2 =  self.ht.cage.write( self.ch, (num_row-1)*self.rowlen+offset, cf_link , Kerr) 
            if rc2 == False:
                set_err_int (Kerr, Mod_name,   'r_links '+self.ht.ht_name+'-'+ str(self.maf_num),  6, \
                            message='Error write null CF block address in MAF.' )
                return False 
            return  links

        if  old_dim != ( len_cf_block  - 16 - 8 ) /8: # need to correct length, 
                        #  because update cf change dimension of links array
            new_len_cf_block = 16 + 8+ old_dim*8 
            new_cf_link = struct.pack ('>QQ', cf_addr,  new_len_cf_block )
            rc3 =  self.ht.cage.write( self.ch,   (num_row-1)*self.rowlen+offset,   new_cf_link , Kerr)
            if rc3 == False:
                set_err_int (Kerr, Mod_name, 'r_links '+self.ht.ht_name+'-'+ str(self.maf_num),  7 , \
                        message='Error write MAF.' )
                return False      

        if  maf_num !=  self.maf_num   or \
            natr != attr_num or \
            nrow != num_row:
            set_err_int (Kerr, Mod_name,  'r_links '+self.ht.ht_name+'-'+  str( self.maf_num ) ,  8 , \
                        message='Data corruption in C file.' )
            return False

        lena = Types_htms.types[ "*link" ][ 1 ]
        for a in range(old_dim):
            link = struct.unpack('>Li', cf_block [16+a*lena*2: 16+(a+1)*lena*2 ] )
            if link[ 0 ] != 0:
                if not ( link[ 0 ]  in self.ht.mafs  and self.ht.mafs [ link[ 0 ] ]['name'][ : 8] != 'deleted:') :
                    set_err_int (Kerr, Mod_name,  'r_links '+self.ht.ht_name+'-'+  str( self.maf_num ) ,  9 , \
                        message='Data corruption in C file. Links contains link to not existed MAF no. = %d' % link[ 0 ] ) 
                    return False
                elif  link[ 1 ] < 0 or  link[ 1 ] > self.ht.mafs [ link[ 0 ] ]['rows'] :
                    set_err_int (Kerr, Mod_name,  'r_links '+self.ht.ht_name+'-'+  str( self.maf_num ) ,  10 , \
                        message='Data corruption in C file. Links contains link to not existed row  %d  in MAF no. = %d' % (link[1], link[ 0 ] ) )
                    return False

                links += (link,)

        # delete duplicates

        # delete links to deleted rows and mafs

        return links


# -------------------------------------------------------------------------------------------

    def u_links (self, Kerr = [] , attr_num=0, num_row=0,  u_link =(), rollback=False):     
                    # u_link = () - clear all element   
                    # u_link =(nmaf, 0) - set row =0 - indicate link to whole maf
                    # u_link =(-nmaf, *) - delete all links to maf
                    # u_link =(nmaf,-num_row,) - delete one link to maf , to  num_row
                    # u_link =(nmaf,num_row,) -  add  link to maf - num_row, if not exist

        if self.ht.mode in ('rs', 'rm' ) :
            set_err_int (Kerr, Mod_name,  'u_links '+self.ht.ht_name+'-'+ str(self.maf_num),  1 , \
                    message='Mode "%s" incompatible for links modify in MAF for HT "%s".'
                    % (self.ht.mode, self.ht.ht_name)
            )
            return False

        if num_row > self.rows:
            set_err_int (Kerr, Mod_name,  'u_links '+self.ht.ht_name+'-'+ str(self.maf_num),  1 , \
                    message='num_row parameter over the  number of MAF rows.' )
            return False
        if self.attr_type( attr_num ) != "*link":
            set_err_int (Kerr, Mod_name,  'u_links '+self.ht.ht_name+'-'+ str(self.maf_num),  2 , \
                    message='attr_num parameter not corresponds with field data.' )
            return False

        offset , length = self.offsets [attr_num] 
        #pr ( str (abs( u_link[ 1 ] ) ) +'??'+ str (self.ht.mafs [ u_link[ 0 ] ]['rows'] ) )

        if  u_link == ():
            new_cf_link = b'\xFF' *16
            rc =  self.ht.cage.write( self.ch,  ( num_row - 1 )*self.rowlen+offset, new_cf_link , Kerr)   
            if rc == False:
                set_err_int (Kerr, Mod_name, 'u_links '+self.ht.ht_name+'-'+ self.maf_name,  3 , \
                                        message='Error write to MAF. ' )
                return False

            self.updated = time.time()
            self.ht.mafs [ self.maf_num]['updated']= self.updated
            self.ht.updated =self.updated 

            return True

        elif u_link != ():
            if not ( abs (u_link[ 0 ] )   in self.ht.mafs  and self.ht.mafs [ abs (u_link[ 0 ] ) ]['name'][ : 8] != 'deleted:') :
                set_err_int (Kerr, Mod_name,  'u_links '+self.ht.ht_name+'-'+  str( self.maf_num ) ,  4, \
                        message='Link to not existed MAF no. = %d' % u_link[ 0 ] ) 
                return False            
            elif  u_link[ 1 ] !=0  and abs( u_link[ 1 ] ) > self.ht.mafs [ u_link[ 0 ] ]['rows'] :
                set_err_int (Kerr, Mod_name,  'u_links '+self.ht.ht_name+'-'+  str( self.maf_num ) ,  5 , \
                        message='Link to not existed row  %d  in MAF no. = %d' % ( abs (u_link[1] ), u_link[ 0 ] ) )
                return False

        new_cf_links=()

        cf_link =  self.ht.cage.read( self.ch,  ( num_row - 1 )*self.rowlen+offset, 16 , Kerr)   
        if cf_link == False:
            set_err_int (Kerr, Mod_name, 'u_links '+self.ht.ht_name+'-'+ self.maf_name,  6 , \
                                message='Error read from MAF. ' )
            return False

        if cf_link == b'\xFF'*16:
            if  u_link[ 0 ]  < 0 or u_link [ 1 ] <0:
                set_warn_int (Kerr, Mod_name,   'u_links '+self.ht.ht_name+'-'+ str(self.maf_num),  7 , \
                                message='Address of the set ot the links in MAF empty.' )
                return True
            # create new set of links
            rc = self.w_links (Kerr, attr_num,  num_row , links = (u_link,) )
            if rc == False:
                set_err_int (Kerr, Mod_name, 'u_links '+self.ht.ht_name+'-'+ self.maf_name,  8 , \
                                        message='Error in w_links. ' )
                return False
            """
            if not self.ht.cage.put_pages ( self.ch, Kerr):
                set_err_int (Kerr, Mod_name,   'u_links '+self.ht.ht_name+'-'+ str(self.maf_num),  9 , \
                                message='Error MAF push modified page ' )
                return False
            """
            self.updated = time.time()
            self.ht.mafs [ self.maf_num]['updated']= self.updated
            self.ht.updated =self.updated 

            return True

        cf_addr, len_cf_block =   struct.unpack( '>QQ', cf_link )

        cf_block =  self.ht.cage.read( self.ht.channels [ 'cf' ], cf_addr, len_cf_block , Kerr)
        if cf_block == False:
            set_err_int (Kerr, Mod_name, 'u_links '+self.ht.ht_name+'-'+ self.maf_name,  10 , \
                                message='Error read from CF. ' )
            return False  

        old_dim, maf_num, natr, nrow = struct.unpack ('>LLLL', cf_block[ 0 : 16])

        if old_dim == 0 or maf_num == 0:
            if u_link[ 0 ]  < 0 or u_link [ 1 ] < 0 :            
                set_err_int (Kerr, Mod_name,   'u_links '+self.ht.ht_name+'-'+ str(self.maf_num),  11 , \
                                message='Set ot the links in MAF empty.' )
                zero_link = b'\xFF'* 16 # indicator of "null" set of links 
                rc2 =  self.ht.cage.write( self.ch,  ( num_row - 1 )*self.rowlen+offset, zero_link , Kerr) 
                if rc2 == False:
                    set_err_int (Kerr, Mod_name,    'u_links '+self.ht.ht_name+'-'+ str(self.maf_num),  12 , \
                                message='Error write null CF block address in MAF.' )
                return False 
            else:
                rc = self.w_links (self, Kerr, attr_num,   num_row , links = (u_link,) )
                if rc == False:
                    set_err_int (Kerr, Mod_name, 'u_links '+self.ht.ht_name+'-'+ self.maf_name,  13 , \
                                            message='Error in w_links. ' )
                    return False
                """
                if not self.ht.cage.put_pages ( self.ch, Kerr):
                    set_err_int (Kerr, Mod_name,   'u_links '+self.ht.ht_name+'-'+ str(self.maf_num),  14 , \
                                    message='Error MAF push modified page ' )
                    return False
                """
                self.updated = time.time()
                self.ht.mafs [ self.maf_num]['updated']= self.updated
                self.ht.updated =self.updated 

                return True

        if  old_dim != ( len_cf_block  - 16 - 8 ) /8: # need to correct length, 
                        #  because update cf change dimension of links array
            new_len_cf_block = 16 + 8+ old_dim*8 
            new_cf_link = struct.pack ('>QQ', cf_addr,  new_len_cf_block )
            rc3 =  self.ht.cage.write( self.ch,   (num_row-1)*self.rowlen+offset,   new_cf_link , Kerr)
            if rc3 == False:
                set_err_int (Kerr, Mod_name, 'u_links '+self.ht.ht_name+'-'+ str(self.maf_num),  15 , \
                        message='Error write MAF.' )
                return False      

        if  maf_num != self.maf_num or \
            attr_num != natr or \
            nrow != num_row:
            set_err_int (Kerr, Mod_name,  'u_links '+self.ht.ht_name+'-'+ str(self.maf_num),  16, \
                        message='Data corruption in C file.' )
            return False 

        change = False
        execute = False
        new_cf_links = b''
                
        if u_link[ 0 ]< 0:
            maf_to_delete = - u_link[ 0 ]
        else:
            maf_to_delete = - 1

        lena = Types_htms.types[ "*link" ][ 1 ]
        for a in range(old_dim):
            lnk = struct.unpack('>Li', cf_block [16+a*lena*2: 16+(a+1)*lena*2 ] )

            if lnk [ 0 ] == 0: # found null link - miss it
                change =  True
                continue
            else:
                if not ( lnk[ 0 ]  in self.ht.mafs  and self.ht.mafs [ lnk[ 0 ] ]['name'][ : 8] != 'deleted:') :
                    set_err_int (Kerr, Mod_name,  'u_links '+self.ht.ht_name+'-'+  str( self.maf_num ) ,  17, \
                        message='Data corruption in C file. Links contains link to not existed MAF no. = %d' % lnk[ 0 ] ) 
                    return False
                elif  lnk[ 1 ] < 0 or  lnk[ 1 ] > self.ht.mafs [ lnk[ 0 ] ]['rows'] :
                    set_err_int (Kerr, Mod_name,  'r_links '+self.ht.ht_name+'-'+  str( self.maf_num ) ,  18, \
                        message='Data corruption in C file. Links contains link to not existed row  %d  in MAF no. = %d' % (lnk[1], lnk[ 0 ] ) )
                    return False
                elif  lnk [ 0 ] == maf_to_delete : # delete link
                    execute = True
                    change =  True
                    continue
                elif  lnk [ 0 ] != u_link [ 0 ] : # not match - repeat link
                    new_cf_links += cf_block [16+a*lena*2: 16+ (a+1)*lena*2 ] 
                    continue
                # match  -  lnk [ 0 ] == u_link [ 0 ]
                elif  lnk[ 1 ] ==  u_link [ 1 ] :  #already exist - repeat link
                    execute = True
                    new_cf_links += cf_block [16+a*lena*2: 16+ (a+1)*lena*2 ] 
                    continue
                elif u_link [ 1 ] ==0  : # need to set row num to 0 - miss old link
                    change =  True
                    continue
                elif lnk[ 1 ]== - u_link [ 1 ]  : # need to delete this link - miss it
                    execute = True
                    change =  True
                    continue
                else :    # need to add new link - repeat old link
                    new_cf_links += cf_block [16+a*lena*2: 16+ (a+1)*lena*2 ] 
                    continue
       
        if not execute and u_link [ 1 ] >= 0 and maf_to_delete == -1 :
            new_cf_links += struct.pack( '>Li', u_link[0], u_link[1])    
            change =  True

        if change:

            new_dim = int (len (new_cf_links)/8 )

            if new_dim == 0:
                new_cf_link = b'\xFF' *16
                rc =  self.ht.cage.write( self.ch,  ( num_row - 1 )*self.rowlen+offset, new_cf_link , Kerr)   
                if rc == False:
                    set_err_int (Kerr, Mod_name, 'u_links '+self.ht.ht_name+'-'+ self.maf_name,  19 , \
                                            message='Error write to MAF. ' )
                    return False
                """
                if not self.ht.cage.put_pages ( self.ch, Kerr):
                    set_err_int (Kerr, Mod_name,   'u_links '+self.ht.ht_name+'-'+ str(self.maf_num),  20 , \
                                    message='Error MAF push modified page ' )
                    return False
                """
                self.updated = time.time()
                self.ht.mafs [ self.maf_num]['updated']= self.updated
                self.ht.updated =self.updated 

                return True

            new_cf_descr = struct.pack ('>LLLL', new_dim, maf_num, attr_num, nrow )
            new_cf_block = new_cf_descr + new_cf_links +b'\xFF'*8
            len_new_cf_block =  len (new_cf_block)

            if new_dim <= old_dim and not rollback: # write block on old place (with overlap)
                rc1 =  self.ht.cage.write( self.ht.channels [ 'cf' ],  cf_addr, (new_cf_block +  (old_dim-new_dim) * b'\xFF' *8 ), Kerr )
                if rc1 ==False:
                    set_err_int (Kerr, Mod_name,  'u_link '+self.ht.ht_name+'-'+ str(self.maf_num),  21, \
                            message='Error 1 write to CF' )
                    return False
                new_cf_link =   struct.pack ('>QQ',   cf_addr,  len_new_cf_block)
            else:   # write block on new place
                # null to dim in old links descriptor
                maf_descr_zero = struct.pack('>L', 0 )

                #pr('  U LINK Zero Descr    maf %d -->0,    atr=%d,     row=%d ' % \
                                  #  (  maf_num, natr, nrow) )   

                rc0 =  self.ht.cage.write( self.ht.channels [ 'cf' ],  cf_addr+4, maf_descr_zero,  Kerr)   
                if rc0 == False:
                    set_err_int (Kerr, Mod_name,  'u_link '+self.ht.ht_name+'-'+ str(self.maf_num),  22, \
                            message='Error 2 write to CF' )
                    return False      
                rc1 =  self.ht.cage.write( self.ht.channels [ 'cf' ],  self.ht.c_free,  new_cf_block , Kerr )
                if rc1 ==False:
                    set_err_int (Kerr, Mod_name,  'u_link '+self.ht.ht_name+'-'+ str(self.maf_num),  23, \
                            message='Error write to CF' )
                    return False
                new_cf_link =     struct.pack ('>QQ',  self.ht.c_free,  len_new_cf_block)

                self.ht.c_free += len_new_cf_block
                """
                if not self.ht.save_adt(Kerr):
                    set_err_int (Kerr, Mod_name,   'u_links '+self.ht.ht_name+'-'+ str(self.maf_num),  24 , \
                                    message='Error save HTD.' )
                    return False
                """
            """
            if not self.ht.cage.put_pages ( self.ht.channels [ 'cf' ], Kerr):
                set_err_int (Kerr, Mod_name,   'u_links '+self.ht.ht_name+'-'+ str(self.maf_num),  25 , \
                                message='Error CF push modified page ' )
                return False
            """
            rc2 =  self.ht.cage.write( self.ch,  ( num_row - 1 )*self.rowlen+offset, new_cf_link , Kerr)   
            if rc2 ==False:
                set_err_int (Kerr, Mod_name,  'u_link '+self.ht.ht_name+'-'+ str(self.maf_num),  26, \
                            message='Error write to MAF.' )
                return False
            """
            if not self.ht.cage.put_pages ( self.ch, Kerr):
                set_err_int (Kerr, Mod_name,   'u_links '+self.ht.ht_name+'-'+ str(self.maf_num),  27 , \
                                message='Error MAF push modified page ' )
                return False
            """
        self.updated = time.time()
        self.ht.mafs [ self.maf_num]['updated']= self.updated
        self.ht.updated =self.updated 

        return True

# -------------------------------------------------------------------------------------------

    def w_elem (self, Kerr = [] , attr_num=0, num_row =0, elem =b''):

        if self.ht.mode in ('rs', 'rm' ):
            set_err_int (Kerr, Mod_name,  'w_elem '+self.ht.ht_name+'-'+ str(self.maf_num),  5 , \
                    message='Mode "%s" incompatible for elem modify - w_elem in MAF for HT "%s".'
                    % (self.ht.mode, self.ht.ht_name)
            )
            return False

        if elem == None:
             elem =b''
        if num_row > self.rows:
            set_err_int (Kerr, Mod_name,  'w_elem '+self.ht.ht_name+'-'+ str(self.maf_num),  1 , \
                    message='num_row parameter over the  number of MAF rows.' )
            return False
        offset , length = self.offsets [attr_num] 
        len_elem = len ( elem)
        if len_elem > length:
            set_err_int (Kerr, Mod_name,   'w_elem '+self.ht.ht_name+'-'+ self.maf_name,  2 , \
                        message='Length of data dismatch with attribute data type.' )
            return False
        elem = elem + b'\x00' * (length - len_elem)
        rc =  self.ht.cage.write( self.ch,  ( num_row - 1 )  *self.rowlen+offset, elem , Kerr)   
        if rc == False:
            set_err_int (Kerr, Mod_name, 'w_elem '+self.ht.ht_name+'-'+ self.maf_name,  3 , \
                                message='Error write MAF. ' )
            return False
        """
        if not self.ht.cage.put_pages ( self.ch, Kerr):
            set_err_int (Kerr, Mod_name,  'w_elem '+self.ht.ht_name+'-'+ str(self.maf_num),  4, \
                        message='Error MAF push modified pages.' )
            return False
        """
        self.updated = time.time()
        self.ht.mafs [ self.maf_num]['updated']= self.updated
        self.ht.updated =self.updated 

        return True

# -------------------------------------------------------------------------------------------

    def r_elem (self, Kerr = [] ,  attr_num=0, num_row = 0 ):

        if num_row > self.rows:
            set_err_int (Kerr, Mod_name,  'r_elem '+self.ht.ht_name+'-'+ str(self.maf_num),  1 , \
                    message='num_row parameter over the  number of MAF rows.' )
            return False

        offset , length = self.offsets [attr_num] 
        rc =  self.ht.cage.read( self.ch,  ( num_row - 1 )  *self.rowlen+offset, length , Kerr)
        if rc == False:
            set_err_int (Kerr, Mod_name, 'r_elem '+self.ht.ht_name+'-'+ self.maf_name,  2 , \
                                message='Error read from MAF. ' )
            return False

        return rc 

# -------------------------------------------------------------------------------------------

    def r_utf8 (self, Kerr = [] , attr_num=0 , num_row =0 ):

        if num_row > self.rows:
            set_err_int (Kerr, Mod_name,  'w_links '+self.ht.ht_name+'-'+ str(self.maf_num),  1 , \
                    message='num_row parameter over the  number of MAF rows.' )
            return False
        if  not (self.ht.attrs [ attr_num ] ['type'][ : 3 ]  in ( 'utf', 'dat') ):
            set_err_int (Kerr, Mod_name,   'r_utf8 '+self.ht.ht_name+'-'+ self.maf_name,  2 , \
                        message='Incorrect attribute type for read string.' )
            return False
        offset , length = self.offsets [attr_num] 

        print('\n *** CAGE READ channel=%d,  from=%d , length=%d  ***'% 
            (self.ch,  ( num_row - 1 )*self.rowlen+offset, length )
            )

        rc =  self.ht.cage.read( self.ch,  ( num_row - 1 )  *self.rowlen+offset, length , Kerr)

        if rc:
            print('\n *** maf_name=%s,  attr_num=%d , num_row=%d  ***\n%s'% 
            (self.maf_name, attr_num, num_row, rc.decode( "utf-8", "ignore"))
            )
        else:
            print('\n *** maf_name=%s,  attr_num=%d , num_row=%d  ***\n!!!!!!!!!!NONE!!!!!!!!!!!!!!'% 
            (self.maf_name, attr_num, num_row)
            )

        if rc == False:
            set_err_int (Kerr, Mod_name, 'r_utf8 '+self.ht.ht_name+'-'+ self.maf_name,  3 , \
                                message='Error read from MAF. ' )
            return False
        eos = rc.find(b'\xFF')
        if eos == 0:
            utf = ''
        elif eos >0:
            str = rc[ : eos]
            utf = str.decode( "utf-8", "ignore")
        else:
            str = rc 
            utf = str.decode( "utf-8", "ignore")
        return utf

# -------------------------------------------------------------------------------------------

    def w_utf8 (self, Kerr = [] , attr_num=0, num_row =0, string = ''):

        if self.ht.mode in ('rs', 'rm' ):
            set_err_int (Kerr, Mod_name,  'w_links '+self.ht.ht_name+'-'+ str(self.maf_num),  6 , \
                    message='Mode "%s" incompatible for elem modify - w_utf8 in MAF for HT "%s".'
                    % (self.ht.mode, self.ht.ht_name)
            )
            return False

        if string == None:
             string = ''
        if num_row > self.rows:
            set_err_int (Kerr, Mod_name,  'w_links '+self.ht.ht_name+'-'+ str(self.maf_num),  1 , \
                    message='num_row parameter over the  number of MAF rows.' )
            return False
        atr_type = self.ht.attrs [ attr_num ] ['type'] 
        if not ( atr_type[ : 3 ]  in  ( 'utf', 'dat') ) :
            set_err_int (Kerr, Mod_name,   'w_utf8 '+self.ht.ht_name+'-'+ self.maf_name,  2 , \
                        message='Incorrect attribute type for write string.' )
            return False
        offset , length = self.offsets [attr_num] 
        data=  string.encode( "utf-8", "replace"  ) 
        len_data = len ( data )
        if len_data > length:
            set_err_int (Kerr, Mod_name,   'w_utf8 '+self.ht.ht_name+'-'+ self.maf_name,  3 , \
                        message='Length of data dismatch with attribute data type.' )
            return False
        data = data+Types_htms.types[ atr_type ][ 2 ][ len_data : ]
        rc =  self.ht.cage.write( self.ch,  ( num_row - 1 )*self.rowlen+offset, data , Kerr) 
        if rc == False:
            set_err_int (Kerr, Mod_name, 'w_utf8 '+self.ht.ht_name+'-'+ self.maf_name,  4 , \
                                message='Error read from MAF. ' )
            return False
        """
        if not self.ht.cage.put_pages ( self.ch, Kerr):
            set_err_int (Kerr, Mod_name,  'w_utf8 '+self.ht.ht_name+'-'+ str(self.maf_num),  5, \
                        message='Error MAF push modified pages.' )
            return False
        """
        self.updated = time.time()
        self.ht.mafs [ self.maf_num]['updated']= self.updated
        self.ht.updated =self.updated 

        return True

# -------------------------------------------------------------------------------------------


    def r_numbers (self, Kerr = [] ,  attr_num=0, num_row =0): 

        if num_row > self.rows:
            set_err_int (Kerr, Mod_name,  'r_numbers '+self.ht.ht_name+'-'+ str(self.maf_num),  1 , \
                    message='num_row parameter over the  number of MAF rows.' )
            return False

        atr_type = self.ht.attrs [ attr_num ] ['type'] 
        offset , length = self.offsets [attr_num] 

        if atr_type in ( "int4", "int8","float4","float8","time") :
            data =  self.ht.cage.read( self.ch,  ( num_row - 1 )  *self.rowlen+offset, length , Kerr)
            if data == False:
                set_err_int (Kerr, Mod_name,   'r_numbers '+self.ht.ht_name+'-'+ self.maf_name,  2, \
                        message='Error read element from MAF.' )
                return False            
            number = struct.unpack ( Types_htms.types[ atr_type ][ 3 ], data)
            return number[0]
        
        elif  self.ht.attrs [ attr_num ] ['type'] in ( "*int4", "*int8","*float4","*float8") :
            af_link =  self.ht.cage.read( self.ch,  ( num_row - 1 )  *self.rowlen+offset, length , Kerr)
            if af_link == False:
                set_err_int (Kerr, Mod_name,   'r_numbers '+self.ht.ht_name+'-'+ self.maf_name,  3, \
                        message='Error read AF block address in MAF.' )
                return False
            elif af_link ==  b'\xFF'* 16 :
                return  None

            af_addr, len_af_block =   struct.unpack( '>QQ', af_link )
            if len_af_block ==0 :
                return  ()

            af_block =  self.ht.cage.read( self.ht.channels [ 'af' ], af_addr, len_af_block , Kerr)
            if af_block == False:
                set_err_int (Kerr, Mod_name,   'r_numbers '+self.ht.ht_name+'-'+ self.maf_name,  4, \
                        message='Error read data from AF.' )
                return False

            len_elem = Types_htms.types[ atr_type ][ 1 ]            
            dim = int ( len_af_block / len_elem )

            numbers = ()
            for i in range (0 , dim):
                memb = af_block[ i * len_elem : (i+1) *len_elem ]
                numbers +=  struct.unpack ( Types_htms.types[ atr_type ][ 3 ], memb)
            return numbers 
        else:
            set_err_int (Kerr, Mod_name,   'r_numbers '+self.ht.ht_name+'-'+ self.maf_name,  5 , \
                        message='Incorrect attribute type for read numbers.' )
            return False

# -------------------------------------------------------------------------------------------

    def w_numbers (self, Kerr = [] , attr_num=0, num_row =0, numbers= 0, rollback=False):

        if self.ht.mode in ('rs', 'rm' ):
            set_err_int (Kerr, Mod_name,  'w_numbers '+self.ht.ht_name+'-'+ str(self.maf_num),  11 , \
                    message='Mode "%s" incompatible for elem modify - w_numbers in MAF for HT "%s".'
                    % (self.ht.mode, self.ht.ht_name)
            )
            return False

        if numbers== None:
            numbers= 0
        if num_row > self.rows:
            set_err_int (Kerr, Mod_name,  'w_numbers '+self.ht.ht_name+'-'+ str(self.maf_num),  1 , \
                    message='num_row parameter over the  number of MAF rows.' )
            return False

        atr_type = self.ht.attrs [ attr_num ] ['type']
        offset , length = self.offsets [attr_num] 

        if atr_type in ( "int4", "int8","float4","float8","time") :
            if type( numbers).__name__ == 'tuple':
                number = numbers[0]
            else:
                number = numbers
            data = struct.pack ( Types_htms.types[ atr_type ][ 3 ], number)
            rc =  self.ht.cage.write( self.ch,  ( num_row - 1 )  *self.rowlen+offset, data , Kerr)
            if rc == False:
                set_err_int (Kerr, Mod_name, 'w_numbers '+self.ht.ht_name+'-'+ self.maf_name,  2 , \
                                        message='Error write MAF. ' )
                return False
            """
            if not self.ht.cage.put_pages ( self.ch, Kerr):
                set_err_int (Kerr, Mod_name,  'w_numbers '+self.ht.ht_name+'-'+ str(self.maf_num),  3, \
                            message='Error MAF push modified pages.' )
                return False
            """
            self.updated = time.time()
            self.ht.mafs [ self.maf_num]['updated']= self.updated
            self.ht.updated =self.updated 

            return True
        
        elif  self.ht.attrs [ attr_num ] ['type'] in ( "*int4", "*int8","*float4","*float8") :
            if  numbers == None:
                new_af_link = b'\xFF'* 16
            elif numbers == ():
                new_af_link =struct.pack ('>QQ',  0,   0 )
            else:
                if type( numbers).__name__ != 'tuple':
                    numbers = (numbers,)
                
                dim = len( numbers )

                af_block = b''
                for i in range (0 , dim):
                    af_block +=  struct.pack ( Types_htms.types[ atr_type ][ 3 ], numbers [ i ] )

                len_af_block = dim * Types_htms.types[ atr_type ][ 1 ]

                overwrite = False               # write block on new place

                if not rollback:
                    old_af_link =  self.ht.cage.read( self.ch,  ( num_row - 1 )*self.rowlen+offset, 16 , Kerr)   
                    if old_af_link == False:
                        set_err_int (Kerr, Mod_name,  'w_numbers '+self.ht.ht_name+'-'+ str(self.maf_num),  4 , \
                            message='Error read MAF.' )
                        return False
                    if old_af_link != b'\xFF'*16:
                        old_af_addr, len_old_af_block =   struct.unpack( '>QQ', old_af_link )
                        if   len_af_block <=  len_old_af_block :
                            overwrite = True        # write block on old place (with overlap)

                if overwrite:
                    rc1 =  self.ht.cage.write( self.ht.channels [ 'af' ],  old_af_addr,   af_block , Kerr)   
                    if rc1 ==False:
                        set_err_int (Kerr, Mod_name,  'w_numbers '+self.ht.ht_name+'-'+ str(self.maf_num),  5 , \
                            message='Error write to AF.' )
                        return False
                    new_af_link =                        struct.pack ('>QQ',  old_af_addr,   len_af_block )
                else:
                    rc1 =  self.ht.cage.write( self.ht.channels [ 'af' ],  self.ht.a_free,  af_block , Kerr)   
                    if rc1 ==False:
                        set_err_int (Kerr, Mod_name,  'w_numbers '+self.ht.ht_name+'-'+ str(self.maf_num),  6 , \
                            message='Error write to AF.' )
                        return False
                    new_af_link =                        struct.pack ('>QQ',  self.ht.a_free,  len_af_block )
                    self.ht.a_free += len_af_block               
                    """
                    if not self.ht.save_adt(Kerr):
                        set_err_int (Kerr, Mod_name,   'w_numbers '+self.ht.ht_name+'-'+ str(self.maf_num),  7 , \
                                        message='Error save HTD.' )                
                        return False
                    """
            rc2 =  self.ht.cage.write( self.ch,   ( num_row - 1 ) *self.rowlen+offset,   new_af_link , Kerr)   
            if rc2 ==False:
                set_err_int (Kerr, Mod_name,  'w_numbers '+self.ht.ht_name+'-'+ str(self.maf_num),  8 , \
                            message='Error write to MAF.' )
                return False
            """
            if not self.ht.cage.put_pages ( self.ch, Kerr):
                set_err_int (Kerr, Mod_name,  'w_numbers '+self.ht.ht_name+'-'+ str(self.maf_num),  9, \
                            message='Error MAF push modified pages.' )
                return False
            if not self.ht.cage.put_pages ( self.ht.channels [ 'af' ], Kerr):
                set_err_int (Kerr, Mod_name,  'w_numbers '+self.ht.ht_name+'-'+ str(self.maf_num),  10, \
                            message='Error AF push modified pages.' )
                return False
            """
            self.updated = time.time()
            self.ht.mafs [ self.maf_num]['updated']= self.updated
            self.ht.updated =self.updated 

            return True
        else:
            set_err_int (Kerr, Mod_name,   'w_numbers '+self.ht.ht_name+'-'+ self.maf_name,  10 , \
                        message='Incorrect type of input data' )
            return False

# -------------------------------------------------------------------------------------------

    def r_bytes (self, Kerr = [] , attr_num=0, num_row =0): 

        if num_row > self.rows:
            set_err_int (Kerr, Mod_name,  'r_bytes '+self.ht.ht_name+'-'+ str(self.maf_num),  1 , \
                    message='num_row parameter over the  number of MAF rows.' )
            return False
        if self.attr_type( attr_num ) != "*byte":
            set_err_int (Kerr, Mod_name,  'r_bytes '+self.ht.ht_name+'-'+ str(self.maf_num),  2 , \
                    message='attr_num parameter not corresponds with field data.' )
            return False

        offset , length = self.offsets [attr_num] 

        if length> MAXSTRLEN1:
            set_err_int (Kerr, Mod_name,  'r_bytes '+self.ht.ht_name+'-'+ str(self.maf_num),  3 , \
                    message='string length exceeds MAXSTRLEN1.' )
            return False

        af_link =  self.ht.cage.read( self.ch,  ( num_row - 1 )  *self.rowlen+offset, length , Kerr)
        if af_link == False:
            set_err_int (Kerr, Mod_name,   'r_bytes '+self.ht.ht_name+'-'+ self.maf_name,  4, \
                        message='Error read AF block address in MAF.' )
            return False
        elif af_link ==  b'\xFF'* 16 :
            return  None

        af_addr, len_af_block =   struct.unpack( '>QQ', af_link )
        if len_af_block ==0 :
                return  b''

        af_block =  self.ht.cage.read( self.ht.channels [ 'af' ], af_addr, len_af_block , Kerr)
        if af_block == False:
            set_err_int (Kerr, Mod_name,   'r_bytes '+self.ht.ht_name+'-'+ self.maf_name,  5, \
                        message='Error read data from AF.' )
            return False

        return af_block 

# -------------------------------------------------------------------------------------------

    def w_bytes (self, Kerr = [] , attr_num=0, num_row =0,  bytes= b'', rollback=False):

        if self.ht.mode in ('rs', 'rm' ):
            set_err_int (Kerr, Mod_name,  'w_bytes '+self.ht.ht_name+'-'+ str(self.maf_num),  11 , \
                    message='Mode "%s" incompatible for elem modify - w_bytes in MAF for HT "%s".'
                    % (self.ht.mode, self.ht.ht_name)
            )
            return False

        if bytes==None:
            bytes= b''
        if num_row > self.rows:
            set_err_int (Kerr, Mod_name,  'w_bytes '+self.ht.ht_name+'-'+ str(self.maf_num),  1 , \
                    message='num_row parameter over the  number of MAF rows.' )
            return False
        if self.attr_type( attr_num ) != "*byte":
            set_err_int (Kerr, Mod_name,  'w_bytes '+self.ht.ht_name+'-'+ str(self.maf_num),  2 , \
                    message='attr_num parameter not corresponds with field data.' )
            return False

        if len(bytes)> MAXSTRLEN1:
            set_err_int (Kerr, Mod_name,  'w_bytes '+self.ht.ht_name+'-'+ str(self.maf_num),  3 , \
                    message='string length exceeds MAXSTRLEN1.' )
            return False

        offset , length = self.offsets [attr_num] 
        if  bytes == None:
            new_af_link = b'\xFF'* 16
        elif len( bytes ) == 0:
            new_af_link =struct.pack ('>QQ',  0,   0 )
        else:
            len_af_block = len( bytes )
            overwrite = False               # write block on new place

            if not rollback:
                old_af_link =  self.ht.cage.read( self.ch,  ( num_row - 1 )*self.rowlen+offset, 16 , Kerr)   
                if old_af_link == False:
                    set_err_int (Kerr, Mod_name,  'w_bytes '+self.ht.ht_name+'-'+ str(self.maf_num),  4 , \
                            message='Error read MAF.' )
                    return False
                if old_af_link != b'\xFF'*16:
                    old_af_addr, len_old_af_block =   struct.unpack( '>QQ', old_af_link )
                    if   len_af_block <=  len_old_af_block :
                        overwrite = True        # write block on old place (with overlap)

            if overwrite:
                rc1 =  self.ht.cage.write( self.ht.channels [ 'af' ],  old_af_addr,   bytes, Kerr)   
                if rc1 == False:
                    set_err_int (Kerr, Mod_name,  'w_bytes '+self.ht.ht_name+'-'+ str(self.maf_num),  5 , \
                            message='Error write AF.' )
                    return False
                new_af_link =                        struct.pack ('>QQ',  old_af_addr,   len_af_block )
            else:
                rc1 =  self.ht.cage.write( self.ht.channels [ 'af' ],  self.ht.a_free,  bytes , Kerr)   
                if rc1 == False:
                    set_err_int (Kerr, Mod_name,  'w_bytes '+self.ht.ht_name+'-'+ str(self.maf_num),  6 , \
                            message='Error write AF.' )
                    return False
                new_af_link =                        struct.pack ('>QQ',  self.ht.a_free,  len_af_block )
                self.ht.a_free += len_af_block               
                """
                if not self.ht.save_adt(Kerr):
                    set_err_int (Kerr, Mod_name,   'w_bytes '+self.ht.ht_name+'-'+ str(self.maf_num),  7 , \
                                    message='Error save HTD.' )                
                    return False
                """
        rc2 =  self.ht.cage.write( self.ch,   ( num_row - 1 ) *self.rowlen+offset,   new_af_link , Kerr)   
        if rc2 == False:
            set_err_int (Kerr, Mod_name,  'w_bytes '+self.ht.ht_name+'-'+ str(self.maf_num),  8 , \
                            message='Error write MAF.' )
            return False
        """
        if not self.ht.cage.put_pages ( self.ch, Kerr):
            set_err_int (Kerr, Mod_name,  'w_bytes '+self.ht.ht_name+'-'+ str(self.maf_num),  9, \
                        message='Error MAF push modified pages.' )
            return False
        if not self.ht.cage.put_pages ( self.ht.channels [ 'af' ], Kerr):
            set_err_int (Kerr, Mod_name,  'w_bytes '+self.ht.ht_name+'-'+ str(self.maf_num),  10, \
                        message='Error AF push modified pages.' )
            return False
        """
        self.updated = time.time()
        self.ht.mafs [ self.maf_num]['updated']= self.updated
        self.ht.updated =self.updated 

        return True

# -------------------------------------------------------------------------------------------

    def r_str(self, Kerr = [] , attr_num=0, num_row =0): 

        if num_row > self.rows:
            set_err_int (Kerr, Mod_name,  'r_str '+self.ht.ht_name+'-'+ str(self.maf_num),  1 , \
                    message='num_row parameter over the  number of MAF rows.' )
            return False
        if self.attr_type( attr_num ) != "*utf":
            set_err_int (Kerr, Mod_name,  'r_str '+self.ht.ht_name+'-'+ str(self.maf_num),  2 , \
                    message='attr_num parameter not corresponds with field data.' )
            return False

        offset , length = self.offsets [attr_num] 

        if math.ceil(length/2)> MAXSTRLEN1:
            set_err_int (Kerr, Mod_name,  'r_str '+self.ht.ht_name+'-'+ str(self.maf_num),  3 , \
                    message='string length exceeds MAXSTRLEN1.' )
            return False

        af_link =  self.ht.cage.read( self.ch,  ( num_row - 1 )  *self.rowlen+offset, length , Kerr)
        if af_link == False:
            set_err_int (Kerr, Mod_name,   'r_str '+self.ht.ht_name+'-'+ self.maf_name,  4, \
                        message='Error read AF block address in MAF.' )
            return False
        elif af_link ==  b'\xFF'* 16 :
            return  None

        af_addr, len_af_block =   struct.unpack( '>QQ', af_link )
        if len_af_block ==0 :
                return  ''

        af_block =  self.ht.cage.read( self.ht.channels [ 'af' ], af_addr, len_af_block , Kerr )
        if af_block == False:
            set_err_int (Kerr, Mod_name,   'r_str '+self.ht.ht_name+'-'+ self.maf_name,  5, \
                        message='Error read data from AF.' )
            return False
        string = af_block.decode( "utf-8", "ignore")
        return string 

# -------------------------------------------------------------------------------------------

    def w_str (self, Kerr = [] , attr_num=0, num_row =0,  string= '', rollback=False):

        if self.ht.mode in ('rs', 'rm' ):
            set_err_int (Kerr, Mod_name,  'w_str '+self.ht.ht_name+'-'+ str(self.maf_num),  8 , \
                    message='Mode "%s" incompatible for elem modify - w_str in MAF for HT "%s".'
                    % (self.ht.mode, self.ht.ht_name)
            )
            return False

        if string==None:
            string= ''
        if num_row > self.rows:
            set_err_int (Kerr, Mod_name,  'w_str '+self.ht.ht_name+'-'+ str(self.maf_num),  1 , \
                    message='num_row parameter over the  number of MAF rows.' )
            return False
        if self.attr_type( attr_num ) != "*utf":
            set_err_int (Kerr, Mod_name,  'w_str '+self.ht.ht_name+'-'+ str(self.maf_num),  2 , \
                    message='attr_num parameter not corresponds with field data.' )
            return False
        if math.ceil(len(string)/2)> MAXSTRLEN1:
            set_err_int (Kerr, Mod_name,  'w_str '+self.ht.ht_name+'-'+ str(self.maf_num),  3 , \
                    message='string length exceeds MAXSTRLEN1.' )
            return False

        offset , length = self.offsets [attr_num] 
        if  string == None:
            new_af_link = b'\xFF'* 16
        elif  len( string ) == 0 :            
            new_af_link =  struct.pack ('>QQ',  0,   0 )
        else:            
            data=  string.encode( "utf-8", "replace"  ) 
            len_af_block = len ( data )
            overwrite = False               # write block on new place

            if not rollback:
                old_af_link =  self.ht.cage.read( self.ch,  ( num_row - 1 )*self.rowlen+offset, 16 , Kerr)   
                if old_af_link == False:
                    set_err_int (Kerr, Mod_name,  'w_str '+self.ht.ht_name+'-'+ str(self.maf_num),  3 , \
                        message='Error read MAF.' )
                    return False
                if old_af_link != b'\xFF'*16:
                    old_af_addr, len_old_af_block =   struct.unpack( '>QQ', old_af_link )
                    if   len_af_block <=  len_old_af_block :
                        overwrite = True        # write block on old place (with overlap)

            if overwrite:
                rc1 =  self.ht.cage.write( self.ht.channels [ 'af' ],  old_af_addr,   data, Kerr)   
                if rc1 ==False:
                    set_err_int (Kerr, Mod_name,  'w_str '+self.ht.ht_name+'-'+ str(self.maf_num),  4, \
                                message='Error write AF.' )
                    return False
                new_af_link =                        struct.pack ('>QQ',  old_af_addr,   len_af_block )
            else:
                rc1 =  self.ht.cage.write( self.ht.channels [ 'af' ],  self.ht.a_free,  data , Kerr)   
                if rc1 ==False:
                    set_err_int (Kerr, Mod_name,  'w_str '+self.ht.ht_name+'-'+ str(self.maf_num),  5, \
                                message='Error write AF..' )
                    return False
                new_af_link =                        struct.pack ('>QQ',  self.ht.a_free,  len_af_block )
                self.ht.a_free += len_af_block      
                """
                if not self.ht.save_adt(Kerr):
                    set_err_int (Kerr, Mod_name,   'attribute '+self.ht.ht_name+'-'+ str(self.maf_num),  6 , \
                                    message='Error save HTD.' )                
                    return False
                """
        rc2 =  self.ht.cage.write( self.ch,   ( num_row - 1 ) *self.rowlen+offset,   new_af_link , Kerr)   
        if rc2 ==False:
            set_err_int (Kerr, Mod_name,   'w_str '+self.ht.ht_name+'-'+ self.maf_name,  7, \
                            message='Error write MAF.' )
            return False
        """
        if not self.ht.cage.put_pages ( self.ch, Kerr):
            set_err_int (Kerr, Mod_name,  'w_str '+self.ht.ht_name+'-'+ str(self.maf_num),  8, \
                        message='Error MAF push modified pages.' )
            return False
        if not self.ht.cage.put_pages ( self.ht.channels [ 'af' ], Kerr):
            set_err_int (Kerr, Mod_name,  'w_str '+self.ht.ht_name+'-'+ str(self.maf_num),  9, \
                        message='Error AF push modified pages.' )
            return False
        """
        self.updated = time.time()
        self.ht.mafs [ self.maf_num]['updated']= self.updated
        self.ht.updated =self.updated 

        return True

# -------------------------------------------------------------------------------------------

    def download_file (self, Kerr = [] , attr_num=0, num_row =0, to_path=''):

        if to_path==None:
            to_path=''
        file_descr= self.r_file_descr (Kerr = Kerr , attr_num=attr_num, num_row =num_row,  )
        if file_descr == False:
            return False
        if file_descr == None:
            return None

        offset , length = self.offsets [attr_num] 

        maf_elem =  self.ht.cage.read( self.ch,   ( num_row - 1 ) *self.rowlen+offset, length, Kerr)   
        if maf_elem ==  b'\xFF'*32 :
            return  None

        bf_addr_file , file_length =   struct.unpack( '>QQQQ', maf_elem) [ 2: ]        
        if file_length > 0:
            try:
                with open(to_path, 'wb') as f_down:            

                    num_chunks = math.ceil(file_length/PAGESIZE2)
                    for chunk in range(num_chunks):
                        if chunk<num_chunks-1:
                            size=PAGESIZE2
                        else:
                            size= file_length - PAGESIZE2*chunk  
                        data =  self.ht.cage2.read( 
                                self.ht.channels [ 'bf' ],  
                                bf_addr_file+PAGESIZE2*chunk,   
                                size, 
                                Kerr)   
                        if data == False:
                            set_err_int (Kerr, Mod_name,  'download_file '+self.ht.ht_name+'-'+ str(self.maf_num),  1, \
                                        message='Error read AF.' )
                            return False
                        pos = f_down.seek(PAGESIZE2*chunk)

                        rc= f_down.write( data )
                        if rc == False:
                            set_err_int (Kerr, Mod_name,  'download_file '+self.ht.ht_name+'-'+ str(self.maf_num),  2, \
                                        message='Error write temp file.' )
                            return False
                f_down.close()
               #time.sleep(0.1)
            except Exception as rc:
                set_err_int (Kerr, Mod_name,  'download_file '+self.ht.ht_name+'-'+ str(self.maf_num),  3 , \
                            message='Error download file operations rc=.%s'%str(rc) )
                return False

        return file_descr

# -------------------------------------------------------------------------------------------

    def r_file_descr (self, Kerr = [] , attr_num=0, num_row =0,  ): 

        if num_row > self.rows:
            set_err_int (Kerr, Mod_name,  'r_file_desc '+self.ht.ht_name+'-'+ str(self.maf_num),  1 , \
                    message='num_row parameter over the  number of MAF rows.' )
            return False
        if self.attr_type( attr_num ) != "file":
            set_err_int (Kerr, Mod_name,  'r_file_desc '+self.ht.ht_name+'-'+ str(self.maf_num),  2 , \
                    message='attr_num parameter not corresponds with field type.' )
            return False        
                                      
        offset , length = self.offsets [attr_num]         

        maf_elem =  self.ht.cage.read( self.ch,   ( num_row - 1 ) *self.rowlen+offset, length, Kerr)   
        if maf_elem == False:
            set_err_int (Kerr, Mod_name,  'r_file_descr '+self.ht.ht_name+'-'+ str(self.maf_num),  3 , \
                            message='Error read MAF.' )
            return False
        elif maf_elem ==  b'\xFF'*32 :
            return  None

        af_addr_descr,  file_descr_length, bf_addr_file , file_length =   struct.unpack( '>QQQQ', maf_elem )

        file_descr={}
        if file_descr_length !=0 :
            file_descr =  self.ht.cage.read( self.ht.channels [ 'af' ],  af_addr_descr,  file_descr_length , Kerr)   
            if file_descr == False:
                set_err_int (Kerr, Mod_name,  'r_file_desc '+self.ht.ht_name+'-'+ str(self.maf_num),  4 , \
                            message='Error read BF.' )
                return False
            try:
                file_descr= pickle.loads( file_descr)
            except:
                set_err_int (Kerr, Mod_name,  'r_file_desc '+self.ht.ht_name+'-'+ str(self.maf_num),  5 , \
                            message='unpickling error' )
                return False

        return file_descr

# -------------------------------------------------------------------------------------------
    def upload_file (self, Kerr = [] , attr_num=0, num_row =0,  
                from_path='', real_file_name='', file_e='', content_t='',  file_d={},
                rollback=False
        ): 

        if self.ht.mode in ('rs', 'rm' ):
            set_err_int (Kerr, Mod_name,  'w_file '+self.ht.ht_name+'-'+ str(self.maf_num),  16 , \
                    message='Mode "%s" incompatible for elem modify - upload_file in MAF for HT "%s".'
                    % (self.ht.mode, self.ht.ht_name)
            )
            return False

        if from_path==None:
            from_path=''
        if num_row > self.rows:
            set_err_int (Kerr, Mod_name,  'w_file '+self.ht.ht_name+'-'+ str(self.maf_num),  1 , \
                    message='num_row parameter over the  number of MAF rows.' )
            return False
        if self.attr_type( attr_num ) != "file":
            set_err_int (Kerr, Mod_name,  'w_file '+self.ht.ht_name+'-'+ str(self.maf_num),  2 , \
                    message='attr_num parameter not corresponds with field type.' )
            return False        
        
        try:
            file_length= os.path.getsize(from_path)
        except Exception as rc:
            set_err_int (Kerr, Mod_name,  'w_file '+self.ht.ht_name+'-'+ str(self.maf_num),  3 , \
                    message='file path incorrect rc =%s.' % str(rc) )
            return False

        if file_length > MAX_LEN_FILE_BODY:
            set_err_int (Kerr, Mod_name,  'w_file '+self.ht.ht_name+'-'+ str(self.maf_num),  4 , \
                    message='length of file body exceeds  MAX_LEN_FILE_BODY .' )
            return False

        new_file_descr={}

        if  file_e !='':
            if  file_e in Types_htms.file_to_mime:
                content_type= Types_htms.file_to_mime[ file_e ]
                if content_t != '' and content_t != content_type:
                    set_err_int (Kerr, Mod_name,  'w_file'+self.ht.ht_name+'-'+ str(self.maf_num),  5 , \
                            message='content_type and file_ext not correspond each other.' )
                    return False
            else:
                content_type=''
            file_ext = file_e

        elif content_t !='':
            file_ext = Types_htms.mime_to_file( content_t )
            if len(file_ext) ==1:
                file_ext=file_ext[0]
            else:
                file_ext=''
            content_type=content_t

        else:
            file_ext=''                                                                                                                                                     
            content_type=''

        new_file_descr= file_d
        new_file_descr.update({
                'file_name':real_file_name, 'file_ext':file_ext, 'content_type':content_type, 'file_length':file_length
                })
        try:
            new_file_descr= pickle.dumps ( new_file_descr)
        except:
            set_err_int (Kerr, Mod_name,  'w_file '+self.ht.ht_name+'-'+ str(self.maf_num),  6 , \
                    message='pickling error .' )
            return False

        if len(new_file_descr) > MAX_LEN_FILE_DESCRIPTOR:
            set_err_int (Kerr, Mod_name,  'w_file '+self.ht.ht_name+'-'+ str(self.maf_num),  7 , \
                    message='length of file descriptor exceeds MAX_LEN_FILE_DESCRIPTOR .' )
            return False
                                      
        offset , length = self.offsets [attr_num] 

        if  from_path == '' and real_file_name=='':
            new_maf_elem = b'\xFF'* 32

        elif file_length== 0 or from_path == '':
            rc0 =  self.ht.cage.write( self.ht.channels [ 'af' ],  self.ht.b_free,  new_file_descr , Kerr)   
            if rc0 == False:
                set_err_int (Kerr, Mod_name,  'w_file '+self.ht.ht_name+'-'+ str(self.maf_num),  8 , \
                            message='Error write AF.' )
                return False
            new_maf_elem =  struct.pack ('>QQQQ',  self.ht.b_free,  len(new_file_descr), 0 , 0 )
            self.ht.a_free+= len(new_file_descr)
        else:
            len_af_block = file_length
            overwrite_descr = False               # write descr on new place
            overwrite_file = False                  # write file on new place
            if not rollback:
                old_maf_elem =  self.ht.cage.read( self.ch,  ( num_row - 1 )*self.rowlen+offset, 32 , Kerr)   
                if old_maf_elem == False:
                    set_err_int (Kerr, Mod_name,  'w_file '+self.ht.ht_name+'-'+ str(self.maf_num),  9 , \
                            message='Error read MAF.' )
                    return False
                if old_maf_elem != b'\xFF'*32:
                    old_af_addr_descr, len_old_af_block_descr, \
                    old_bf_addr_file, len_old_af_block_file =   struct.unpack( '>QQQQ', old_maf_elem )
                    if   file_length <=  len_old_af_block_file :
                        overwrite_file = True        # write block on old place (with overlap)
                    if   len(new_file_descr) <=  len_old_af_block_descr :
                        overwrite_descr = True        # write block on old place (with overlap)

            if overwrite_descr:
                af_addr_descr=old_af_addr_descr
            else:
                af_addr_descr=self.ht.a_free
                self.ht.a_free +=  len(new_file_descr)    
            if overwrite_file:
                bf_addr_file = old_bf_addr_file
            else:
                bf_addr_file =self.ht.b_free
                self.ht.b_free +=  file_length    

            rc1 =  self.ht.cage.write( self.ht.channels [ 'af' ],  af_addr_descr,   new_file_descr, Kerr)   
            if rc1 == False:
                set_err_int (Kerr, Mod_name,  'w_file '+self.ht.ht_name+'-'+ str(self.maf_num),  10 , \
                                message='Error write AF.' )
                #self.ht.a_free -=  len(new_file_descr) 
                return False
            try:
                with open(from_path, 'rb') as f_up:            

                    num_chunks = math.ceil(file_length/PAGESIZE2)
                    for chunk in range(num_chunks):
                        pos = f_up.seek(PAGESIZE2*chunk)
                        if chunk<num_chunks-1:
                            size=PAGESIZE2
                        else:
                            size= file_length - PAGESIZE2*chunk  

                        data= f_up.read( size )
                        rc2 =  self.ht.cage2.write( 
                                self.ht.channels [ 'bf' ],  
                                bf_addr_file+PAGESIZE2*chunk,   
                                data, 
                                Kerr)   
                        if rc2 == False:
                            set_err_int (Kerr, Mod_name,  'w_file '+self.ht.ht_name+'-'+ str(self.maf_num),  11, \
                                        message='Error write BF.' )
                            #self.ht.a_free -=  len(new_file_descr)    
                            return False

                f_up.close()
               #time.sleep(0.1)
            except Exception as rc:
                set_err_int (Kerr, Mod_name,  'w_file '+self.ht.ht_name+'-'+ str(self.maf_num),  12 , \
                            message='Error upload file operations rc=.%s'%str(rc) )
                return False

            new_maf_elem =  struct.pack ('>QQQQ',  
                        af_addr_descr,  len(new_file_descr), 
                        bf_addr_file , file_length )
        
        rc3 =  self.ht.cage.write( self.ch,   ( num_row - 1 ) *self.rowlen+offset,   new_maf_elem , Kerr)   
        if rc3 == False:
            set_err_int (Kerr, Mod_name,  'w_file '+self.ht.ht_name+'-'+ str(self.maf_num),  13 , \
                            message='Error write MAF.' )
            return False
        """
        if not self.ht.cage.put_pages ( self.ch, Kerr):
            set_err_int (Kerr, Mod_name,  'w_file '+self.ht.ht_name+'-'+ str(self.maf_num),  14, \
                        message='Error MAF push modified pages.' )
            return False
        if not self.ht.cage2.put_pages ( self.ht.channels [ 'bf' ], Kerr):
            set_err_int (Kerr, Mod_name,  'w_file '+self.ht.ht_name+'-'+ str(self.maf_num),  15, \
                        message='Error BF push modified pages.' )
            return False
        """
        self.updated = time.time()
        self.ht.mafs [ self.maf_num]['updated']= self.updated
        self.ht.updated =self.updated 

        return True    

# -------------------------------------------------------------------------------------------

    def clean_file_descr (self, Kerr = [] , attr_num=0, num_row =0,  ): 

        if self.ht.mode in ('rs', 'rm' ):
            set_err_int (Kerr, Mod_name,  'clean_file_descr '+self.ht.ht_name+'-'+ str(self.maf_num),  4 , \
                    message='Mode "%s" incompatible for elem modify - clean_file_descr in MAF for HT "%s".'
                    % (self.ht.mode, self.ht.ht_name)
            )
            return False

        if num_row > self.rows:
            set_err_int (Kerr, Mod_name,  'clean_file_descr '+self.ht.ht_name+'-'+ str(self.maf_num),  1 , \
                    message='num_row parameter over the  number of MAF rows.' )
            return False
        if self.attr_type( attr_num ) != "file":
            set_err_int (Kerr, Mod_name,  'clean_file_descr '+self.ht.ht_name+'-'+ str(self.maf_num),  2 , \
                    message='attr_num parameter not corresponds with field type.' )
            return False        
                                      
        offset , length = self.offsets [attr_num]         

        new_maf_elem = b'\xFF'* 32

        rc =  self.ht.cage.write( self.ch,   ( num_row - 1 ) *self.rowlen+offset,   new_maf_elem , Kerr)   
        if rc == False:
            set_err_int (Kerr, Mod_name,  'clean_file_descr '+self.ht.ht_name+'-'+ str(self.maf_num),  3 , \
                            message='Error write MAF.' )
            return False

        self.updated = time.time()
        self.ht.mafs [ self.maf_num]['updated']= self.updated
        self.ht.updated =self.updated 

        return True


        #dir=os.path.dirname(path)
        #name_ext=os.path.basename(path)
        #name = os.path.splitext(name_ext)[0]
        #extension = os.path.splitext(name_ext)[1][1:]



