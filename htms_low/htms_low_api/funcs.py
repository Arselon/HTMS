# HTMS low level  v. 3.1.0 (Cage class v. 3.1.0)
# © A.S.Aliev, 2018-2022

from cage_api             import  *

from .htms_par_low        import   *
from .data_types          import   *


#----------------------------------------------------------------------------------------------------

def match ( sample,  is_num_array, is_string_array, is_bytes_array, attr_type, tested):

                f=      sample
                elem=   tested
        # f - it is sample,  elem - data for compare,  attr_type - data type of elem 
        #       f  = ( oper, value1, value2|none)
        #       oper:  ==/ != / >= / <=/ in / not in / find ...
        # return :
        #                 1 :  match OK
        #                 0 :  NOT match
        #                -1 :  Invalid parameters type
        #                -2 :  Invalid combination of parameters
        #                -3 :  Invalid compare function and/or parameters in search field
        #                -4 :  Invalid sample for oper 'in', 'not', 'find', when attr type - single number (not array)


                if  type(  is_num_array ).__name__ != 'bool' or \
                    type(  is_string_array ).__name__ != 'bool' or \
                    type(  is_bytes_array ).__name__ != 'bool':
                    result = -1
                    return result

                if is_num_array + is_string_array + is_bytes_array >1:
                    result = -2
                    return result

                if type( f ).__name__ != 'tuple' or ( is_num_array and type( f [ 0 ] ).__name__ != 'str' ) :
                    f = (  '==', f )

                result = -3            

                func = f  [ 0 ]

                if           func == '==':
                    if     elem == f   [ 1 ] :
                        result = 1
                    else:
                        result = 0
                elif        func == '!='  :
                    if    elem != f   [ 1 ] :
                        result = 1
                    else:
                        result = 0
                elif        func == '<'   :  
                    if    elem  < f   [ 1 ] :
                        result = 1
                    else:
                        result = 0
                elif        func == '>'   :  
                    if    elem  > f   [ 1 ] :
                        result = 1
                    else:
                        result = 0
                elif        func == '<=':   
                    if    elem <= f   [ 1 ] :
                        result = 1
                    else:
                        result = 0
                elif       func == '>=':  
                    if    elem >= f   [ 1 ]:
                        result = 1
                    else:
                        result = 0
                elif   func == 'in':
                    if attr_type in  ('int4', 'int8', 'float4', 'float8', 'time'):
                        if len (f) != 3:
                            result = -4
                        if elem >= f[ 1 ] and elem <= f[ 2]:
                            result = 1
                        else:
                            result = 0
                    elif is_bytes_array:
                        if  len (f) ==3 and f [ 2 ]  > 0 and  f [ 1 ]. rfind( elem , 0 , elem [ 2 ]   ) != -1 or \
                            len (f) ==2 and   f [ 1 ]. find( elem  ) != -1:
                            result = 1
                        else:
                            result = 0
                    elif is_string_array:
                        if  len (f) ==3 and f  [ 2 ]  > 0 and  f [ 1 ]. rfind( elem , 0 , elem  [ 2 ]  ) != -1 or \
                            len (f) ==2 and   f [ 1 ]. find( elem  ) != -1:
                            result = 1
                        else:
                            result = 0
                    else: # is_nym_array
                        if len ( elem ) >  len ( f [ 1 ] ):
                            result=0
                        else:
                            result=0
                            for t1 in range ( len ( f [ 1 ] ) ):
                                if elem [ 0 ] ==  f [ 1] [ t1 ] and len ( f [ 1 ] ) - t1 >= len ( elem ) :
                                    result= 1
                                    for t2 in range (1, len ( elem ) ):
                                        if elem [ t2 ] == f [ 1] [ t1 +t2 ] :
                                            continue
                                        else:
                                            result = 0
                                            break
                                    break
                elif func == 'not in':
                    if attr_type in  ('int4', 'int8', 'float4', 'float8', 'time'):
                        if len (f) != 3:
                            result = -4
                        if elem >= f[ 1 ] and elem <= f[ 2]:
                            result = 0
                        else:
                            result = 1
                    elif is_bytes_array:
                        if   len (f) ==3 and f [ 2 ]  > 0 and  f [ 1 ]. rfind( elem , 0 , elem [ 2 ] ) == -1 or \
                             len (f) ==2 and     f [ 1 ]. find( elem  ) == -1:
                            result = 1
                        else:
                            result = 0
                    elif is_string_array:
                        if   len (f) ==3 and f   [ 2 ]  > 0 and  f [ 1 ]. rfind( elem , 0 , elem [ 2 ] ) == -1 or \
                             len (f) ==2 and     f [ 1 ]. find( elem  ) == -1:
                            result = 1
                        else:
                            result = 0
                    else: # is_nym_array
                        if len ( elem ) >  len ( f [ 1 ] ):
                            result=1
                        else:
                            result=1
                            for t1 in range ( len ( f [ 1 ] ) ):
                                if elem [ 0 ] ==  f [ 1] [ t1 ] and len ( f [ 1 ] ) - t1 >= len ( elem ) :
                                    result=0
                                    for t2 in range (1, len ( elem ) ):
                                        if elem [ t2 ] == f [ 1] [ t1 +t2 ] :
                                            continue
                                        else:
                                            result = 1
                                            break
                                    break 
                elif   func == 'find':
                    if attr_type in  ('int4', 'int8', 'float4', 'float8', 'time'):
                        result = -4
                    elif is_bytes_array:
                        if  len (f) ==2 and  elem . find( f [ 1 ]  ) != -1:
                            result = 1
                        else:
                            result = 0
                    elif is_string_array:
                        if  len (f) ==2 and  elem . find( f [ 1 ]  ) != -1:
                            result = 1
                        else:
                            result = 0
                    else: # is_nym_array
                            result=0
                            for t1 in range ( len ( elem ) ):
                                if elem [ t1 ] ==  f [ 1] :
                                    result= 1
                                    break

           
                return result

#----------------------------------------------------------------------------------------------------------------

def links_dump (  maf, level="", links=True, weights=True ):

    if level not in ("", "up", "down"):
        return

    maf.ht.cage.push_all ()
    print ('\n --- htms link dump ---  table: "%20s"  (maf num:  %4d )' % 
           (  maf.maf_name, maf.maf_num,  )  )
    for row in range (1, maf.rows+1):
        kerr=[]

        if level == "" or  level=="down":
            print ('\n    row  %4d  ' % row  )
            l=[ ]
            for attr in maf.ht.attrs:
                t= maf.attr_type( attr )
                if  links and t == '*link'  and attr not in (1,3):
                    kerr =[]
                    ls = maf.r_links( kerr, attr_num = attr, num_row= row)
                    if is_err( kerr ) >= 0 :      
                        print ('91 HTMS_Low_Err    Error read link field. attr=%s, row=%s, err = %s' % 
                               (maf.ht.attrs[attr]['name'],str(row),str (kerr) ) )
                        #raise HTMS_Low_Err('91 HTMS_Low_Err    Error read link field.  err = %s' % str (kerr)  )
                    if ls != ():
                            print ('   %20s  -   links : %s' %  
                                   ( maf.ht.attrs[ attr ][ 'name' ] , str ( ls ) ) )
                elif  weights and t == '*weight' :
                    kerr =[]
                    ls = maf.r_weights( kerr, attr_num = attr, num_row= row)
                    if is_err( kerr ) >= 0 :      
                        print ('92 HTMS_Low_Err    Error read weight field. attr=%s, row=%s, err = %s' % 
                               (maf.ht.attrs[attr]['name'],str(row),str (kerr) ) )
                        #raise HTMS_Low_Err('91 HTMS_Low_Err    Error read weight field.  err = %s' % str (kerr)  )
                    if ls != ():
                            print ('   %20s  -   weights : %s' %  ( maf.ht.attrs[ attr ][ 'name' ] , str ( ls ) ) )

        if level == "" or  level=="up":
            back_link = maf.r_links( kerr, attr_num = 1, num_row= row)
            if is_err( kerr ) >= 0 :      
                print ('90 HTMS_Low_Err    Error read Back_link field. row=%s, err = %s' % (str(row),str (kerr) ) )
                #raise HTMS_Low_Err('90 HTMS_Low_Err    Error read link field.  err = %s' % str (kerr)  )
            if back_link != ():
                print ('    row  %4d  -  back links : %s' %  ( row, str ( back_link ) ) )
            else:
                print ('    row  %4d  -  no back links' % row  )
            back_weight = maf.r_links( kerr, attr_num = 3, num_row= row)
            if is_err( kerr ) >= 0 :      
                print ('91 HTMS_Low_Err    Error read Back_weight field. row=%s, err = %s' % (str(row),str (kerr) ) )
                #raise HTMS_Low_Err('90 HTMS_Low_Err    Error read link field.  err = %s' % str (kerr)  )
            if back_weight != ():
                print ('    row  %4d  -  back weights : %s' %  ( row, str ( back_weight ) ) )
            else:
                print ('    row  %4d  -  no back weights' % row  )
    print('')
    return

#----------------------------------------------------------------------------------------------------------------

def ht_dump ( Kerr, ht, maf_num_p=0):
        ht.cage.push_all ( Kerr)
        if maf_num_p ==0:
            print("\n   ***          HT = %s    server - '%s'    ht_root - '%s' " % (ht.ht_name, ht.server_ip, ht.ht_root ) ) 
            print("                MAFs    = %3d"% len(ht.mafs) )
            print("                ATRs    = %3d"%  len(ht.attrs) )
            print("                A_free  = %10d    channel AF  =%4d"% ( ht.a_free,  ht.channels [ 'af' ]) )
            print("                B_free  = %10d    channel BF  =%4d"% ( ht.b_free,  ht.channels [ 'bf' ]) )
            print("                C_free  = %10d    channel CF = %4d"% ( ht.c_free,  ht.channels [ 'cf' ]) )

        if len(ht.mafs) > 0:
            for maf_num in ht.mafs:
                if maf_num_p ==0 or maf_num == maf_num_p:
                    if ht.mafs[ maf_num ][ 'opened' ]:
                        chan = str ( ht.mafs_opened[ maf_num].ch )
                        attr_maf_number= len ( ht.mafs_opened[ maf_num ].fields )
                        print("     MAF name - '%16s'    maf_num=%3d    channel =%4s    attrs (columns) = %2d    rows (objects) =%4d"% \
                        ( ht.mafs[ maf_num][ 'name' ], maf_num,  chan,  attr_maf_number,    ht.mafs[ maf_num][ 'rows' ]) )    
                        print("         attributes : %s" % str( tuple( ' '+ ht.attrs[ attr ][ 'name' ]   \
                            for attr in  ht.attrs if  ht.models [ maf_num] [ attr ] ) ) )      

                    else:
                        chan = ' ** ' 



