# HTMS low level  v. 2.3 (Cage class v. 2.9)
# © A.S.Aliev, 2018-2021

PAGESIZE1 = 16 * 2 ** 10  # 16Kb     size of one page in buffer

NUMPAGES1 = 2 ** 10  # 1024    number of pages in buffer

MAXSTRLEN1 = 16 * 2 ** 20  # 16Mb   max length (amount) of byte's data arrays
#   in read/write file operations

PAGESIZE2=                     16 * 2**20      #16 Mb   size of one page in buffer

NUMPAGES2=                     3                      # number of pages in buffer

SERVER_IP_DNS =                '127.1.0.0'          # default cage file server IP address

MAIN_SERVER_PORT=              '3570'                 # default cage file server common port

HTDB_ROOT=                      ''

MAX_FILE_NAME_LEN=              32

MAX_LEN_FILE_DESCRIPTOR=        256

MAX_LEN_FILE_BODY=              16 * 2**30   #  16 Gb 

CAGE_SERVER=                   'htms_cage_server'       # conditional name for cage server with HTMS files

DEBUG_UPDATE_CF_1 =            False

DEBUG_UPDATE_CF_2 =            False

DEBUG_UPDATE_RAM =             False

JWTOKEN =                      ""

CAGE_SERVER_WWW =              '127.1.0.0:3570' #     SERVER_IP_DNS+":"+ MAIN_SERVER_PORT

#-----------------------------------------------------

class HTMS_Low_Err(Exception):
    pass