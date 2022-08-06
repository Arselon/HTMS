# HTMS low level  v. 3.1.0 (Cage class v. 3.1.0)
# © A.S.Aliev, 2018-2022

PAGESIZE1 =                 1 * 2 ** 20         # 1 Mb  size of one Cage page in buffer
                                                    #   for all HTMS files except bf
NUMPAGES1 =                 2 #** 10             # 1024  number of Cage pages in buffer
                                                    #   for all HTMS files except bf
MAXSTRLEN1 =                16 * 2 ** 20        # 16 Mb   max length (amount) of byte's data arrays
                                                    #        in read/write file operations
PAGESIZE2=                  16 * 2 ** 20        # 16 Mb  size of one Cage page in buffer 
                                                    #        for HTMS bf files
NUMPAGES2=                  3                   # number of Cage pages in buffer

MAX_FILE_NAME_LEN=          32

MAX_LEN_FILE_DESCRIPTOR=    256

MAX_LEN_FILE_BODY=          16 * 2**30          #  16 Gb  - for files in HTMS bf file 

MAX_GENERIC_ATTR_NUM =      100

CAGE_SERVER=                'htms_cage_server'  # conditional name for cage server with HTMS files
                                                    #        for HTMS bf files
SERVER_IP_DNS =             '127.1.0.0'         # default cage file server IP address

MAIN_SERVER_PORT=           '3570'              # default cage file server common port

HTDB_ROOT=                  ''

DEBUG_UPDATE_CF_1 =         False
DEBUG_UPDATE_CF_2 =         False
DEBUG_UPDATE_RAM =          False

JWTOKEN =                   ""

CAGE_SERVER_WWW =           '127.1.0.0:3570'    #  SERVER_IP_DNS+":"+ MAIN_SERVER_PORT   (used for debug)

CAGE_LOCAL_ROOT=            ""

#-----------------------------------------------------

class HTMS_Low_Err(Exception):
    pass