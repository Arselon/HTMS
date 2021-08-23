# HTMS low level  v. 2.3 (Cage class v. 2.9)
# © A.S.Aliev, 2018-2021

import os
import posixpath
import pickle
import struct
import weakref
import copy
import time
import os.path as path
import jwt           
from jwt.exceptions import InvalidTokenError
import zmq
  
from cage_api             import  *

from .htms_par_low        import  *
from .data_types          import  *
from .maf                 import  *

Mod_name = "*" + __name__

from tqdm import tqdm
tqdm.monitor_interval = 0

class HT(object):

    _instances = set()

    def __str__(self):
        return self.ht_name

    def __init__(
        self,
        ht_name="",
        server_ip="",  # ip address:port
        cage_name="",
        ht_root="",
        ext={
            "maf": ".maf",
            "adt": ".htd",
            "af": ".af",
            "bf": ".bf",
            "cf": ".cf",
            "bak_htd": ".htb",
            "bak_maf": ".mab",
            "tmp": ".tmp",
            "log": ".htl",
        },
        new=False,
        jwtoken="",
        zmq_context=False,
        from_subclass=False,
        mode='wm'
    ):

        #tqdm.monitor_interval = 0
        if ht_name == "":
            self.ht_name = "htms_test"
        else:
            self.ht_name = ht_name

        if server_ip == "":
            if (
                "SERVER_IP_DNS" not in globals() or SERVER_IP_DNS == ""
            ):  # SERVER_IP_DNS - setting  from htms_par_low.py
                self.server_ip = "127.0.0.1:"
            else:
                self.server_ip = SERVER_IP_DNS + ":"
            if (
                "MAIN_SERVER_PORT" not in globals()
            ):  # MAIN_SERVER_PORT - setting  from htms_par_low.py
                self.server_ip += str(3570)
            else:
                self.server_ip += str(MAIN_SERVER_PORT)
        else:
            self.server_ip = server_ip

        for h_t in HT.getinstances():
            if  hasattr(h_t, 'ht_root') and\
                h_t.server_ip == self.server_ip and\
                h_t.ht_name == self.ht_name and\
                h_t.ht_root == ht_root:

                if h_t.mode == 'rs':
                    if mode == 'rs':
                        pr ('02-0 HTMS_Low_Err HT   WARNING  Data base  "%s" already exist and opened readonly. '
                            % ht_name )
                    else:
                        pr ('02-1 HTMS_Low_Err HT     Data base  "%s" already exist and opened, that is incompatible with exclusive status.'% ht_name)
                        raise HTMS_Low_Err('02-1 HTMS_Low_Err HT     Data base  "%s" already exist and opened, that is incompatible with exclusive status.'% ht_name )
                        
                else:
                    pr ('02-2 HTMS_Low_Err HT     Data base  "%s" already exist and opened in incompatible mode "%s". '%
                       (ht_name, h_t.mode) )
                    raise HTMS_Low_Err('02-2 HTMS_Low_Err HT     Data base  "%s" already exist and opened in incompatible mode "%s". '%
                       (ht_name, h_t.mode) )

        if not from_subclass:
            self.weak= weakref.ref(self)
            self._instances.add(self.weak)
            self.__class__._instances.add(self.weak)
        else:
            weak= self._instances
            self.__class__._instances.update(weak)
           

        self.mode= mode
        self.ht_root = ht_root
        self.ext = ext

        self.channels = {}

        if zmq_context== None or zmq_context==False :
            self.zmq_context = zmq.Context()
        else:
            self.zmq_context= zmq_context

        if cage_name == "":
            self.cage_name = self.ht_name
        else:
            self.cage_name = cage_name

        """
        if self.ht_root == "":
            if (
                "HTDB_ROOT" not in globals() or HTDB_ROOT == ""
            ):  # HTDB_ROOT - setting  from htms_par_low.py
                p = self.server_ip.find(":")
                host = self.server_ip[:p]
                if host in ("localhost", "127.0.0.1"):
                    base_dir = os.path.dirname(
                        os.path.dirname(os.path.abspath(__file__))
                    )
                    self.ht_root = posixpath.join(
                        *(base_dir.split(os.path.sep) + ["htms_db"])
                    )
                else:
                    self.ht_root = "C:/htms"
            else:
                self.ht_root = HTDB_ROOT
        """

        self.cage2_name =  "2"+self.cage_name

        if len(self.ht_name) > MAX_FILE_NAME_LEN:
            self.ht_name = self.ht_name[0:MAX_FILE_NAME_LEN]
            pr( "HTMS_Low_Warning    HT name %s truncated up to  %d chars." % 
               (self.ht_name, MAX_FILE_NAME_LEN)
                )

        if jwtoken== ""  : 
            pr('01 HTMS_Low_Err HT  Error during initializing HT "%s" . JWT empty.' % self.ht_name)
            raise HTMS_Low_Err(
                '01 HTMS_Low_Err HT  Error during initializing HT "%s" . JWT empty.' % self.ht_name
            )
        else:
            self.jwtoken =jwtoken
            try:
                self.payload = jwt.decode(self.jwtoken, algorithms=['HS256'], options={"verify_signature": False})
            except InvalidTokenError as err:
                pr( '02HTMS_Low_Err HT  Error during initializing HT "%s" . Invalid JW token, error: %s' \
                    % (self.ht_name, HTMS_Low_Err) 
                )     
                raise HTMS_Low_Err(
                    '02 HTMS_Low_Err HT  Error during initializing HT "%s" . Invalid JW token, error: %s' \
                    % (self.ht_name, HTMS_Low_Err) 
                )    
        if new and \
            (self.payload [ 'permission'] == 'low' or \
            self.mode == "rs"):
                pr(
                    '030 HTMS_Low_Err HT  JWT or mode incompatible for creating new HT "%s".'
                    % (self.mode, self.ht_name)
                )
                raise HTMS_Low_Err(
                    '030 HTMS_Low_Err HT  JWT or mode incompatible for creating new HT "%s".'
                    % (self.mode, self.ht_name)
                )           
        elif self.payload [ 'permission'] == 'low' and self.mode != "rs":
                pr( 'Attempt open HT with mode "%s"  for permission "%s". Mode was downgraded to "rs". '
                                            % (self.mode, self.payload [ 'permission'])
                    )
                self.mode = "rs"
        elif self.payload [ 'permission'] == 'standard' and self.mode == "rm" :
                pr( 'Attempt open/create HT with mode "%s"  for permission "%s". Mode was downgraded to "rs". '
                                            % (self.mode, self.payload [ 'permission'])
                    )
                self.mode = "rs"
        elif self.payload [ 'permission'] == 'standard' and self.mode in ("wm","sp") :
                pr( 'Attempt open/create HT with mode "%s"  for permission "%s". Mode was downgraded to "ws". '
                                            % (self.mode, self.payload [ 'permission'])
                    )
                self.mode = "ws"
        elif self.payload [ 'permission'] == 'high' and self.mode == "sp" :
                pr( 'Attempt open/create HT with mode "%s"  for permission "%s". Mode was downgraded to "wm". '
                                            % (self.mode, self.payload [ 'permission'])
                    )
                self.mode = "wm"


        if new:

            self.a_free = 0
            self.b_free = 0
            self.c_free = 0
            self.created = time.time()
            self.updated = self.created

            self.namef = {}

            self.attrs = {}
            self.mafs = {}
            self.mafs_opened = {}
            self.models = ((False,),)
            self.maxstrlen = MAXSTRLEN1

            if not self.create_files():
                pr(
                    '03 HTMS_Low_Err HT  Error during creating files for new HT "%s".'
                    % self.ht_name
                )
                raise HTMS_Low_Err(
                    '03 HTMS_Low_Err HT  Error during creating files for new HT "%s".'
                    % self.ht_name
                )
            if not self.save_adt():
                pr(
                    '04 HTMS_Low_Err HT  Error during saving memory of new HT "%s" in HTD file.'
                    % self.ht_name
                )
                raise HTMS_Low_Err(
                    '04 HTMS_Low_Err HT  Error during saving memory of new HT "%s" in HTD file.'
                    % self.ht_name
                )

        else:
            try:
                self.cage = Cage(
                    cage_name=self.cage_name+(b"\x00" * 4).decode('utf-8')+self.jwtoken,
                    pagesize=PAGESIZE1,
                    numpages=NUMPAGES1,
                    maxstrlen=MAXSTRLEN1,
                    server_ip={CAGE_SERVER_NAME: self.server_ip},
                    awake=False,
                    cache_file=CACHE_FILE,
                    zmq_context=self.zmq_context,
                    mode=self.mode
                )
            except Exception as er:
                pr('05 HTMS_Low_Err HT  Error "%s" from Cage One during initializing HT "%s" .' % (er,self.ht_name))
                raise HTMS_Low_Err(
                    '05 HTMS_Low_Err HT  Error "%s" from Cage One during initializing HT "%s" .' % (er,self.ht_name)
                )
            else:
               #time.sleep(2.0)
                try:
                    self.cage2 = Cage(
                        cage_name=self.cage2_name+(b"\x00" * 4).decode('utf-8')+self.jwtoken,
                        pagesize=PAGESIZE2,
                        numpages=NUMPAGES2,
                        maxstrlen=MAXSTRLEN1,
                        server_ip={CAGE_SERVER_NAME: self.server_ip},
                        awake=False,
                        cache_file=CACHE_FILE2,  # CACHE_FILE - setting  from cage_par_cl.py
                        zmq_context=self.zmq_context,
                        mode=self.mode
                    )
                except Exception as er:
                    pr('06 HTMS_Low_Err HT  Error "%s" from Cage Two during initializing HT "%s" .' % (er,self.ht_name))
                    raise HTMS_Low_Err(
                        '06 HTMS_Low_Err HT  Error "%s" from Cage Two during initializing HT "%s" .' % (er,self.ht_name)
                    )
       #time.sleep(2.0)
        if not self.activate():
            del self.cage
            del self.cage2
            pr('07 HTMS_Low_Err HT  Error during initializing HT "%s" .' % self.ht_name)
            raise HTMS_Low_Err(
                '07 HTMS_Low_Err HT  Error during initializing HT "%s" .' % self.ht_name
            )
        pr('HT  "%s"  initialized'% self.ht_name)

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

    # ------------------------------------------------------------------------------------------------

    def __del__(self):
        try:
            self.close()
           #time.sleep(0.1)           
            del self
        except:
            pass

    # ----------------------------------------------------------------------------------------------------

    def get_maf_num(self, maf_name=""):
        for maf_num in self.mafs:
            if self.mafs[maf_num]["name"] == maf_name:
                return maf_num
        return 0

    # -------------------------------------------------------------------------------------------

    def get_attr_num_and_type(self, attr_name=""):
        for attr_num in self.attrs:
            if self.attrs[attr_num]["name"] == attr_name:
                return (attr_num, self.attrs[attr_num]["type"])
        return ()

    # -------------------------------------------------------------------------------------------

    def create_files(self):
        Kerr = []
        try:
            self.cage = Cage(
                cage_name=self.cage_name+(b"\x00" * 4).decode('utf-8')+self.jwtoken,
                Kerr=Kerr,
                pagesize=PAGESIZE1,
                numpages=NUMPAGES1,
                maxstrlen=MAXSTRLEN1,
                server_ip={CAGE_SERVER_NAME: self.server_ip},
                awake=False,
                cache_file=CACHE_FILE,  # Cache_file - setting  from cage_par_cl.py
                mode=self.mode
            )
        except Exception as er:
                pr('10 HTMS_Low_Err HT  Error "%s" from Cage during creating HT "%s" .' % (er,self.ht_name))
                raise HTMS_Low_Err(
                    '10 HTMS_Low_Err HT  Error "%s" from Cage during creating HT "%s" .' % (er,self.ht_name)
                )     
        else:
           #time.sleep(1.0)
            try:
                self.cage2 = Cage(
                    cage_name=self.cage2_name+(b"\x00" * 4).decode('utf-8')+self.jwtoken,
                    Kerr=Kerr,
                    pagesize=PAGESIZE2,
                    numpages=NUMPAGES2,
                    maxstrlen=MAXSTRLEN1,
                    server_ip={CAGE_SERVER_NAME: self.server_ip},
                    awake=False,
                    cache_file=CACHE_FILE2,  # Cache_file - setting  from cage_par_cl.py
                    mode=self.mode
                )
            except Exception as er:
                    pr('11 HTMS_Low_Err HT  Error "%s" from Cage during creating HT "%s" .' % (er,self.ht_name))
                    raise HTMS_Low_Err(
                        '11 HTMS_Low_Err HT  Error "%s" from Cage during creating HT "%s" .' % (er,self.ht_name)
                    )
        self.namef["adt"] = posixpath.join(
            *(self.ht_root.split(os.path.sep) + [self.ht_name + self.ext["adt"]])
        )
        self.namef["af"] = posixpath.join(
            *(self.ht_root.split(os.path.sep) + [self.ht_name + self.ext["af"]])
        )
        self.namef["bf"] = posixpath.join(
            *(self.ht_root.split(os.path.sep) + [self.ht_name + self.ext["bf"]])
        )
        self.namef["cf"] = posixpath.join(
            *(self.ht_root.split(os.path.sep) + [self.ht_name + self.ext["cf"]])
        )

        for f in self.namef:
           #time.sleep(0.5)
            if f == "bf":
                rc1 = self.cage2.file_create(CAGE_SERVER_NAME, self.namef[f], Kerr)
            else:

                rc1 = self.cage.file_create(CAGE_SERVER_NAME, self.namef[f], Kerr)
            if rc1 == True:
                continue
            elif (
                rc1 == -1 or rc1 == -2
            ):  # file already exist and closed (-1)/ opened(-2)
                # delete old file because new HT creating

                if len(Kerr) > 0:
                    Kerr.pop()
                if len(Kerr) > 0:
                    Kerr.pop()

                pr('***  HT ***  create files - FILE %s  already exist and rc = %d  ( closed (-1)/ opened(-2) )'%(self.namef[f], rc1))

                rc11 = self.cage.file_remove(CAGE_SERVER_NAME, self.namef[f], Kerr)
               #time.sleep(0.1)
                if rc11 == True:
                    rc12 = self.cage.file_create(CAGE_SERVER_NAME, self.namef[f], Kerr)
                    if rc12 == True:
                        continue
                set_err_int(
                    Kerr,
                    Mod_name,
                    "create_files " + self.ht_name,
                    1,
                    message="Error during new file %s creating." % self.namef[f],
                )
                return False
            else:
                set_err_int(
                    Kerr,
                    Mod_name,
                    "create_files " + self.ht_name,
                    2,
                    message="Error during new file %s creating." % self.namef[f],
                )
                return False

        return True

    # -------------------------------------------------------------------------------------------

    def save_adt(self, Kerr=[]):

        if self.mode in ('rs', 'rm' ):
                set_warn_int(
                    Kerr,
                    Mod_name,
                    "save_adt " + self.ht_name,
                    20,
                    ' Mode "%s" incompatible in save_adt for HT "%s".'
                    % (self.mode, self.ht_name)
                )
                return True

        mafs={}
        if "mafs" in self.__dict__ and self.mafs !={}:
            for nmaf in self.mafs.keys():
                mafs[ nmaf ]= copy.deepcopy(self.mafs[nmaf]) 
                mafs[ nmaf ]["opened"] = False

        self.updated = time.time()
        dict_adt =  {
            "server_ip": self.server_ip,
            "ht_name": self.ht_name,
            "maxstrlen": self.maxstrlen,
            "models": self.models,
            "mafs": mafs,
            "attrs": self.attrs,
            "updated": self.updated,
            "created": self.created,
            "c_free": self.c_free,
            "b_free": self.b_free,
            "a_free": self.a_free,    
        }

        if  hasattr(self,"relations") :
             dict_adt ["relations"]= self.relations

        if  hasattr(self,"db_name") :
             dict_adt ["db_name"]= self.db_name
        
        mem = pickle.dumps(dict_adt)
        len_adt = len(mem)
        len_adt_bytes = struct.pack(">L", len_adt)

        if self.channels.get("adt") == None:
                self.channels = {}
                self.channels["adt"] = self.cage.open(
                    CAGE_SERVER_NAME, self.namef["adt"], Kerr )
               #time.sleep(0.1)
        if self.channels["adt"] == False:
                    set_err_int(
                        Kerr,
                        Mod_name,
                        "save_adt " + self.ht_name,
                        1,
                        message="HT ADT file not opened.",
                    )
                    return False
        elif self.channels["adt"] < 0 :  # File  %s  already opened by this client in  mode requared
                    set_warn_int(
                        Kerr,
                        Mod_name,
                        "save_adt " + self.ht_name,
                        2,
                        message="ADT file already opened by this client.",
                    )
                    self.channels["adt"] = -self.channels["adt"]

        if not self.cage.write(self.channels["adt"], 0, len_adt_bytes, Kerr):
                set_err_int(
                    Kerr,
                    Mod_name,
                    "save_adt " + self.ht_name,
                    3,
                    message="Counter not saved in ADT file.",
                )
                return False
           #time.sleep(0.1)
        if not self.cage.write(self.channels["adt"], 4, mem, Kerr):
                set_err_int(
                    Kerr,
                    Mod_name,
                    "save_adt " + self.ht_name,
                    4,
                    message="Dictionary of HT instance not saved in ADT file.",
                )
                return False

        self.cage.put_pages(self.channels["adt"], Kerr)

        self.updated = time.time()

        return True

    # -------------------------------------------------------------------------------------------

    def activate(self):
        Kerr = []
        nameadt = posixpath.join(
            *(self.ht_root.split(os.path.sep) + [self.ht_name + self.ext["adt"]])
        )
        self.channels["adt"] = self.cage.open(CAGE_SERVER_NAME, nameadt, Kerr)
       #time.sleep(0.1)
        if self.channels["adt"] == False:
            set_err_int(
                Kerr,
                Mod_name,
                "activate " + self.ht_name,
                1,
                message="HT ADT file not opened.",
            )
            return False
        Kerr = []
        len_adt_bytes = self.cage.read(self.channels["adt"], 0, 4, Kerr)
        len_adt = struct.unpack(">L", len_adt_bytes)[0]
        mem = self.cage.read(self.channels["adt"], 4, len_adt, Kerr)
        if mem == False:
            set_err_int(
                Kerr,
                Mod_name,
                "activate " + self.ht_name,
                2,
                message="HT ADT file not opened.",
            )
            return False

        dict_adt = pickle.loads(mem)

        if self.ht_name != dict_adt["ht_name"]  :
            set_err_int(
                Kerr,
                Mod_name,
                "activate " + self.ht_name,
                5,
                message="HT name not valid.\n",
            )
            return False

        self.a_free = dict_adt["a_free"]
        self.b_free = dict_adt["b_free"]
        self.c_free = dict_adt["c_free"]
        self.created = dict_adt["created"]
        self.updated = dict_adt["updated"]

        self.attrs = dict_adt["attrs"]
        self.mafs = dict_adt["mafs"]
        if len(self.mafs) > 0:
            for nmaf in self.mafs:
                self.mafs[nmaf]["opened"] = False
        self.models = dict_adt["models"]
        self.namef = {}  # self.namef=         dict_adt[ 'namef' ]

        if "relations" in dict_adt:
            self.relations = dict_adt["relations"]

        if "db_name" in dict_adt:
            self.db_name = dict_adt["db_name"]

        if dict_adt["maxstrlen"]<= MAXSTRLEN1:
            self.maxstrlen = dict_adt["maxstrlen"]
        else:
            self.maxstrlen = MAXSTRLEN1

        self.mafs_opened = {}

        if self.ht_name != dict_adt["ht_name"]:
            set_warn_int(
                Kerr,
                Mod_name,
                "activate " + self.ht_name,
                3,
                message="HT name mismutch: __init__ parameter and readed from ADT file.\n Accepted parameter (ht_name).",
            )

        if self.server_ip != dict_adt["server_ip"]:
            set_warn_int(
                Kerr,
                Mod_name,
                "activate " + self.ht_name,
                4,
                message="HT ip adress:port mismutch: __init__ parameter and readed from ADT file.\n Accepted parameter ( server_ip ).",
            )

        # if  self.ht_root  != dict_adt[ 'ht_root' ]:
        # set_warn_int (Kerr, Mod_name, 'activate '+self.ht_name, 5, \
        #      message='Full pathes mismutch: __init__ parameter and readed from ADT file.\n WIll be generated from parameter ( ht_root ).')
        self.namef["adt"] = nameadt
        self.namef["af"] = posixpath.join(
            *(self.ht_root.split(os.path.sep) + [self.ht_name + self.ext["af"]])
        )
        self.namef["bf"] = posixpath.join(
            *(self.ht_root.split(os.path.sep) + [self.ht_name + self.ext["bf"]])
        )
        self.namef["cf"] = posixpath.join(
            *(self.ht_root.split(os.path.sep) + [self.ht_name + self.ext["cf"]])
        )

        self.channels["af"] = self.cage.open(
            CAGE_SERVER_NAME, self.namef["af"], Kerr )
       #time.sleep(0.1)
        self.channels["cf"] = self.cage.open(
            CAGE_SERVER_NAME, self.namef["cf"], Kerr )
        self.channels["bf"] = self.cage2.open(
            CAGE_SERVER_NAME, self.namef["bf"], Kerr )
       #time.sleep(0.1)
       #time.sleep(0.1)
        if (
            not self.channels["af"]
            or not self.channels["bf"]
            or not self.channels["cf"]
        ):
            set_err_int(
                Kerr,
                Mod_name,
                "activate " + self.ht_name,
                6,
                message="HT file(-s) not opened.\n",
            )
            return False

        return True

    # -------------------------------------------------------------------------------------------

    def close(self, Kerr=[]):

        if "channels" in self.__dict__ and self.channels!={}:

            if "mafs" in self.__dict__ and self.mafs != {}:
                for maf_num in self.mafs:
                    if self.mafs[maf_num]["opened"]:
                        if self.mafs_opened != {} and maf_num in self.mafs_opened:
                            if not self.mafs_opened[maf_num].close(Kerr):
                                pr(
                                    "17 HTMS_Low_Err    MAF not closed - %d ( %s )."
                                    % (maf_num, self.mafs[maf_num]["name"])
                                )
                                Kerr = []
                                raise HTMS_Low_Err(
                                    "17 HTMS_Low_Err    MAF not closed - %d ( %s )."
                                    % (maf_num, self.mafs[maf_num]["name"])
                                )
                           #time.sleep(0.1)
                        self.mafs[maf_num]["opened"] = False

            if self.mode not in ('rs', 'rm' ):
                try:
                    self.save_adt(Kerr)
                except:
                    pr("18 HTMS_Low_Err    ADT file not saved.  Kerr= %s" % str (Kerr))
                    Kerr = []
                    raise HTMS_Low_Err("18 HTMS_Low_Err   ADT file not saved.  Kerr= %s" % str (Kerr))

            if "cage" in self.__dict__:

                # self.cage.stat(Kerr)
                rc1= True
                rc2= True
                rc3= True
                rc4= True
                if 'cf' in self.channels: 
                    rc1 = self.cage.close(self.channels["cf"], Kerr)
                    del self.channels["cf"]
                   #time.sleep(0.1)
                if 'bf' in self.channels: 
                    rc2 = self.cage2.close(self.channels["bf"], Kerr)
                    del self.channels["bf"]
                   #time.sleep(0.1)
                if 'af' in self.channels: 
                    rc3 = self.cage.close(self.channels["af"], Kerr)
                    del self.channels["af"]
                   #time.sleep(0.1)
                if 'adt' in self.channels: 
                    rc4 = self.cage.close(self.channels["adt"], Kerr)
                    del self.channels["adt"]
                   #time.sleep(0.1)
                if not rc1 or not rc2 or not rc3 or not rc4:
                    pr("19 HTMS_Low_Err   HT file(-s) not closed.  rc= cf:%s  bf: %s  af:%s  adt: %s "%\
                        (str(rc1), str(rc2),str(rc3),str(rc4)))
                    Kerr = []
                    raise HTMS_Low_Err("19 HTMS_Low_Err   HT file(-s) not closed. rc= cf:%s  bf: %s  af:%s  adt: %s "% \
                        (str(rc1), str(rc2),str(rc3),str(rc4)))

                # self.cage.stat(Kerr)

        if "cage" in self.__dict__:
            del self.cage

        if "cage2" in self.__dict__:

            del self.cage2

        HT.removeinstances(self)

        del self
        return True

    # -------------------------------------------------------------------------------------------

    def update_attrs(self, add_attrs={}):

        if self.mode in ('rs', 'rm' ) :
                pr(
                    '201 HTMS_Low_Err HT  Mode "%s" incompatible for atributes modify in HT "%s".'
                    % (self.mode, self.ht_name)
                )
                raise HTMS_Low_Err(
                    '201 HTMS_Low_Err HT  Mode "%s" incompatible for atributes modify in HT "%s".'
                    % (self.mode, self.ht_name)
                )

        """
        if  len( self.attrs) ==0 :
            kerr=[]
            self.attribute(kerr, fun='add', attr_name='Back_links', type= '*link')
            if is_err( kerr ) >= 0 :      
                pr ('19 HTMS_Low_Err    Error create "Back_links" HT atrribute.  err = %s' % str (kerr) )
                raise HTMS_Low_Err('19 HTMS_Low_Err    Error create new HT atrribute.  err = %s' % str (kerr)  )
            kerr=[]
        """
        if len(add_attrs) == 0:
            return 0

        found = False
        for attr_name in add_attrs:
            for attr_num in self.attrs.keys():
                if self.attrs[attr_num]["name"] == attr_name:
                    found = True
                    break
            if found:
                continue
            else:
                kerr = []
                self.attribute(
                    kerr, fun="add", attr_name=attr_name, type=add_attrs[attr_name]
                )
                if is_err(kerr) >= 0:
                    pr(
                        "20 HTMS_Low_Err    Error create new HT atrribute.  err = %s"
                        % str(kerr)
                    )
                    raise HTMS_Low_Err(
                        "20 HTMS_Low_Err    Error create new HT atrribute.  err = %s"
                        % str(kerr)
                    )

        self.updated = time.time()

        return len(add_attrs)

    # -------------------------------------------------------------------------------------------

    def attribute(
        self, Kerr=[], fun="add", attr_name="", type=None, newname="", attr_num_p=0
    ):

        if self.mode in ('rs', 'rm' ) and fun != "info":
            set_err_int(
                    Kerr,
                    Mod_name,
                    "attribute " + self.ht_name,
                    13,
                    message='Mode "%s" incompatible for atributes modify in HT "%s".'
                    % (self.mode, self.ht_name)
            )
            return False

        if fun == "add":
            if type not in Types_htms.types:
                set_err_int(
                    Kerr,
                    Mod_name,
                    "attribute " + self.ht_name,
                    1,
                    message='Argument "type" is wrong.',
                )
                return False
            t = time.time()
            if len(self.attrs) == 0:
                self.attrs[1] = {
                    "name": attr_name,
                    "type": type,
                    "setted": t,
                    "updated": t,
                }
                self.updated = time.time()
                return True
            else:
                for natr in self.attrs:
                    if attr_name == self.attrs[natr]["name"]:
                        set_err_int(
                            Kerr,
                            Mod_name,
                            "attribute " + self.ht_name,
                            2,
                            message='Attribute name "%s" is wrong - already used.'
                            % attr_name,
                        )
                        return False

                new_num = max(self.attrs.keys()) + 1
                self.attrs[new_num] = {
                    "name": attr_name,
                    "type": type,
                    "setted": t,
                    "updated": t,
                }

                new_models = (False,)
                if len(self.mafs) > 0:
                    for nmaf in self.mafs:
                        new_models += (self.models[nmaf] + (False,),)

                self.models = new_models
                del new_models
                self.updated = time.time()

                if self.mode not in ('rs', 'rm' ):
                    if not self.save_adt(Kerr):
                        set_err_int(
                            Kerr,
                            Mod_name,
                            "attribute " + self.ht_name + "-" + str(self.maf_num),
                            3,
                            message="Error save HTD.",
                        )
                        return False

                self.updated = time.time()

                return True

        elif fun == "new_name":
            if len(self.attrs) == 0:
                set_err_int(
                    Kerr,
                    Mod_name,
                    "attribute " + self.ht_name,
                    4,
                    message="No atributes in HT.",
                )
                return False
            if newname == "":
                set_err_int(
                    Kerr,
                    Mod_name,
                    "attrs " + self.ht_name,
                    5,
                    message='Argument "newname" is empty.',
                )
                return False
            for natr in self.attrs.keys():
                if attr_name == self.attrs[natr]["name"]:
                    self.attrs[natr]["name"] = newname
                    t = time.time()
                    self.attrs[natr]["update"] = t
                    self.updated = time.time()

                    if "relations" in self.__dict__ and attr_name in self.relations:
                        self.relations[newname] = self.relations[attr_name]
                        del self.relations[attr_name]

                    if self.mode not in ('rs', 'rm' ):
                        if not self.save_adt(Kerr):
                            set_err_int(
                                Kerr,
                                Mod_name,
                                "attribute " + self.ht_name + "-" + str(self.maf_num),
                                6,
                                message="Error save HTD.",
                            )

                            return False

                    self.updated = time.time()

                    return True

            set_err_int(
                Kerr,
                Mod_name,
                "attribute " + self.ht_name,
                7,
                message='Attribute with "attr_name" not found.',
            )
            return False

        elif fun == "delete":
            set_warn_int(
                Kerr,
                Mod_name,
                "attribute " + self.ht_name,
                8,
                message="Delete attribute not supported at low level in HT.",
            )
            return False

        elif fun == "info":
            if len(self.attrs) == 0:
                set_warn_int(
                    Kerr,
                    Mod_name,
                    "attribute " + self.ht_name,
                    9,
                    message="No atributes in HT.",
                )
                return False
            if len(attr_name) > 0:
                for natr in self.attrs.keys():
                    if attr_name == self.attrs[natr]["name"]:

                        self.updated = time.time()

                        return natr

                set_err_int(
                    Kerr,
                    Mod_name,
                    "attribute " + self.ht_name,
                    10,
                    message='Attribute with "attr_name" not found.',
                )
                return False
            else:
                if attr_num_p not in self.attrs:
                    set_err_int(
                        Kerr,
                        Mod_name,
                        "attribute " + self.ht_name,
                        11,
                        message='Attribute with number "attr_num_p" not found.',
                    )
                    return False
                else:

                    self.updated = time.time()

                    return self.attrs[attr_num_p]["name"]
        else:
            set_err_int(
                Kerr,
                Mod_name,
                "attribute " + self.ht_name,
                12,
                message="Invalid function.",
            )
            return False

    # -------------------------------------------------------------------------------------------

    def update_cf(
        self, Kerr=[], fun="", attr_num_p=0, maf_num_p=0, after_row=-1, num_rows=0
    ):

        if self.mode in ('rs', 'rm' ):
                set_err_int(
                    Kerr,
                    Mod_name,
                    "update_cf " + self.ht_name,
                    204,
                    ' Mode "%s" incompatible for update cs in MAF for HT "%s".'
                    % (self.mode, self.ht_name)
                )
                return False

        #from htorm import Obj_RAM

        if DEBUG_UPDATE_CF_1 or DEBUG_UPDATE_CF_2:
            pr(
                "\n\n  UPDATE_CF ---   fun = %s, attr_num_p=%d, maf_num_p=%d,  after_row = %d, num_rows = %d"
                % (fun, attr_num_p, maf_num_p, after_row, num_rows)
            )

        atr_remove = False
        maf_remove = False
        field_remove = False
        row_add = False
        row_delete = False

        zero = struct.pack(">LL", 0, 0)

        len_elem = 4

        if fun == "atr_remove":
            atr_remove = True
            set_warn_int(
                Kerr,
                Mod_name,
                "update_cf " + self.ht_name,
                1,
                message="atr_remove not supported in this version of HTMS.",
            )
            return False
        if fun == "maf_remove":
            maf_remove = True
        if fun == "field_remove":
            field_remove = True
        if fun == "row_add":
            row_add = True
        if fun == "row_delete":
            row_delete = True

        if len(self.mafs) > 0:
            max_maf_num = max(self.mafs.keys())
        else:
            max_maf_num = 0

        if len(self.attrs) > 0:
            max_attr_num = max(self.attrs.keys())
        else:
            max_attr_num = 0

        if row_add or row_delete:
            if num_rows == 0:
                return True
            else:
                if row_add:
                    old_max_row_num = self.mafs[maf_num_p]["rows"] - num_rows
                else:
                    old_max_row_num = self.mafs[maf_num_p]["rows"] + num_rows

        if atr_remove:
            if attr_num_p < 1 or attr_num_p > max_attr_num:
                set_err_int(
                    Kerr,
                    Mod_name,
                    "update_cf " + self.ht_name,
                    2,
                    message="attr_num_p out of range.",
                )
                return False
            maf_num_p = 0
            after_row = 0
        else:
            if max_maf_num == 0:
                return True
            elif maf_num_p < 0 or maf_num_p > max_maf_num:
                set_err_int(
                    Kerr,
                    Mod_name,
                    "update_cf " + self.ht_name,
                    3,
                    message="maf_num_p out of range.",
                )
                return False

            if maf_remove:
                attr_num_p = 0  # apply to all fields of maf
                after_row = 0  # apply to all rows of maf

            else:
                if field_remove:
                    if attr_num_p < 1 or attr_num_p > max_attr_num:
                        set_err_int(
                            Kerr,
                            Mod_name,
                            "update_cf " + self.ht_name,
                            4,
                            message="attr_num_p out of range. ",
                        )
                        return False
                    after_row = 0  # apply to all rows of maf

                if row_add or row_delete:
                    if after_row < 0 or after_row > old_max_row_num:
                        set_err_int(
                            Kerr,
                            Mod_name,
                            "update_cf " + self.ht_name,
                            6,
                            message="after_row out of range.",
                        )
                        return False
                    if num_rows < 1:
                        set_err_int(
                            Kerr,
                            Mod_name,
                            "update_cf " + self.ht_name,
                            7,
                            message=" num_rows  out of range.",
                        )
                        return False

        if self.c_free == 0:
            return True

        # structure of each update_fields in cf - each elem - packed in 4 bytes integer
        #                                   |------------array of links----------------------|
        #  dim-nmaf-attr_num-nrow-{nmaf-nrow nmaf-nrow ....  nmaf-nrow} FFF
        #  |---- descriptor--------|          0                1                (num_rows-1)        | end marker b'\xFFFFFFFFFFFFFFFF'
        #  |--source of update_fields--|

        if maf_remove or atr_remove or row_delete or row_add or field_remove:
            #      edit cf
            shift = 0
            err = ""
            delay_kerr = 0
            #    loop on sets of adresses
            while shift < self.c_free:

                adr_list_new = b""  #
                descr = self.cage.read(self.channels["cf"], shift, len_elem * 4, Kerr)
                shift_descr = shift
                if descr == False:
                    set_err_int(
                        Kerr,
                        Mod_name,
                        "update_cf " + self.ht_name,
                        9,
                        message="cf - descriptor read error. shift = %d." % shift,
                    )
                    return False

                try:
                    dim, maf, atr, nrow = struct.unpack(">LLLL", descr)
                except struct.error as err:
                    set_err_int(
                        Kerr,
                        Mod_name,
                        "update_cf " + self.ht_name,
                        10,
                        message="cf unpack descriptor error = %s." % err,
                    )
                    return False

                if DEBUG_UPDATE_CF_2:
                    pr(
                        "  update_cf --  read descr :   dim =%d,    maf=%d,    atr=%d,     nrow =%d"
                        % (dim, maf, atr, nrow)
                    )

                if dim == 0 or maf == 0:  #  descriptor already cleared
                    pass

                else:
                    if (
                        maf < 0
                        or maf > max_maf_num
                        or atr < 1
                        or atr > max_attr_num
                        or nrow < 1
                    ):
                        set_err_int(
                            Kerr,
                            Mod_name,
                            "update_cf " + self.ht_name,
                            11,
                            message="cf - descriptor data are invalid.",
                        )
                        return False
                    elif (
                        (row_add or row_delete)
                        and maf == maf_num_p
                        and nrow > old_max_row_num
                    ):
                        set_err_int(
                            Kerr,
                            Mod_name,
                            "update_cf " + self.ht_name,
                            12,
                            message="cf - descriptor data are invalid.",
                        )
                        return False

                    new_nrow = 0
                    #  read and modify (if need ) descriptor of sef of adresses

                    if maf == maf_num_p or maf_num_p == 0:
                        # MAF matched or no need to consider
                        if maf_remove and maf_num_p != 0:  #  deleted MAF match
                            # clear set of adresses
                            maf = 0
                        else:
                            if atr != 0 and (atr == attr_num_p or attr_num_p == 0):
                                # attribute MAF match or not need to consider
                                if (
                                    atr_remove and attr_num_p != 0
                                ):  #  deleted attribute MAF match
                                    # clear set of adresses
                                    maf = 0
                                elif (
                                    field_remove and maf_num_p != 0 and attr_num_p != 0
                                ):
                                    # deleted attribute MAF match
                                    # clear set of adresses
                                    maf = 0
                                else:

                                    if row_delete:
                                        if (
                                            nrow > (after_row + num_rows)
                                            and nrow <= old_max_row_num
                                        ):
                                            # MAF row after deleted range - correct number
                                            new_nrow = nrow - num_rows
                                        elif nrow > after_row and nrow <= (
                                            after_row + num_rows
                                        ):
                                            # MAF row in deleted range - delete set
                                            maf = 0
                                        elif nrow > old_max_row_num:
                                            # in CF found row number greater then number of last MAF row- delete set,
                                            maf = 0
                                            delay_kerr = 470

                                    elif row_add:
                                        if nrow > after_row and nrow <= old_max_row_num:
                                            # MAF row in deleted range  and saved- correct number
                                            new_nrow = nrow + num_rows
                                        elif nrow > old_max_row_num:
                                            # found row number greater than last number before adding new rows -  delete set
                                            maf = 0
                                            delay_kerr = 471

                    if maf == 0:  # need rewrite descriptor
                        new_descr = struct.pack(
                            ">LLLL", dim, 0, atr, nrow
                        )  # null to dim in old links descriptor
                    elif new_nrow > 0:  # need rewrite descriptor
                        new_descr = struct.pack(">LLLL", dim, maf, atr, new_nrow)

                    if maf == 0 or new_nrow > 0:  # rewrite descriptor

                        if DEBUG_UPDATE_CF_2:
                            pr(
                                "  update_cf --  NEW descr  :   dim =%d,    maf=%d,    atr=%d,     nrow =%d  newrow=%d"
                                % (dim, maf, atr, nrow, new_nrow)
                            )

                        rc = self.cage.write(
                            self.channels["cf"], shift_descr, new_descr, Kerr
                        )
                        if rc == False:
                            set_err_int(
                                Kerr,
                                Mod_name,
                                "update_cf " + self.ht_name,
                                13,
                                message="cf write error.",
                            )
                            return False

                if maf == 0 or dim == 0 or atr_remove or field_remove:
                    shift += len_elem * 4 + dim * len_elem * 2

                else:  #        if maf_remove or row_delete or row_add
                    shift += len_elem * 4
                    adr_list = self.cage.read(
                        self.channels["cf"], shift, len_elem * 2 * dim, Kerr
                    )
                    pos_adr_list = shift
                    if adr_list == False:
                        set_err_int(
                            Kerr,
                            Mod_name,
                            "update_cf " + self.ht_name,
                            14,
                            message="cf - addresses set read error. shift = %d."
                            % shift,
                        )
                        return False

                    shift += dim * len_elem * 2
                    change = False
                    dim_new = dim
                    for i in range(0, dim):
                        # loop on adresses (links) in set
                        try:
                            maf1, row1 = struct.unpack(
                                ">LL",
                                adr_list[i * len_elem * 2 : (i + 1) * len_elem * 2],
                            )
                        except struct.error as err:
                            set_err_int(
                                Kerr,
                                Mod_name,
                                "update_cf " + self.ht_name,
                                15,
                                message="cf unpack address unit error = %s." % err,
                            )
                            return False

                        if DEBUG_UPDATE_CF_2:
                            pr(
                                "  update_cf --                 i =%d,    maf[ i ]=%d,   row[ i ]=%d "
                                % (i, maf1, row1)
                            )

                        if maf1 == 0 or row1 == 0:  # link is zero or link to all maf
                            continue

                        if maf1 != 0 and maf1 == maf_num_p:  # MAF matched
                            if maf_remove:  # removed MAF matched
                                # delete link
                                adr_list_new = (
                                    adr_list_new[: i * len_elem * 2]
                                    + adr_list[(i + 1) * len_elem * 2 :]
                                )
                                dim_new -= 1
                                change = True
                            elif row_delete:
                                if (
                                    row1 > (after_row + num_rows)
                                    and row1 <= old_max_row_num
                                ):
                                    # MAF row after deleted range -
                                    # correct number
                                    new_row = row1 - num_rows
                                    new_link = struct.pack(">LL", maf1, new_row)
                                    adr_list_new = (
                                        adr_list_new[: i * len_elem * 2] + new_link
                                    )  # +   adr_list [  (i+1)*len_elem*2 : ]
                                    change = True

                                    if DEBUG_UPDATE_CF_2:
                                        pr(
                                            "  update_cf --                                                      new_row[ i ]=%d "
                                            % new_row
                                        )

                                elif row1 > after_row and row1 <= (
                                    after_row + num_rows
                                ):
                                    # clear link
                                    # adr_list_new = adr_list_new [ :  i*len_elem*2 ] +  adr_list [  (i+1)*len_elem*2 : ]
                                    dim_new -= 1
                                    change = True

                                    if DEBUG_UPDATE_CF_2:
                                        pr(
                                            "  update_cf --                                                      link deleted "
                                        )

                                elif row1 > old_max_row_num:
                                    # found row number greater than last number - clear it
                                    # adr_list_new = adr_list_new [ :  i*len_elem*2 ] +   adr_list [  (i+1)*len_elem*2 : ]
                                    dim_new -= 1
                                    change = True
                                    delay_kerr = 472
                                else:
                                    adr_list_new = (
                                        adr_list_new[: i * len_elem * 2]
                                        + adr_list[
                                            i * len_elem * 2 : (i + 1) * len_elem * 2
                                        ]
                                    )

                            else:  #  if (row_add ) :
                                if row1 > after_row and row1 <= old_max_row_num:
                                    # MAF row after added range  and saved- correct number
                                    new_row = row1 + num_rows
                                    new_link = struct.pack(">LL", maf1, new_row)
                                    adr_list_new = (
                                        adr_list_new[: i * len_elem * 2] + new_link
                                    )  # +   adr_list [  (i+1)*len_elem*2 : ]
                                    change = True

                                    if DEBUG_UPDATE_CF_2:
                                        pr(
                                            "  update_cf --                                                      new_row[ i ]=%d "
                                            % new_row
                                        )

                                elif row1 > old_max_row_num:
                                    # found row number greater than last number before adding new rows -  delete set
                                    # adr_list_new = adr_list_new[ :  i*len_elem*2 ] + adr_list [  (i+1)*len_elem*2 : ]
                                    dim_new -= 1
                                    change = True
                                    delay_kerr = 473
                                else:
                                    adr_list_new = (
                                        adr_list_new[: i * len_elem * 2]
                                        + adr_list[
                                            i * len_elem * 2 : (i + 1) * len_elem * 2
                                        ]
                                    )
                        else:
                            adr_list_new = (
                                adr_list_new[: i * len_elem * 2]
                                + adr_list[i * len_elem * 2 :]
                            )

                    if change:
                        rc = self.cage.write(
                            self.channels["cf"], pos_adr_list, adr_list_new, Kerr
                        )
                        if rc == False:
                            set_err_int(
                                Kerr,
                                Mod_name,
                                "update_cf " + self.ht_name,
                                16,
                                message="cf - addresses set write error. shift = %d."
                                % shift,
                            )
                            return False
                    if dim_new < dim:  # rewrite descriptor
                        if new_nrow > 0:
                            new_descr = struct.pack(
                                ">LLLL", dim_new, maf, atr, new_nrow
                            )

                            if DEBUG_UPDATE_CF_2:
                                pr(
                                    "  update_cf --   NEW DESCR       dim =%d,    maf=%d,    atr=%d,     new_nrow=%d "
                                    % (dim_new, maf, atr, new_nrow)
                                )

                        else:
                            new_descr = struct.pack(">LLLL", dim_new, maf, atr, nrow)

                            if DEBUG_UPDATE_CF_2:
                                pr(
                                    "  update_cf --   NEW DESCR       dim =%d,    maf=%d,    atr=%d,     nrow =%d "
                                    % (dim_new, maf, atr, nrow)
                                )

                        rc = self.cage.write(
                            self.channels["cf"], shift_descr, new_descr, Kerr
                        )
                        if rc == False:
                            set_err_int(
                                Kerr,
                                Mod_name,
                                "update_cf " + self.ht_name,
                                17,
                                message="cf write error.",
                            )
                            return False

                # search of end marker
                m_found = False
                while shift < self.c_free:
                    marker = self.cage.read(
                        self.channels["cf"], shift, len_elem * 2, Kerr
                    )
                    shift += len_elem * 2
                    if marker == False:
                        set_err_int(
                            Kerr,
                            Mod_name,
                            "update_cf " + self.ht_name,
                            18,
                            message="cf read error.",
                        )
                        return False
                    elif marker == b"\xFF" * 8:
                        m_found = True
                    elif marker != b"\xFF" * 8 and m_found:
                        shift -= len_elem * 2
                        break

                if not m_found:
                    # marker not found
                    set_err_int(
                        Kerr,
                        Mod_name,
                        "update_cf " + self.ht_name,
                        19,
                        message="marker of end of link update_fields not found.",
                    )
                    return False

            if delay_kerr != 0:
                set_warn_int(
                    Kerr,
                    Mod_name,
                    "update_cf " + self.ht_name,
                    20,
                    message="cf read delayed error No= %d." % delay_kerr,
                )
                return False
            """
            if not self.cage.put_pages(self.channels["cf"], Kerr):
                set_err_int(
                    Kerr,
                    Mod_name,
                    "update_cf " + self.ht_name,
                    21,
                    message="CF push modified pages error.",
                )
                return False
            """
            if DEBUG_UPDATE_CF_1 or DEBUG_UPDATE_CF_2:
                pr("  UPDATE_CF ---   FINISH")

            if DEBUG_UPDATE_CF_1:
                for tab in self.mafs_opened:
                    links_dump(self.mafs_opened[tab])

            self.updated = time.time()

            return True
        else:
            set_err_int(
                Kerr,
                Mod_name,
                "update_cf " + self.ht_name,
                23,
                message="wrong function.",
            )
        return False


# ------------------------------------------------------------------------------------------------


def rename_ht(Kerr=[], server_ip="", ht_name="", ht_root="", 
              new_ht_name="", jwt_temp_cage="",cage_name= "", zmq_context =False):

    for db in HT.getinstances():
        if db.ht_name == ht_name:
            pr('30 HTMS_Low_Err    Data base  "%s" opened. ' % ht_name)
            raise HTMS_Low_Err('30 HTMS_Low_Err     Data base  "%s" opened. ' % ht_name)

    if jwt_temp_cage== "" and ( ("JWTOKEN" not in globals()) or JWTOKEN==None or JWTOKEN=="") : 
        pr('32 HTMS_Low_Err HT  Error during initializing HT "%s" . JWT empty.' % ht_name)
        raise HTMS_Low_Err(
                '32 HTMS_Low_Err HT  Error during initializing HT "%s" . JWT empty.' % ht_name
        )
    else:
        if jwt_temp_cage =='':
            jwtoken= JWTOKEN
        else:
            jwtoken= jwt_temp_cage
    try:
        payload = jwt.decode(jwtoken, algorithms=['HS256'], options={"verify_signature": False})
    except InvalidTokenError as err:
        pr( '33HTMS_Low_Err HT  Error during  new HT "%s" . Invalid JW token, error: %s' \
                    % (ht_name, HTMS_Low_Err) 
        )
        try:
            ht.close()
        except:
            pass
        raise HTMS_Low_Err(
            '33 HTMS_Low_Err HT  Error during initializing new HT "%s" . Invalid JW token, error: %s' \
                    % (ht_name, HTMS_Low_Err) 
        )   
    
    if zmq_context== None or zmq_context==False :
        zmq_cont = zmq.Context()
    else:
        zmq_cont= zmq_context

    ht=False
    try:
        ht = HT(
            server_ip=server_ip, 
            ht_name=ht_name, 
            ht_root=ht_root, 
            jwtoken=jwtoken,
            new=False,
            cage_name= cage_name,
            zmq_context =zmq_cont,
            mode='wm'
            )
    except:
        pr('31 HTMS_Low_Err    HT  "%s" open error. ' % ht_name)
        try:
            ht.close()
        except:
            pass
        raise HTMS_Low_Err('31 HTMS_Low_Err     HT  "%s" open error. ' % ht_name)
   #time.sleep(2.0) 

    old_file_name = {}
    old_file_name["adt"] = posixpath.join(
            *(ht_root.split(os.path.sep) + [ht_name + ht.ext["adt"]])
    )
    old_file_name["af"] = posixpath.join(
            *(ht_root.split(os.path.sep) + [ht_name + ht.ext["af"]])
    )
    old_file_name["bf"] = posixpath.join(
            *(ht_root.split(os.path.sep) + [ht_name + ht.ext["bf"]])
    )
    old_file_name["cf"] = posixpath.join(
            *(ht_root.split(os.path.sep) + [ht_name + ht.ext["cf"]])
    )

    new_file_name = {}
    new_file_name["adt"] = posixpath.join(
            *(ht_root.split(os.path.sep) + [new_ht_name + ht.ext["adt"]])
    )
    new_file_name["af"] = posixpath.join(
            *(ht_root.split(os.path.sep) + [new_ht_name + ht.ext["af"]])
    )
    new_file_name["bf"] = posixpath.join(
            *(ht_root.split(os.path.sep) + [new_ht_name + ht.ext["bf"]])
    )
    new_file_name["cf"] = posixpath.join(
            *(ht_root.split(os.path.sep) + [new_ht_name + ht.ext["cf"]])
    )

    for nmaf in ht.mafs:
        old_file_name[str(nmaf)] =posixpath.join(
            *(ht_root.split(os.path.sep) + [ht_name + "_" + str(nmaf) + ht.ext["maf"]])
        )
        new_file_name[str(nmaf)] = posixpath.join(
            *(ht_root.split(os.path.sep) + [new_ht_name + "_" + str(nmaf) + ht.ext["maf"]])
        )

    server_ip={CAGE_SERVER_NAME: ht.server_ip}

    temp_cage_name="temp_"+ht.cage_name+(b"\x00" * 4).decode('utf-8')+jwtoken

    try:
            ht.close()
    except:
            pass

    try:
        temp_cage = Cage(
            cage_name=temp_cage_name,
            pagesize=PAGESIZE1,
            numpages=NUMPAGES1,
            maxstrlen=MAXSTRLEN1,
            server_ip=server_ip,
            zmq_context =zmq_cont,
            mode='wm'
        )
    except Exception as er:
                pr('35 HTMS_Low_Err HT  Error "%s" from Cage in rename HT "%s" .' % (er,ht_name))
                raise HTMS_Low_Err(
                    '35 HTMS_Low_Err HT  Error "%s" from Cage in rename HT "%s" .' % (er,ht_name)
                )           
   #time.sleep(0.1)        
    try:
        for fil in old_file_name:
            temp_cage.file_rename(
                 server=CAGE_SERVER_NAME, 
                 path=old_file_name[fil], 
                 new_name=new_file_name[fil], 
                 Kerr=[]
            )
       #time.sleep(0.1)
    except Exception as rc:
        pr(
            '37 HTMS_Low_Err   Renaming files of HT "%s" error rc =%s '
            % (ht_name, str(rc))
        )
        del temp_cage
        raise HTMS_Low_Err(
            '37 HTMS_Low_Err    Renaming files of HT "%s" error rc =%s '
            % (ht_name, str(rc))
        )

    adt_file = temp_cage.open(CAGE_SERVER_NAME, new_file_name["adt"], Kerr)
   #time.sleep(0.1)
    if adt_file == False:
        pr('38 HTMS_Low_Err    ADT  "%s" open error. ' % ht_name)
        del temp_cage
        raise HTMS_Low_Err('38 HTMS_Low_Err     ADT  "%s" open error. ' % ht_name)
    Kerr = []
    len_adt_bytes = temp_cage.read(adt_file, 0, 4, Kerr)
    len_adt = struct.unpack(">L", len_adt_bytes)[0]
    mem = temp_cage.read(adt_file, 4, len_adt, Kerr)
    if mem == False:
        pr('39 HTMS_Low_Err    ADT  "%s" read error. ' % ht_name)
        del temp_cage
        raise HTMS_Low_Err('39 HTMS_Low_Err     ADT  "%s" read error. ' % ht_name)

    dict_adt = pickle.loads(mem)

    dict_adt["ht_name"] = new_ht_name

    if "db_name" in dict_adt:
        dict_adt["db_name"] = new_ht_name

    mem = pickle.dumps(dict_adt)
    len_adt = len(mem)
    len_adt_bytes = struct.pack(">L", len_adt)
   #time.sleep(0.1)
    if not temp_cage.write(adt_file, 0, len_adt_bytes, Kerr):
        pr(
            '42 HTMS_Low_Err    Counter not saved in ADT file  "%s" - write error. '
            % ht_name
        )
        del temp_cage
        raise HTMS_Low_Err(
            '42 HTMS_Counter not saved in ADT file  "%s" - write errorr. ' % ht_name
        )
   #time.sleep(0.1)
    if not temp_cage.write(adt_file, 4, mem, Kerr):
        pr(
            '43 HTMS_Low_Err    Dictionary of HT instance not saved in ADT file  "%s" - write error. '
            % ht_name
        )
        del temp_cage
        raise HTMS_Low_Err(
            '44HTMS_Dictionary of HT instance not saved in ADT file  "%s" - write errorr. '
            % ht_name
        )

    temp_cage.put_pages(adt_file, Kerr)

    del temp_cage
    #time.sleep(1.)
    return True

# ------------------------------------------------------------------------------------------------

def delete_ht(Kerr=[], server_ip="", ht_name="", ht_root="", jwt_temp_cage="", cage_name= "", zmq_context =False):

    for db in HT.getinstances():
        if db.ht_name == ht_name:
            pr('40 HTMS_Low_Err    Data base  "%s" opened. ' % ht_name)
            raise HTMS_Low_Err('30 HTMS_Low_Err     Data base  "%s" opened. ' % ht_name)

    if jwt_temp_cage== "" and ( ("JWTOKEN" not in globals()) or JWTOKEN==None or JWTOKEN=="") : 
        pr('46 HTMS_Low_Err HT  Error during initializing HT "%s" . JWT empty.' % ht_name)
        raise HTMS_Low_Err(
                '46 HTMS_Low_Err HT  Error during initializing HT "%s" . JWT empty.' % ht_name
        )
    else:
        if jwt_temp_cage =='':
            jwtoken= JWTOKEN
        else:
            jwtoken= jwt_temp_cage
    try:
        payload = jwt.decode(jwtoken, algorithms=['HS256'], options={"verify_signature": False})
    except InvalidTokenError as err:
        pr( '47HTMS_Low_Err HT  Error during  new HT "%s" . Invalid JW token, error: %s' \
                    % (ht_name, HTMS_Low_Err) 
        )     
        try:
            ht.close()
        except:
            pass
        raise HTMS_Low_Err(
            '47 HTMS_Low_Err HT  Error during initializing new HT "%s" . Invalid JW token, error: %s' \
                    % (ht_name, HTMS_Low_Err) 
        )  
    ht= False

    if zmq_context== None or zmq_context==False :
        zmq_cont = zmq.Context()
    else:
        zmq_cont= zmq_context

    try:
        ht = HT(
            server_ip = server_ip, 
            ht_name=ht_name, 
            ht_root=ht_root, 
            jwtoken=jwtoken,
            new=False,
            cage_name= cage_name,
            zmq_context =zmq_cont,
            mode='wm'
            )
    except:
        pr('45 HTMS_Low_Err    HT  "%s" open error. ' % ht_name)
        try:
            ht.close()
        except:
            pass
        raise HTMS_Low_Err('45 HTMS_Low_Err     HT  "%s" open error. ' % ht_name)          
   #time.sleep(2.0)
    old_file_name = {}
    old_file_name["adt"] = posixpath.join(
            *(ht_root.split(os.path.sep) + [ht_name + ht.ext["adt"]])
    )
    old_file_name["af"] = posixpath.join(
            *(ht_root.split(os.path.sep) + [ht_name + ht.ext["af"]])
    )
    old_file_name["bf"] = posixpath.join(
            *(ht_root.split(os.path.sep) + [ht_name + ht.ext["bf"]])
    )
    old_file_name["cf"] = posixpath.join(
            *(ht_root.split(os.path.sep) + [ht_name + ht.ext["cf"]])
    )

    for nmaf in ht.mafs:
        old_file_name[str(nmaf)] =posixpath.join(
            *(ht_root.split(os.path.sep) + [ht_name + "_" + str(nmaf) + ht.ext["maf"]])
        )

    server_ip={CAGE_SERVER_NAME: ht.server_ip}
    temp_cage_name="temp_"+cage_name+(b"\x00" * 4).decode('utf-8')+jwtoken

    try:
            ht.close()
    except:
            pass

    try:
        temp_cage = Cage(
            cage_name=temp_cage_name,
            pagesize=PAGESIZE1,
            numpages=NUMPAGES1,
            maxstrlen=MAXSTRLEN1,
            server_ip=server_ip,
            zmq_context =zmq_cont,
            mode='wm'
        )
    except Exception as er:
                pr('48 HTMS_Low_Err HT  Error "%s" from Cage in delete HT "%s" .' % (er,ht_name))
                del temp_cage
                raise HTMS_Low_Err(
                    '48 HTMS_Low_Err HT  Error "%s" from Cage in delete HT "%s" .' % (er,ht_name)
                ) 
   #time.sleep(0.1)
    try:
        for fil in old_file_name:                    
            temp_cage.file_remove(
                 CAGE_SERVER_NAME, 
                 old_file_name[fil], 
                 Kerr=[]
            )
            
           #time.sleep(0.1)
    except Exception as rc:
        pr(
            '49 HTMS_Low_Err   Deleting files of HT "%s" error rc =%s '
            % (ht_name, str(rc))
        )
        del temp_cage
        raise HTMS_Low_Err(
            '49 HTMS_Low_Err    Deleting files of HT "%s" error rc =%s '
            % (ht_name, str(rc))
        )
    
    del temp_cage
    #time.sleep(1.)
    return True
                                                                     
# ------------------------------------------------------------------------------------------------

def get_maf( ht_name, n_maf ):
    maf_name=''
    rows=0

    ht_obj=None
    for h_t  in HT.getinstances():
        if  h_t.ht_name ==  ht_name :
            ht_obj= h_t
            break
    if ht_obj and n_maf in ht_obj.mafs and ht_obj.mafs[n_maf]['name'][0:8] != 'deleted:':
            maf_name=ht_obj.mafs[n_maf]['name']
            rows=ht_obj.mafs[n_maf]['rows']
    return maf_name, rows


# ------------------------------------------------------------------------------------------------

def deepcopy_ht(Kerr=[], server_ip="", ht_name="", ht_root="", 
                new_ht_name="", new_ht_root ='', 
                jwt_cage="",cage_name= "",zmq_context =False):

        for db in HT.getinstances():
            if db.ht_name == ht_name:
                pr('60 HTMS_Low_Err    Data base  "%s" opened. ' % ht_name)
                raise HTMS_Low_Err('60 HTMS_Low_Err     Data base  "%s" opened. ' % ht_name)       

        if jwt_cage== "" and ( ("JWTOKEN" not in globals()) or JWTOKEN==None or JWTOKEN=="") : 
            pr('46 HTMS_Low_Err HT  Error during initializing HT "%s" . JWT empty.' % ht_name)
            raise HTMS_Low_Err(
                    '46 HTMS_Low_Err HT  Error during initializing HT "%s" . JWT empty.' % ht_name
            )
        else:
            if jwt_cage =='':
                jwtoken= JWTOKEN
            else:
                jwtoken= jwt_cage

        try:
                payload = jwt.decode(jwtoken, algorithms=['HS256'], options={"verify_signature": False})
        except InvalidTokenError as err:
                pr( '61HTMS_Low_Err HT  Error during  copy HT "%s" . Invalid JW token, error: %s' \
                                % (ht_name, HTMS_Low_Err) 
                )     
                raise HTMS_Low_Err(
                        '61 HTMS_Low_Err HT  Error during copy HT "%s" . Invalid JW token, error: %s' \
                                % (ht_name, HTMS_Low_Err) 
                ) 

        if new_ht_root =='':
             new_ht_root =ht_root

        if zmq_context== None or zmq_context==False :
            zmq_cont = zmq.Context()
        else:
            zmq_cont= zmq_context

        try:
            ht = HT(
                server_ip=server_ip, 
                ht_name=ht_name, 
                ht_root=ht_root, 
                jwtoken=jwtoken,
                new=False,
                cage_name= cage_name,
                zmq_context =zmq_cont,
                mode='wm'
                )
        except:
            pr('63 HTMS_Low_Err  Old HT  "%s" open error. ' % ht_name)
            raise HTMS_Low_Err('63 HTMS_Low_Err   Old HT  "%s" open error. ' % ht_name)

        try:
            new_ht = HT( 
                server_ip=server_ip,
                ht_name= new_ht_name, 
                ht_root = new_ht_root, 
                jwtoken= jwtoken,
                new = True,
                cage_name= '*'+cage_name,
                zmq_context =zmq_cont,
                mode='wm'
                )
        except:
            pr ('64 HTMS_Low_Err     Error creating/opening new HT for copy. ')
            try:
                ht.close()
            except:
                pass
            raise HTMS_Low_Err('64 HTMS_Low_Err      Error creating new HT for copy. ' )  

        if hasattr(ht,"relations"):
            new_ht.relations={}

       #time.sleep(0.1)
        add_attrs= {   ht.attrs[ nattr] ['name'] : ht.attrs[ nattr] ['type']    \
                                for nattr in ht.attrs   if   \
                                    not ( "relations" in ht.__dict__ and \
                                            ht.attrs[ nattr] ['name'] in ht.relations and\
                                            ht.relations[ ht.attrs[ nattr] ['name']] == 'erased')
                            }

        new_ht.update_attrs( add_attrs=add_attrs )

        new_nmafs={}
        #is_file_fields= False
        is_link_fields= False

        for old_nmaf in ht.mafs:

            if ht.mafs[old_nmaf]['name'] [ : 8] != 'deleted:':

                new_maf= MAF ( ht=new_ht , maf_name= ht.mafs[ old_nmaf] ['name'] )
               #time.sleep(0.1)
                new_nmaf=  new_maf.maf_num
                new_nmafs[ old_nmaf ] = new_nmaf
                old_maf=None
                """
                is_file_fields= False
                is_link_fields= False
                """
                for nattr in new_ht.attrs:
                    #if nattr in (1,2):
                        #continue
                    attr_name = new_ht.attrs [ nattr]['name']
                    old_nattr, old_attr_type = ht.get_attr_num_and_type( attr_name=attr_name)
                    if ht.models [ old_nmaf ] [ old_nattr ] :
                        new_maf.field( attr_num_f= nattr )
                    #if old_attr_type == 'file':
                        #is_file_fields= True
                    elif old_attr_type == '*link':
                        is_link_fields= True

                if ht.mafs[old_nmaf]['rows'] >0:
                    old_maf= MAF ( ht=ht , maf_num = old_nmaf)
                   #time.sleep(0.1)
                    for nrow in range (1, ht.mafs[old_nmaf]['rows']+1):
                        kerr=[]
                        rc=new_maf.row(kerr, fun='add',  after =nrow-1, number=1 )
                        if rc == False or  is_err( kerr ) >= 0 :    
                            Kerr=kerr
                            pr ('65 HTMS_Low_Err     Error creating new row in MAF for copy. ')
                            try:
                                ht.close()
                            except:
                                pass
                            try:
                                new_ht.close()
                            except:
                                pass
                            raise HTMS_Low_Err('65 HTMS_Low_Err      Error creating new new row in MAF for copy. ' ) 

                        for nattr in new_ht.attrs:
                            kerr=[]
                            attr_name = new_ht.attrs [ nattr]['name']
                            data_type = new_ht.attrs [ nattr][ 'type' ]
                            old_nattr = ht.get_attr_num_and_type( attr_name=attr_name)[0]

                            if  new_ht.models [ new_nmaf] [ nattr ] and  data_type != '*link':
                                rc=True
                                if data_type[:3]  == 'dat':
                                    df=  old_maf.r_utf8(Kerr=kerr, attr_num=old_nattr, num_row =nrow)
                                    if df == None:
                                        df=' '
                                    rc=  new_maf.w_utf8(Kerr=kerr,  attr_num=nattr, num_row =nrow, string=df )
                                elif data_type in ( "int4", "int8","float4","float8","time") :                   
                                    numb= old_maf.r_numbers (Kerr=kerr,  attr_num=old_nattr, num_row =nrow)
                                    rc= new_maf.w_numbers (Kerr=kerr,  attr_num=nattr, num_row =nrow, numbers = numb)
                                elif data_type in ( "*int4", "*int8","*float4","*float8") :
                                    numbers = old_maf.r_numbers (Kerr=kerr,  attr_num=old_nattr, num_row =nrow)
                                    rc= new_maf.w_numbers (Kerr=kerr,  attr_num=nattr, num_row =nrow, numbers = numbers)
                                elif data_type.find( "byte") != -1 :
                                    if data_type[0] == '*':
                                        bytes = old_maf.r_bytes(Kerr=kerr,  attr_num=old_nattr, num_row =nrow)
                                        rc =new_maf.w_bytes(Kerr=kerr,  attr_num=nattr, num_row =nrow, bytes=bytes)
                                    else:
                                        bytes = old_maf.r_elem(Kerr=kerr,  attr_num=old_nattr, num_row =nrow)
                                        rc = new_maf.w_elem(Kerr=kerr,  attr_num=nattr, num_row =nrow, elem=bytes)
                                elif data_type.find("utf") != -1 :
                                    if data_type[0] == '*':
                                        chars = old_maf.r_str (Kerr=kerr,  attr_num=old_nattr, num_row =nrow)
                                        rc = new_maf.w_str (Kerr=kerr,  attr_num=nattr, num_row =nrow, string=chars)
                                    else:
                                        chars = old_maf.r_utf8(Kerr=kerr,  attr_num=old_nattr, num_row =nrow)
                                        rc = new_maf.w_utf8(Kerr=kerr,  attr_num=nattr, num_row =nrow,  string=chars)
                                elif data_type =="file" :
                                    old_maf_elem_offset , old_maf_elem_length = old_maf.offsets [old_nattr] 

                                    old_maf_elem =  ht.cage.read(
                                            old_maf.ch,   
                                            ( nrow - 1 ) *old_maf.rowlen+old_maf_elem_offset, 
                                            old_maf_elem_length,
                                            Kerr
                                    )   
                                    if  old_maf_elem != b'\xFF'*32:
                                        old_bf_addr_file , old_file_length =   struct.unpack( '>QQQQ', old_maf_elem) [ 2: ]
                                        old_file_descr= old_maf.r_file_descr (Kerr = kerr , attr_num=old_nattr, num_row =nrow,  )

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
                                                    try:
                                                        ht.close()
                                                    except:
                                                        pass
                                                    try:
                                                        new_ht.close()
                                                    except:
                                                        pass
                                                    raise HTMS_Low_Err ('66 HTMS_Low_Err     Error read file in HT for copy. ')

                                            rc1 =  new_ht.cage2.write( 
                                                        new_ht.channels [ 'bf' ],  
                                                        new_bf_addr_file+PAGESIZE2*chunk,   
                                                        data, 
                                                        Kerr)   
                                            if rc1 == False:
                                                    pr ('67 HTMS_Low_Err     Error write file to HT copy. ')
                                                    try:
                                                        ht.close()
                                                    except:
                                                        pass
                                                    try:
                                                        new_ht.close()
                                                    except:
                                                        pass
                                                    raise HTMS_Low_Err ('67 HTMS_Low_Err     Error write file to HT copy. ')

                                        new_file_length= old_file_length
                                        new_ht.b_free +=  new_file_length

                                        try:
                                            new_file_descr= pickle.dumps ( old_file_descr)
                                        except:
                                                        pr ('68 HTMS_Low_Err     Error write file descriptor to HT copy. ')
                                                        try:
                                                            ht.close()
                                                        except:
                                                            pass
                                                        try:
                                                            new_ht.close()
                                                        except:
                                                            pass
                                                        raise HTMS_Low_Err ('68 HTMS_Low_Err     Error write file descriptor to HT copy.. ')

                                        new_af_addr_descr=new_ht.a_free

                                        rc2 =  new_ht.cage.write( 
                                                new_ht.channels [ 'af' ],  
                                                new_af_addr_descr,   
                                                new_file_descr, 
                                                kerr)   
                                        if rc2 == False:
                                                        pr ('70 HTMS_Low_Err     Error write file descriptor to HT copy. ')
                                                        try:
                                                            ht.close()
                                                        except:
                                                            pass
                                                        try:
                                                            new_ht.close()
                                                        except:
                                                            pass
                                                        raise HTMS_Low_Err ('70 HTMS_Low_Err     Error write file descriptor to HT copy.. ')
                                        
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
                                            ( nrow - 1 ) *new_maf.rowlen+new_maf_elem_offset,   
                                            new_maf_elem , 
                                            kerr
                                        )   
                                    if rc3 == False:
                                            try:
                                                ht.close()
                                            except:
                                                pass
                                            try:
                                                new_ht.close()
                                            except:
                                                pass
                                            raise HTMS_Low_Err ('72 HTMS_Low_Err   Error write MAF.' )

                new_maf.close() 
               #time.sleep(0.1)
                if old_maf != None:
                    old_maf.close()
               #time.sleep(0.1)

        if is_link_fields:
            for old_nmaf in new_nmafs:
                new_nmaf=  new_nmafs[old_nmaf]
                new_maf = MAF ( ht= new_ht, maf_num = new_nmaf )
               #time.sleep(0.1)
                old_maf =   MAF ( ht= ht, maf_num = old_nmaf)
               #time.sleep(0.1)
                if new_ht.mafs[new_nmaf]['rows'] >0:
                   for nrow in range (1, new_ht.mafs[new_nmaf]['rows'] +1):
                        for nattr in new_ht.attrs:
                            kerr=[]
                            attr_name = new_ht.attrs [ nattr]['name']
                            data_type = new_ht.attrs [ nattr][ 'type' ]
                            old_nattr = ht.get_attr_num_and_type( attr_name=attr_name)[0]
                            if  new_ht.models [ new_nmaf] [ nattr ]  and  data_type == '*link':
                                    rc=True
                                    links = old_maf.r_links(Kerr=kerr,  attr_num=old_nattr, num_row =nrow)
                                    if links==False:
                                        raise HTMS_Low_Err (
                                            '73 HTMS_Low_Err   Error read links field "%s"  in row %d  of MAF %s (%d). Kerr= %s'  %    \
                                                    ( attr_name, nrow, old_maf.maf_name, old_nmaf, str(kerr)) 
                                        )
                                    new_links=()
                                    if links !=():
                                        for li in links:
                                            nmaf_in_link=li[0]
                                            nrow_in_link=li[1]
                                            new_links+=( (new_nmafs[ nmaf_in_link], nrow_in_link),)
                                        rc = new_maf.w_links(Kerr=kerr,  attr_num=nattr, num_row =nrow, links =new_links)
                                        if rc == False:
                                            pass
                new_maf.close() 
               #time.sleep(0.1)
                old_maf.close()
               #time.sleep(0.1)

       #time.sleep(0.1)
        ht.close()
       #time.sleep(0.1)
        new_ht.close()

        return True

# -------------------------------------------------------------------------------------------


def compress_ht(Kerr=[], server = '', ht_name='', ht_root = '', jwt_temp_cage="",cage_name="", zmq_context =False):

        for db  in HT.getinstances():
            if  db.ht_name == ht_name:
                pr ('80 HTMS_Low_Err     Data base  "%s" opened. '% ht_name )
                raise HTMS_Low_Err('80 HTMS_Low_Err     Data base  "%s" opened. '% ht_name )       

        try:

            temp_db_name='_temp_'+ht_name

            if zmq_context== None or zmq_context==False :
                zmq_cont = zmq.Context()
            else:
                zmq_cont= zmq_context

            deepcopy_ht( 
                server_ip=server, 
                ht_name=ht_name, 
                ht_root=ht_root, 
                new_ht_name=temp_db_name, 
                jwt_cage=jwt_temp_cage,
                cage_name= cage_name,
                zmq_context =zmq_cont
            )
            #print ('\n\n   temp_db_name copied')
           #time.sleep(1)

            #print ('\n   db_name closed')     
           #time.sleep(1)            
            delete_ht(
                server_ip=server, 
                ht_name=ht_name,  
                ht_root = ht_root , 
                jwt_temp_cage=jwt_temp_cage,
                cage_name=cage_name,
                zmq_context =zmq_cont
            )
            #print ('\n\n   db_name deleted')    
           #time.sleep(1)
            rename_ht(
                server_ip=server, 
                ht_root=ht_root, 
                ht_name=temp_db_name, 
                new_ht_name=ht_name, 
                jwt_temp_cage=jwt_temp_cage,
                cage_name= cage_name ,
                zmq_context =zmq_cont
            )
            #print ('\n\n   db_name renamed')

        except Exception as rc:
            pr ('81 HTMS_Low_Err   Compressing of HT "%s" error   rc= %s'% (ht_name, rc) )
            raise HTMS_Low_Err('81 HTMS_Low_Err     Compressing of HT "%s" error   rc= %s'% (ht_name, rc) )

        return True

#------------------------------------------------------------------------------------------------