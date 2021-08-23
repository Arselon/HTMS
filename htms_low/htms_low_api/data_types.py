# HTMS  v. 2.3
# © A.S.Aliev, 2018-2021

class Types_htms( object ):

    max_int4 =  2147483647                      #   struct.pack('>i', 2147483647 ) ==                      b'\x7F\xFF\xFF\xFF'
    max_int8 =  9223372036854775807    #   struct.pack('>q', 9223372036854775807 ) ==   b'\x7F\xFF\xFF\xFF\xFF\xFF\xFF\xFF'
    nan_float4 = float('nan')                        #   struct.pack ('>f', float('nan')) ==                         b'\x7f\xc0\x00\x00'
    nan_float8 = float('nan')                        #   struct.pack ('>d', float('nan')) ==                        b'\x7f\xf8\x00\x00\x00\x00\x00\x00'
  
    types ={
	        "byte1":(1,1,b'\x00' ), "byte4":(4,4,b'\x00'*4), "byte8":(8,8,b'\x00'*8,), 	       #		elementary data - bytes array
            "utf50":(100, 100, b'\xFF'*100 ),  "utf100":(200,200, b'\xFF'*200 ),               #      elementary data - string UTF-8 (without BOM)  
            "int4":  (4,4,max_int4, '>i'),    "int8":  (8,8,max_int8, '>q' ), 		                   #		elementary data - integer
            "float4":(4,4,nan_float4, '>f'), "float8":(8,8,nan_float8, '>d'), 		                   #		elementary data - float
            "time":(8,8,0.0, '>d'),                     #    elementary data -  time.time() UTC, as float
            "datetime":(50, 50, b'\xFF'*50),      #    elementary data - date and time in user format, as UTF-8 string
                                                                  #    for example:  datetime.datetime.now().strftime('%d.%m.%y  %H:%M:%S .%f') Local time 

	        "*link":(16,4,(-1,-1)),                      #   offset and length of "link's block"(LB) in CF file 
                                                                  #   LB = LB descriptor + LB array + end marker ( b'\xFF'* 16)
                                                                  #   LB descriptor =( dimention of LB array,  maf_num origin, attr_num origin, num_row origin)
                                                                  #       dimention of LB array == 0 signalize that all content of LB is obsolete
                                                                  #   LB array = ( (nmaf, nrow), ... (nmaf, nrow))
	        "*byte":(16,1,b'\x00'),                    #   bytes array size <= MAXSTRLEN1( stores in AF file)
	        "*utf":(16,2, b'\xFF',)             ,               #  string UTF-8 size <= MAXSTRLEN1/2 ( stores in AF file, without BOM)  
            "*int4":  (16,4,max_int4, '>i'),   "*int8":  (16,8,max_int8, '>q'),	    #		array of integers ( stores in AF file)
            "*float4":(16,4,nan_float4, '>f'),"*float8":(16,8,nan_float8, '>d'),   #		array of floats ( stores in AF file)
            #              |   |   |                   |
            #              |   |   |                   format for struct pack/unpack functions
            #              |   |   data indicates null value of element in MAF 
            #              |   lengths of element in MAF (bytes)
            #              lenghts of element in MAF row (bytes) - elementary data or address of data in AF (LB) or CF file (array of elementary data)
	        "file":(32,1,b'\x00'),                        #   bytes array any size ( descriptor stores in AF file, file body - in BF file)
            #        |         
            #        lengths of 2 elements in MAF (bytes): first 16 - address of file descr in AF, second 16 - address of file body in BF
            #                              
        }
    file_to_mime=    {       
	"mid": "audio/mid" ,
	"midi": "audio/midi" , 
    "mp3": "audio/mp3" , 
	"m4a": "audio/mp4" ,  
	"mpeg": "audio/mpeg" ,  
	"ogg": "audio/ogg" ,  
	"wav": "audio/wav" , 
    "flac": "audio/x-flac" ,  
    "js": "application/javascript" , 
	"json": "application/json" , 
	"doc": "application/msword" , 
	"bin": "application/octet-stream" ,  
	"exe": "application/octet-stream" ,  
	"iso": "application/octet-stream" ,  
	"jar": "application/octet-stream" ,  
	"msi": "application/octet-stream",   
	"sql": "application/octet-stream",  
	"pdf": "application/pdf" ,  
    "ai": "application/postscript",   
	"rtf": "application/rtf" ,  
	"pptx": "application/vnd.openxmlformats-officedocument.presentationml.presentation" ,  
	"xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" ,  
	"docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document" ,
    "ods": "application/vnd.oasis.opendocument.spreadsheet" ,  
	"xls": "application/vnd.ms-excel" ,  
	"csv": "application/vnd.ms-excel" ,  
	"swf": "application/x-shockwave-flash" ,  
    "jar": "application/x-zip-compressed" , 
	"xml": "application/xml" ,  
	"zip": "application/zip" ,  
    "apk": "application/x-zip-compressed" ,  
    "gz": "application/x-gzip" ,  
	"otf": "font/opentype" , 
	"jpeg": "image/jpeg" , 
	"jpg":"image/jpeg" ,  
	"ico":"image/vnd.microsoft.icon" ,  
	"bmp":"image/bmp" ,  
	"gif":"image/gif" ,  
	"png": "image/png" , 
	"tiff":"image/tiff" ,  
	"tif": "image/tif" , 
	"txt": "text/plain" , 
	"htc": "text/x-component" , 
	"vcf":"text/x-vcard" ,  
	"css": "text/css" , 
	"xml": "text/xml" , 
	"html": "text/html", 
	"avi": "video/avi" , 
	"mov": "video/quicktime" , 
	"mpg": "video/mpg" , 
    "mp4": "video/mp4" , 
	"ogg": "video/ogg" , 
    }

    @classmethod
    def mime_to_file (cls,mime):
        file_ext=[]
        for file_e in cls.file_to_mime:
            if cls.file_to_mime[file_ext] == mime:
                file_ext.append(file_e)
        return file_ext

 