import os
import shutil
import re
from stat import ST_MODE, ST_SIZE, ST_ATIME, ST_CTIME, ST_MTIME, S_ISDIR, S_IMODE
from types import StringType, StringTypes, ListType
# # from DIRAC
from DIRAC import gLogger, S_OK, S_ERROR, gConfig
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC.ConfigurationSystem.Client.Helpers.Registry import getVOForGroup

class HDFSStorageElementHandler( RequestHandler ):
    '''
    classdocs
    '''


    def __init__( self, params ):
        '''
        Constructor
        '''
