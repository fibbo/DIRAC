########################################################################
# $HeadURL$
# File: HDFSStorageElementHandler.py
########################################################################
"""
:mod: HDFSStorageElementHandler

.. module: HDFSStorageElementHandler
  :synopsis: HDFSStorageElementHandler is the implementation of a simple StorageElement
  service with the HDFS SE as a backend

The following methods are available in the Service interface

getMetadata() - get file metadata
listDirectory() - get directory listing
remove() - remove one file
removeDirectory() - remove on directory recursively
removeFileList() - remove files in the list
getAdminInfo() - get administration information about the SE status

The handler implements also the DISET data transfer calls
toClient(), fromClient(), bulkToClient(), bulkFromClient
which support single file, directory and file list upload and download

The class can be used as the basis for more advanced StorageElement implementations

"""

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
