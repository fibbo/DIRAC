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
import pydoop
import pydoop.hdfs as hdfs
# # from DIRAC
from DIRAC import gLogger, S_OK, S_ERROR, gConfig
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC.ConfigurationSystem.Client.Helpers.Registry import getVOForGroup

HDFS_HOST = None
HDFS_PORT = None
BASE_PATH = ""
HDFS_RESOURCE = None


def initializeHDFSStorageElementHandler( serviceInfo ):
  """ Initialize Storage Element global settings

  This is all speculative as I don't know yet how the HDFS storage will come to life... but assuming we can get what we need
  from the CS in the way the IRODSSEHandler did it.
  """

  global HDFS_HOST
  global HDFS_PORT
  global BASE_PATH
  global HDFS_RESOURCE

  cfgPath = serviceInfo['serviceSectionPath']

  HDFS_HOST = gConfig.getValue( "%s/HDFSServer" % cfgPath , HDFS_HOST )
  if not HDFS_HOST:
    gLogger.error( 'Failed to get HDFS server host' )
    return S_ERROR( 'Failed to get HDFS server host' )

  HDFS_PORT = gConfig.getValue( "%s/HDFSPort" % cfgPath , HDFS_PORT )
  try:
    HDFS_PORT = int( HDFS_PORT )
  except:
    pass
  if not HDFS_PORT:
    gLogger.error( 'Failed to get HDFS server port' )
    return S_ERROR( 'Failed to get HDFS server port' )


  gLogger.info( 'Starting HDFS Storage Element' )
  gLogger.info( 'HDFS server: %s' % HDFS_HOST )
  gLogger.info( 'HDFS port: %s' % HDFS_PORT )
  gLogger.info( 'HDFS resource: %s' % HDFS_RESOURCE )
  return S_OK()

class HDFSStorageElementHandler( RequestHandler ):
  '''
  classdocs
  '''
  def __HDFSClient( self ):

    hdfs_ctx = hdfs.hdfs( HDFS_HOST, HDFS_PORT )


