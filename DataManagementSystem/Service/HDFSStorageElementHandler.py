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
from DIRAC.Core.DISET.RequestHandler import RequestHandler, getServiceOption
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

  BASE_PATH = getServiceOption( serviceInfo, "BasePath", BASE_PATH )
  if not BASE_PATH:
    gLogger.error( 'Failed to get the base path' )
    return S_ERROR( 'Failed to get the base path' )
  if not os.path.exists( BASE_PATH ):
    os.makedirs( BASE_PATH )

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
  
  def __init__(self):

    self.log = gLogger.getSubLogger( "HDFSStorageElementHandler", True )

    try:
      self.hdfs_ctx = hdfs.hdfs( HDFS_HOST, HDFS_PORT )
    except:
      errStr = 'HDFSStorageElementHandler.__init__: failed to initialize a HDFS instance.'
      self.log.debug( errStr )

  def __del__(self):
    try:
      self.hdfs_ctx.close()
    except Exception, e:
      errStr = 'HDFSStorageElementHandler.__del__: failed to close HDFS instance. Error: %s' % e
      self.log.debug( errStr )
        

  # ## TODO: just copied from StorageElementHandler, probably needs adjustment
  def __resolveFileID( self, fileID ):
    """ get path to file for a given :fileID: """

    port = self.getCSOption( 'Port', '' )
    if not port:
      return ''

    if ":%s" % port in fileID:
      loc = fileID.find( ":%s" % port )
      if loc >= 0:
        fileID = fileID[loc + len( ":%s" % port ):]

    serviceName = self.serviceInfoDict['serviceName']
    loc = fileID.find( serviceName )
    if loc >= 0:
      fileID = fileID[loc + len( serviceName ):]

    loc = fileID.find( '?=' )
    if loc >= 0:
      fileID = fileID[loc + 2:]

    if fileID.find( BASE_PATH ) == 0:
      return fileID
    while fileID and fileID[0] == '/':
      fileID = fileID[1:]
    return os.path.join( BASE_PATH, fileID )



  def export_exists( self, fileID ):
    """ Check existence of the fileID """

    file_path = self.__resolveFileID( fileID )
    gLogger.debug( "HDFSStorageElementHandler.export_exists: checking if %s exists" % file_path )

    try:
      res = self.hdfs_ctx.exists( file_path )
      if res:
        return S_OK( True )
      else:
        return S_OK( False )
      
    except Exception, e:
      errStr = 'HDFSStorageElementHandler.export_exists: Error while checking for existence: %s' % e
      self.log.debug( errStr )
      
