""" :mod: HDFSStorage
    =================

    .. module: python
    :synopsis: HDFS class from StorageElement using pydoop.
"""
# # imports
import os
import datetime
import errno
import pydoop
import pydoop.hdfs as hdfs

from types import StringType
from stat import S_ISREG, S_ISDIR, S_IXUSR, S_IRUSR, S_IWUSR, \
  S_IRWXG, S_IRWXU, S_IRWXO
# # from DIRAC
from DIRAC import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Resources.Utilities import checkArgumentFormat
from DIRAC.Resources.Storage.StorageBase import StorageBase
from DIRAC.Core.Utilities.File import getSize


# # RCSID
__RCSID__ = "$Id$"

class HDFSStorage( StorageBase ):
  """ .. class:: HDFSStorage

  HDFS interface to StorageElement using pydoop
  """

  def __init__( self, storageName, parameters ):
    """ c'tor

    :param self: self reference
    :param str storageName: SE name
    :param dict parameters: storage parameters


    """

    StorageBase.__init__( self, storageName, parameters )

    # dlevel = self.log.getLevel()
    self.name = storageName
    self.pluginName = 'HDFS'
    self.log = gLogger.getSubLogger( "HDFSStorage", True )
    # some operations need an hdfs instance (e.g. to get metadata)
    try:
      self.hdfs_ctx = hdfs.hdfs( parameters['Host'], parameters['Port'] )
    except:
      errStr = 'HDFSStorageElementHandler.__init__: failed to initialize a HDFS instance. Some operations might not work.'
      self.log.debug( errStr )

  def __del__( self ):
    try:
      self.hdfs_ctx.close()
    except Exception, e:
      errStr = 'HDFSStorage.__del__: failed to close HDFS instance. Error: %s' % e
      self.log.debug( errStr )

  def exists( self, path ):
    """ Check existence of the path
    :param self: self reference
    :param str path: a single path or a list of paths to be checked

    :return Successful and failed dict. Successful dicts have a bool as value whether or not the path exists
                                        failed dicts have the error message as value
    """

    res = checkArgumentFormat( path )
    if not res['OK']:
      return res
    urls = res['Value']
    

    successful = {}
    failed = {}
    
    for url in urls:
      res = self.__singleExists( url )
      if res['OK']:
        successful[url] = res['Value']
      else:
        failed[url] = res['Value']

    return S_OK( { 'Failed' : failed, 'Successful' : successful } )
      
  def __singleExists(self, path):
    ''' checking for the single existence of path
    :param self: self reference
    :param str path: path to check
    :return S_OK( bool ) if the path exists or not
            S_ERROR( errStr ) in case of an error
    
    '''
    gLogger.debug( "HDFSStorage.__singleExists: checking if path %s exists" % path )
    
    try:
      res = hdfs.path.exists( path )
      return S_OK( res )

    except Exception, e:
      errStr = 'HDFSStorage.__singleExists: error while checking for existence %s' % e
      return S_ERROR( errStr )
      

  #############################################################
  #
  # These are the methods for file manipulation
  #

  def isFile( self, path ):
    """Check if the given path exists and it is a file

    :param self: self reference
    :param str: path or list of paths to be checked ('hdfs://...')
    :returns Failed dict: {path : error message}
             Successful dict: {path : bool}
             S_ERROR in case of argument problems
    """

    res = checkArgumentFormat( path )
    if not res['OK']:
      return res
    urls = res['Value']

    self.log.debug( "HDFSStorage.isFile: checking %s path(s) if they are file(s) or not" % len( urls ) )

    successful = {}
    failed = {}

    for url in urls:
      res = self.__isSingleFile( url )

      if res['OK']:
        successful[url] = res['Value']
      else:
        failed[url] = res['Message']

    return S_OK( {'Failed' : failed, 'Successful' : successful} )

  def __isSingleFile( self, path ):
    """ Checks for a single path if it exists and if it's a file

    :param self: self reference
    :param str: path to be checked
    :returns S_ERROR in case of an error
             S_OK( bool ) if it is a file or not
    """

    self.log.debug( "HDFSStorage.__isSingleFile: checking if %s is a file or not" % path )
    
    try:
      # lsl returns a list of dictionaries. since we only check for one file. the first entry
      # is the metadata dict we are interested int
      res = hdfs.lsl( path )
      if res[0]['kind'] == 'file':
        return S_OK( True )
      else:
        return S_OK( False )
    except Exception, e:
      if 'No such file' in str( e ):
        errStr = "HDFSStorage.__isSingleFile: File does not exist."
        self.log.debug( errStr )
        S_ERROR( errStr )
      else:
        errStr = "HDFSStorage.__isSingleFile: error while retrieving path properties %s" % e
        self.log.error( errStr )
        return S_ERROR( errStr )


  def getFile( self, path, localPath = False ):
    """Get a local copy of the file specified by its path

    :param self: self reference
    :param str path: path or list of paths on the storage
    :returns Successful dict: {path : size}

    """
    return S_ERROR( "Storage.getFile: implement me!" )

  def putFile( self, *parms, **kws ):
    """Put a copy of the local file to the current directory on the
       physical storage
    """
    return S_ERROR( "Storage.putFile: implement me!" )

  def removeFile( self, *parms, **kws ):
    """Remove physically the file specified by its path
    """
    return S_ERROR( "Storage.removeFile: implement me!" )

  def getFileMetadata( self, *parms, **kws ):
    """  Get metadata associated to the file
    """
    return S_ERROR( "Storage.getFileMetadata: implement me!" )

  def getFileSize( self, path ):
    """Get the physical size of the given file

    :param self: self reference
    :param str path: path of list of paths on the storage
    :returns Successful dicht {path : size}
             Failed dict {path : error message}
             S_ERROR in case of argument problems
    """
    res = checkArgumentFormat( path )
    if not res['OK']:
      return res
    urls = res['Value']

    self.log.debug( "HDFSStorage.getFileSize: Attempting to determine file size of %s files" % len( urls ) )

    successful = {}
    failed = {}

    for url in urls:
      res = self.__getSingeFileSize( url )
      if res['OK']:
        successful[url] = res['Value']
      else:
        failed[url] = res['Message']

    return S_OK( { 'Failed' : failed, 'Successful' : successful} )


  def __getSingeFileSize( self, path ):
    """ Get physical file size for a single file on the storage

    :param self: self reference
    :param str path: path to the file
    :returns S_OK( fileSize )
             S_ERROR( error message ) when file size could not be determined
    """

    self.log.debug( "HDFSStorage.__getSingleFileSize: Determining size of file %s" % path )

    res = self.__isSingleFile( path )
    if not res['OK']:
      return res

    if not res['Value']:
      errStr = "HDFSStorage.__getSingleFileSize: Path is not a file."
      self.log.debug( errStr )
      return S_ERROR( errStr )
    else:
      try:
        statInfo = hdfs.lsl(path)
        return S_OK( statInfo[0]['size'] )
      except Exception, e:
        errStr = "HDFSStorage.__getSingleFileSize: Failed to determine file size: %s" % e
        self.log.error( errStr )
        return S_ERROR( errStr )
    pass

  def prestageFile( self, *parms, **kws ):
    """ Issue prestage request for file
    """
    return S_ERROR( "Storage.prestageFile: implement me!" )

  def prestageFileStatus( self, *parms, **kws ):
    """ Obtain the status of the prestage request
    """
    return S_ERROR( "Storage.prestageFileStatus: implement me!" )

  def pinFile( self, *parms, **kws ):
    """ Pin the file on the destination storage element
    """
    return S_ERROR( "Storage.pinFile: implement me!" )

  def releaseFile( self, *parms, **kws ):
    """ Release the file on the destination storage element
    """
    return S_ERROR( "Storage.releaseFile: implement me!" )

  #############################################################
  #
  # These are the methods for directory manipulation
  #

  def isDirectory( self, *parms, **kws ):
    """Check if the given path exists and it is a directory
    """
    return S_ERROR( "Storage.isDirectory: implement me!" )

  def getDirectory( self, *parms, **kws ):
    """Get locally a directory from the physical storage together with all its
       files and subdirectories.
    """
    return S_ERROR( "Storage.getDirectory: implement me!" )

  def putDirectory( self, *parms, **kws ):
    """Put a local directory to the physical storage together with all its
       files and subdirectories.
    """
    return S_ERROR( "Storage.putDirectory: implement me!" )

  def createDirectory( self, *parms, **kws ):
    """ Make a new directory on the physical storage
    """
    return S_ERROR( "Storage.createDirectory: implement me!" )

  def removeDirectory( self, *parms, **kws ):
    """Remove a directory on the physical storage together with all its files and
       subdirectories.
    """
    return S_ERROR( "Storage.removeDirectory: implement me!" )

  def listDirectory( self, *parms, **kws ):
    """ List the supplied path
    """
    return S_ERROR( "Storage.listDirectory: implement me!" )

  def getDirectoryMetadata( self, *parms, **kws ):
    """ Get the metadata for the directory
    """
    return S_ERROR( "Storage.getDirectoryMetadata: implement me!" )

  def getDirectorySize( self, *parms, **kws ):
    """ Get the size of the directory on the storage
    """
    return S_ERROR( "Storage.getDirectorySize: implement me!" )
      
