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
      
      
