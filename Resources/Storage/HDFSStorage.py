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
from DIRAC.DataManagementSystem.Utilities.lfc_dfc_copy import path


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


  def __existsHelper( self, path ):
    """ When pydoop tries an operation on a file that doesn't exist, the return ERROR is a general
        IOError with no more information. So we have this helper that checks whether or not a path exists
        so we at least know this.

        :param str path: single path to check
        :return bool: True if the path exists, False if not
                S_ERROR( error message ) in case of an error
    """
    res = self.__singleExists( path )
    if res['OK']:
      return res['Value']
    else:
      return res


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
      # TODO: might need to do an exist check first because
      # path.isfile just returns false if file doesnt exist
      res = hdfs.path.isfile( path )
      return S_OK( res )
    except IOError, e:
      if 'No such file' in str( e ):
        errStr = "HDFSStorage.__isSingleFile: File does not exist."
        self.log.debug( errStr )
        S_ERROR( errStr )
      else:
        errStr = "HDFSStorage.__isSingleFile: IOError while retrieving path properties %s" % e
        self.log.error( errStr )
        return S_ERROR( errStr )
    except Exception, e:
        errStr = "HDFSStorage.__isSingleFile: Exception caught while retrieving path properties %s" % e
        self.log.error( errStr )
        return S_ERROR( errStr )



  def getFile( self, path, localPath = False ):
    """Get a local copy of the file specified by its path

    :param self: self reference
    :param str path: path or list of paths on the storage
    :returns Successful dict: {path : size}
    """
    res = checkArgumentFormat( path )
    if not res['OK']:
      return res
    urls = res['Value']

    self.log.debug( "HDFSStorage.getFile: Attempting to download %s file(s)." % len( urls ) )

    successful = {}
    failed = {}

    for src_url in urls:
      fileName = os.path.basename( src_url )
      if localPath:
        dest_file = '%s/%s' % ( localPath, fileName )
      else:
        dest_file = ( os.getcwd(), fileName )

      res = self.__getSingleFile( src_url, dest_file )

      if res['OK']:
        successful[src_url] = res['Value']
      else:
        failed[src_url] = res['Message']

    return S_OK( { 'Failed' : failed, 'Successful' : successful} )



  def __getSingleFile( self, src_url, dest_file ):
    """ Copy a file from storage to local file system

    :param str src_url: url on the remote storage
    :param str dest_file: destionation file name on the file system
    :return S_ERROR( error message ) in case of an error
            S_OK( file size ) if copying is successful
    """

    res = self.__getSingeFileSize( src_url )

    if not res['OK']:
      errStr = "HDFSStorage.__getSingleFile: Error while determinig file size: %s" % res['Message']
      self.log.error( errStr )
      return S_ERROR( errStr )

    remoteSize = res['Value']

    if not dest_file.startswith( 'file:' ):
      dest = 'file://' + dest_file
    else:
      dest = dest_file
      
    try:
      hdfs.cp( src_url, dest)
    except IOError, e :
      if 'Cannot open file' in str( e ):
        errStr = "HDFSStorage.__getSingleFile: No access to create directory/file in destination."
        self.log.error( errStr )
        return S_ERROR( errStr )
      else:
        errStr = "HDFSStorage.__getSingleFile: IOError while trying to download file: %s" % e
        self.log.error( errStr )
        return S_ERROR( errStr )
    except Exception, e:
      errStr = "HDFSStorage.__getSingleFile: error while downloading file: %s" % e
      self.log.error( errStr )
      return S_ERROR( errStr )

    destSize = getSize( dest_file )
    if destSize == remoteSize:
      return S_OK( remoteSize )
    else:
      errStr = "HDFSStorage.__getSingleFile: File sizes don't match. Something went wrong, removing local file %s" % dest_file
      if os.path.exists( dest_file ):
        os.remove( dest_file )
      return S_ERROR( errStr )


  def putFile( self, *parms, **kws ):
    """Put a copy of the local file to the current directory on the
       physical storage
    """
    return S_ERROR( "Storage.putFile: implement me!" )

  def removeFile( self, path ):
    """Remove physically the file specified by its path

    A non existing file will be considered successfully removed

    :param str path: path or list of paths to be removed
    :returns Successful dict {path : True}
             Failed dict {path : error message}
    """

    res = checkArgumentFormat( path )
    if not res['OK']:
      return res
    urls = res['Value']

    self.log.debug( "HDFSStorage.removeFile: Attempting to remove %s file(s)" % len( urls ) )

    failed = {}
    successful = {}

    for url in urls:
      res = self.__removeSingleFile( url )

      if res['OK']:
        successful[url] = res['Value']
      else:
        failed[url] = res['Message']

    return S_OK( { 'Failed' : failed, 'Successful' : successful} )

  def __removeSingleFile( self, path ):
    """ Physically remove the file specified by path
    :param str path: path on storage (hdfs://...)
    :returns
             S_OK( True )  if the removal was successful (also if file didnt exist in the first place)
             S_ERROR( errStr ) if there was a problem removing the file
    """

    self.log.debug( "HDFSStorage.__removeSingleFile: Attempting to remove file %s" % path )

    try:
      hdfs.rmr( path )
      return S_OK( True )
    except IOError, e:
      res = self.__existsHelper( path )
      if not res:
        return S_OK( True )
      errStr = 'HDFSStorage.__removeSingleFile: IOError occured while trying to remove a file: %s' % e
      self.log.error( errStr )
      return S_ERROR( errStr )
    except Exception, e:
      errStr = 'HDFSStorage.__removeSingleFile: Exception occured while trying to remove a file: %s' % e
      self.log.error( errStr )
      return S_ERROR( errStr )



  def getFileMetadata( self, path ):
    """  Get metadata associated to the file

    :param self: self reference
    :param str path: path (or list of paths) on the storage
    :returns successful dict { path : metadata }
             failed dict { path : error message }
    """

    res = checkArgumentFormat( path )
    if not res['OK']:
      return res
    urls = res['Value']

    self.log.debug( "HDFSStorage.getFileMetadata: Attempting to retrieve metadata for %s file(s)" % len( urls ) )

    successful = {}
    failed = {}

    for url in urls:
      res = self.__getSingleMetadata( url, 'file' )

      if res['OK']:
        successful[url] = res['Value']
      else:
        failed[url] = res['Message']

    return S_OK( { 'Failed' : failed, 'Successful' : successful } )


  def __getSingleFileMetadata( self, path ):
    """ Fetch the metadata associated to the file
    :param self: self reference
    :param str path: path on the storage
    :returns S_OK( metadatadict ) if we could get the metadata
             S_ERROR( error message ) in case of an error
    """
    res = self.__getSingleMetadata( path )
    if not res['OK']:
      return res

    metadataDict = res['Value']

    if not metadataDict['File']:
      errStr = "HDFSStorage.__getSingleMetadata: supplied path is not a file."
      self.log.error( errStr, path )
      return S_ERROR( errStr )

    return S_OK( metadataDict )

  def __getSingleMetadata( self, path, recursive = False ):
    """ Get the meta data of a path or a list of paths.

    :param self: self reference
    :param str path: path or a list of paths
    :returns S_OK( metadata dict )
             S_ERROR( error message )
    """

    self.log.debug( "HDFSStorage.__getSingleMetadata: reading metadata for %s" % path )
    try:
      stats = hdfs.lsl( path, recursive )
      metadataDict = self.__convertMetadataDict( stats )
      return S_OK( metadataDict )
    except IOError, e:
      errStr = "HDFSStorage.__getSingleMetadata: Error while reading metadata: %s" % e
      self.log.error( errStr )
      return S_ERROR( errStr )


  def __convertMetadataDict( self, stats ):
    metadata = {}
    metadata['File'] = stats[0]['kind'] == 'file'
    metadata['Directory'] = stats[0]['kind'] == 'directory'
    metadata['Mode'] = stats[0]['permissions']
    if metadata['File']:
      metadata['ModTime'] = self.__convertTime( stats[0]['last_mod'] ) if stats[0]['last_mod'] else 'Never'
      metadata['Executable'] = bool( stats[0]['permissions'] & S_IXUSR )
      metadata['Readable'] = bool( stats[0]['permissions'] & S_IRUSR )
      metadata['Writeable'] = bool( stats[0]['permissions'] & S_IWUSR )
      metadata['Size'] = long( stats[0]['size'] )
    return S_OK( metadata )

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

  def isDirectory( self, path ):
    """Check if the given path exists and it is a directory

    :param self: self reference
    :param str path: path or list of paths to check if they are directories or not
    :returns successful dict { path : bool }
             failed dict { path : error message }

    """

    res = checkArgumentFormat( path )
    if not res['OK']:
      return res
    urls = res['Value']

    self.log.debug( "HDFSStorage.isDirectory: Determining whether %s paths are directories." % len( urls ) )

    successful = {}
    failed = {}

    for url in urls:
      res = self.__isSingleDirectory( url )

      if res['OK']:
        successful[url] = res['Value']
      else:
        failed[url] = res['Message']
        
    
    return S_OK( { 'Failed' : failed, 'Successful' : successful } )

  def __isSingleDirectory( self, path ):
    """ Check if given path is a directory
    
    :param self: self reference
    :param str path: path to be checked
    :returns S_OK( bool ) whether path is a directory or not
             S_ERROR( error message ) if there is an error    
    """
    
    try:
      # lsl returns a list of dictionaries. since we only check for one file. the first entry
      # is the metadata dict we are interested int
      res = hdfs.lsl( path )
      if res[0]['kind'] == 'directory':
        return S_OK( True )
      else:
        return S_OK( False )
    except IOError, e:
      if 'No such file' in str( e ):
        errStr = "HDFSStorage.__isSingleDirectory: File does not exist."
        self.log.debug( errStr )
        S_ERROR( errStr )
      else:
        errStr = "HDFSStorage.__isSingleDirectory: IOError while retrieving path properties %s" % e
        self.log.error( errStr )
        return S_ERROR( errStr )
    except Exception, e:
      errStr = "HDFSStorage.__isSingleDirectory: Exception caught while retrieving path properties %s" % e
      self.log.error( errStr )
      return S_ERROR( errStr )


  def putDirectory( self, path ):
    """ Puts one or more local directories to the physical storage together with all its files
    :param self: self reference
    :param str path: dictionary { hdfs://... (destination) : localdir (source dir) }
    :return successful and failed dictionaries. The keys are the paths,
            the values are dictionary {'Files' : amount of files uploaded, 'Size' : amount of data upload }
            S_ERROR in case of argument problems
    """
    res = checkArgumentFormat( path )
    if not res['OK']:
      return res
    urls = res['Value']

    self.log.debug( 'HDFSStorage.putDirectory: Attempting to put %s directories to remote storage' % len( urls ) )

    successful = {}
    failed = {}

    for destDir, sourceDir in urls.items():
      if not sourceDir:
        self.log.debug( 'SourceDir: %s' % sourceDir )
        errStr = 'HDFSStorage.putDirectory: No source directory set, make sure the input format is correct { dest. dir : source dir }'
        return S_ERROR( errStr )
      res = self.__putSingleDirectory( sourceDir, destDir )
      if res['OK']:
        if res['Value']['AllPut']:
          self.log.debug( "HDFSStorage.putDirectory: Successfully put directory to remote storage: %s" % destDir )
          successful[destDir] = { 'Files' : res['Value']['Files'], 'Size' : res['Value']['Size']}
        else:
          self.log.error( "HDFSStorage.putDirectory: Failed to put entire directory to remote storage.", destDir )
          failed[destDir] = { 'Files' : res['Value']['Files'], 'Size' : res['Value']['Size']}
      else:
        self.log.error( "HDFSStorage.putDirectory: Completely failed to put directory to remote storage.", destDir )
        failed[destDir] = { "Files" : 0, "Size" : 0 }
    return S_OK( { "Failed" : failed, "Successful" : successful } )


  def __putSingleDirectory( self, src_directory, dest_directory ):
    """ puts one local directory to the physical storage together with all its files and subdirectories
        :param self: self reference
        :param src_directory : the local directory to copy
        :param dest_directory: pfn (hdfs://...) where to copy
        :returns: S_ERROR if there is a fatal error
                  S_OK if we could upload something :
                                    'AllPut': boolean of whether we could upload everything
                                    'Files': amount of files uploaded
                                    'Size': amount of data uploaded
    """
    self.log.debug( 'HDFSStorage.__putSingleDirectory: trying to upload %s to %s' % ( src_directory, dest_directory ) )

    filesPut = 0
    sizePut = 0

    if not os.path.isdir( src_directory ):
      errStr = 'HDFSStorage.__putSingleDirectory: The supplied source directory does not exist or is not a directory.'
      self.log.error( errStr, src_directory )
      return S_ERROR( errStr )
    try:
      hdfs.cp( src_directory, dest_directory )
      allSuccessful = True

    except IOError, e:
      errStr = "HDFSStorage.__putSingleDirectory: IOError while trying to put folder on the HDFS storage: %s" % e
      self.log.error( errStr )
      return S_ERROR( errStr )
    except Exception, e:
      errStr = "HDFSStorage.__putSingleDirectory: Exception caught while trying to put folder on the HDFS storage: %s" % e
      self.log.error( errStr )
      return S_ERROR( errStr )
    return S_OK( { 'AllPut' : allSuccessful, 'Files' : filesPut, 'Size' : sizePut} )

  def getDirectory( self, *parms, **kws ):
    """Put a local directory to the physical storage together with all its
       files and subdirectories.
    """
    return S_ERROR( "Storage.putDirectory: implement me!" )

  def createDirectory( self, path ):
    """ Make a new directory on the physical storage

    :param self: self reference
    :param str path: path to be created on the storage (hdfs://...)
    :returns Successful dict {path : True}
             Failed dicht { path : error message }
             S_ERROR in case of argument problems
    """

    urls = checkArgumentFormat( path )
    if not urls['OK']:
      return urls
    urls = urls['Value']

    successful = {}
    failed = {}

    self.log.debug( "HDFSStorage.createDirectory: Attempting to create %s directory/ies" ) % len( urls )

    for url in urls:
      res = self.__createSingleDirectory( url )
      if res['OK']:
        successful[url] = True
      else:
        failed[url] = res['Message']

    return S_OK( { 'Failed' : failed, 'Successful' : successful} )

  def __createSingleDirectory( self, path ):
    """ Create directory :path: on the storage. Also creates non existing parent directories.

    :param self: self reference
    :param str path: path to be created
    :returns S_OK() if creation was successful
             S_ERROR() if there was an error
    """

    try:
      hdfs.mkdir( path )
      return S_OK()
    except IOError, e:
      errStr = "HDFSStorage.__createSingleDirectory: Error while creating directory: %s" % e
      self.log.error( errStr )
      return S_ERROR( errStr )

  def removeDirectory( self, *parms, **kws ):
    """Remove a directory on the physical storage together with all its files and
       subdirectories.
    """
    return S_ERROR( "Storage.removeDirectory: implement me!" )

  def listDirectory( self, path ):
    """ List the content of the path provided

    :param str path: single or list of paths (hdfs://...)
    :return failed  dict {path : message }
            successful dict { path :  {'SubDirs' : subDirs, 'Files' : files} }.
            They keys are the paths, the values are the dictionary 'SubDirs' and 'Files'.
            Each are dictionaries with path as key and metadata as values
            S_ERROR in case of argument problems
    """
    res = checkArgumentFormat( path )
    if not res['OK']:
      return res
    urls = res['Value']

    self.log.debug( "HDFSStorage.listDirectory: Attempting to list %s directories" % len( urls ) )

    res = self.isDirectory( urls )
    if not res['OK']:
      return res
    successful = {}
    failed = res['Value']['Failed']

    directories = []

    for url, isDirectory in res['Value']['Successful'].items():
      if isDirectory:
        directories.append( url )
      else:
        errStr = "HDFSStorage.listDirectory: path is not a directory"
        gLogger.error( errStr, url )
        failed[url] = errStr

    for directory in directories:
      res = self.__listSingleDirectory( directory )
      if not res['OK']:
        failed[directory] = res['Message']
      else:
        successful[directory] = res['Value']


    resDict = { 'Failed' : failed, 'Successful' : successful }
    return S_OK( resDict )


  def __listSingleDirectory( self, path, recursive = False ):
    """ List the content of the single directory provided
    :param self: self reference
    :param str path: single path on storage (hdfs://...)
    :returns S_ERROR( errStr ) if there is an error
             S_OK( dictionary ): Key: SubDirs and Files
                                 The values of the Files are dictionaries with filename as key and metadata as value
                                 The values of SubDirs are just the dirnames as key and True as value
    """
    
    self.log.debug("HDFSStorage.__listSingleDirectory: Attemping to list content of directory")
    try:
      listing = hdfs.lsl( path, recursive )

    except IOError, e:
      if 'No such file' in str( e ):
        errStr = "HDFSStorage.__listSingleDirectory: Path does not exist."
        self.log.error( errStr )
        return S_ERROR( errStr )
      else:
        errStr = "HDFSStorage.__listSingleDirectory: IOError while trying to list the directory: %s" % e
        self.log.error( errStr )
        return S_ERROR( errStr )
    except Exception, e:
      errStr = "HDFSStorage.__listSingleDirectory: Exception caught while trying to list the directory: %s" % e
      self.log.error( errStr )
      return S_ERROR( errStr )

    files = {}
    subDirs = {}

    for entry in listing:
      metadataDict = self.__convertMetadataDict( entry )

      if metadataDict['Directory']:
        subDirs[entry['name']] = metadataDict
      elif metadataDict['File']:
        files[entry['name']] = metadataDict
      else:
        self.log.debug( "HDFSStorage.__listSingleDirectory: Found item which is neither file nor directory: %s" ) % entry['name']

    return S_OK( { 'SubDirs' : subDirs, 'Files' : files } )

  def getDirectoryMetadata( self, path ):
    """ Get metadata for the directory(ies) provided

    :param self: self reference
    :param str path: path (or list of paths) on storage (hdfs://...)
    :returns Successful dict {path : metadata}
             Failed dict {path : errStr}
             S_ERROR in case of argument problems
    """
    res = checkArgumentFormat( path )
    if not res['OK']:
      return res
    urls = res['Value']

    self.log.debug( "HDFSStorage.getDirectoryMetadata: Attempting to fetch metadata for %s directory/ies." ) % len( urls )

    failed = {}
    successful = {}

    for url in urls:
      res = self.__getSingleDirectoryMetadata( url )

      if not res['OK']:
        failed[url] = res['Message']
      else:
        successful[url] = res['Value']

    return S_OK( { 'Failed' : failed, 'Successful' : successful} )

  def __getSingleDirectoryMetadata( self, path ):
    """ Fetch the metadata of the provided path
    :param self: self reference
    :param str path: path (only 1) on the storage (hdfs://...)
    :returns
      S_OK( metadataDict ) if we could get the metadata
      S_ERROR( errStr )if there was a problem getting the metadata or path isn't a directory
    """
    self.log.debug( "HDFSStorage.__getSingleDirectoryMetadata: Fetching metadata of directory %s." % path )

    res = self.__getSingleMetadata( path )

    if not res['OK']:
      return res

    metadataDict = res['Value']

    if not metadataDict['Directory']:
      errStr = "HDFSStorage.__getSingleDirectoryMetadata: Provided path is not a directory."
      self.log.error( errStr, path )
      return S_ERROR( errStr )

    return S_OK( metadataDict )


  def getDirectorySize( self, *parms, **kws ):
    """ Get the size of the directory on the storage
    """
    return S_ERROR( "Storage.getDirectorySize: implement me!" )
      
