########################################################################
# $HeadURL$
# File :   SiteSEMapping.py
########################################################################

"""  The SiteSEMapping module performs the necessary CS gymnastics to
     resolve site and SE combinations.  These manipulations are necessary
     in several components.
"""

__RCSID__ = "$Id$"

from DIRAC import S_OK, S_ERROR
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.ConfigurationSystem.Client.Helpers.Resources import Resources, getSiteFullNames

#############################################################################
def getSiteSEMapping():
  """ Returns a dictionary of all sites and their localSEs as a list, e.g.
      {'LCG.CERN.ch':['CERN-RAW','CERN-RDST',...]}
      If gridName is specified, result is restricted to that Grid type.
  """
  siteSEMapping = {}
  resourceHelper = Resources()
  result = resourceHelper.getEligibleSites()
  if not result['OK']:
    return result
  sites = result['Value']
  
  for site in sites:
    result = resourceHelper.getEligibleResources( 'Storage', {'Site':site} )
    if not result['OK']:
      continue
    seList = result['Value']
    
    result = getSiteFullNames( site )
    if not result['OK']:
      continue
    for sName in result['Value']:
      siteSEMapping[sName] = seList   

  # Add Sites from the SiteToLocalSEMapping in the CS
  opsHelper = Operations()
  result = opsHelper.getSiteMapping( 'Storage', 'LocalSE' )
  if result['OK']:
    mapping = result['Value']
    for site in mapping:
      if site not in siteSEMapping:
        siteSEMapping[site] = mapping[site]
      else:  
        for se in mapping[site]:
          if se not in siteSEMapping[site]:
            siteSEMapping[site].append( se )

  return S_OK( siteSEMapping )

#############################################################################
def getSESiteMapping( gridName = '' ):
  """ Returns a dictionary of all SEs and their associated site(s), e.g.
      {'CERN-RAW':'LCG.CERN.ch','CERN-RDST':'LCG.CERN.ch',...]}
      Although normally one site exists for a given SE, it is possible over all
      Grid types to have multiple entries.
      If gridName is specified, result is restricted to that Grid type.
  """
  seSiteMapping = {}
  resourceHelper = Resources()
  result = resourceHelper.getEligibleResources( 'Storage' )
  if not result['OK']:
    return result
  seList = result['Value']
  for se in seList:
    result = getSitesForSE( se )
    if not result['OK']:
      continue
    site = result['Value']
    seSiteMapping[se] = site

  return S_OK( seSiteMapping )

#############################################################################
def getSitesForSE( storageElement, gridName = '' ):
  """ Given a DIRAC SE name this method returns a list of corresponding sites.
      Optionally restrict to Grid specified by name.
  """

  result = getSiteSEMapping( gridName )
  if not result['OK']:
    return result

  mapping = result['Value']

  finalSites = []

  for site in mapping:
    if storageElement in mapping[site]:
      finalSites.append( site )
  return S_OK( finalSites )


#############################################################################
def getSEsForSite( siteName ):
  """ Given a DIRAC site name this method returns a list of corresponding SEs.
  """
  result = getSiteSEMapping()
  if not result['OK']:
    return result

  mapping = result['Value']
  if siteName in mapping:
    return S_OK( mapping[siteName] )

  return S_OK( [] )

#############################################################################
def isSameSiteSE( se1, se2 ):
  """ Check if the 2 SEs are from the same site
  """
  if se1 == se2:
    return S_OK( True )

  result = getSitesForSE( se1 )
  if not result['OK']:
    return result
  sites1 = result['Value']
  result = getSitesForSE( se2 )
  if not result['OK']:
    return result
  sites2 = result['Value']

  for site in sites1:
    if site in sites2:
      return S_OK( True )

  return S_OK( False )

#############################################################################
def getSEsForCountry( country ):
  """ Determines the associated SEs from the country code
  """
  mappedCountries = [country]
  opsHelper = Operations()
  while True:
    mappedCountry = opsHelper.getValue( '/Countries/%s/AssignedTo' % country, country )
    if mappedCountry == country:
      break
    elif mappedCountry in mappedCountries:
      return S_ERROR( 'Circular mapping detected for %s' % country )
    else:
      country = mappedCountry
      mappedCountries.append( mappedCountry )
  res = opsHelper.getOptionsDict( '/Countries/%s/AssociatedSEs' % country )
  if not res['OK']:
    return S_ERROR( 'Failed to obtain AssociatedSEs for %s' % country )
  return S_OK( res['Value'].values() )

#############################################################################
def getSitesGroupedByTierLevel( siteName = '' ):
  """
    It builds a dictionary which key is the tier level obtained from the CS using the MoUTierLevel attribute.
    By default the Tier level is 3. The Tier level retrieved from the CS can be Tier0, Tier1, Tier2, Tier3
  """

  tierSiteMapping = {}

  gridTypes = gConfig.getSections( '/Resources/Sites/' )
  if not gridTypes['OK']:
    gLogger.warn( 'Problem retrieving sections in /Resources/Sites' )
    return gridTypes

  gridTypes = gridTypes['Value']

  for grid in gridTypes:
    sites = gConfig.getSections( '/Resources/Sites/%s' % grid )
    if not sites['OK']:  # gConfig returns S_ERROR for empty sections until version
      gLogger.warn( 'Problem retrieving /Resources/Sites/%s section' % grid )
      return sites
    if sites:
      if siteName:
        if siteName in sites['Value']:
          tierLevel = gConfig.getValue( '/Resources/Sites/%s/%s/MoUTierLevel' % ( grid, siteName ), 3 )
          tierkey = 'Tier%d' % tierLevel
          if tierkey not in tierSiteMapping:
            tierSiteMapping[tierkey] = []
          tierSiteMapping[tierkey] += [siteName]
      else:
        for candidate in sites['Value']:
          tierLevel = gConfig.getValue( '/Resources/Sites/%s/%s/MoUTierLevel' % ( grid, candidate ), 3 )
          tierkey = 'Tier%d' % tierLevel
          if tierkey not in tierSiteMapping:
            tierSiteMapping[tierkey] = []
          tierSiteMapping[tierkey] += [candidate]
  return S_OK( tierSiteMapping )

#############################################################################
def getTier1WithAttachedTier2( siteName = '' ):
  """ this method iterates on the T2 sites and check the SE of the T2.
      In case a SE is found then the T2 is attached to the corresponding T1
  """
  tier1andTier2Maps = {}
  tiers = getSitesGroupedByTierLevel( siteName )
  if not tiers['OK']:
    return tiers

  tier1 = []
  if 'Tier0' in tiers['Value']:
    tier1 += tiers['Value']['Tier0']

  if 'Tier1' in tiers['Value']:
    tier1 += tiers['Value']['Tier1']

  tier2s = getSitesGroupedByTierLevel()

  if not tier2s['OK']:
    return tier2s

  tier2s = tier2s['Value']['Tier2']
  for site in tier1:
    t1SE = getSEsForSite( site )
    if not t1SE['OK']:
      return t1SE

    t1SE = t1SE['Value']
    for tier2 in tier2s:
      t2SE = getSEsForSite( tier2 )
      if not t2SE['OK']:
        return t2SE

      t2SE = t2SE['Value']
      if len( t2SE ) > 0:
        if __isOneSEFound( t1SE, t2SE ):
          if site not in tier1andTier2Maps:
            tier1andTier2Maps[site] = []
          tier1andTier2Maps[site] += [tier2]

  return S_OK( tier1andTier2Maps )

#############################################################################
def getTier1WithTier2( siteName = '' ):
  """
  It returns the T1 sites with the attached T2 using the SiteLocalSEMapping
  """
  tier1andTier2Maps = {}
  retVal = Operations().getOptionsDict( 'SiteLocalSEMapping' )
  if not retVal['OK']:
    return retVal
  else:
    storages = retVal['Value']

  tiers = getSitesGroupedByTierLevel( siteName )
  if not tiers['OK']:
    return tiers
  tier1andTier2Maps = {}  # initialize the dictionary with T1 sites.
  # no T2 associated to a T2 by default.
  if 'Tier0' in tiers['Value']:
    for site in tiers['Value']['Tier0']:
      tier1andTier2Maps[site] = []

  if 'Tier1' in tiers['Value']:
    for site in tiers['Value']['Tier1']:
      tier1andTier2Maps[site] = []

  for site in storages:
    sites = getSitesForSE( storages[site] )
    if not sites['OK']:
      return sites

    sites = sites['Value']
    for i in sites:
      if i in tier1andTier2Maps:
        # it associates the tier2 site the corresponding Tier 1 site.
        tier1andTier2Maps[i] += [site]

  return S_OK( tier1andTier2Maps )

#############################################################################
def __isOneSEFound( se1, se2 ):
  """
  It compares two list which contains different SEs. The two list not have to be identical,
  because we never attach a Tier2 all the SEs which provided by a Tier1.
  """
  if len( se1 ) >= len( se2 ):
    for i in se2:
      for j in se1:
        if i == j:
          return True
    return False
  elif len( se1 ) < len( se2 ):
    for i in se1:
      for j in se2:
        if i == j :
          return True
  return False
