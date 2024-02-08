
from astropy.io import fits

# DSIMULATOR produces MDFs which writes FITS ASCII tables
# When Keck asked UCO/Lick to incorporate the LRIS masks
# into the DEIMOS mask milling workflow we wrote
# Tcl program lsc2df which writes FITS binary tables
FITStabTypes = [fits.TableHDU, fits.BinTableHDU]
REQUIRED        = 1
OPTIONAL        = 0

class colAttr:
    """
    an object with attributes of one column we expect in a table in a MDF
    """

    def __init__(self, dtpre, required):
        # numpy dtype prefix that we should see for data in this column
        # dtpre is tricky because astropy FITS does not provide a consistent
        # mechanism to ascertain the datatype of a column in a FITS table HDU
        self.dtpre = dtpre

        # is this column required to be present
        self.req = required


class hduAttr:
    """
    an object with attributes of one HDU we expect in a table in a MDF
    """

    def __init__(self, hdutypes, required, knownCols):
        # astropy.io.fits hdutype for this XTENSION
        self.hdutypes   = hdutypes
        # is this table required to be present
        self.req        = required
        # dictionary of columns and their attributes
        self.knownCols  = knownCols


########################################################################

"""
LRIS mask design programs do not produce any outputs with information about 
the objects in the slitlets. LRIS mask design programs only describe the 
coordinates of the slitlets on the metal of the mask. 

When we use Tcl program lsc2df to translate an LRIS file <something>. file3 into 
a MDF it produces zero rows for tables: (1) ObjectCat (2) CatFiles (3) SlitObjMap

We do produce rows for table DesiSlits but those have NULL information about 
celestial coordinates. This omission in the LRIS mask design programs means that
the FITS files from LRIS contain no information about the objects which produced 
the spectra.
"""
mdfcontent = {
    # dictionary of known HDUs and their attributes
    # key is EXTNAME of a TABLE HDU
    'ObjectCat' :
        hduAttr(
            FITStabTypes,   REQUIRED,

            # dictionary of known columns and their attributes
            {
                # key is TTYPEn           val.dtpre       val.req
                'ObjectId'      : colAttr('int',        REQUIRED),
                'OBJECT'        : colAttr('chararray',  REQUIRED),
                # DEIMOS was designed before IAU designated ICRS
                # therefore DEIMOS code knows only FK4 and FK5.
                'RA_OBJ'        : colAttr('float',      REQUIRED),
                'DEC_OBJ'       : colAttr('float',      REQUIRED),
                'RADESYS'       : colAttr('chararray',  REQUIRED),
                'EQUINOX'       : colAttr('float',      REQUIRED),
                'MJD-OBS'       : colAttr('float',      REQUIRED),
                # DSIMULATOR creates files which contain these optional fields
                # but most input catalogs have no such info about the objects
                'mag'           : colAttr('float',      OPTIONAL),
                'pBand'         : colAttr('chararray',  OPTIONAL),
                'RadVel'        : colAttr('float',      OPTIONAL),
                'MajAxis'       : colAttr('float',      OPTIONAL),
                'MajAxPA'       : colAttr('float',      OPTIONAL),
                'MinAxis'       : colAttr('float',      OPTIONAL),
                'PM_RA'         : colAttr('float',      OPTIONAL),
                'PM_Dec'        : colAttr('float',      OPTIONAL),
                'Parallax'      : colAttr('float',      OPTIONAL),
                'ObjClass'      : colAttr('chararray',  REQUIRED),
                'CatFilePK'     : colAttr('int',        REQUIRED),
            },
        ),
    # end dict entry for ObjectCat

    'CatFiles'  :
        hduAttr(
            FITStabTypes,   REQUIRED,
            # CatFiles exists because we wanted it to be possible to
            # read a mask design out of the database and use it as the
            # basis for designing a new mask related to the old one.
            # No such capability was ever added to the slitmask design programs.

            # dictionary of known columns and their attributes
            {
                # key is TTYPEn           val.dtpre       val.req
                'CatFilePK'     : colAttr('int',        REQUIRED),
                'CatFileName'   : colAttr('chararray',  REQUIRED),
            },
        ),
    # end dict entry for CatFiles

    'MaskDesign':
        hduAttr(
            FITStabTypes,   REQUIRED,

            # the Keck CoverSheet scheme with its program Ids
            # was unknown outside of Keck until several years after
            # DEIMOS was delivered.
            # Even if the program Ids had existed it is not clear that
            # they would belong in these mask tables because it is routine
            # for a mask designed by a team to be used for different programs
            # which share members of that team, and it is routine for a mask
            # designed for one semester to be used during a subsequent semester.

            # dictionary of known columns and their attributes
            {
                # key is TTYPEn           val.dtpre       val.req
                'DesId'         : colAttr('int',        REQUIRED),
                'DesName'       : colAttr('chararray',  REQUIRED),
                'DesAuth'       : colAttr('chararray',  REQUIRED),
                'DesCreat'      : colAttr('chararray',  REQUIRED),
                'DesDate'       : colAttr('chararray',  REQUIRED),
                'DesNslit'      : colAttr('int',        REQUIRED),
                'DesNobj'       : colAttr('int',        REQUIRED),
                'ProjName'      : colAttr('chararray',  REQUIRED),
                'INSTRUME'      : colAttr('chararray',  REQUIRED),
                'MaskType'      : colAttr('chararray',  REQUIRED),
                'RA_PNT'        : colAttr('float',      REQUIRED),
                'DEC_PNT'       : colAttr('float',      REQUIRED),
                'RADEPNT'       : colAttr('chararray',  REQUIRED),
                'EQUINPNT'      : colAttr('float',      REQUIRED),
                'PA_PNT'        : colAttr('float',      REQUIRED),
                'DATE_PNT'      : colAttr('chararray',  REQUIRED),
                'LST_PNT'       : colAttr('float',      REQUIRED),
            },
        ),
    # end dict entry for MaskDesign

    'DesiSlits' :
        hduAttr(
            FITStabTypes,   REQUIRED,
            # dictionary of known columns and their attributes
            {
                # key is TTYPEn           val.dtpre       val.req
                'dSlitId'       : colAttr('int',        REQUIRED),
                'DesId'         : colAttr('int',        REQUIRED),
                'SlitName'      : colAttr('chararray',  REQUIRED),
                'slitRA'        : colAttr('float',      REQUIRED),
                'slitDec'       : colAttr('float',      REQUIRED),
                'slitTyp'       : colAttr('chararray',  REQUIRED),
                'slitLen'       : colAttr('float',      REQUIRED),
                'slitLPA'       : colAttr('float',      REQUIRED),
                'slitWid'       : colAttr('float',      REQUIRED),
                'slitWPA'       : colAttr('float',      REQUIRED),
            },
        ),
    # end dict entry for DesiSlits

    'SlitObjMap':
        hduAttr(
            FITStabTypes,   REQUIRED,
            # dictionary of known columns and their attributes
            {
                # key is TTYPEn           val.dtpre       val.req
                'DesId'         : colAttr('int',        REQUIRED),
                'ObjectId'      : colAttr('int',        REQUIRED),
                'dSlitId'       : colAttr('int',        REQUIRED),
                'TopDist'       : colAttr('float',      REQUIRED),
                'BotDist'       : colAttr('float',      REQUIRED),
            },
        ),
    # end dict entry for SlitObjMap

    'MaskBlu'   :
        hduAttr(
            FITStabTypes,   REQUIRED,
            # dictionary of known columns and their attributes
            {
                # key is TTYPEn           val.dtpre       val.req
                'BluId'         : colAttr('int',        REQUIRED),
                'DesId'         : colAttr('int',        REQUIRED),
                'BluName'       : colAttr('chararray',  REQUIRED),
                'guiname'       : colAttr('chararray',  REQUIRED),
                'BluObsvr'      : colAttr('chararray',  REQUIRED),
                'BluCreat'      : colAttr('chararray',  REQUIRED),
                'BluDate'       : colAttr('chararray',  REQUIRED),
                'LST_Use'       : colAttr('float',      REQUIRED),
                'Date_Use'      : colAttr('chararray',  REQUIRED),
                'TELESCOP'      : colAttr('chararray',  REQUIRED),
                'RefrAlg'       : colAttr('chararray',  REQUIRED),
                'AtmTempC'      : colAttr('float',      REQUIRED),
                'AtmPres'       : colAttr('float',      REQUIRED),
                'AtmHumid'      : colAttr('float',      REQUIRED),
                'AtmTTLap'      : colAttr('float',      REQUIRED),
                'RefWave'       : colAttr('float',      REQUIRED),
                'DistMeth'      : colAttr('chararray',  REQUIRED),
            },
        ),
    # end dict entry for MaskBlu

    'BluSlits'  :
        hduAttr(
            FITStabTypes,   REQUIRED,
            # dictionary of known columns and their attributes
            {
                # key is TTYPEn           val.dtpre       val.req
                'bSlitId'       : colAttr('int',        REQUIRED),
                'BluId'         : colAttr('int',        REQUIRED),
                'dSlitId'       : colAttr('int',        REQUIRED),
                'slitX1'        : colAttr('float',      REQUIRED),
                'slitY1'        : colAttr('float',      REQUIRED),
                'slitX2'        : colAttr('float',      REQUIRED),
                'slitY2'        : colAttr('float',      REQUIRED),
                'slitX3'        : colAttr('float',      REQUIRED),
                'slitY3'        : colAttr('float',      REQUIRED),
                'slitX4'        : colAttr('float',      REQUIRED),
                'slitY4'        : colAttr('float',      REQUIRED),
            },
        ),
    # end dict entry for BluSlits

    'RDBmap'    :
        hduAttr(
            FITStabTypes,   OPTIONAL,
            # The table content of RDBmap itself is a dictionary of
            # known columns and their attributes.
            # In the original Tcl-based scheme the content of this table
            # was used as part of the ingestion process.
            # The table content was used to generate Tcl code in conjunction
            # with data structures stored in Sybase.
            # This was an extraordinarly bad idea for many different reasons.

            # dictionary of known columns and their attributes
            {
                # key is TTYPEn           val.dtpre       val.req
                'MEMBER_NAME'   : colAttr('chararray',  REQUIRED),
                'KwdOrCol'      : colAttr('chararray',  REQUIRED),
                'Element'       : colAttr('chararray',  REQUIRED),
                'RDBtable'      : colAttr('chararray',  REQUIRED),
                'RDBfield'      : colAttr('chararray',  REQUIRED),
            },
        ),
    # end dict entry for RDBmap

} # end dict mdfcontent{}
