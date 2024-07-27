#! /usr/bin/python3

# tools for access to DEIMOS multi-HDU FITS slitmask description file (MDF)

# Suppress astropy header keyword warnings
import warnings
warnings.filterwarnings('ignore', message='The following header keyword is invalid', category=UserWarning)
warnings.filterwarnings('ignore', message='Invalid keyword for column', category=UserWarning)

from astropy.io import fits

import logger_utils as log_fun
import validate_utils as valid_utils
from mask_validation import MaskValidation
from general_utils import commitOrRollback

from mdf_content import mdfcontent
from slitmask_queries import get_query
from mask_insert import MaskInsert
########################################################################

# to read a MDF we must
# validate that its table structure conforms to the schema
# validate that its table content is self-consistent


class mdf2dbmaps:
    """
    This object holds all of the maps which we construct
    as part of translating primary keys of the MDF into
    primary keys of the tables in the slitmask database.
    The mapping is one way from input FITS file to db, so
    each map is stored as a dictionary
    """

    # this map is to the ObId in ucolick Sybase
    # Keck will want to create a map to their KeckId
    obid = {}
    desid = {}
    bluid = {}
    dslitid = {}
    bslitid = {}
    objectid = {}

    # Keck 1 is 2, Keck 2 is 1 because DEIMOS defined the MDF scheme
    # and LRIS was added later into the MDF scheme
    teleid = {'Keck I': 2, 'Keck II': 1}


########################################################################


########################################################################

class IngestFun:
    def __init__(self, user_info, db, obs_info):
        self.maps = mdf2dbmaps()
        self.user_info = user_info
        self.obs_info = obs_info
        self.log = log_fun.get_log()

        if db is None:
            self.log.error('no database object connection defined.')
        self.db = db

    def get_maps(self):
        return self.maps

    def validate_mdf_content(self, hdul):
        """
        hdul,   # HDU list from opening a FITS file

        examine table contents of multi-extension FITS MDF
        return FAILURE if the content violates any rules
        """
        err_report = []

        ####################################################################
        # MASK Design

        # assume MDF contains exactly one MaskDesign
        valid, err_report = valid_utils.mdf_table_rows(hdul, err_report, self.log)
        if not valid:
            return False, err_report

        self.maps = valid_utils.set_design_pid(self.db, hdul, self.maps, self.obs_info)

        ####################################################################
        # MASK BLUE

        # assume MDF contains exactly one MaskBlu
        valid, err_report = valid_utils.mask_blue_rows(hdul, err_report, self.log)
        if not valid:
            return False, err_report

        self.maps = valid_utils.set_blue_pid(self.db, hdul, self.maps, self.obs_info)

        ####################################################################

        validate = MaskValidation(self.maps, hdul, err_report, self.log)

        # TODO Michael is fixing the SMDT for this
        validate.slit_number()
        validate.has_emails()
        validate.has_guiname()
        validate.instrument()
        validate.telescope()
        validate.date_pnt()
        # TODO temporary to test old masks
        # validate.date_use()
        validate.design_slits()
        validate.blue_slits()
        validate.slit_object_map()
        validate.object_catalogs()

        err_report = validate.get_err_report()

        ####################################################################

        if err_report:
            msg = f"There are {len(err_report)} error(s). The file cannot " \
                  f"be ingested."
            err_report.append(msg)

            return False, err_report

        return True, err_report


    ########################################################################

    def validate_MDF(self, hdul):
        """
        hdul,           # astropy FITS hdulist
        db,             # connection to slitmask database
        maps,           # mdf2dbmaps object

        attempt to read hdul
        ascertain whether it is a multi-HDU FITS mask description file (MDF)
        ascertain whether the data in the MDF satisfy all validity rules
        """
        missing = []

        # does this FITS file contain all known HDUs?
        for hdu in mdfcontent.keys():
            if hdu not in hdul:
                missing.append(f"Did not find HDU {hdu}")

        if missing:
            missing.append(f"MDF cannot be ingested, it is missing tables:")
            return False, missing

        hdu_report = []

        # loop over all HDUs in this FITS file
        for hdu in hdul:
            msg = None
            if isinstance(hdu, fits.PrimaryHDU):
                pass
            elif hdu.name not in mdfcontent:
                # not an error, but surprising if extra HDUs exist
                msg = f"unexpected EXTNAME {hdu.name}"
            elif type(hdu) not in mdfcontent[hdu.name].hdutypes:
                msg = f"hdutype {type(hdu)} for EXTNAME {hdu.name} " \
                      f"mdfcontent[hdu.name].hdutypes {mdfcontent[hdu.name].hdutypes}" \
                      f"wrong hdutype {type(hdu).__name__} for EXTNAME {hdu.name}"

            if msg:
                hdu_report.append(msg)
                self.log.warning(msg)

        if hdu_report:
            msg = f"There is an issue(s) with the MDF HDUs."
            msg += "\n".join([f"* {item}" for item in hdu_report])
            self.log.error(msg)
            return False, hdu_report

        badtables = None
        # loop over all FITS HDUs that we expect in a MDF
        for extname in mdfcontent.keys():
            # does the table structure of this HDU match our expectations?
            badtables = valid_utils.valTableExt(hdul, extname)

        if badtables:
            msg = f"The MDF file has malformed tables."
            msg += "\n".join([f"* {item}" for item in badtables])
            self.log.error(msg)
            return False, badtables

        # validate the content
        status, err_report = self.validate_mdf_content(hdul)

        return status, err_report


    ########################################################################

    def ingestMDF(self, file, save_path):
        """

        file,           # path to a MDF file
        db,             # connection to slitmask database
        maps            # mdf2dbmaps object

        suppose file might be a MDF
        validate the structure and content of the file
        insert data from its FITS tables into the database
        """
        # open the FITS file
        try:
            hdul = fits.open(file)
        except Exception as e:
            msg = f"could not open file: {file.filename}: check that it is a FITS file!"
            self.log.error(f"{msg}: exception: {e} ")
            return False, [msg]

        # validate the structure and content of the file
        try:
            valid, err_report = self.validate_MDF(hdul)
        except Exception as err:
            msg = f"There was an error validating the MDF: {err}"
            err_report = [msg]
            valid = False

        # save file,  pass if there is an issue saving the file
        try:
            hdul.writeto(save_path, overwrite=True)
        except Exception as err:
            self.log.warning(f"Error saving file: {err}")

        if not valid:
            return False, err_report

        if self.db is None:
            err_report.append("The mask user is missing!")

            return False, err_report

        ####################
        insert = MaskInsert(self.user_info, hdul, self.db, self.maps,
                            self.log, err_report)

        # MaskDesign -> maskdesign
        #   @validate lookup map DesAuth from e-mail to ObId as DesPId
        #   @insert   use DesPId from validate
        #   @insert   create map DesId to value of new primary key in maskdesign

        # table MaskDesign was created with
        # DesId       SERIAL                  PRIMARY KEY,
        # stamp       timestamp without time zone DEFAULT now()
        mask_design_query = get_query('mask_design_insert')

        for row in hdul['MaskDesign'].data:
            insert.mask_design(row, mask_design_query)

        ####################

        # MaskBlu -> maskblu
        #   @validate lookup map BluObsvr from e-mail to ObId as BluPId
        #   @validate lookup GUIname to see if it already exists
        #   @insert   use BluPId from validate
        #   @insert   create map BluId to value of new primary key in maskblu
        #   @insert   use DesId map from MaskDesign -> maskdesign
        #   @insert   hack GUIname to be unique

        # table MaskBlu was created with
        # BluId       SERIAL                  PRIMARY KEY,
        # stamp       timestamp without time zone DEFAULT now()
        mask_blue_query = get_query('mask_blue_insert')

        for row in hdul['MaskBlu'].data:
            insert.mask_blue(row, mask_blue_query)

        ####################

        # DesiSlits -> desislits
        #   @insert   use DesId map from MaskDesign -> maskdesign
        #   @insert   create map all dSlitId to value of new primary key in desislits

        # table DesiSlits was created with
        # dSlitId     SERIAL                  PRIMARY KEY,
        design_slit_query = get_query('design_slit_insert')

        for row in hdul['DesiSlits'].data:
            insert.design_slit(row, design_slit_query)

        ####################

        # BluSlits  -> bluslits
        #   @insert   use BluId map from MaskBlu -> maskblu
        #   @insert   use all dSlitId map from DesiSlits -> desislits
        #   @insert   create map all bSlitId to value of new primary key in bluslits
        #             but that map we never use later

        # table BluSlits was created with
        # bSlitId     SERIAL                  PRIMARY KEY
        # bad         INTEGER                 DEFAULT 0
        blue_slit_query = get_query('blue_slit_insert')

        for row in hdul['BluSlits'].data:
            insert.blue_slit(row, blue_slit_query)

        ####################

        # ObjectCat -> objects
        #   @insert   create map all ObjectId to value of new primary key in objects
        # ExtendObj -> objects
        #   only if any of those fields in Objects were not NULL
        #   @insert   use all ObjectId map from ObjectCat -> objects
        # NearObj -> objects
        #   only if any of those fields in Objects were not NULL
        #   @insert   use all ObjectId map from ObjectCat -> objects

        # table objects was created with
        # ObjectId    SERIAL                  PRIMARY KEY
        # bad         INTEGER                 DEFAULT 0
        target_query = get_query('target_insert')
        extended_target_query = get_query('extended_target_insert')
        nearby_target_query = get_query('nearby_target_insert')

        for row in hdul['ObjectCat'].data:
            result = insert.target(row, target_query)

            if not result:
                continue

            # all indicators say that astropy FITS table I/O
            # does not detect NULL values in FITS table
            # sla sees here that NULL values are reported as value 0.
            # therefore this code tests against 0.
            if (row['MajAxPA'] != 0.) or (row['MinAxis'] != 0.):
                insert.extended_target(row, extended_target_query, result)
            else:
                pass

            # all indicators say that astropy FITS table I/O
            # does not detect NULL values in FITS table
            # sla sees here that NULL values are reported as value 0.
            # therefore this code tests against 0.
            if (row['PM_RA'] != 0.) or (row['PM_Dec'] != 0.) or (row['Parallax'] != 0.):
                insert.nearby_target(row, nearby_target_query, result)
            else:
                pass

        err_report = insert.get_err_report()

        ####################

        # SlitObjMap
        #   @insert   use DesId map from MaskDesign -> maskdesign
        #   @insert   use all ObjectId map from ObjectCat -> objects
        #   @insert   use all dSlitId map from DesiSlits -> desislits
        slit_object_query = get_query('slit_target_insert')

        for row in hdul['SlitObjMap'].data:
            insert.slit_target(row, slit_object_query)

        ####################

        if len(err_report) != 0:
            err_report.append(f"We have errors before ingesting!")
            return False, err_report

        committed, message = commitOrRollback(self.db)

        if committed:
            self.log.info("commitOrRollback worked, self.db should be changed")
            success = True
        else:
            msg = f"There was an error ingesting file!  The file did not store" \
                  f"in the database properly."
            err_report.append(msg)

            msg += f"(commitOrRollback failed): {message}"
            self.log.info(msg)

            success = False

        ####################

        # clear maps before we do next MDF
        self.maps.obid.clear()
        self.maps.desid.clear()

        hdul.close()

        return success, err_report


    def convertLRIStoMDF(file3path, email, date_use):
        """
        given a .file3 file created by the LRIS mask design software
        convert that to a mask design FITS

        The .file3 files have contents which are basically the same as the
        input for the Windows-based Surfcam program.
        They contain coordinates on metal for the edges of slitlets
        which are to be milled into a mask for LRIS.
        They do not contain any of the celestial coordinate metadata
        nor any of the celestial object metadata which are part of
        MDF files for DEIMOS.
        Therefore the MDF files created by this function have
        FITS tables where the records for those metadata are NULL.

        In the 2023/2024 rewrite for PostgreSQL the lsc2df program
        remains as a Tcl script which is found in Keck SVN under
        kroot/util/slitmask/xfer2keck/tcl/lsc2df

        inputs:
        file3path   path to .file3 Surfcam file from LRIS mask design software
        email       e-mail address of a known mask submitter to put into MDF
        date_use    FITS DATE* string value to put into MDF

        outputs:
        MDFfile     path to DEIMOS-like mask design FITS tables (MDF) file
        """

        # convention is that we name the output MDF file like the input .file3
        file3 = os.path.basename(file3path)
        mdfname = None

        # Despite the 2023/2024 rewrite for PostgreSQL the lsc2df Tcl code
        # outputs some messages to stdout and stderr.
        # We expect that sometimes stdout and stderr will be useful
        # for debugging problems.
        # When we last checked the Makefile for lsc2df creates KROOT/var/lsc2df/log
        lsc2dfOut = "@KROOT@/var/lsc2df/log/%s.out" % file3
        lsc2dferr = "@KROOT@/var/lsc2df/log/%s.err" % file3
        # we choose 'w' with the expectation that we want to overwrite
        # any previous attempts to process the same input file
        STDOUT = open(lsc2dfOut, 'w')
        STDERR = open(lsc2dferr, 'w')

        # the 2023/2024 version of lsc2df code is in ../tcl
        # When we last checked the Makefile for lsc2df has BINSUB = maskpgtcl
        lsc2df = "@RELDIR@/bin/maskpgtcl/lsc2df"

        # we are going to use subprocess.call even if we are python3
        status = subprocess.call([lsc2df,
        "%s %s %s %s" % (file3path, email, mdfname, date_use)],
        stdout=STDOUT, stderr=STDERR)
        # make sure output gets flushed
        STDOUT.close()
        STDERR.close()

        if status != 0:
            tclog.logger.error(
            "%s failed: see stdout %s and stderr %s"
            % (lsc2df, lsc2dfOut, lsc2dferr) )

            # return empty string as the path of the output file
            return ""
        # end if status

        # when we last checked the Makefile for lsc2df creates KROOT/var/lsc2df
        # we expect that lsc2df has created a MDF file with this name
        mdfOutD = "@KROOT@/var/lsc2df"
        maskfits = "%s/%s.fits" % (mdfOutD, file3)

        return maskfits
