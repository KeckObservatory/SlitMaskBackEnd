from datetime import datetime
import string

ONLYONE = 0


class MaskInsert:
    def __init__(self, user_info, hdul, db, maps, log, err_report):
        self.keck_id = user_info.keck_id
        self.user_email = user_info.email
        self.hdul = hdul
        self.design_id = hdul['MaskDesign'].data['DesId'][ONLYONE]
        self.db = db
        self.maps = maps
        self.log = log
        self.err_report = err_report
        self.guiname = self.unique_gui_name()

    def get_err_report(self):
        return self.err_report

    def get_maps(self):
        return self.maps

    def log_exception(self, query_name, e):
        msg = f"{query_name} failed: {self.db.cursor.query}: exception: {e}"
        self.log.error(msg)
        self.err_report.append(msg)

    def mask_design(self, row, query):
        try:
            params = (
                # design_id (default)
                row['DesName'],
                int(self.maps.obid[row['DesAuth']]),  # DesPId becomes ObId matching DesAuth
                row['DesCreat'],
                row['DesDate'],  # FITS date is ISO8601 and pgsql groks that
                int(row['DesNslit']),
                int(row['DesNobj']),
                row['ProjName'],
                row['INSTRUME'],
                row['MaskType'],
                float(row['RA_PNT']),  # truly double
                float(row['DEC_PNT']),  # truly double
                row['RADEPNT'],
                float(row['EQUINPNT']),
                float(row['PA_PNT']),
                row['DATE_PNT'], # FITS date is ISO8601 and pgsql groks that
                float(row['LST_PNT']),
                # time_stamp (default)
                self.user_email
            )
        except Exception as e:
            msg = f"Invalid Parameters: {e}"
            self.log.error(msg)
            self.err_report.append(msg)

        try:
            self.db.cursor.execute(query, params)
        except Exception as e:
            self.log_exception("Mask Design Insert", e)
        else:
            result = self.db.cursor.fetchone()
            self.maps.desid[row['DesId']] = result['desid']

    def mask_blue(self, row, query):
        try:
            params = (
                # BluId gets default for new primary key
                self.maps.desid[row['DesId']],  # this is set by mask_design()
                row['BluName'],
                int(self.maps.obid[row['BluObsvr']]),  # BluPId becomes ObId matching BluObsvr
                row['BluCreat'],
                row['BluDate'],  # FITS date is ISO8601 and pgsql groks that
                float(row['LST_Use']),
                row['DATE_USE'],  # FITS date is ISO8601 and pgsql groks that
                int(self.maps.teleid[row['TELESCOP']]),
                float(row['AtmTempC']),
                float(row['AtmPres']),
                float(row['AtmHumid']),
                float(row['AtmTTLap']),
                float(row['RefWave']),
                self.guiname,
                # millseq is NULL at ingest
                # status is NULL at ingest
                # loc is NULL at ingest
                # maskblu.stamp gets default now()
                row['RefrAlg'],
                row['DistMeth'],
            )
        except Exception as e:
            msg = f"Invalid Parameters: {e}"
            self.log.error(msg)
            self.err_report.append(msg)

        try:
            self.db.cursor.execute(query, params)
        except Exception as e:
            self.log_exception("Mask Blue Insert", e)
        else:
            result = self.db.cursor.fetchone()
            # maps.bluid is a dictonary like:  {1: 18756}
            self.maps.bluid[row['BluId']] = result['bluid']

    def design_slit(self, row, query):
        try:
            self.db.cursor.execute(
                query,
                (
                    # dSlitId gets default for new primary key
                    int(self.maps.desid[row['DesId']]),
                    float(row['slitRA']),             # truly double
                    float(row['slitDec']),            # truly double
                    row['slitTyp'],
                    float(row['slitLen']),
                    float(row['slitLPA']),
                    float(row['slitWid']),
                    float(row['slitWPA']),
                    row['slitName'],
                )
            )
        except Exception as e:
            self.log_exception("Slit Design Insert", e)
        else:
            result = self.db.cursor.fetchone()
            self.maps.dslitid[row['dSlitId']] = result['dslitid']

    def blue_slit(self, row, query):
        try:
            self.db.cursor.execute(
                query,
                (
                    int(self.maps.bluid[row['BluId']]),
                    int(self.maps.dslitid[row['dSlitId']]),
                    float(row['slitX1']),
                    float(row['slitY1']),
                    float(row['slitX2']),
                    float(row['slitY2']),
                    float(row['slitX3']),
                    float(row['slitY3']),
                    float(row['slitX4']),
                    float(row['slitY4']),
                )
            )
        except Exception as e:
            self.log_exception("Blue Slit Insert", e)
        else:
            result = self.db.cursor.fetchone()
            self.maps.bslitid[row['bSlitId']] = result['bslitid']

    def target(self, row, query):
        try:
            self.db.cursor.execute(
                query, (
                  # ObjectId gets default for new primary key
                  row['OBJECT'],
                  float(row['RA_OBJ']),
                  float(row['DEC_OBJ']),
                  row['RADESYS'],
                  float(row['EQUINOX']),
                  float(row['MJD-OBS']),            # truly double
                  float(row['mag']),
                  row['pBand'],
                  float(row['RadVel']),
                  float(row['MajAxis']),
                  row['ObjClass'],
                )
            )
        except Exception as e:
            self.log_exception("Target Insert ", e)
            result = None
        else:
            result = self.db.cursor.fetchone()
            self.maps.objectid[row['ObjectId']] = result['objectid']

        return result

    def extended_target(self, row, query, result):
        try:
            self.db.cursor.execute(
                query, (
                  result['objectid'],
                  float(row['MajAxPA']),
                  float(row['MinAxis']),
                )
            )
        except Exception as e:
            self.log_exception("Extended Object Insert ", e)

    def nearby_target(self, row, query, result):
        try:
            self.db.cursor.execute(
                query, (
                  result['objectid'],
                  float(row['PM_RA']),
                  float(row['PM_Dec']),
                  float(row['Parallax']),
                )
            )
        except Exception as e:
            self.log_exception("Nearby Target Insert ", e)

    def slit_target(self, row, query):
        try:
            self.db.cursor.execute(
                query, (
                  int(self.maps.desid[row['DesId']]),
                  int(self.maps.objectid[row['ObjectId']]),
                  int(self.maps.dslitid[row['dSlitId']]),
                  float(row['TopDist']),
                  float(row['BotDist']),
                )
            )
        except Exception as e:
            self.log_exception("Slit Target Insert ", e)

    def unique_gui_name(self):
        """
        outputs:
            unique_gui_name
                a possibly modified variant of GUIname which is at the
                moment not a duplicate of any existing GUIname in database
        """
        # There are special rules for GUIname
        # because of constraints on how and where that is used.

        # MaskBlu.GUIname comes from the DEIMOS MDF file.
        # The mask designer gets to suggest a value.
        # That value may be modified during or after mask ingestion.

        # The original cgiTcl code ingested MaskBlu.GUIname exactly as
        # found in the DEIMOS MDF files submitted to the cgiTcl web code.
        # The post-delivery rewrite that added LRIS into the Sybase
        # scheme used Tcl program lsc2df which generated a GUIname
        # using the original name of the Autoslit .file3 file that
        # was submitted through the cgiTcl web code.
        # In that old SybTcl world the values of GUIname were reviewed
        # after ingestion by a daily cron job MaskKeeper.

        # In this new python code from the 2023/2024 transfer project
        # we endeavour to modify GUIname during ingestion so that there
        # should never be duplicate values in the PostgreSQL database.
        # Nevertheless it remains the case that the PostgreSQL database
        # should still be subjected to at least occasional reviews by
        # code other than the web submission interface because there
        # will likely be cases of mask data which are not adequately
        # handled at the time of mask ingestion.

        # When the mill operator using slitmaskpc runs the MillMasks GUI
        # and scans the mask as milled then the MillMasks GUI will copy
        # MaskBlu.GUIname into Mask.GUIname
        # This duplication of data was done consciously for convenience.
        # It means that a physical mask could have a blueprint where the
        # GUIname values differ in those two table.
        # It also means that in various different sections of code that
        # interact with the slitmask database some code may use
        # MaskBlu.GUIname while other code may use Mask.GUIname
        # So the code can be confusing, especially in the case of the
        # Tcl code where global variables come into existence simply by
        # performing SQL queries.

        # Note that the SQL table definition for Mask.GUIname has
        # allowed for 12 characters, but mask ingestion web code and
        # and mask maintenance cron jobs (e.g., MaskKeeper) have
        # restricted the name to 8 characters.
        # The restriction in length of GUIname is important for
        # 1) the MillMasks program which runs on slitmaskpc during mask milling
        # 2) the physical masks themselves into which the GUIname will be milled
        # 3) the DEIMOS setup/observing GUIs which display the GUIname

        # On DEIMOS masks the text milled onto the mask is in the
        # unilluminated corner of the mask, and for DEIMOS masks the
        # GUIname could be much longer than 8 characters with no problems.

        # On LRIS masks there is no unilluminated region and all of the
        # text milled onto an LRIS mask risks colliding with slitlets
        # because LRIS and its mask design program Autoslit predate the
        # DEIMOS slitmask database processing scheme so they are ignorant
        # of the practical issues of humans handling piles of slitmasks
        # which are effectively indistinguishable.
        # For LRIS masks the GUIname dare not be longer than 8 characters.

        # we require that MaskBlu.GUIname not have whitespace
        GUIname = self.hdul['MaskBlu'].data['guiname'][0]
        okGUIname = GUIname.strip()
        okGUIwords = okGUIname.split()
        lenokGUI = len(okGUIwords)
        if lenokGUI == 0:
            msg = "MaskBlu.GUIname is empty"
            self.log.warning(msg)
        elif lenokGUI > 1:
            msg = ("MaskBlu.GUIname '%s' has embedded whitespace" % (GUIname,))
            self.log.warning(msg)
            # collapse whitespace
            newGUIname = ''.join(okGUIwords)
        else:
            newGUIname = GUIname

        # we require that  MaskBlu.GUIname be just printable ASCII
        # because KTL and the mill code generator only know that
        # For DEIMOS masks that is easy because FITS only knows ASCII.
        # For LRIS masks we need to make sure that lsc2df throws away
        # utf8 that is not ASCII when it uses the input file name.

        # we require that MaskBlu.GUIname be unique in the database
        # originally performed by Tcl Tlib proc notifyDupNames
        # strategy here is different than that code
        # in order to economize on SQL calls
        # replace final character of new GUIname with %
        # select all existing GUIname like that
        # Hope that not all possible final characters have
        # already been used, and use one of those unused.
        gnSelect = ("SELECT GUIname FROM MaskBlu WHERE GUIname LIKE %s;")

        shortGUIname = newGUIname[:7]
        shortGUIlike = newGUIname[:7] + "%"

        try:
            self.db.cursor.execute(gnSelect, (shortGUIlike,))
        except Exception as e:
            msg = ("gnSelect failed: %s: exception class %s: %s" % (db.cursor.query, e.__class__.__name__, e))
            self.log.error(msg)
        # end try gnSelect

        # fetch one at a time and match using listyness
        guinamelist = []
        for resrow in self.db.cursor:
            rowguiname = resrow['guiname'].strip()
            if rowguiname not in guinamelist:
                guinamelist.append(rowguiname)
            else:
                self.log.warning(f"GUIname '{rowguiname}' has dups in db")
        # end for resrow
        for lastchar in string.ascii_letters + string.digits + "_:":
            tryGUIname = shortGUIname + lastchar
            if tryGUIname not in guinamelist:
                self.log.warning(f"tryGUIname '{tryGUIname}' was not in guinamelist {guinamelist}")
                newGUIname = tryGUIname
                break  # end if tryGUIname
        # end for lastchar

        # Note that we have a race here.
        # If another connection to the database inserts the newGUIname
        # that we have selected then there will still be duplicates.
        # We are not going to try to lock the database here so we probably
        # still need something like MaskKeeper to review the database for
        # duplicate GUIname and any other problems that might arise.

        if (newGUIname != GUIname):
            msg = (f"we change MaskBlu.GUIname to {newGUIname}")
            self.log.warning(msg)
            GUIname = newGUIname
        # end if we changed GUIname

        return GUIname


