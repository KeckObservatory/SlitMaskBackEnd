from datetime import datetime, timedelta
from dateutil import parser as date_parser
from dateutil.parser import ParserError


class MaskValidation:
    def __init__(self, map, hdul, err_report, log):
        self.hdul = hdul
        self.log = log
        self.err_report = err_report
        self.map = map

    def get_err_report(self):
        return self.err_report

    def telescope(self):
        """
        Confirm the telescope is defined as either: Keck I or Keck II.

        :return: <bool> True if the MaskBlu.TELESCOP = Keck I or Keck II.
        """
        tel = self.hdul['MaskBlu'].data['TELESCOP'][0]

        # we require that MaskBlu.TELESCOP be recognized
        if tel in ('Keck II', 'Keck I'):
            return True

        msg = f"The defined Telescope (MaskBlu.TELESCOP) {tel} is " \
              f"not recognized."

        self.log.warning(msg)
        self.err_report.append(msg)

        return False

    def instrument(self):
        """
        Confirm the instrument is defined as either: DEIMOS or LRIS.

        :return: <bool> True if the MaskDesign.INSTRUME = DEIMOS or LRIS.
        """
        inst = self.hdul['MaskDesign'].data['INSTRUME'][0]

        # we require that MaskDesign.INSTRUME be recognized
        if inst in ('DEIMOS', 'LRIS'):
            return True

        msg = f"The defined instrument (MaskDesign.INSTRUME) {inst} is " \
              f"not recognized."

        self.log.warning(msg)
        self.err_report.append(msg)

        return False

    def has_emails(self):
        """
        Confirm the MDF has email addresses set for the
            Design Author: MaskDesign.DesAuth
            Observer Email: MaskBlu.BluObsvr

        :return: <bool> True if both emails exist
        """
        state = True
        DesAuth = self.hdul['MaskDesign'].data['DesAuth'][0]

        if not self.map.obid[DesAuth]:
            msg = f"The (MaskDesign.DesAuth) design author email address =" \
                  f" '{DesAuth}' is invalid or not a current Keck Account" \
                  f"Email.  Format is 'user name <email@address.edu>'."

            self.log.warning(msg)
            self.err_report.append(msg)

            state = False

        BluObsvr = self.hdul['MaskBlu'].data['BluObsvr'][0]

        if not self.map.obid[BluObsvr]:
            msg = f"The (MaskBlu.BluObsvr) blueprint observer email address =" \
                  f" '{BluObsvr}'is invalid or not a current Keck Account" \
                  f"Email.  Format is 'user name <email@address.edu>'."

            self.log.warning(msg)
            self.err_report.append(msg)

            state = False

        return state

    def has_guiname(self):
        """
        Check if the MDF file contains a guiname.

        :return: <bool> True if GUINAME found in the MDF file.
        """
        msg = None
        try:
            mdf_gui_name = self.hdul['MaskBlu'].data['guiname'][0]
        except Exception as err:
            mdf_gui_name = None

        if not mdf_gui_name:
            msg = f"The Mask Blueprint Name (MaskBlu.guiname) is missing: {err}."

        if msg:
            self.log.warning(msg)
            self.err_report.append(msg)

            return False

        return True

    def slit_number(self):
        """
        Check that the number of slits stated in the Mask Design is equal to the
        number of slits in the design.
            hdul['MaskDesign'].data['DesNslit'][0] = integer written by the
                design software.  Should be an array of 1.
            hdul['DesiSlits'].data.shape[0] = the length of the array
                containing the slits.

        :return: <bool> True if the the two numbers are equal.
        """
        data_shape = self.hdul['DesiSlits'].data.shape[0]
        design_nslits = self.hdul['MaskDesign'].data['DesNslit'][0]

        # we require that rows in table DesiSlits match count(DesiSlits.dSlitId)
        if design_nslits == data_shape:
            return True

        msg = f"Design Number of slits (MaskDesign.DesNslit): " \
              f"{design_nslits} is not equal to the number of slit rows " \
              f"(DesiSlitRows): {data_shape}."

        self.log.warning(msg)
        self.err_report.append(msg)

        return False

    def date_use(self):
        """
        we require that MaskBlu.Date_Use be in the future
        python3 date parse code is fragile because the python devs
        could not stop messing with the methods in datetime
        This code supposes python3.6 or later
        This code supposes that FITS Date_Use values are '%Y-%m-%d'

        :return: <bool> True - Date_Use is okay,  after yesterday
        """

        mask_use_date = self.hdul['MaskBlu'].data['Date_Use'][0]
        mask_date_use_dt = self._mask_date_str_dt(mask_use_date)

        yesterday = datetime.now() - timedelta(days=1)

        if mask_date_use_dt > yesterday:
            return True

        # msg = f"MaskBlu.Date_Time {mask_use_date} is before yesterday: {yesterday}"
        msg = f"Invalid mask blueprint date,  the date (MaskBlu.Date_Use) " \
              f"{mask_use_date} is before: {yesterday.strftime('%Y-%m-%d')}."
        self.log.warning(msg)
        self.err_report.append(msg)

        return False

    def date_pnt(self):
        """
        Check that the date pointing is not before 1900.

        :return: <bool> True if date > 1900
        """
        design_date_pnt = self.hdul['MaskDesign'].data['DATE_PNT'][0]
        date_pnt_dt = self._mask_date_str_dt(design_date_pnt)

        b1900iso = '1900-01-01'
        b1900 = datetime.strptime(b1900iso, '%Y-%m-%d')

        if date_pnt_dt < b1900:
            msg = f"The Date of Pointing (MaskDesign.DATE_PNT) " \
                  f"{design_date_pnt} is before B1900 {b1900iso}."

            self.log.warning(msg)
            self.err_report.append(msg)

            return False

        return True

    def design_slits(self):
        """
        Check that the design's slits are okay by loop over content of DesiSlits.

        For LRIS: MDF files created from LRIS .file3 designs have fake DesiSlits.

        :return: <bool> True if no problem found in the design's slits.
        """
        state = True

        weirddesid = -1
        for row in self.hdul['DesiSlits'].data:
            # we require that all DesiSlits.DesId be in MaskDesign.DesId
            desid = row['DesId']
            if desid not in self.hdul['MaskDesign'].data['DesId']:
                # foo want to mark row as do not ingest
                if desid != weirddesid:
                    msg = f"The slit design (DesiSlits) has a slit ID " \
                          f"(DesId) {desid} that is not in the Mask Design " \
                          f"(MaskDesign.DesId)."

                    self.log.warning(msg)
                    self.err_report.append(msg)

                    weirddesid = desid

                    state = False

        return state

    def blue_slits(self):
        """
        Check the Blue Slits (BluSlits) against Mask Blue (MaskBlu) and
        Design Slits (DesiSlits).  Require Blue Slits to be in both Mask Blue
        and Design Slits.

        :return: <bool> True if all slits check out.
        """
        blue_id = self.hdul['MaskBlu'].data['BluId'][0]

        # loop over content of BluSlits
        weirdbluid = -1
        weirddslitid = -1
        for row in self.hdul['BluSlits'].data:

            # we require that all BluSlits.BluId = MaskBlu.BluId
            row_blue_id = row['BluId']
            if row_blue_id != blue_id:
                # want to mark row as do not ingest
                if row_blue_id != weirdbluid:
                    msg = f"A slit ID in Blueprint Slits (BluSlits) has an ID" \
                          f" (BluId) {row_blue_id} that is not equal to the " \
                          f"Mask Blueprint (MaskBlu.Bluid) {blue_id}. "

                    self.log.warning(msg)
                    self.err_report.append(msg)

                    weirdbluid = row_blue_id

            # we require that all BluSlits.dSlitId be in DesiSlits.dSlitId
            # MDF files created from LRIS .file3 designs have fake dSlitId
            dslitid = row['dSlitId']
            if dslitid not in self.hdul['DesiSlits'].data['dSlitId']:
                # foo want to mark row as do not ingest
                if dslitid != weirddslitid:
                    msg = f"The blueprint (BluSlits) has slit ID (dSlitId) " \
                          f"{dslitid} that is not in the design slits " \
                          f"(DesiSlits.dSlitId)."

                    self.log.warning(msg)
                    self.err_report.append(msg)

                    weirddslitid = dslitid

            if weirdbluid != -1 or weirddslitid != -1:
                return False

            return True

    def slit_object_map(self):
        """
        Check the content of SlitObjMap and confirm

        * the DesId in both SlitObjMap and MaskDesign match,
          SlitObjMap.DesId = MaskDesign.DesId.

        * The ObjectId matches the ObjectId in the ObjectCat
          SlitObjMap.ObjectId be in ObjectCat.ObjectId

        * The object map design slit id is in the design slits design slit id
          SlitObjMap.dSlitId be in DesiSlits.dSlitId

        LRIS: MDF files created from LRIS mask designs have no objects, so no rows.

        :return: <bool> True if all criteria are meet.
        """
        design_id = self.hdul['MaskDesign'].data['DesId'][0]

        state = True
        weirddesid = -1

        for row in self.hdul['SlitObjMap'].data:

            # we require that all SlitObjMap.DesId = MaskDesign.DesId
            slit_design_id = row['DesId']
            if slit_design_id != design_id:
                # want to mark row as do not ingest
                if slit_design_id != weirddesid:
                    msg = f"The slit object mapping (SlitObjMap) has a slit" \
                          f"(DesId) {slit_design_id} not in the Mask Design" \
                          f"(MaskDesign.DesId)."

                    self.log.warning(msg)
                    self.err_report.append(msg)

                    weirddesid = slit_design_id
                    state = False

            # we require that all SlitObjMap.ObjectId be in ObjectCat.ObjectId
            objectid = row['ObjectId']
            if objectid not in self.hdul['ObjectCat'].data['ObjectId']:
                # want to mark row as do not ingest
                msg = f"The slit object mapping (SlitObjMap) has an object ID" \
                      f"(ObjectdId) {objectid} that is not in the Object" \
                      f"Catalog (ObjectCat.ObjectId)."

                self.log.warning(msg)
                self.err_report.append(msg)

                state = False

            # we require that all SlitObjMap.dSlitId be in DesiSlits.dSlitId
            dslitid = row['dSlitId']
            if dslitid not in self.hdul['DesiSlits'].data['dSlitId']:
                # want to mark row as do not ingest
                msg = f"The slit object mapping (SlitObjMap) has a design" \
                      f"slit (dSlitId) {dslitid} that is not in the design" \
                      f"slit IDs (DesiSlits.dSlitId)."

                self.log.warning(msg)
                self.err_report.append(msg)

                state = False

            return state

    def object_catalogs(self):
        """
        Check the content of the obsject catalog,  ObjectCat.

        * confirm ObjectCat.CatFilePK be in CatFiles.CatFilePK

        * confirm the object ID in the object catalog is the same as
          the object id in slit object map.
            ObjectCat.ObjectId be in SlitObjMap.ObjectId

        LRIS: MDF files created from LRIS mask designs have no objects, so no rows.

        :return: <bool> True if all criteria are meet.
        """
        # loop over content of ObjectCat
        # MDF files created from LRIS mask designs have no objects, so no rows.
        state = True
        for row in self.hdul['ObjectCat'].data:
            catfilepk = row['CatFilePK']
            if row['CatFilePK'] not in self.hdul['CatFiles'].data['CatFilePK']:
                # we require that all ObjectCat.CatFilePK be in CatFiles.CatFilePK
                # No tool which creates MDFs produces records like this.
                msg = f"The object catalog (ObjectCat) has a catalog file " \
                      f"(CatFilePK) {catfilepk} not in the the catalog files" \
                      f"(CatFiles.CatFilePK)."

                self.log.warning(msg)
                self.err_report.append(msg)

                state = False

            objectid = row['ObjectId']
            if row['ObjClass'] == 'Guide_Star':
                # DSIMULATOR has always included guide stars in its object catalog table
                # We ingest those guide stars because we are not sure.
                # We think that they do not correspond to a slitlet.
                # They may be important when setting telescope and rotator
                # position during mask alignment on sky before exposure.
                # They may be important during data reduction.
                pass
            elif row['ObjectId'] not in self.hdul['SlitObjMap'].data['ObjectId']:
                # we require that all ObjectCat.ObjectId be in SlitObjMap.ObjectId
                msg = f"The object catalog (ObjectCat) has an object ID " \
                      f"(ObjectId) {objectid} that is not in slit-object " \
                      f"mapping (SlitObjMap.ObjectId)."

                self.log.warning(msg)
                self.err_report.append(msg)

                state = False

        return state

    def _mask_date_str_dt(self, header_date_str):
        """
        Helper to convert the mask date strings to datetime objects.

        :param header_date_str: <str> the date string from the header / table.

        :return: <obj/None> the datetime object,  or None if not successful.
        """
        try:
            date_obj = date_parser.parse(header_date_str)
        except ParserError as err:
            msg = f"The date: {header_date_str} is invalid, error: {err}."
            self.log.warning(msg)
            self.err_report.append(msg)
            return None

        date_str = f"{date_obj.year}-{date_obj.month}-{date_obj.day}"
        date_dt = datetime.strptime(date_str, '%Y-%m-%d')

        return date_dt

