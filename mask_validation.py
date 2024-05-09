from datetime import datetime
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
        tel = self.hdul['MaskBlu'].data['TELESCOP'][0]

        # we require that MaskBlu.TELESCOP be recognized
        if tel in ('Keck II', 'Keck I'):
            return True

        msg = f"MaskBlu.TELESCOP {tel} is not recognized"
        self.log.warning(msg)
        self.err_report.append(msg)

        return False

    def instrument(self):
        inst = self.hdul['MaskDesign'].data['INSTRUME'][0]

        # we require that MaskDesign.INSTRUME be recognized
        if inst in ('DEIMOS', 'LRIS'):
            return True

        msg = f"MaskDesign.INSTRUME {inst} is not recognized"
        self.log.warning(msg)
        self.err_report.append(msg)

        return False

    def has_emails(self):
        DesAuth = self.hdul['MaskDesign'].data['DesAuth'][0]

        if not self.map.obid[DesAuth]:
            msg = f"The design author email address is missing"
            self.log.warning(msg)
            self.err_report.append(msg)

        BluObsvr = self.hdul['MaskBlu'].data['BluObsvr'][0]
        if not self.map.obid[BluObsvr]:
            msg = f"The Blueprint Observer email address is missing"
            self.log.warning(msg)
            self.err_report.append(msg)

    def has_guiname(self):
        msg = None
        try:
            mdf_gui_name = self.hdul['MaskBlu'].data['guiname'][0]
        except Exception as err:
            mdf_gui_name = None
            msg = f"no MaskBlu keyword: {err}"

        if not mdf_gui_name:
            if not msg:
                msg = f"error finding the mask guiname - MaskBlu.guiname"

        if msg:
            self.log.warning(msg)
            self.err_report.append(msg)

    def slit_number(self):
        data_shape = self.hdul['DesiSlits'].data.shape[0]
        design_nslits = self.hdul['MaskDesign'].data['DesNslit'][0]

        # we require that rows in table DesiSlits match count(DesiSlits.dSlitId)
        if design_nslits == data_shape:
            return True

        msg = ("MaskDesign.DesNslit %s != DesiSlitRows %s", (design_nslits, data_shape))
        self.log.warning(msg)
        self.err_report.append(msg)

        return False

    def date_use(self):
        # TODO
        # +  # we require that MaskBlu.DATE_USE not be too old
        # +  # Currently too old is set at yesterday.
        # +  # Maybe we should tolerate last week, but the idea here is
        # +  # that we do not want people submitting really old mask designs
        # +  # without updating their astrometric characteristics.
        # +  # use astropy.Time because that handles all FITS DATE* values
        # +    BluDATE_USE = hdul['MaskBlu'].data['DATE_USE'][ONLYONE]
        # +    date_use = Time(BluDATE_USE, format='fits')
        # +    tooOld = Time.now() - 1
        # +
        # if date_use < tooOld:
        #     +        msg = ("MaskBlu.DATE_USE %s is older than %s" % (BluDATE_USE, tooOld.fits))
        #

        # we require that MaskBlu.Date_Use be in the future
        # python3 date parse code is fragile because the python devs
        # could not stop messing with the methods in datetime
        # This code supposes python3.6 or later
        # This code supposes that FITS Date_Use values are '%Y-%m-%d'
        mask_use_date = self.hdul['MaskBlu'].data['Date_Use'][0]
        mask_date_use_dt = self._mask_date_str_dt(mask_use_date)

        now = datetime.strptime('2020-01-01', '%Y-%m-%d')
        nowiso = datetime.strftime(now, '%Y-%m-%d')

        if mask_date_use_dt > now:
            return True

        msg = f"MaskBlu.Date_Time {mask_use_date} is before current date: {nowiso}"
        self.log.warning(msg)
        self.err_report.append(msg)

        return False

    def date_pnt(self):
        design_date_pnt = self.hdul['MaskDesign'].data['DATE_PNT'][0]
        date_pnt_dt = self._mask_date_str_dt(design_date_pnt)

        b1900iso = '1900-01-01'
        b1900 = datetime.strptime(b1900iso, '%Y-%m-%d')

        if date_pnt_dt < b1900:
            msg = f"MaskDesign.DATE_PNT {design_date_pnt} is before B1900 {b1900iso}"
            self.log.warning(msg)
            self.err_report.append(msg)
            return False

        return True

    def design_slits(self):
        # loop over content of DesiSlits
        # MDF files created from LRIS .file3 designs have fake DesiSlits
        weirddesid = -1
        for row in self.hdul['DesiSlits'].data:
            # we require that all DesiSlits.DesId be in MaskDesign.DesId
            desid = row['DesId']
            if desid not in self.hdul['MaskDesign'].data['DesId']:
                # foo want to mark row as do not ingest
                if desid != weirddesid:
                    msg = f"DesiSlits has DesId {desid} not in MaskDesign.DesId will not be ingested"
                    self.log.warning(msg)
                    self.err_report.append(msg)

                    weirddesid = desid

    def blue_slits(self):
        blue_id = self.hdul['MaskBlu'].data['BluId'][0]

        # loop over content of BluSlits
        weirdbluid = -1
        weirddslitid = -1
        for row in self.hdul['BluSlits'].data:

            # we require that all BluSlits.BluId = MaskBlu.BluId
            row_blue_id = row['BluId']
            if row_blue_id != blue_id:
                # foo want to mark row as do not ingest
                if row_blue_id != weirdbluid:
                    msg = ("BluSlits has BluId %s != MaskBlu.Bluid %s will not be ingested" % (row_blue_id, blue_id))
                    self.log.warning(msg)
                    self.err_report.append(msg)

                    weirdbluid = row_blue_id

            # we require that all BluSlits.dSlitId be in DesiSlits.dSlitId
            # MDF files created from LRIS .file3 designs have fake dSlitId
            dslitid = row['dSlitId']
            if dslitid not in self.hdul['DesiSlits'].data['dSlitId']:
                # foo want to mark row as do not ingest
                if dslitid != weirddslitid:
                    msg = ("BluSlits has dSlitId %s not in DesiSlits.dSlitId will not be ingested" % (dslitid,))
                    self.log.warning(msg)
                    self.err_report.append(msg)

                    weirddslitid = dslitid

    def slit_object_map(self):
        design_id = self.hdul['MaskDesign'].data['DesId'][0]

        # loop over content of SlitObjMap
        # MDF files created from LRIS mask designs have no objects, so no rows.
        weirddesid = -1
        for row in self.hdul['SlitObjMap'].data:

            # we require that all SlitObjMap.DesId = MaskDesign.DesId
            slit_design_id = row['DesId']
            if slit_design_id != design_id:
                # foo want to mark row as do not ingest
                if slit_design_id != weirddesid:
                    msg = ("SlitObjMap has DesId %s not in MaskDesign.DesId will not be ingested" % (slit_design_id,))
                    self.log.warning(msg)
                    self.err_report.append(msg)

                    weirddesid = slit_design_id

            # we require that all SlitObjMap.ObjectId be in ObjectCat.ObjectId
            objectid = row['ObjectId']
            if objectid not in self.hdul['ObjectCat'].data['ObjectId']:
                # foo want to mark row as do not ingest
                msg = ("SlitObjMap has ObjectdId %s not in ObjectCat.ObjectId will not be ingested" % (objectid,))
                self.log.warning(msg)
                self.err_report.append(msg)

            # we require that all SlitObjMap.dSlitId be in DesiSlits.dSlitId
            dslitid = row['dSlitId']
            if dslitid not in self.hdul['DesiSlits'].data['dSlitId']:
                # foo want to mark row as do not ingest
                msg = ("SlitObjMap has dSlitId %s not in DesiSlits.dSlitId will not be ingested" % (dslitid,))
                self.log.warning(msg)
                self.err_report.append(msg)

    def object_catalogs(self):
        # loop over content of ObjectCat
        # MDF files created from LRIS mask designs have no objects, so no rows.
        for row in self.hdul['ObjectCat'].data:
            catfilepk = row['CatFilePK']
            if row['CatFilePK'] not in self.hdul['CatFiles'].data['CatFilePK']:
                # we require that all ObjectCat.CatFilePK be in CatFiles.CatFilePK
                # No tool which creates MDFs produces records like this.
                msg = ("ObjectCat has CatFilePK %s not in CatFiles.CatFilePK will not be ingested" % (catfilepk,))
                self.log.warning(msg)
                self.err_report.append(msg)

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
                msg = ("ObjectCat has ObjectId %s not in SlitObjMap.ObjectId will not be ingested" % (objectid,))
                self.log.warning(msg)
                self.err_report.append(msg)

    def _mask_date_str_dt(self, header_date_str):
        try:
            date_obj = date_parser.parse(header_date_str)
        except ParserError as err:
            msg = f"The date: {header_date_str} is invalid, error: {err}"
            self.log.warning(msg)
            self.err_report.append(msg)
            return None

        date_str = f"{date_obj.year}-{date_obj.month}-{date_obj.day}"
        date_dt = datetime.strptime(date_str, '%Y-%m-%d')

        return date_dt

