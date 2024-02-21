from datetime import datetime

class MaskInsert:
    def __init__(self, keck_id, db, maps, log, err_report):
        self.keck_id = keck_id
        self.db = db
        self.maps = maps
        self.log = log
        self.err_report = err_report

    def get_err_report(self):
        return self.err_report

    def get_maps(self):
        return self.maps

    def log_exception(self, query_name, e):
        msg = f"{query_name} failed: {self.db.cursor.query}: exception: {e}"
        self.log.error(msg)
        self.err_report.append(msg)

    def mask_design(self, row, query):
        time_stamp = datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
        print(self.keck_id, time_stamp)
        try:
            self.db.cursor.execute(
                query,
                (
                    # DesId
                    row['DesName'],
                    int(self.maps.obid[row['DesAuth']]),  # DesPId becomes ObId matching DesAuth
                    row['DesCreat'], row['DesDate'],  # FITS date is ISO8601 and pgsql groks that
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

                    float(row['LST_PNT']),  # maskdesign.stamp gets default now()
                    self.keck_id,
                    time_stamp
                    # float(row['LST_PNT']), # maskdesign.stamp gets default now()
                )
            )
        except Exception as e:
            self.log_exception("Mask Design Insert", e)
        else:
            result = self.db.cursor.fetchone()
            self.maps.desid[row['DesId']] = result['desid']

    def mask_blue(self, row, query, guiname):
        try:
            self.db.cursor.execute(
                query,
                (
                    # BluId gets default for new primary key
                    int(self.maps.desid[row['DesId']]),
                    row['BluName'],
                    int(self.maps.obid[row['BluObsvr']]),  # BluPId becomes ObId matching BluObsvr
                    row['BluCreat'],
                    row['BluDate'],                   # FITS date is ISO8601 and pgsql groks that
                    float(row['LST_Use']),
                    row['DATE_USE'],                  # FITS date is ISO8601 and pgsql groks that
                    int(self.maps.teleid[row['TELESCOP']]),
                    float(row['AtmTempC']),
                    float(row['AtmPres']),
                    float(row['AtmHumid']),
                    float(row['AtmTTLap']),
                    float(row['RefWave']),
                    guiname,
                    # millseq is NULL at ingest
                    # status is NULL at ingest
                    # loc is NULL at ingest
                    # maskblu.stamp gets default now()
                    row['RefrAlg'],
                    row['DistMeth'],
                )
            )
        except Exception as e:
            self.log_exception("Mask Blue Insert", e)
        else:
            result = self.db.cursor.fetchone()
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


