import re
import numpy
from datetime import datetime

from mdf_content import mdfcontent
from apiutils import MaskUserObId
import logger_utils as log_fun


def mdf_table_rows(hdul, err_report, log):
    MaskDesignRows = hdul['MaskDesign'].data.shape[0]

    if MaskDesignRows == 0:
        msg = "MaskDesign has no rows."
        log.error(msg)
        err_report.append(msg)
        return False, None

    if MaskDesignRows > 1:
        msg = "MaskDesign has more than one row.  We will ingest only one."
        log.warning(msg)
        err_report.append(msg)
        # return True, err_report

    # DesId = hdul['MaskDesign'].data['DesId'][ONLYONE]
    # INSTRUME = hdul['MaskDesign'].data['INSTRUME'][ONLYONE]
    # DesNslit = hdul['MaskDesign'].data['DesNslit'][ONLYONE]
    # DesDATE_PNT = hdul['MaskDesign'].data['DATE_PNT'][ONLYONE]
    #
    # # find e-mail address of mask designer
    # DesAuth = hdul['MaskDesign'].data['DesAuth'][ONLYONE]
    # DesAuthEmail = mbox2email(DesAuth)

    return True, err_report


def mask_blue_rows(hdul, err_report, log):
    MaskBluRows = hdul['MaskBlu'].data.shape[0]

    if MaskBluRows == 0:
        msg = "MaskBlu has no rows."
        log.error(msg)
        err_report.append(msg)
        return False, None
    if MaskBluRows > 1:
        msg = "MaskBlu has more than one row.  We will ingest only one."
        log.warning(msg)
        err_report.append(msg)
        return True, err_report

    return True, err_report


def set_design_pid(db, hdul, maps):
    log = log_fun.get_log()

    # TODO this should be associated to keck-id
    # find e-mail address of mask designer
    DesAuth = hdul['MaskDesign'].data['DesAuth'][0]
    DesAuthEmail = mbox2email(DesAuth)

    # TODO this needs to be keck ID
    # we require that MaskDesign.DesAuth contain a known user e-mail
    # find the primary key for that user
    design_pid = MaskUserObId(db, DesAuthEmail)

    if design_pid is None:
        msg = "no design pid"
        log.error(msg)
        # err_report.append(msg)
    else:
        maps.obid[DesAuth] = design_pid

    return maps


def set_blue_pid(db, hdul, maps):
    log = log_fun.get_log()

    # TODO this needs to be keck-id
    # find e-mail address of mask observer
    BluObsvr = hdul['MaskBlu'].data['BluObsvr'][0]
    BluObsvrEmail = mbox2email(BluObsvr)

    # we require that MaskBlu.BluObsvr contain a known user e-mail
    # find the primary key for that user
    BluPId = MaskUserObId(db, BluObsvrEmail)
    if BluPId is None:
        msg = "no blue pid"
        log.error(msg)
        # err_report.append(msg)
    else:
        maps.obid[BluObsvr] = BluPId

    return maps


def mbox2email(string):
    """
    string  # character string containing a mailbox (RFC 5322 page 45)

    suppose that string contains an RFC 5322 compliant mailbox like
    Preferred Name <user@domain>
    if found return "user@domain"
    """

    # match most any sane e-mail address
    # but not all non-English possibilities
    # https://www.regular-expressions.info/email.html
    emailre = ".*<([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})>.*"

    m = re.match(emailre, string)

    glist = m.groups()

    if len(glist) == 1:
        return glist[0]
    elif len(glist) > 1:
        print("more than one e-mail in %s" % (string))
        return None
    else:
        print("no e-mail in %s" % (string))
        return None
    # end if

# end def mbox2email()


def valTableExt(hdul, extname):
    """
    look at FITS table hdu with EXTNAME extname
    ascertain whether the structure matches our expectations

    Inputs:
        hdul - HDU list from opening a FITS file
        extname - value of EXTNAME in one of those HDUs
    """
    err_report = []

    # does this FITS table contain all known cols?
    for col in mdfcontent[extname].knownCols.keys():
        if col not in hdul[extname].columns.names:
            msg = f"did not find col {col} in extname {extname}"
            err_report.append(msg)

    if err_report:
        # we cannot ingest table extname because it is missing a column
        return err_report

    # do we recognize every col in this FITS table extname?
    for col in hdul[extname].columns.names:

        # Lots of code variations in how we analyze table structure
        # because astropy FITS table reading does not provide
        # clearly documented means of introspection.

        # This is at least partly because of the way astropy uses
        # numpy.recarray objects for FITS table data where everything
        # in from ASCII table is stored as strings until the data
        # array is parsed into numpy.ndarray objects.

        # In fact, it looks like astropy FITS table reading
        # pretty much expects that the FITS table will always
        # conform to what the code expects ... no surprises.

        # That is not a strategy that dare be adopted when
        # accepting input data from the web.
        # We have to validate both the structure and the content.

        # this next fails with tables of zero length
        #print("type(hdul[%s].data[%s][0]) = %s" %
        #(extname, col, type(hdul[extname].data[col][0]))
        #)

        if col not in mdfcontent[extname].knownCols.keys():
            # not an error, but surprising if extra cols exist
            print("unexpected col %s in extname %s" % (col, extname))
        elif mdfcontent[extname].knownCols[col].dtpre in type(hdul[extname].data[col]).__name__:
            # we expect this is true for data columns
            # where the datatype is numpy.chararray
            #print("WHOLECOL %s col %s dtpre %s in col.__name__ %s" %
            #(extname, col,
            #mdfcontent[extname].knownCols[col].dtpre,
            #type(hdul[extname].data[col]).__name__)
            #)
            pass
        elif len(hdul[extname].data[col]) == 0 and (type(hdul[extname].data[col]) == numpy.ndarray):
            # this is probably a BinTable for LRIS
            #print("ZEROROWS %s col %s 0 rows and %s ndarray" %
            #(extname, col,
            #type(hdul[extname].data[col]))
            #)
            pass
        elif mdfcontent[extname].knownCols[col].dtpre in type(hdul[extname].data[col][0]).__name__:
            # we expect this is true for numeric data columns
            # where the datatype is numpy.intNN or numpy.floatNN
            #print("INITROW  %s col %s dtpre %s in [0].__name__ %s" %
            #(extname, col,
            #mdfcontent[extname].knownCols[col].dtpre,
            #type(hdul[extname].data[col][0]).__name__)
            #)
            pass
        else:
            # data[col] is a numpy.ndarray
            print("BADWRONG %s col %s we say %s [col].__name__ %s [col][0].__name__ %s" %
            (extname, col, mdfcontent[extname].knownCols[col].dtpre,
            type(hdul[extname].data[col]).__name__,
            type(hdul[extname].data[col][0]).__name__)
            )

    return err_report

