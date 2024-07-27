import re
import numpy
from datetime import datetime

from mdf_content import mdfcontent
from apiutils import mask_user_id
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


def set_design_pid(db, hdul, maps, obs_info):
    log = log_fun.get_log()

    # parse design author e-mail address
    DesAuth = hdul['MaskDesign'].data['DesAuth'][0]
    log.info('Mask design author email (MaskDesign.DesAuth)')
    DesAuthEmail = mbox2email(DesAuth)
    log.info('Parsed mask design author email (DesAuthEmail)')

    # get user
    design_pid = mask_user_id(db, DesAuthEmail, obs_info)

    # required that the design author is a known email address (validated later)
    if design_pid is None:
        log.error("no design pid")
        maps.obid[DesAuth] = None
    else:
        maps.obid[DesAuth] = design_pid

    return maps


def set_blue_pid(db, hdul, maps, obs_info):
    log = log_fun.get_log()

    # parse mask blue observer e-mail address
    BluObsvr = hdul['MaskBlu'].data['BluObsvr'][0]
    BluObsvrEmail = mbox2email(BluObsvr)

    # we require that MaskBlu.BluObsvr contain a known user e-mail (validated later)
    # find the primary key for that user
    BluPId = mask_user_id(db, BluObsvrEmail, obs_info)
    if BluPId is None:
        log.error("no blue pid")
        maps.obid[BluObsvr] = None
    else:
        maps.obid[BluObsvr] = BluPId

    return maps


def mbox2email(email_str):
    """
    email_str  # character string containing a mailbox (RFC 5322 page 45)

    Suppose that string contains an RFC 5322 compliant mailbox like
        Preferred Name <user@domain>
            if found return "user@domain"

    Also matches user@domain,  and checks that only one email is input

    """
    log = log_fun.get_log()

    email_regex = r'<?([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})>?'

    match = re.search(email_regex, email_str)

    if not match:
        return None

    emails_tuple = match.groups()
    emails_len = len(emails_tuple)

    if emails_len == 1:
        return emails_tuple[0]
    elif emails_len > 1:
        log.error(f"more than one e-mail in {email_str}")
        return None

    log.error(f"no e-mail in {email_str}")
    
    return None



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

