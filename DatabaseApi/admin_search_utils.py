import re

from apiutils import mask_user_id
import logger_utils as log_fun

from mask_constants import READY

from slitmask_queries import get_query


def admin_search(options, db, obs_info):
    """
    A list of known keys which may be found in dict.

    The original cgiTcl web page code displayed these options to the user in order.
    The original cgiTcl web page code looked at this list in order.
    Only the first match was used to construct the SQL query.

    firstcomelist = [
        'email', value may match e-mail of MaskDesign.DesPId or MaskBlu.BluPid
        'guiname',  value may be like MaskBlu.GUIname
        'name',  value may be like MaskBlu.BluName or MaskDesign.DesName
        'bluid',  value(s) may match MaskBlu.BluId (range)
        'desid',  value(s) may match MaskDesign.DesId (range)
        'millseq',  value(s) may match MaskBlu.MillSeq (range)
        'barcode',  value(s) may match Mask.MaskId (range) the barcode(s) on mask(s)
        'milled',  value may be one of (all, no, yes) default is all
        'caldays'  calendar days until MaskBlu.Date_Use
    ]

    for email
    the slitmask FITS tables structure allows for
    one e-mail address in the MaskDesign table row (Mask Designer)
    one e-mail address in the MaskBlu table row (Mask Observer)
    The web ingestion software has always required that both of these
    e-mail addresses are known in the database of registered observers;
    in the Keck scheme this will be the database of known PI web logins.
    The web ingestion software converts the input FITS table e-mail value
    into the primary key ObId in the database of registered observers.

    for guiname
    Note that mask ingestion software MUST ensure that values
    of GUIname are unique among all masks which are currently
    submitted and not yet destroyed.
    We must write new ingestion code which enforces this uniqueness
    requirement.

    for name
    Note that BluName and DesName are whatever was supplied by the
    mask designers and there is no expectation of uniqueness.
    It is for this reason that this python code differentiates
    guiname from bluname and desname when the original cgiTcl
    web pages did not.

    if desid is a single value then the SQL is
    MaskDesign.DesId=desid
    if desid is two values then the SQL is
    MaskDesign.DesId>=desid1 and MaskDesign.DesId<=desid2
    This roughly works as expected because the mask ingestion
    process assigns the primary key DesId in increasing sequence.
    if desid is more than two values then the SQL is
    MaskDesign.DesId in (desid1,desid2,desid3...)

    for bluid
    the SQL is just like the above for desid
    with comparison to MaskBlu.BluId

    for millseq
    the SQL is just like the above for desid
    except that millseq are alphanumeric rather than integer
    with comparison to MaskBlu.MillSeq
    Note that the mask ingestion software SHOULD ensure that values
    of millseq are sequential so that there is very little chance
    of a duplicate millseq during the interval between submission
    and milling.
    Because millseq is two uppercase alpha characters that by
    itself gives 26*26 values.
    So if each new millseq value is sequential after the the
    previous millseq value then the chance of duplication
    is very small.

    for barcode
    the SQL is just like the above for desid
    with comparison to Mask.MaskId
    Note that barcode and MaskId means the number on the barcode
    sticky label that is applied after the mask is milled.
    Note that barcode is supposed to be unique, but that is only true
    if when Keck re-orders barcode labels they start the next batch
    with a number larger than the previous batch, and we know that
    Keck once failed to do that by ordering new labels which restarted
    from zero and ended up causing ambiguity between new masks and some
    of the very old, very early calibration masks with long lifetimes.
    So NOTE WELL, whoever is ordering more barcode labels for slitmasks
    should always note the high value of the previous order and ask the
    printer to make the next batch starting with greater values.
    The barcodes have six decimal digits so there can be a million
    masks during the lifetime of the DEIMOS and LRIS database.

    for milled
      milled = all (default)
          masks regardless of mill status
      milled = no
          only masks that have unmilled blueprints
              MaskBlu.status < READY
      milled = yes (really anything besides "all" and "no")
          only masks that have milled blueprints
              MaskBlu.status = READY

    The above options are mutually exclusive. First one wins.

    These next two options are not exclusive:

    for limit
    limit the ordered query to the last this many masks

    for inst
    Query only for instrument: DEIMOS, LRIS, both
    MaskDesign.INSTRUME ilike %BLANK%
    we use ilike in this query to handle LRIS and LRIS-ADC
    Note that a query by barcode=MaskId ignores this instrument limitation


    :param options: set of options to query on
    :type options: dict
    :return:
    :rtype:
    """
    log = log_fun.get_log()

    results_str = "d.stamp, d.desid, d.desname, d.desdate, projname, ra_pnt, " \
                  "dec_pnt, radepnt, o.keckid, o.firstnm, o.lastnm, o.email, " \
                  "o.institution, b.status, b.guiname, " \
                  "COALESCE(b.millseq, m.MillSeq) AS millseq"


    # we will construct a SQL query
    # query_args will become the arguments for that SQL query
    query_args = []

    # first we evaluate whether to limit by instrument
    if 'inst' in options:
        inst_query = ""
        # if "DEIMOS" == options['inst']:
        if re.search(r'^DEIMOS.+', options['inst']):
            inst_query = "d.INSTRUME = %s and"
            query_args.append("DEIMOS")
        elif re.search(r'^LRIS.+', options['inst']):
            inst_query = "d.INSTRUME ilike %s and"
            query_args.append("LRIS%")
        else:
            # we take anything else as matching any MaskDesign.INSTRUME
            # and we do not complain about unrecognized values
            pass

        search_q = (f"SELECT {results_str} FROM MaskDesign d "
                               f"JOIN Observers o ON o.ObId = d.DesPId "
                               f"LEFT JOIN MaskBlu b ON b.DesId = d.DesId ")
        if inst_query:
            search_q += f"WHERE {inst_query} "

    # step through the exclusive keys in order
    elif 'email' in options:
        # Before trying to query for matching masks
        # we want to ascertain whether email matches a known user
        # so that we can report a separate error about the
        # unrecognized value of email.

        search_obid = mask_user_id(db, options['email'], obs_info)

        if search_obid == None:
            msg = f"{options['email']} - user is not in database of known mask users."
            log.warning(msg)
            return {'query': None, 'query_args': None, 'msg': msg}

        search_q = get_query('search_email')

        query_args.append(search_obid)  # does DesPId match ObId
        query_args.append(search_obid)  # does BluPId match ObId

    elif 'guiname' in options and options['guiname'] != "":
        # match GUIname - which is unique by definition
        search_q = get_query('search_guiname')

        # '%guiname%' for GUIname ilike match
        query_args.append("%" + options['guiname'] + "%")

    elif 'name' in options and options['name'] != "":
        # match either MaskDesign.DesName or MaskBlu.BluName
        search_q = get_query('search_blue_name')

        query_args.append("%" + options['name'] + "%")
        query_args.append("%" + options['name'] + "%")

    elif 'bluid' in options and options['bluid'] != "" and options['bluid'] != [""]:
        # options['bluid'] should be a list of MaskBlu.BluId values
        numBlu = len(options['bluid'])

        if numBlu == 2:
            # query the range between the MaskBlu.BluId values
            # bilist = sorted(options['bluid'])
            bilist = options['bluid']
            minbi = bilist[0]
            maxbi = bilist[-1]

            search_q = get_query('search_blue_id_eq2')

            # arguments for BluId between
            query_args.append(minbi)
            query_args.append(maxbi)

        elif numBlu > 2:
            # query the list of given MaskBlu.BluId values
            search_q = get_query('search_blue_id_gt2')
            search_q += f" AND b2.BluId IN (" + ",".join("%s" for i in options['bluid']) + ")) "

            # arguments for BluId in ()
            for bluid in options['bluid']:
                query_args.append(bluid)  # end for bluid

        else:
            # numBlu == 1
            search_q = get_query('search_blue_id_eq1')

            # argument for BluId = match
            query_args.append(options['bluid'][0])  # end if numBlu

    elif 'desid' in options and options['desid'] != "" and options['desid'] != [""]:
        # options['desid'] should be a list of MaskDesign.DesId values
        numDes = len(options['desid'])

        # query for desid is easier than for bluid
        if numDes == 2:
            # query between the given MaskDesign.DesId values
            # dilist = sorted(options['desid'])
            dilist = options['desid']
            mindi = dilist[0]
            maxdi = dilist[-1]

            search_q = get_query('search_design_id_eq2')

            # arguments for DesId between
            query_args.append(mindi)
            query_args.append(maxdi)

        elif numDes > 2:
            # query the list of given MaskDesign.DesId values
            search_q = get_query('search_design_id_gt2')
            search_q += f" WHERE d.DesId in (" + ",".join("%s" for i in options['desid']) + ") "

            # arguments for DesId in ()
            for desid in options['desid']:
                query_args.append(desid)  # end for desid
        else:
            search_q = get_query('search_design_id_eq1')

            # argument for DesId = match
            query_args.append(options['desid'][0])

    elif 'millseq' in options and options['millseq'] != "":
        # options['desid'] should be a list of MaskDesign.DesId values
        numSeq = len(options['millseq'])

        if numSeq == 2:
            # query between the given MaskBlu.MillSeq/Mask.MillSeq values
            mslist = options['millseq']
            minms = mslist[0]
            maxms = mslist[-1]

            search_q = get_query('search_millseq_eq2')

            # arguments for MaskBlu.MillSeq between
            query_args.append(minms)
            query_args.append(maxms)

            # arguments for Mask.MillSeq between
            query_args.append(minms)
            query_args.append(maxms)

        elif numSeq > 2:
            # query the list of given MaskBlu.MillSeq values
            search_q = get_query('search_millseq_gt2')
            search_q += (
                    f"AND MillSeq IN (" + ",".join("%s" for i in options['millseq']) + ")) "
                    f"OR EXISTS (SELECT * FROM MaskBlu WHERE DesId = d.DesId "
                    f"AND BluId IN (SELECT BluId FROM Mask WHERE MillSeq IN "
                    f"(" + ",".join("%s" for i in options['millseq']) + ")))) "
            )

            # arguments for MaskBlu.MillSeq in ()
            for millseq in options['millseq']:
                query_args.append(millseq)

            # end for millseq
            # arguments for Mask.MillSeq in ()
            for millseq in options['millseq']:
                query_args.append(millseq)  # end for millseq

        else:
            # numSeq == 1
            search_q = get_query('search_millseq_eq1')

            # arguments for MaskBlu.MillSeq =
            query_args.append(options['millseq'][0])

            # arguments for Mask.MillSeq =
            query_args.append(options['millseq'][0])

    elif 'barcode' in options and options['barcode'] != "" and options['barcode'] != [""]:
        # options['barcode'] should be a list of barcode=maskId values
        numMasks = len(options['barcode'])

        # these SQL statements ignore the instrument because
        # the instrument is inherent for each milled mask
        if numMasks == 2:
            # query between the given MaskId=barcode values
            bclist = options['barcode']

            minbc = bclist[0]
            maxbc = bclist[-1]

            search_q = get_query('search_barcode_eq2')

            # arguments for Mask.MaskId between
            query_args.append(minbc)
            query_args.append(maxbc)

        elif numMasks > 2:
            # query the list of given MaskId=barcode values
            search_q = get_query('search_barcode_gt2')
            search_q += ",".join("%s" for i in options['barcode']) + "))) "

            # arguments for Mask.MaskId in ()
            for barcode in options['barcode']:
                query_args.append(barcode)

        else:
            # numMasks == 1
            search_q = get_query('search_barcode_eq1')

            # arguments for Mask.MaskId =
            query_args.append(options['barcode'][0])

    elif ('milled' in options) and (options['milled'] != "all") and options['milled'] != "":
        if options['milled'] == "no":
            search_q = get_query('search_milled_no')

        else:
            # assume options['milled'] = "yes"
            search_q = get_query('search_milled_yes')

        query_args.append(READY)

    elif 'caldays' in options and options['caldays'] != "":
        search_q = get_query('search_cal_days')

        # argument for Date_Use diff between 0 and caldays
        query_args.append(options['caldays'])

    else:
        # this is the default admin query when nothing in dict
        search_q = get_query('search_other')

    if search_q:
        search_q += f"ORDER BY d.stamp DESC;"

    # convert the argument list into a tuple
    queryargtup = tuple(i for i in query_args)

    return {'query': search_q, 'query_args': queryargtup, 'msg': None}


