import re

from apiutils import mask_user_id
import logger_utils as log_fun

from mask_constants import MaskBluStatusMILLED


def admin_search(options, db, sql_params):
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
              MaskBlu.status < MaskBluStatusMILLED
      milled = yes (really anything besides "all" and "no")
          only masks that have milled blueprints
              MaskBlu.status = MaskBluStatusMILLED

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

    results_str = "d.desid, d.desname, d.desdate, projname, ra_pnt, dec_pnt, " \
                  "radepnt, o.keckid, o.firstnm, o.lastnm, o.email, o.institution"


    # we will construct a SQL query
    # query_args will become the arguments for that SQL query
    query_args = []

    # first we evaluate whether to limit by instrument
    inst_query = ""
    if 'inst' in options:
        if "DEIMOS" == options['inst']:
            # with DEIMOS MaskDesign.INSTRUME is always DEIMOS
            inst_query = "d.INSTRUME = %s and"
            query_args.append("DEIMOS")
        elif re.search(r'^LRIS.+', options['inst']):
            # with LRIS MaskDesign.INSTRUME might be like LRIS-ADC
            inst_query = "d.INSTRUME ilike %s and"
            query_args.append("LRIS%")
        else:
            # we take anything else as matching any MaskDesign.INSTRUME
            # and we do not complain about unrecognized values
            pass

    # step through the exclusive keys in order
    if 'email' in options:
        # Before trying to query for matching masks
        # we want to ascertain whether email matches a known user
        # so that we can report a separate error about the
        # unrecognized value of email.

        search_obid = mask_user_id(db, options['email'], sql_params)
        # obid = user_info.ob_id

        if search_obid == None:
            log.warning(f"user is not in database of known mask users {options['email']}")
            return None, None

        adminInventoryQuery = (
            f"select {results_str} from MaskDesign d, Observers o where {inst_query} "
            f"( d.DesPId = %s or d.DesId in (select DesId from MaskBlu "
            f"where BluPId = %s)) and o.ObId = d.DesPId order by d.stamp desc;"
        )
        query_args.append(search_obid)  # does DesPId match ObId
        query_args.append(search_obid)  # does BluPId match ObId

    elif 'guiname' in options and options['guiname'] != "":
        # match GUIname which the mask ingestion software should make unique
        print("found 'guiname'  = %s" % (options['guiname'],))
        adminInventoryQuery = (
            f"select {results_str} from MaskDesign d, Observers o where {inst_query} "
            f"d.DesId in (select DesId from MaskBlu where GUIname ilike %s) "
            f"and o.obid = d.DesPId order by d.stamp desc;"
        )

        # '%guiname%' for GUIname ilike match
        query_args.append("%" + options['guiname'] + "%")

    elif 'name' in options and options['name'] != "":
        # match either MaskDesign.DesName or MaskBlu.BluName
        print("found 'name'  = %s" % (options['name'],))
        adminInventoryQuery = (
            f"select {results_str} from MaskDesign d, Observers o where {inst_query} "
            f"d.DesName ilike %s or d.DesId in (select DesId from MaskBlu"
            f" where BluName ilike %s) and o.obid = d.DesPId order by "
            f"d.stamp desc;"
        )
        # '%name%' for DesName ilike match
        query_args.append("%" + options['name'] + "%")
        # '%name%' for BluName ilike match
        query_args.append("%" + options['name'] + "%")

    elif 'bluid' in options:
        print("found 'bluid' = %s" % (options['bluid'],))

        # options['bluid'] should be a list of MaskBlu.BluId values
        numBlu = len(options['bluid'])

        if numBlu == 2:
            # query between the given MaskBlu.BluId values
            bilist = sorted(options['bluid'])
            minbi = bilist[0]
            maxbi = bilist[-1]

            adminInventoryQuery = (
                f"select {results_str} from MaskDesign d, Observers o where {inst_query} "
                f"exists (select * from MaskBlu where DesId = d.DesId and BluId "
                f"in (select BluId from MaskBlu where BluId between %s and %s)) "
                f"and o.ObId = d.DesPId order by d.stamp desc;"
            )

            # arguments for BluId between
            query_args.append(minbi)
            query_args.append(maxbi)

        elif numBlu > 2:
            # query the list of given MaskBlu.BluId values
            adminInventoryQuery = (
                f"select {results_str} from MaskDesign d, Observers o where exists (select "
                "* from MaskBlu where DesId = d.DesId and BluId in (select "
                "BluId from Mask where BluId in (" +
                ",".join("%s" for i in options['bluid']) +
                "))) and o.ObId = d.DesPId order by d.stamp desc;"
            )
            
            # arguments for BluId in ()
            for bluid in options['bluid']:
                query_args.append(bluid)  # end for bluid

        else:
            # numBlu == 1
            adminInventoryQuery = (
                f"select {results_str} from MaskDesign d, Observers o where {inst_query} "
                f"d.DesId in (select DesId from MaskBlu where BluId = %s)"
                f" and o.ObId = d.DesPId order by d.stamp desc;"
            )

            # argument for BluId = match
            query_args.append(options['bluid'][0])  # end if numBlu

    elif 'desid' in options:
        print("found 'desid' = %s" % (options['desid'],))

        # options['desid'] should be a list of MaskDesign.DesId values
        numDes = len(options['desid'])

        # query for desid is easier than for bluid
        if numDes == 2:
            # query between the given MaskDesign.DesId values
            dilist = sorted(options['desid'])
            mindi = dilist[0]
            maxdi = dilist[-1]

            adminInventoryQuery = (
                f"select {results_str} from MaskDesign d, Observers o where {inst_query} "
                f"d.DesId between %s and %s and o.ObId = d.DesPId order by "
                f"d.stamp desc;"
            )

            # arguments for DesId between
            query_args.append(mindi)
            query_args.append(maxdi)
        elif numDes > 2:
            # query the list of given MaskDesign.DesId values
            adminInventoryQuery = (
                f"select {results_str} from MaskDesign d, Observers o where {inst_query} "
                f"d.DesId in (" + ",".join("%s" for i in options['desid']) +
                f" ) and o.ObId = d.DesPId order by d.stamp desc;"
            )
            # arguments for DesId in ()
            for desid in options['desid']:
                query_args.append(desid)  # end for desid
        else:
            # numDes == 1
            print("found 'desid' = %s" % (options['desid'],))
            adminInventoryQuery = (
                f"select {results_str} from MaskDesign d, Observers o where {inst_query} "
                f"d.DesId = %s  and o.ObId = d.DesPId order by d.stamp desc;"
            )
            # argument for DesId = match
            query_args.append(options['desid'][0])  # end if numDes

    elif 'millseq' in options:
        print("found 'millseq' = %s" % (options['millseq'],))

        # options['desid'] should be a list of MaskDesign.DesId values
        numSeq = len(options['millseq'])

        if numSeq == 2:
            # query between the given MaskBlu.MillSeq/Mask.MillSeq values
            mslist = sorted(options['millseq'])
            minms = mslist[0]
            maxms = mslist[-1]

            adminInventoryQuery = (
                f"select {results_str} from MaskDesign d, Observers o where ("
                #   look in table MaskBlu for maybe not yet milled blueprints
                "   exists ( select * from MaskBlu where DesId = d.DesId "
                "and MillSeq between %s and %s ) or"
                #   look in table Mask for maybe long ago milled masks
                " exists ( select * from MaskBlu where DesId = d.DesId "
                "and BluId in ( select BluId from Mask where MillSeq between "
                "%s and %s ))) and o.Obid = d.DesPid order by d.stamp desc;"
            )

            # arguments for MaskBlu.MillSeq between
            query_args.append(minms)
            query_args.append(maxms)
            # arguments for Mask.MillSeq between
            query_args.append(minms)
            query_args.append(maxms)

        elif numSeq > 2:
            # query the list of given MaskBlu.MillSeq values
            adminInventoryQuery = (
                f"select {results_str} from MaskDesign d, Observers o where ("
                #   look in table MaskBlu for maybe not yet milled blueprints
                " exists ( select * from MaskBlu where DesId = d.DesId and "
                "MillSeq in (" + ",".join("%s" for i in options['millseq']) +
                ")) or"
                #   look in table Mask for maybe long ago milled masks
                " exists ( select * from MaskBlu where DesId = d.DesId and "
                "BluId in ( select BluId from Mask where MillSeq in "
                "(" + ",".join("%s" for i in options['millseq']) + ")))) "
                "and o.Obid = d.DesPid order by d.stamp desc;"
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

            adminInventoryQuery = (
                f"select {results_str} from MaskDesign d, Observers o where ("
                #   look in table MaskBlu for maybe not yet milled blueprints
                " exists ( select * from MaskBlu where DesId = d.DesId and "
                "MillSeq = %s ) or"
                #   look in table Mask for maybe long ago milled masks
                " exists ( select * from MaskBlu where DesId = d.DesId and "
                "BluId in ( select BluId from Mask where MillSeq = %s ))) and "
                "o.Obid = d.DesPid order by d.stamp desc;"
            )

            # arguments for MaskBlu.MillSeq =
            query_args.append(options['millseq'][0])

            # arguments for Mask.MillSeq =
            query_args.append(options['millseq'][0])

        # end if numSeq

    elif 'barcode' in options:
        print("found 'barcode' = %s" % (options['barcode'],))

        # options['barcode'] should be a list of barcode=maskId values
        numMasks = len(options['barcode'])

        # these SQL statements ignore the instrument because
        # the instrument is inherent for each milled mask
        if numMasks == 2:
            # query between the given MaskId=barcode values
            bclist = sorted(options['barcode'])
            minbc = bclist[0]
            maxbc = bclist[-1]

            adminInventoryQuery = (
                f"select {results_str} from MaskDesign d, Observers o where exists ("
                "select * from MaskBlu where DesId = d.DesId and BluId in "
                "(select BluId from Mask where MaskId between %s and %s )) "
                "and o.ObId = d.DesPId order by d.stamp desc;"
            )

            # arguments for Mask.MaskId between
            query_args.append(minbc)
            query_args.append(maxbc)

        elif numMasks > 2:
            # query the list of given MaskId=barcode values

            adminInventoryQuery = (
                f"select {results_str} from MaskDesign d, Observers o where exists ("
                "select * from MaskBlu where DesId = d.DesId and BluId in ("
                "select BluId from Mask where MaskId in (" +
                ",".join("%s" for i in options['barcode']) +
                "))) and o.ObId = d.DesPId order by d.stamp desc;"
            )

            # arguments for Mask.MaskId in ()
            for barcode in options['barcode']:
                query_args.append(barcode)  # end for barcode

        else:
            # numMasks == 1

            adminInventoryQuery = (
                f"select {results_str} from MaskDesign d, Observers o where exists ("
                "select * from MaskBlu where DesId = d.DesId and BluId in ("
                "select BluId from Mask where MaskId = %s )) and "
                "o.ObId = d.DesPId order by d.stamp desc;"
            )

            # arguments for Mask.MaskId =
            query_args.append(options['barcode'][0])

        # end if numMasks

    elif ('milled' in options) and (options['milled'] != "all"):
        print("found 'milled' = %s" % (options['milled'],))

        if options['milled'] == "no":

            adminInventoryQuery = (
                f"select {results_str} from MaskDesign d, Observers o where {inst_query}"
                f" exists (select * from MaskBlu where DesId = d.DesId and "
                f"status < %s and o.ObId = d.DesPId order by d.stamp desc;"
            )

        else:
            # assume options['milled'] = "yes"

            adminInventoryQuery = (
                f"select {results_str} from MaskDesign d, Observers o where {inst_query} "
                f"exists (select * from MaskBlu where DesId = d.DesId and "
                f"status = %s and o.ObId = d.DesPId order by d.stamp desc;"
            )

        # end if options['milled']
        # argument for MaskBlu.status
        query_args.append(MaskBluStatusMILLED)

    elif 'caldays' in options:
        print("found 'caldays' = %s" % (options['caldays'],))

        adminInventoryQuery = (
            f"select {results_str} from MaskDesign d, Observers o where {inst_query} "
            f"date_part('day', (select max(Date_Use) from MaskBlu where "
            f"DesId = d.DesId) - now()) between 0 and %s and o.ObId = d.DesPId "
            f"order by d.stamp desc;")
        # argument for Date_Use diff between 0 and caldays
        query_args.append(options['caldays'])

    else:
        print("found no key in dict")
        # this is the default admin query when nothing in dict

        adminInventoryQuery = (
            f"select {results_str} from MaskDesign d, Observers o where {inst_query} "
            f"o.ObId = d.DesPId order by d.stamp desc;"
        )

    # end if stepping through exclusive query keys

    # convert the argument list into a tuple
    queryargtup = tuple(i for i in query_args)

    # during development display the query
    print(adminInventoryQuery % queryargtup)

    return adminInventoryQuery, queryargtup

