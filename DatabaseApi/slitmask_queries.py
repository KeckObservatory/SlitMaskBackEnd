from mask_constants import READY, ARCHIVED, PERPETUAL_DATE

ownership_queries = {
    "blue_person": """
        SELECT DesId AS MaskId
        FROM MaskBlu
        WHERE BluId = %s and BluPId = %s;
        """,

    "design_person": """
        SELECT DesPId AS MaskId
        FROM MaskDesign
        WHERE DesId = %s and DesPId = %s
        UNION
        SELECT BluPId AS MaskId
        FROM MaskBlu
        WHERE DesId = %s and BluPId = %s;
        """,

    "design_to_blue": "SELECT bluid FROM maskblu WHERE desid = %s",

    "blue_to_design": "SELECT desid FROM maskblu WHERE bluid = %s",

    # used to get all the < yr 2024 account obids,  > yr 2024 accounts obid=keckid
    "obid_column": "SELECT obid, keckid FROM observers",
    "keckid_from_obid": "SELECT keckid FROM observers WHERE obid = %s",

    # used to find emails
    "blue_pi": "SELECT blupid FROM maskblu WHERE bluid = %s",
    "design_pi": "SELECT despid FROM maskdesign WHERE desid = %s",
    "pi_keck_id": "SELECT keckid FROM observers WHERE obid = %s",
}


retrieval_queries = {
    "mill": f"""
        SELECT b.BluId, b.status, b.Date_Use, b.stamp, b.GUIname,
               b.millseq, d.desid, d.desnslit, d.desname, d.instrume
        FROM MaskBlu b
        JOIN MaskDesign d ON d.DesId = b.DesId
        WHERE (b.status < {READY} OR b.status IS NULL
               OR (b.BluId NOT IN (SELECT BluId FROM Mask) AND b.status < {ARCHIVED}))
        ORDER BY b.Date_Use;
    """,

    "standard_mask": f"""
        SELECT m.MaskId, b.GUIname, b.BluName, b.BluId, b.Date_Use,
               m.milldate, d.instrume, d.desid
        FROM MaskBlu b, Mask m, MaskDesign d
        WHERE b.status < {ARCHIVED} 
              AND b.Date_Use >= TIMESTAMP '{PERPETUAL_DATE}'
              AND m.bluid = b.bluid AND d.DesId = b.DesId
        """,

    "user_inventory": """
        SELECT d.*, b.guiname, b.status, b.Date_Use
        FROM MaskDesign d
        LEFT JOIN MaskBlu b ON d.DesId = b.DesId
        WHERE d.DesPId IN (
            SELECT id FROM unnest(%s) AS id
        )
        AND (d.DesPId = %s OR d.DesId IN 
            (SELECT DesId FROM MaskBlu WHERE BluPId = %s)) 
        ORDER BY d.stamp DESC;
    """,

    "blueprint": """
        SELECT d.instrume, b.bluname, b.guiname
        FROM MaskBlu b, MaskDesign d
        WHERE b.BluId = %s and d.DesId = b.DesId
        """,

    "slit": """
        SELECT b.bad, b.slitX1, b.slitY1, b.slitX2, b.slitY2, b.slitX3, 
               b.slitY3, b.slitX4, b.slitY4, b.dSlitId, d.slitTyp
        FROM BluSlits b, DesiSlits d
        WHERE b.BluId = %s and d.dSlitId = b.dSlitId
        """,

    "design": "SELECT * FROM MaskDesign WHERE DesId = %s",

    # "design_author_obs": "SELECT * FROM Observers WHERE ObId = %s;",

    "objects": """
        SELECT * FROM Objects WHERE ObjectId IN
        (SELECT ObjectId FROM SlitObjMap WHERE DesId = %s)
        """,

    "slit_obj": "SELECT * FROM SlitObjMap WHERE DesId = %s ORDER BY dSlitId",

    "design_slits": "SELECT * FROM DesiSlits WHERE DesId = %s",

    "mask_blue": "SELECT * FROM MaskBlu WHERE DesId = %s",

    # "blue_obs_obs": "SELECT * FROM Observers WHERE ObId = %s",

    "blue_slit": "SELECT * FROM BluSlits WHERE BluId = %s",

    "blue_mask": "SELECT * FROM Mask WHERE BluId = %s",

    "extend_update": """
        UPDATE MaskBlu SET Date_Use =
         Date_Use + (%s * INTERVAL '1 day'),
         stamp = CURRENT_DATE 
         WHERE DesId = %s;            
        """,

    "chk_design": "select DesPId from MaskDesign where DesID = %s",

    "mask_exists_blue": "select status from MaskBlu where bluid = %s",

    "chk_mask": "select maskid from Mask where maskid = %s",
    "chk_barcode_blue": "select maskid from Mask where maskid = %s and bluid = %s"
}

admin_queries = {

    "recent": """
        SELECT m.MillDate, m.MillId, m.GUIname, b.millseq, m.maskid,
        d.DesName, d.DesId,
        b.BluId, b.status,
        d.DesNslit, d.INSTRUME,
        b.Date_Use
        FROM Mask m, MaskBlu b, MaskDesign d
        WHERE m.MillDate >= %s
        AND b.BluId = m.BluId
        AND d.DesId = b.DesId
        ORDER BY m.MillDate
        """,

    "recent_barcode": """
        SELECT m.MillDate, m.MillId, m.GUIname, b.millseq, m.maskid,
        d.DesName, d.DesId,
        b.BluId, b.status,
        d.DesNslit, d.INSTRUME,
        b.Date_Use
        FROM Mask m, MaskBlu b, MaskDesign d
        WHERE m.MillDate >= %s
        AND b.BluId = m.BluId
        AND d.DesId = b.DesId
        ORDER BY m.MaskId
        """,

    "recent_barcode_owner": """
        SELECT m.MillDate, m.GUIname, m.millseq, m.maskid,
        d.DesName, d.DesId, d.despid,
        b.BluId, b.bluname,
        d.DesNslit, d.INSTRUME,
        b.Date_Use
        FROM Mask m, MaskBlu b, MaskDesign d
        WHERE m.MillDate >= %s
        AND b.BluId = m.BluId
        AND d.DesId = b.DesId
        ORDER BY m.MaskId
        """,

    "timeline": """
        SELECT d.stamp, b.Date_Use, b.bluid, b.GUIname, b.millseq, d.DesId, 
        b.status, d.DesName, d.DesNslit, d.INSTRUME, m.MillDate
        FROM MaskDesign d, Mask m RIGHT JOIN MaskBlu b
        ON m.BluId = b.BluId
        WHERE d.stamp >= %s AND d.DesId = b.DesId 
        ORDER BY b.Date_Use, d.INSTRUME
        """,

    "mask_valid": """
    SELECT 
        m.MaskId, m.GUIname, m.MillSeq, b.Date_Use, d.desid, b.bluid,
        b.status, d.INSTRUME, subquery.obid
    FROM 
        Mask m, MaskBlu b, MaskDesign d
    JOIN 
        (SELECT unnest(%s) AS obid) AS subquery ON d.DesPId = subquery.obid
    WHERE 
        b.BluId = m.BluId
        AND d.DesId = b.DesId
    ORDER BY 
        d.INSTRUME, m.MaskId
        """,

    "remill_set_date": "UPDATE MaskBlu SET date_use = TIMESTAMP %s WHERE bluid = %s",

    # mask delete queries
    "mask_table_delete": "DELETE FROM Mask WHERE MaskId = %s",
    "mask_table_select": "SELECT * FROM Mask WHERE MaskId = %s",
    "mask_table_bluid": "SELECT * FROM Mask WHERE Bluid = %s",
    "blueprint_status": "SELECT * FROM MaskBlu WHERE Bluid = %s",

    "update_perpetual": f"""
        UPDATE MaskBlu SET Date_Use = TIMESTAMP '{PERPETUAL_DATE}' 
        WHERE DesId = %s""",

    "forgotten_status": f"""
        UPDATE MaskBlu SET status = {READY} 
        WHERE bluid IN (
            SELECT BluId FROM MaskBlu 
            WHERE DesId = %s AND status = {ARCHIVED}
            )        
        """,

}

ingest_queries = {
    "mask_design_insert": """
    INSERT INTO MaskDesign (
        DesId,
        DesName,
        DesPId,
        DesCreat,
        DesDate,
        DesNslit,
        DesNobj,
        ProjName,
        INSTRUME,
        MaskType,
        RA_PNT,
        DEC_PNT,
        RADEPNT,
        EQUINPNT,
        PA_PNT,
        DATE_PNT,
        LST_PNT,
        stamp,
        maskumail
    ) VALUES (
        DEFAULT, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
        DEFAULT, %s) 
    RETURNING desid
    """,

    "mask_blue_insert": """
        INSERT INTO maskblu (
            BluId,
            DesId,
            BluName,
            BluPId,
            BluCreat,
            BluDate,
            LST_Use,
            DATE_USE,
            TeleId,
            AtmTempC,
            AtmPres,
            AtmHumid,
            AtmTTLap,
            RefWave,
            guiname, 
            millseq, 
            status, 
            loc, 
            stamp,
            RefrAlg,
            DistMeth
        ) VALUES (
            DEFAULT, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
            %s, NULL, %s, NULL, DEFAULT, %s, %s) 
        RETURNING bluid
        """, 
    "design_slit_insert": """
        INSERT INTO desislits (
            dSlitId,
            DesId,
            slitRA,
            slitDec,
            slitTyp,
            slitLen,
            slitLPA,
            slitWid,
            slitWPA,
            slitName
        ) values (
            DEFAULT, %s, %s, %s, %s, %s, %s, %s, %s, %s
        ) RETURNING dslitid
        """, 
    "blue_slit_insert": """
        INSERT INTO bluslits (
            bSlitId,
            BluId,
            dSlitId,
            slitX1,
            slitY1,
            slitX2,
            slitY2,
            slitX3,
            slitY3,
            slitX4,
            slitY4,
            bad
        ) VALUES (
            DEFAULT, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, DEFAULT
        ) RETURNING bslitid
        """, 
    "target_insert": """
        INSERT INTO objects (
            ObjectId,
            OBJECT,
            RA_OBJ,
            DEC_OBJ,
            RADECSYS,
            EQUINOX,
            MJD_OBS,
            mag,
            pBand,
            RadVel,
            MajAxis,
            ObjClass
        ) VALUES (
            DEFAULT, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        ) RETURNING objectid
        """, 
    "extended_target_insert": """
        INSERT INTO extendobj (
            ObjectId,
            MajAxPA,
            MinAxis
        ) VALUES (
            %s, %s, %s
        )
        """, 
    "nearby_target_insert": """
        INSERT INTO nearobj (
            ObjectId,
            PM_RA,
            PM_Dec,
            Parallax
        ) VALUES (
            %s, %s, %s, %s
        )
        """,

    "slit_target_insert": """
        INSERT INTO slitobjmap (
            DesId,
            ObjectId,
            dSlitId,
            TopDist,
            BotDist
        ) VALUES (
            %s, %s, %s, %s, %s
        )
        """

}

validate_queries = {

    "align_box_query": """
        SELECT d.DesId, d.dSlitId, d.slitWid, d.slitLen, d.slitLPA, 
                d.slitWPA, b.bSlitId from DesiSlits d, BluSlits b 
        WHERE d.slitTyp = 'A' 
            AND d.DesId = (select DesId from MaskBlu where BluId = %s) 
            AND b.dSlitId = d.dSlitId and b.BluId = %s
        """
}

auxiliary_queries = {
    "barcode_to_pointing": f"""
        SELECT mb.bluid, mb.desid, mb.guiname, md.ra_pnt, md.dec_pnt, 
               md.equinpnt, md.pa_pnt
        FROM maskblu mb 
        JOIN mask m ON mb.bluid = m.bluid 
        JOIN maskdesign md ON mb.desid = md.desid 
        WHERE m.maskid = %s;
        """,
    "guiname_to_pointing": f"""
        SELECT mb.bluid, mb.desid, mb.guiname, md.ra_pnt, md.dec_pnt, 
               md.equinpnt, md.pa_pnt
        FROM maskblu mb 
        JOIN mask m ON mb.bluid = m.bluid 
        JOIN maskdesign md ON mb.desid = md.desid 
        WHERE mb.guiname = %s;
        """,
    "sias_type1": """
        SELECT b.date_use,c.maskid,b.guiname,a.instrume,d.lastnm,
              d.firstnm,b.bluid 
        FROM MaskDesign a, MaskBlu b, Mask c, observers d 
        WHERE date_use>= %s 
            AND date_use <= %s 
            AND (b.status<9 OR b.status IS NULL) 
            AND c.bluid=b.bluid 
            AND a.desid=b.desid 
            AND d.obid=b.blupid 
        ORDER BY date_use
          """,
    "sias_type2": """
        SELECT b.date_use,b.guiname,a.instrume,c.lastnm,c.firstnm,b.bluid 
        FROM MaskDesign a, MaskBlu b, observers c 
        WHERE date_use >= %s AND date_use <= %s
            AND (b.status<9 or b.status is null) 
            AND a.desid=b.desid and c.obid=b.blupid 
            AND NOT exists (SELECT * FROM Mask WHERE bluid=b.bluid) 
            ORDER BY date_use
    """
}

# the results to return for the admin search table
results_str = "d.stamp, d.desid, d.desname, d.desdate, d.instrume, projname, " \
              "ra_pnt, dec_pnt, radepnt, o.keckid, o.firstnm, o.lastnm, " \
              "o.email, o.institution, b.status, b.guiname, " \
              "COALESCE(b.millseq, m.MillSeq) AS millseq"

# the admin search table queries,  one query per search option
admin_search_queries = {
    "search_email": f"SELECT {results_str} FROM MaskDesign d "
                    "JOIN Observers o ON o.ObId = d.DesPId "
                    "LEFT JOIN MaskBlu b ON b.DesId = d.DesId "
                    "LEFT JOIN Mask m ON m.BluId = b.BluId "
                    "WHERE (d.DesPId = %s OR d.DesId IN "
                    "(SELECT DesId FROM MaskBlu WHERE BluPId = %s))",

    "search_guiname": f"SELECT {results_str} FROM MaskDesign d "
                      "JOIN Observers o ON o.ObId = d.DesPId "
                      "JOIN MaskBlu b ON b.DesId = d.DesId "
                      "LEFT JOIN Mask m ON m.BluId = b.BluId "
                      "WHERE b.GUIname ILIKE %s ",

    "search_blue_name": f"SELECT {results_str} FROM MaskDesign d "
                        "JOIN Observers o ON o.ObId = d.DesPId "
                        "LEFT JOIN MaskBlu b ON b.DesId = d.DesId "
                        "LEFT JOIN Mask m ON m.BluId = b.BluId "
                        "WHERE (d.DesName ILIKE %s OR b.BluName ILIKE %s) ",

    "search_blue_id_eq2": f"SELECT {results_str} FROM MaskDesign d "
                          "JOIN Observers o ON o.ObId = d.DesPId "
                          "LEFT JOIN MaskBlu b ON b.DesId = d.DesId "
                          "LEFT JOIN Mask m ON m.BluId = b.BluId "
                          "WHERE EXISTS ("
                          "    SELECT 1 FROM MaskBlu b2 WHERE b2.DesId = d.DesId "
                          "AND b2.BluId BETWEEN %s AND %s)",

    "search_blue_id_gt2": f"SELECT {results_str} FROM MaskDesign d "
                          "JOIN Observers o ON o.ObId = d.DesPId "
                          "JOIN MaskBlu b ON b.DesId = d.DesId "
                          "LEFT JOIN Mask m ON m.BluId = b.BluId "
                          "WHERE EXISTS ("
                          "    SELECT 1 FROM MaskBlu b2 WHERE b2.DesId = d.DesId ",

    "search_blue_id_eq1": f"SELECT {results_str} FROM MaskDesign d "
                          "JOIN Observers o ON o.ObId = d.DesPId "
                          "JOIN MaskBlu b ON b.DesId = d.DesId "
                          "LEFT JOIN Mask m ON m.BluId = b.BluId "
                          "WHERE d.DesId IN (SELECT DesId FROM MaskBlu WHERE BluId = %s)",

    "search_design_id_eq2": f"SELECT {results_str} FROM MaskDesign d "
                            "JOIN Observers o ON o.ObId = d.DesPId "
                            "LEFT JOIN MaskBlu b ON b.DesId = d.DesId "
                            "LEFT JOIN Mask m ON m.BluId = b.BluId "
                            "WHERE d.DesId BETWEEN %s AND %s ",

    "search_design_id_gt2": f"SELECT {results_str} FROM MaskDesign d "
                            "JOIN Observers o on o.ObId = d.DesPId "
                            "LEFT join MaskBlu b on b.DesId = d.DesId "
                            "LEFT JOIN Mask m ON m.BluId = b.BluId ",

    "search_design_id_eq1": f"SELECT {results_str} FROM MaskDesign d "
                            "JOIN Observers o ON o.ObId = d.DesPId "
                            "LEFT JOIN MaskBlu b ON b.DesId = d.DesId "
                            "LEFT JOIN Mask m ON m.BluId = b.BluId "
                            "WHERE d.DesId = %s",
    "search_millseq_eq2": f"SELECT {results_str} FROM MaskDesign d "
                          "JOIN Observers o ON o.Obid = d.DesPid "
                          "LEFT JOIN MaskBlu b ON b.DesId = d.DesId "
                          "LEFT JOIN Mask m ON m.BluId = b.BluId "
                          "WHERE ("
                          "   exists (SELECT * FROM MaskBlu WHERE DesId = d.DesId "
                          "           AND MillSeq BETWEEN %s and %s) OR "
                          "   exists (SELECT * FROM MaskBlu WHERE DesId = d.DesId "
                          "           AND BluId IN (SELECT BluId FROM Mask "
                          "              WHERE MillSeq BETWEEN %s AND %s)) )",

    "search_millseq_gt2": f"SELECT {results_str} FROM MaskDesign d "
                          "JOIN Observers o ON o.Obid = d.DesPid "
                            "LEFT JOIN MaskBlu b ON b.DesId = d.DesId "
                            "LEFT JOIN Mask m ON m.BluId = b.BluId "
                            "WHERE (EXISTS (SELECT * FROM MaskBlu WHERE DesId = d.DesId ",

    "search_millseq_eq1": f"SELECT {results_str} FROM MaskDesign d "
                        "JOIN Observers o ON o.Obid = d.DesPid "
                        "LEFT JOIN MaskBlu b ON b.DesId = d.DesId "
                        "LEFT JOIN Mask m ON m.BluId = b.BluId "
                        "WHERE (exists ( SELECT * FROM MaskBlu WHERE DesId = d.DesId "
                        "AND MillSeq = %s ) OR exists ( SELECT * FROM MaskBlu "
                        "WHERE DesId = d.DesId AND BluId IN ( SELECT BluId "
                        "FROM Mask WHERE MillSeq = %s ))) ",

    "search_barcode_eq2": f"SELECT {results_str}, b.status FROM MaskDesign d "
                    "JOIN Observers o ON o.ObId = d.DesPId "
                    "LEFT JOIN MaskBlu b ON b.DesId = d.DesId "
                    "LEFT JOIN Mask m ON m.BluId = b.BluId "
                    "WHERE EXISTS ("
                    "   SELECT 1 FROM MaskBlu "
                    "   WHERE DesId = d.DesId AND BluId IN ("
                    "       SELECT BluId FROM Mask "
                    "       WHERE MaskId BETWEEN %s AND %s)) ",

    "search_barcode_gt2": f"SELECT {results_str}, b.status FROM MaskDesign d "
                        "JOIN Observers o ON o.ObId = d.DesPId "
                        "LEFT JOIN MaskBlu b ON b.DesId = d.DesId "
                        "LEFT JOIN Mask m ON m.BluId = b.BluId "
                        "WHERE EXISTS ("
                        "   SELECT 1 FROM MaskBlu "
                        "   WHERE DesId = d.DesId AND BluId IN ("
                        "       SELECT BluId FROM Mask "
                        "       WHERE MaskId IN (",

    "search_barcode_eq1": f"SELECT {results_str}, b.status FROM MaskDesign d "
                        "JOIN Observers o on o.ObId = d.DesPID "
                        "LEFT JOIN MaskBlu b ON b.DesId = d.DesId "
                        "LEFT JOIN Mask m ON m.BluId = b.BluId "
                        "WHERE EXISTS ("
                        "  SELECT 1 FROM MaskBlu WHERE DesId = d.DesId AND BluId IN ("
                        "    SELECT BluId FROM Mask WHERE MaskId = %s)) "
                        " AND MaskId = 8840",

    "search_milled_no": f"SELECT {results_str}, b.status FROM MaskDesign d "
                        "JOIN Observers o on o.ObId = d.DesPID "
                        "LEFT JOIN MaskBlu b ON b.BluId = d.DesId "
                        "LEFT JOIN Mask m ON m.BluId = b.BluId "
                        "WHERE b.status != %s ",

    "search_milled_yes": f"SELECT {results_str}, b.status FROM MaskDesign d "
                        "JOIN Observers o on o.ObId = d.DesPID "
                        "LEFT JOIN MaskBlu b ON b.BluId = d.DesId "
                        "LEFT JOIN Mask m ON m.BluId = b.BluId WHERE b.status = %s ",

    "search_cal_days": f"SELECT {results_str}, b.status FROM MaskDesign d "
                        "JOIN Observers o ON o.ObId = d.DesPId "
                        "LEFT JOIN MaskBlu b ON b.DesId = d.DesId "
                        "LEFT JOIN Mask m ON m.BluId = b.BluId "
                        "WHERE date_part('day', "
                        "(SELECT max(Date_Use) FROM MaskBlu WHERE DesId = d.DesId) - now()) "
                        "BETWEEN 0 AND %s ",

    "search_other": f"SELECT {results_str} FROM MaskDesign d "
                    "JOIN Observers o ON o.ObId = d.DesPId "
                    "LEFT JOIN MaskBlu b ON b.DesId = d.DesId "        
                    "LEFT JOIN Mask m ON m.BluId = b.BluId ",

    "search_inst": f"SELECT {results_str} FROM MaskDesign d "
                   "JOIN Observers o ON o.ObId = d.DesPId "
                   "LEFT JOIN MaskBlu b ON b.DesId = d.DesId "
                   "LEFT JOIN Mask m ON m.BluId = b.BluId "

}


def get_query(query_key):
    """
    This way the queries cannot be updated,  to avoid using the dict directly.

    :param query_key:
    :type query_key:
    :return:
    :rtype:
    """
    query_str = ownership_queries.get(query_key)
    if not query_str:
        query_str = retrieval_queries.get(query_key)

    if not query_str:
        query_str = ingest_queries.get(query_key)

    if not query_str:
        query_str = admin_queries.get(query_key)

    if not query_str:
        query_str = validate_queries.get(query_key)

    if not query_str:
        query_str = auxiliary_queries.get(query_key)

    if not query_str:
        query_str = admin_search_queries.get(query_key)

    if not query_str:
        return None

    return query_str

