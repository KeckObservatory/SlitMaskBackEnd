from datetime import datetime

from mask_constants import MaskBluStatusMILLED, MaskBluStatusFORGOTTEN, PERPETUAL_DATE

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
    "obid_column": "SELECT obid, keckid FROM observers"
}


retrieval_queries = {
    "mill": f"""
        SELECT b.BluId, b.status, b.Date_Use, b.stamp, b.GUIname,
               b.millseq, d.desid, d.desnslit, d.desname, d.instrume
        FROM MaskBlu b, MaskDesign d
        WHERE (b.status < {MaskBluStatusMILLED} OR b.status IS NULL)
              AND d.DesId = b.DesId
        ORDER BY b.Date_Use
        """,

    "standard_mask": f"""
        SELECT m.MaskId, b.GUIname, b.BluName, b.BluId, b.Date_Use,
               m.milldate, d.instrume, d.desid
        FROM MaskBlu b, Mask m, MaskDesign d
        WHERE b.status < {MaskBluStatusFORGOTTEN} 
              AND b.Date_Use >= TIMESTAMP '{PERPETUAL_DATE}'
              AND m.bluid = b.bluid AND d.DesId = b.DesId
        """,

    # "user_inventory": """
    #     SELECT d.*
    #     FROM MaskDesign d, Observers o
    #     WHERE o.ObId = d.DesPId
    #     AND (d.DesPId = %s OR d.DesId IN
    #         (SELECT DesId FROM MaskBlu WHERE BluPId = %s))
    #     ORDER BY d.stamp DESC;
    #     """,

    "user_inventory": """
        SELECT d.*
        FROM MaskDesign d
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

    "chk_design": """
        select DesPId from MaskDesign where DesID=%s   
        """,

    "chk_mask": """
        select maskid from Mask where maskid=%s   
        """
}

admin_queries = {

    "recent": """
        SELECT m.MillDate, m.MillId, m.GUIname, m.millseq, m.maskid,
        d.DesName, d.DesId,
        b.BluId,
        d.DesNslit, d.INSTRUME,
        b.Date_Use
        FROM Mask m, MaskBlu b, MaskDesign d
        WHERE m.MillDate >= %s
        AND b.BluId = m.BluId
        AND d.DesId = b.DesId
        ORDER BY m.MillDate
        """,

    "recent_barcode": """
        SELECT m.MillDate, m.MillId, m.GUIname, m.millseq, m.maskid,
        d.DesName, d.DesId,
        b.BluId,
        d.DesNslit, d.INSTRUME,
        b.Date_Use
        FROM Mask m, MaskBlu b, MaskDesign d
        WHERE m.MillDate >= %s
        AND b.BluId = m.BluId
        AND d.DesId = b.DesId
        ORDER BY m.MaskId
        """,

    "timeline": """
        SELECT b.stamp, b.Date_Use, b.bluid, b.GUIname, b.millseq, d.DesId, 
        d.DesName, d.DesNslit, d.INSTRUME, m.MillId, m.MillDate
        FROM MaskDesign D, Mask m RIGHT JOIN MaskBlu b
        ON m.BluId = b.BluId
        WHERE b.stamp >= %s AND d.DesId = b.DesId 
        ORDER BY b.Date_Use, d.INSTRUME
        """,

    # TODO replaced
    # "mask_valid": """
    #     select m.MaskId, m.GUIname, m.MillSeq, b.Date_Use, o.FirstNm,
    #            o.LastNm, b.status, d.INSTRUME, o.keckid
    #     from Mask m, MaskBlu b, Observers o, MaskDesign d
    #     where b.BluId = m.BluId
    #     and d.DesId = b.DesId
    #     and o.ObId = d.DesPId
    #     order by d.INSTRUME, m.MaskId
    #     """,


    "mask_valid": """
    SELECT 
        m.MaskId, m.GUIname, m.MillSeq, b.Date_Use, 
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



    "mask_delete": "DELETE FROM Mask WHERE MaskId = %s",

    "update_perpetual": f"""
        UPDATE MaskBlu SET Date_Use = TIMESTAMP '{PERPETUAL_DATE}' 
        WHERE DesId = %s""",

    "forgotten_status": f"""
        UPDATE MaskBlu SET status = {MaskBluStatusMILLED} 
        WHERE bluid IN (
            SELECT BluId FROM MaskBlu 
            WHERE DesId = %s AND status = {MaskBluStatusFORGOTTEN}
            )        
        """,

    # TODO deemed unnecessary - get_mask_system_users
    # "mask_users": """
    #     SELECT ObId, FirstNm, LastNm FROM Observers
    #     WHERE ObId IN (SELECT DesPId FROM MaskDesign)
    #     OR ObId IN (SELECT BluPId FROM MaskBlu)
    #     """,

    # TODO deemed unnecessary - get_mask_system_users
    # "observer_mask": """
    #     SELECT o.FirstNm, o.LastNm, d.DesPId, d.DesId, b.BluId, b.status,
    #            b.BluPId, m.MaskId
    #     FROM Observers o, MaskDesign d, MaskBlu b, Mask m
    #     WHERE (
    #       o.ObId in (SELECT DesPId FROM MaskDesign)
    #       OR
    #       o.ObId in (SELECT BluPId FROM MaskBlu)
    #     )
    #     AND m.BluId = b.BluId
    #     AND b.DesId = d.DesId
    #     AND
    #     (
    #       (
    #           b.BluPId = d.DesPId
    #       AND b.BluPId = o.ObId
    #       )
    #       OR
    #       (
    #           b.BluPId != d.DesPId
    #       AND b.BluPId = o.ObId
    #       )
    #       OR
    #       (
    #           b.BluPId != d.DesPId
    #       AND d.DesPId = o.ObId
    #       )
    #     )
    #     ORDER BY d.DesPId
    #     """,

    # TODO deemed unnecessary - get_mask_system_users
    # "observer_no_mask": """
    #     select o.FirstNm, o.LastNm, d.DesPId, d.DesId, b.BluId, b.status, b.BluPId, -1 as MaskId, b.Date_Use
    #     from Observers o, MaskDesign d, MaskBlu b
    #     where
    #     b.BluId not in (select BluId from Mask)
    #     and
    #     b.DesId = d.DesId
    #     and
    #     (
    #       ( /* get mask designs of this observer with blueprints of this observer and no mask */
    #           b.BluPId = d.DesPId
    #       and b.BluPId = o.ObId
    #       and o.ObId in (select DesPId from MaskDesign)
    #       )
    #       or
    #       ( /* get mask designs of this observer with blueprints not of this observer and no mask */
    #           b.BluPId != d.DesPId
    #       and d.DesPId = o.ObId
    #       and o.ObId in (select DesPId from MaskDesign)
    #       )
    #       or
    #       ( /* get mask designs not of this observer with blueprints of this observer and no mask */
    #           b.BluPId != d.DesPId
    #       and b.BluPId = o.ObId
    #       and o.ObId in (select BluPId from MaskBlu)
    #       )
    #     )
    #     order by o.ObId
    #     """

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
            %s, NULL, NULL, NULL, DEFAULT, %s, %s) 
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
        INSERT into slitobjmap (
            DesId,
            ObjectId,
            dSlitId,
            TopDist,
            BotDist
        ) values (
            %s, %s, %s, %s, %s
        )
        """

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
        return None

    return query_str

