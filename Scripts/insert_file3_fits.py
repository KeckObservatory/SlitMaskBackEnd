import os
import shutil

import tilsotua
import logging

import psycopg2
import psycopg2.extras
from psycopg2.extras import DictCursor

import argparse


def setup_logging():
    """
    Start the logger.
    """
    log = logging.getLogger()

    fh = logging.FileHandler("file3_insert.log")
    fh.setLevel(logging.INFO)

    # Console handler (optional)
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)

    # Formatter (clean and minimal)
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)

    log.addHandler(fh)
    log.addHandler(ch)

    return log

def add_slit_coordinates(design_id, filename):
    """
    Interface to the TILSOTUA (autoslit backwards) insert functions.
    """

    out_file = filename.replace('.fits', '.out')
    success = tilsotua.insert_lris_db(filename, out_file, design_id)
    if success:
        log.info(
            f'Successfully added slit coordinates for design id: {design_id}, '
            f'filename: {filename}, output filename: {out_file}'
        )
    else:
        log.warning(
            f'Successfully added slit coordinates for design id: {design_id}, '
            f'filename: {filename}, output filename: {out_file}'
        )


def mk_query(query, q_params):
    """
    Connect to the database and execute a query.
    """
    conn = None
    try:
        conn = psycopg2.connect(
            dbname="metabase",
            user="dbadmin",
            password="slit_mask4u!",
            host="localhost",
            port=5432
        )
        curse = conn.cursor(cursor_factory=DictCursor)

        # SAFE parameterized query
        curse.execute(query, q_params)

        rows = curse.fetchall()

        curse.close()

    except Exception as e:
        log.error("Database error:", e)
        return []
    finally:
        if conn:
            conn.close()

    return rows

def get_desid(filepath):
    """
    Determine the design id from the FILE3 name.
    """
    filename = os.path.basename(filepath)
    query = f"select desid, desname, despid, desdate from maskdesign where desname =%s"
    rows = mk_query(query, (filename,))

    print(rows)
    if len(rows) != 1 or len(rows[0]) == 0:
        log.error(f'{len(rows)} results found for {filename}, results {rows}')
        return None

    return rows[0][0]


def read_lris_file3_fits(filepath, design_id=None):
    """
    Read the LRIS FILE# and insert into the objects (Slit Object Information)
    and the slitobjmap (Slit Map) tables in the slitmask DB.
    """
    log.info(f'Reading LRIS file: {filepath}')
    # file3_name = filepath.replace('file3.fits', 'file3')
    file3_name = filepath.replace('.fits', '.file3')

    if not design_id:
        design_id = get_desid(file3_name)

    if not design_id:
        log.error(f'No design id,  or more than one: {file3_name}')
        return None

    print(design_id)

    log.info(f'Design id: {design_id}')

    add_slit_coordinates(design_id, filepath)
    # threading.Thread(target=add_slit_coordinates, args=(design_id, lris_out_file, self.log), daemon=True).start()

    return file3_name

def parse_args():
    parser = argparse.ArgumentParser(description="My script with logging.")
    parser.add_argument(
        "--filepath",
        default=None,
        help="FITS filepath to insert by LRIS FILE3."
    )
    parser.add_argument(
        "--desid",
        default=None,
        help="Design ID of LRIS FILE3 to insert.  Requires filepath"
    )
    parser.add_argument(
        "--dir",
        default=None,
        help="Directory of files to loop inserting LRIS FILE3."
    )
    return parser.parse_args()


def run_insert(filepath, desid, direct):
    """
    Find the FILE3 files and insert them into the database.
    """
    if desid:
        if not filepath:
            log.error(f'No filepath provided for {args.desid}')
            return
        file3_name = read_lris_file3_fits(args.filepath, args.desid)
        log.info(f'Inserted file3: {file3_name}')
        return
    elif args.filepath:
        file3_name = read_lris_file3_fits(args.filepath)
        log.info(f'Inserted file3: {file3_name}')
        return

    elif direct:
        updated_files = []
        for fname in os.listdir(direct):
            log.info(f'Reading file: {fname}')
            updated_file = None
            if fname.endswith(".file3.fits"):
                orig_path = os.path.join(direct, fname)
                new_fname = fname.replace(".file3.fits", ".fits")
                new_path = os.path.join(direct, new_fname)

                # Copy original → new
                shutil.copy2(orig_path, new_path)
                log.info(f"Copied: {orig_path} → {new_path}")

                # Process new file
                updated_file = read_lris_file3_fits(new_path)

            if updated_file:
                updated_files.append(updated_file)

        log.info(f'Updated files: {updated_files}')


if __name__ == '__main__':
    log = setup_logging()
    args = parse_args()
    log.info(args)


    filepath = args.filepath
    direct = args.dir
    desid = args.desid

    run_insert(filepath, desid, direct)




