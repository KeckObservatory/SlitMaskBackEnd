import tilsotua


def add_slit_coordinates(design_id, filename, log):

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


