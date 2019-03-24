from configparser import ConfigParser

VALID_INPUT_FORMATS = ['json']
VALID_BOOL_VALUES = ['true', 'false']
VALID_ENCODING_VALUES = ['utf-8']


def validate_config(cfg: ConfigParser, local_mode: bool=False) -> None:

    """
    Function asserting that all configuration parameters have correct values
    before starting the Spark job

    :param cfg: ConfigParser object
    :param local_mode: Boolean parameter set to True when working with local files for
    debugging purpose
    """

    if not local_mode:

        assert cfg["anonymize"]["source_ids_s3_path"].startswith('s3:'), \
            f"Incorrect value for anonymize.source_ids_s3_path configuration parameter, " \
            f"must start with 's3:'"

        assert cfg["anonymize"]["target_objects_s3_path"].startswith('s3:'), \
            f"Incorrect value for anonymize.target_objects_s3_path configuration parameter, " \
            f"must start with 's3:'"

    assert cfg["anonymize"]["input_fmt"] in VALID_INPUT_FORMATS, \
        f"Incorrect value for anonymize.json configuration parameter, " \
        f"must be one of: {VALID_INPUT_FORMATS}"

    assert cfg["anonymize"]["multiline"] in VALID_BOOL_VALUES, \
        f"Incorrect value for anonymize.target_objects_s3_path configuration parameter, " \
        f"must be one of: {VALID_BOOL_VALUES}"

    assert cfg["anonymize"]["encoding"] in VALID_ENCODING_VALUES, \
        f"Incorrect value for anonymize.encoding configuration parameter, " \
        f"must be on of: {VALID_ENCODING_VALUES}"

    assert cfg["settings"]["development"] in VALID_BOOL_VALUES, \
        f"Incorrect value for settings.development configuration parameter, " \
        f"must be one of: {VALID_BOOL_VALUES}"

    assert cfg["settings"]["debug"] in VALID_BOOL_VALUES, \
        f"Incorrect value for settings.debug configuration parameter, " \
        f"must be one of: {VALID_BOOL_VALUES}"

    assert cfg["settings"]["in_place"] in VALID_BOOL_VALUES, \
        f"Incorrect value for settings.in_place configuration parameter, " \
        f"must be one of: {VALID_BOOL_VALUES}"
