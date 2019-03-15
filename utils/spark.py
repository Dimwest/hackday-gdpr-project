

def df_join(events_df, user_ids_df, join_type, cfg):

    """
    Helper function handling two Dataframe join cases:
        1) Both columns have the same name
        2) Columns have different names

    Adds a new column in_source_ids to events_df in order to operate only on
    selected user_ids

    :param events_df: Dataframe to anonymize
    :param user_ids_df: Dataframe containing the list of user_ids to anonymize
    :param join_type: Join type supported by Spark Dataframes
    :param cfg: ConfigParser object

    :return: Updated events_df
    """

    if cfg['anonymize']['source_id_key'] == cfg['anonymize']['target_id_key']:
        events_df = events_df.join(user_ids_df, cfg['anonymize']['source_id_key'], join_type)
    else:
        join_cond = [user_ids_df[cfg['anonymize']['source_id_key']]
                     == events_df[cfg['anonymize']['target_id_key']]]
        events_df = events_df.join(user_ids_df, join_cond, join_type)

    return events_df
