# Config
# Number of lines/comments/posts in the subset (_ for better readability)
LINES_SUBSET = 80_000_000

posts_subset_base_name = 'data/subset/subset_posts'
comments_subset_file = 'data/subset/subset_comments'
rules_subset_file = 'data/subset/subset_rules'
wikis_subset_file = 'data/subset/subset_wikis'
subreddits_subset_file = 'data/subset/subset_subreddits'

# Content files
comments_file = 'data/comments/RC_2025-01/RC_2025-01'
posts_2025_1_file = 'data/submissions/RS_2025-01/RS_2025-01'
posts_2025_2_file = 'data/submissions/RS_2025-02/RS_2025-02'
posts_2024_12_file = 'data/submissions/RS_2024-12/RS_2024-12'
posts_2024_11_file = 'data/submissions/RS_2024-11/RS_2024-11'
posts_2024_10_file = 'data/submissions/RS_2024-10/RS_2024-10'
posts_2024_09_file = 'data/submissions/RS_2024-10/RS_2024-10'

# Subreddit files
subreddit_rules_file = 'data/subreddits/subreddit_rules_2025-01/subreddit_rules_2025-01'
subreddit_wiki_file = 'data/subreddits/subreddit_wikis_2025-01/subreddit_wikis_2025-01'
subreddits_file = 'data/subreddits/subreddits_2025-01/subreddits_2025-01'
subreddits_metadata_file = 'data/subreddits/subreddits_meta_only_2025-01/subreddits_meta_only_2025-01'

dates_subsets = ['2025-1', '2024-12', '2025-2', '2024-11',
                 '2024-10', '2024-09']  # In order of priority
dates_to_original_file = {'2025-1': posts_2025_1_file,
                          '2024-12': posts_2024_12_file,
                          '2025-2': posts_2025_2_file,
                          '2024-11': posts_2024_11_file,
                          '2024-10': posts_2024_10_file,
                          '2024-09': posts_2024_09_file}

# SQL database
subset_files_tables = {posts_subset_base_name: ['post', 'author', 'banned'],
                       comments_subset_file: ['comment'],
                       rules_subset_file: ['subreddit_rules'],
                       wikis_subset_file: ['wiki', 'revision_wiki'],
                       subreddits_subset_file: ['subreddit', 'subreddit_settings',
                                                'subreddit_media', 'subreddit_permissions', 'subreddit_comment_media'],
                       subreddits_metadata_file: ['subreddit_metadata']}


data_files = [posts_2025_1_file, comments_file,
              subreddits_file, subreddit_rules_file, subreddit_wiki_file]

data_files_tables = {posts_2025_1_file: ['post', 'author', 'banned'],
                     comments_file: ['comment'],
                     subreddit_rules_file: ['subreddit_rules'],
                     subreddit_wiki_file: ['wiki', 'revision_wiki'],
                     subreddits_file: ['subreddit','subreddit_settings',
                                           'subreddit_media', 'subreddit_permissions', 'subreddit_comment_media'],
                     subreddits_metadata_file: ['subreddit_metadata']}

subset_to_original = {comments_subset_file: comments_file,
                      rules_subset_file: subreddit_rules_file,
                      wikis_subset_file: subreddit_wiki_file,
                      subreddits_subset_file: subreddits_file}

data_files_tables_mongo = {posts_2025_1_file: 'post',
                     comments_file: 'comment',
                     subreddit_rules_file: 'subreddit_rules',
                     subreddit_wiki_file: 'wiki',
                     subreddits_file: 'subreddit',
                     subreddits_metadata_file: 'subreddit_metadata'}

# These are standard oracle tables, do not remove them
standard_oracle_tables = {'LOGMNRGGC_GTLO', 'LOGMNRGGC_GTCS', 'LOGMNR_PARAMETER$', 'LOGMNR_SESSION$',
                          'ROLLING$CONNECTIONS', 'ROLLING$DATABASES', 'ROLLING$DIRECTIVES', 'ROLLING$EVENTS',
                          'ROLLING$PARAMETERS', 'ROLLING$PLAN', 'ROLLING$STATISTICS', 'ROLLING$STATUS',
                          'MVIEW$_ADV_WORKLOAD', 'MVIEW$_ADV_BASETABLE', 'MVIEW$_ADV_SQLDEPEND', 'MVIEW$_ADV_PRETTY',
                          'MVIEW$_ADV_TEMP', 'MVIEW$_ADV_FILTER', 'MVIEW$_ADV_LOG', 'MVIEW$_ADV_FILTERINSTANCE',
                          'MVIEW$_ADV_LEVEL', 'MVIEW$_ADV_ROLLUP', 'MVIEW$_ADV_AJG', 'MVIEW$_ADV_FJG', 'MVIEW$_ADV_GC',
                          'MVIEW$_ADV_CLIQUE', 'MVIEW$_ADV_ELIGIBLE', 'MVIEW$_ADV_OUTPUT', 'MVIEW$_ADV_EXCEPTIONS',
                          'MVIEW$_ADV_PARAMETERS', 'MVIEW$_ADV_INFO', 'MVIEW$_ADV_JOURNAL', 'MVIEW$_ADV_PLAN',
                          'AQ$_INTERNET_AGENTS', 'AQ$_INTERNET_AGENT_PRIVS', 'AQ$_QUEUE_TABLES', 'AQ$_KEY_SHARD_MAP',
                          'AQ$_QUEUES', 'AQ$_SCHEDULES', 'SCHEDULER_PROGRAM_ARGS_TBL', 'SCHEDULER_JOB_ARGS_TBL',
                          'LOGSTDBY$PARAMETERS', 'REPL_VALID_COMPAT', 'REPL_SUPPORT_MATRIX', 'SQLPLUS_PRODUCT_PROFILE',
                          'HELP', 'LOGMNR_GT_TAB_INCLUDE$', 'LOGMNR_GT_USER_INCLUDE$', 'LOGMNR_GT_XID_INCLUDE$',
                          'LOGMNRT_MDDL$', 'OL$', 'OL$HINTS', 'OL$NODES', 'LOGMNR_DICTSTATE$', 'LOGMNRC_GTLO',
                          'LOGMNRC_GTCS', 'LOGMNRC_SEQ_GG', 'LOGMNRC_CON_GG', 'LOGMNRC_CONCOL_GG', 'LOGMNRC_IND_GG',
                          'LOGMNRC_INDCOL_GG', 'LOGMNRC_SHARD_TS', 'LOGMNRC_TSPART', 'LOGMNRC_TS', 'LOGMNRC_GSII',
                          'LOGMNRC_GSBA', 'LOGMNRC_USER', 'LOGMNR_SEED$', 'LOGMNR_DICTIONARY$', 'LOGMNR_OBJ$',
                          'LOGMNR_TAB$', 'LOGMNR_COL$', 'LOGMNR_ATTRCOL$', 'LOGMNR_TS$', 'LOGMNR_IND$', 'LOGMNR_USER$',
                          'LOGMNR_TABPART$', 'LOGMNR_TABSUBPART$', 'LOGMNR_TABCOMPART$', 'LOGMNR_TYPE$',
                          'LOGMNR_COLTYPE$', 'LOGMNR_ATTRIBUTE$', 'LOGMNR_LOB$', 'LOGMNR_CON$', 'LOGMNR_CONTAINER$',
                          'LOGMNR_CDEF$', 'LOGMNR_CCOL$', 'LOGMNR_ICOL$', 'LOGMNR_LOBFRAG$', 'LOGMNR_INDPART$',
                          'LOGMNR_INDSUBPART$', 'LOGMNR_INDCOMPART$', 'LOGMNR_LOGMNR_BUILDLOG', 'LOGMNR_NTAB$',
                          'LOGMNR_OPQTYPE$', 'LOGMNR_SUBCOLTYPE$', 'LOGMNR_KOPM$', 'LOGMNR_PROPS$', 'LOGMNR_ENC$',
                          'LOGMNR_REFCON$', 'LOGMNR_IDNSEQ$', 'LOGMNR_PARTOBJ$', 'LOGMNRP_CTAS_PART_MAP',
                          'LOGMNR_SHARD_TS', 'LOGSTDBY$APPLY_PROGRESS', 'LOGMNR_SESSION_EVOLVE$', 'LOGMNR_GLOBAL$',
                          'LOGMNR_PDB_INFO$', 'LOGMNR_DID$', 'LOGMNR_UID$', 'LOGMNRC_DBNAME_UID_MAP', 'LOGMNR_LOG$',
                          'LOGMNR_PROCESSED_LOG$', 'LOGMNR_SPILL$', 'LOGMNR_AGE_SPILL$', 'LOGMNR_RESTART_CKPT_TXINFO$',
                          'LOGMNR_ERROR$', 'LOGMNR_RESTART_CKPT$', 'LOGMNR_FILTER$', 'LOGMNR_SESSION_ACTIONS$',
                          'LOGMNR_PROFILE_TABLE_STATS$', 'LOGMNR_PROFILE_PLSQL_STATS$', 'REDO_DB', 'REDO_LOG',
                          'LOGSTDBY$EVENTS', 'LOGSTDBY$APPLY_MILESTONE', 'LOGSTDBY$SCN', 'LOGSTDBY$FLASHBACK_SCN',
                          'LOGSTDBY$PLSQL', 'LOGSTDBY$SKIP_TRANSACTION', 'LOGSTDBY$SKIP', 'LOGSTDBY$SKIP_SUPPORT',
                          'LOGSTDBY$HISTORY', 'LOGSTDBY$EDS_TABLES'}
