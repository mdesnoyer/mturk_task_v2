"""
Configuration file. Exports relevant information about the database configuration.

Note:
    - Column families are written user under_scores
    - Individual columns are written using camelCase
"""


# TABLE NAMES
WORKER_TABLE = 'workers'
TASK_TABLE = 'tasks'
IMAGE_TABLE = 'images'
PAIR_TABLE = 'pairs'
WIN_TABLE = 'wins'

# COLUMN NAMES, BY FAMILY
WORKER_FAMILIES = {'worker_status': dict(max_versions=1),
                   'worker_stats_task': dict(max_versions=1),
                   'demographics': dict(max_versions=1)}
TASK_FAMILIES = {'task_meta': dict(max_versions=1),
                 'status': dict(max_versions=1),
                 'images': dict(max_versions=1),
                 'tuples': dict(max_versions=1),
                 'tuple_types': dict(max_versions=1),
                 'choices': dict(max_versions=1),
                 'choice_indices': dict(max_versions=1),
                 'reaction_times': dict(max_versions=1),
                 'forbidden_workers': dict(max_versions=1)}
IMAGE_FAMILIES = {'image_meta': dict(max_versions=1),
                  'phash': dict(max_versions=1),
                  'colorname': dict(max_versions=1)}
PAIR_FAMILIES = {'data': dict(max_versions=1)}
WIN_FAMILIES = {'data': dict(max_versions=1)}