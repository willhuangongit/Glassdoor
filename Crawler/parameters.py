# Search Mode
# 1: search by company name
# 2: search by job keywords
search_mode = 2

# Per limit
# The number of companies to search for
# from each keyword search.
per_limit = 10

# Crawler switches
# determine which crawlers to use
crawler_switches = [
    ('overview', True),
    ('reviews', True),
    ('jobs', True),
    ('salaries', True),
    ('interviews', True),
    ('benefits', True),
    ('photos', True)
]

mysql_auth = {
    'user': 'root',
    'host': 'localhost',
    'password': ''
}

session_params = {
    'headers': {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) \
        AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'},
    'username': "",
    'password': "",
    'homepage_url': "https://www.glassdoor.com",
    'login_url': "https://www.glassdoor.ca/profile/login_input.htm",
    'login_post_url': "https://www.glassdoor.ca/profile/ajax/loginAjax.htm",
    'conn_tries': 3,
    'login_tries': 3,
    'avg_wait': 2,
    'soup_decoder': "lxml"
}

compseeker_params = {
    'criteria_list_dir': "./company_search_list.csv",
    'skip_exc_comps': True,
    'skip_ext_comps': False,
    'skip_ext_dps': True,
    'skip_head_data': True,
    'skip_by_update': True,
    'avg_wait': 2.5
}

jobseeker_params = {
    'criteria_list_dir': "./job_search_list.csv",
    'skip_exc_comps': True,
    'skip_ext_comps': False,
    'skip_ext_dps': True,
    'skip_head_data': True,
    'skip_by_update': True,
    'avg_wait': 2.5
}

overview_params = {
    'skip_exc_comps': True,
    'skip_ext_comps': False,
    'skip_ext_dps': True,
    'avg_wait': 2.5
}

reviews_params = {
    'skip_exc_comps': True,
    'skip_ext_comps': False,
    'skip_ext_dps': False,
    'skip_head_data': False,
    'skip_by_update': False,
    'avg_wait': 2.5
}

jobs_params = {
    'skip_exc_comps': True,
    'skip_ext_comps': False,
    'skip_ext_dps': True,
    'skip_head_data': True,
    'skip_by_update': True,
    'avg_wait': 2.5
}

salaries_params = {
    'skip_exc_comps': True,
    'skip_ext_comps': False,
    'skip_ext_dps': False,
    'skip_head_data': True,
    'skip_by_update': True,
    'avg_wait': 2.5
}

interviews_params = {
    'skip_exc_comps': True,
    'skip_ext_comps': False,
    'skip_ext_dps': True,
    'skip_head_data': True,
    'skip_by_update': True,
    'avg_wait': 2.5
}

benefits_params = {
    'skip_exc_comps': True,
    'skip_ext_comps': False,
    'skip_ext_dps': True,
    'skip_head_data': True,
    'skip_by_update': True,
    'avg_wait': 2.5
}

photos_params = {
    'database_dir': "../outputs/07_photos/",
    'target_dir': "../outputs/07_photos/",
    'exc_list_dir': "./company_exclusions.csv",
    'skip_exc_comps': True,
    'skip_ext_comps': False,
    'skip_ext_dps': True,
    'skip_head_data': True,
    'skip_by_update': True,
    'avg_wait': 2.5
}