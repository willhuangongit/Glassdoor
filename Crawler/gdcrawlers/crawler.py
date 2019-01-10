# This module forms the template for all other
# crawler classes to inherit.

# ==========
# Imports

from ast import literal_eval
import datetime

from mysql.connector import errors as sqlerrors
import mysql.connector as sqlc
import progressbar as pb

from assists.localio import *
from assists.utilfunc import *
from assists.webnavi import *



# ==========
# Classes

# Exceptions

class FinalSearchPage(requests.exceptions.HTTPError):
    def __init__(self):
        Exception.__init__(self, "Final search page has been reached.")

class NoDataForAspect(Exception):
    def __init__(self):
        Exception.__init__(self, "No data for the aspect of interest.")

class NoNewUpdate(Exception):
    def __init__(self):
        Exception.__init__(self, "No new update for the aspect of " + 
            "interest.")

class NoDataPoint(Exception):
    def __init__(self):
        Exception.__init__(self, "No data point available.")

class NoLocationMatch(Exception):
    def __init__(self):
        Exception.__init__(self, "No location matches the location " + 
            "required by search criterion.")

class AllRowValuesIdentical(Exception):
    def __init__(self):
        Exception.__init__(self, "A row with all identical values " + 
            "already exists.")

class SkipExcludedCompany(Exception):
    def __init__(self):
        Exception.__init__(self, "Skip excluded company.")

class SkipExistingCompany(Exception):
    def __init__(self):
        Exception.__init__(self, "Skip existing company.")


# Object classes

class Crawler:

    # Default attributes

    def __init__(self, param_dict = None, 
        mysql_auth = None, session_params = None):

        # Default init values that may be overridden
        self._criteria_list_dir = '.'
        self._target_dir = '.'
        self._exc_list_dir = '.'
        self._skip_exc_comps = False
        self._skip_ext_comps = False
        self._skip_ext_dps = False
        self._skip_head_data = False
        self._skip_by_update = False
        self._avg_wait = 0

        # Override default init values if applicable
        if param_dict != None:
            for key, value in param_dict.items():
                setattr(self, "_" + key, value)
        
        # Non-overridable init values
        self._aspect = None
        self._current_company = None
        self._max_data_points_per_page = None
        self._dp_counts = {}
        self._crawled_companies_count = 0
        self._crawled_data_points_count = 0

        # Local database
        self._conn = sqlc.connect(**mysql_auth)
        self._sqlcursor = self._conn.cursor()
        self._db_name = 'glassdoor'

        self._table_defs = []

        self._extra_table_defs = [
            {
                'table_name':'_comp_meta',
                'prefix': None,
                'insert_order': 1,
                'columns': [
                    ('id', 'char(50) PRIMARY KEY'),
                    ('comp_id', 'char(30)'),
                    ('aspect', 'char(30)'),
                    ('dp_count', 'int(11)'),
                    ('last_update', 'date')
                ]
            },
            {
                'table_name': '_excluded_companies',
                'prefix': None,
                'insert_order': 2,
                'columns': [
                    ('id', 'char(30) PRIMARY KEY'),
                    ('notes', 'text')
                ]
            }
        ]

        # Web session
        self._session = Session(param_dict = session_params)
        self._session.login()

    # Properties

    @property
    def dp_counts(self):
        return self._dp_counts

    # ==========
    # Methods

    # Methods for string transformation

    def cleanup(self, string):

        string = re.sub("(\n|\t|\r)","", string)
        string = string.replace("\\","\\\\")
        string = string.replace("    ","")
        string = string.strip()

        return string

    def esc_for_regex(self, string):

        meta_chars = [
           '^', '$', '(', ')', '<', '>', 
           '{', '}', '|', '>', '.', 
           '*', '+', '?']

        string = re.sub("\\\\", "\\\\\\\\", string)

        for char in meta_chars:
            string = re.sub("\\" + char, "\\\\" + char, string)

        return string

    def esc_each_quote(self, unit_str):
        # For SQL
        if unit_str == "'":
            unit_str = "\\'"
        elif unit_str == '"':
            unit_str = '\\\"'
        return unit_str

    def esc_quote(self, string):
        if isinstance(string, str):
            string = list(string)
            string = [self.esc_each_quote(each) 
                for each in string]
            string = "".join(string)
        return string

    def esc_utf8(self, string):
        if isinstance(string, str):
            string = string.encode('utf8')
            string = str(string)[1:]
        return string

    def to_null(self, obj):
        if isinstance(obj, str):
            obj = obj.strip()
        if obj in [None, 'n/a', 'Unknown']:
            obj = 'null'
        return obj

    def prepare_str(self, obj):
        obj = self.to_null(obj)
        if isinstance(obj, str):
            obj = self.esc_quote(obj)
            obj = self.esc_utf8(obj)
        else:
            obj = str(obj)
        return obj

    def str_wrap(self, obj):
        if isinstance(obj, list):
            obj = [self.prepare_str(each) for each in obj]
        else:
            obj = self.prepare_str(obj)
        return obj

    # Methods for database operations

    def gen_equal_conditions(self, columns, values, sep = ", "):
        output = [f'{each[0]} = {self.str_wrap(each[1])}' 
            for each in list(zip(columns, values))]
        output = sep.join(output)
        return output

    def create_database(self):

        self._sqlcursor.execute(
            f"create database if not exists {self._db_name} " +
            "CHARACTER SET utf8mb4 " +
            "COLLATE utf8mb4_unicode_ci;")
        self._conn.commit()

    def create_tables(self, table_defs):

        for table_def in table_defs:

            column_defs = [f'{column[0]} {column[1]}' for column
                in table_def['columns']]

            command = "create table if not exists " + \
                f"{table_def['table_name']} " + \
                f"({', '.join(column_defs)});"

            self._sqlcursor.execute(command)
            self._conn.commit()

    def check_complete_dup(self, row, table_name):

        columns = list(row.keys())
        values = self.str_wrap(list(row.values()))
        eql_conds = self.gen_equal_conditions(
            columns, values, sep = " and ")

        self._sqlcursor.execute(
            f'select * from {table_name} ' + 
            f'where {eql_conds};')
        
        complete_dup = (self._sqlcursor.fetchone() != None)
        
        if complete_dup:
            raise AllRowValuesIdentical

    def insert_row(self, row, table_name, 
        omit_none_col = True, 
        skip_on_dup_key = True,
        skip_on_complete_dup = True):

        # 'row' should be a dictionary.

        try:   

            if skip_on_complete_dup:
                self.check_complete_dup(row, table_name)
                
            if omit_none_col:
                row = {key: value for (key, value) 
                    in row.items() 
                    if value not in [None, 'Unknown', 'n/a']}

            columns = list(row.keys())
            values = list(row.values())
            
            insert_command = f'INSERT INTO {table_name} ' + \
                f'({", ".join(columns)}) ' + \
                f'VALUES ({", ".join(self.str_wrap(values))}) '

            if not skip_on_dup_key:
                insert_command += f'ON DUPLICATE KEY UPDATE ' + \
                    self.gen_equal_conditions(columns, values)

            command = insert_command + ";"
            self._sqlcursor.execute(command)
            self._conn.commit()

        except AllRowValuesIdentical:
            pass

        except sqlerrors.IntegrityError as e:
            if e.errno == 1062:
                # Duplicate primary key
                pass

    def insert_rows(self, rows, table_name, 
        omit_none_col = True, 
        skip_on_dup_key = True,
        skip_on_complete_dup = True):

        for row in rows:

            self.insert_row(row, table_name, 
                omit_none_col = omit_none_col, 
                skip_on_dup_key = skip_on_dup_key,
                skip_on_complete_dup = skip_on_complete_dup)

    def refresh_dp_count(self):

        self._sqlcursor.execute(
            'select comp_id, count(comp_id) ' + \
            f'from {self._aspect[1]} ' + \
            f'group by comp_id;')
        results = self._sqlcursor.fetchall()

        for result in results:

            row = {
                'id': f'{result[0]}_{self._aspect[1]}',
                'comp_id': result[0],
                'aspect': self._aspect[1],
                'dp_count': result[1]
            }

            self.insert_row(row, '_comp_meta',
                skip_on_dup_key = False)

    def insert_company_meta(self, comp_id, new_meta):

        row = {
            'id': f'{comp_id}_{self._aspect[1]}',
            'comp_id': comp_id,
            'aspect': self._aspect[1],
            'last_update': new_meta['last_update']
        }

        self.insert_row(row, '_comp_meta',
            skip_on_dup_key = False)

    def close(self):
        self._conn.close()

    # Methods for skipping data points.

    def check_if_excluded(self, company):
                        
        self._sqlcursor.execute(
            "select id from _excluded_companies;")
        exc_list = self._sqlcursor.fetchall()
        exc_list = [each[0] for each in exc_list]

        if company[0] in exc_list:
            raise SkipExcludedCompany

    def check_if_existing(self, company):

        self._sqlcursor.execute(
            f"select distinct comp_id from {self._aspect[1]};")
        ext_list = self._sqlcursor.fetchall()
        ext_list = [each[0] for each in ext_list]

        if company[0] in ext_list:
            raise SkipExistingCompany

    def skip_data_before_crawl(self, urls_list):

        self._sqlcursor.execute(
            f"select distinct id from {self._aspect[1]};")
        ext_list = self._sqlcursor.fetchall()
        ext_list = [each[0] for each in ext_list]
        urls_list = [each for each in urls_list 
            if not each[0] in ext_list] 

        return urls_list

    # Methods for scenario 1
    # ie. when search result returns a unique company

    def get_gd_id_from_menu(self, page):

        gd_id = page.find('a', class_ = re.compile(
            "eiCell cell.+?reviews"))['href']
        gd_id = re.search("(?<=-E)\d+?(?=\.htm)", 
            gd_id)[0]

        return gd_id

    def get_comp_name_from_menu(self, page):

        comp_name = page.find('div', 
            class_ = 'header cell info').text
        comp_name = comp_name.strip()
        return comp_name

    def get_overview_url_from_menu(self, page):

        url = page.find('a', class_ = re.compile(
            "eiCell cell.+?overviews"))['href']
        url = 'https://www.glassdoor.ca' + url
        return url

    # Methods for scenario 2
    # ie. when search result returns multiple companies

    def get_gd_ids_from_search(self, search_page):
        gd_ids = search_page.find_all('a', 
            class_ = "eiCell cell reviews")
        gd_ids = [re.search("(?<=-E)\d+?(?=.htm)", each['href'])[0]
            for each in gd_ids]
        return gd_ids

    def get_comp_names_from_search(self, search_page):

        comp_names = search_page.find_all('a', 
            class_ = "tightAll h2")

        if comp_names != None:
            comp_names = [(each.text).strip() for each in comp_names]

        else: 
            comp_names = []

        return comp_names

    def get_comp_urls_from_search(self, search_page):

        urls = search_page.find_all("a", class_="tightAll h2")
        urls = ["https://www.glassdoor.ca" + each.attrs['href']
                for each in urls]

        return urls

    # Methods for Glassdoor navigation.

    def check_data_exist_for_asp(self, aspect_page):

        # Check if there is any data for the
        # aspect of interest. Raise NoDataForAspect if
        # none is found. Override as necessary.

        pass

    def get_loc_best_matches(self, loc, max_returns = 1,
        loc_length_cap = 30):

        # Take some location search term (country, city, etc.),
        # and Get a list of closest matches. 
        # max_returns can go up to around 10,000.

        loc_split = self.split_city_prov_state(loc)  
        loc = loc_split['city_short'] + " " + \
            loc_split['province_state']

        if loc_length_cap != None:
            loc = loc[:loc_length_cap]

        request_url = "https://www.glassdoor.ca/" + \
            f'findPopularLocationAjax.htm?term="{loc}"' + \
            f"&maxLocationsToReturn={max_returns}"

        best_matches = self._session.get(request_url, as_soup = True)
        best_matches = literal_eval(best_matches.text)
        
        if best_matches == None:
            raise NoLocationMatch

        else:
            return best_matches

    def get_scenario(self, results_page):

        # Classify the outcome of a company search.
        # Possible returns:
        # 0: No company found.
        # 1: Company exists and is unique.
        # 2: Multiple companies found.

        results_page_text = results_page.text

        # Check for multiple results.
        phrase_count = re.findall("Showing .+? of .+? Companies",
                                  results_page_text)
        phrase_count = len(phrase_count)

        if phrase_count > 0:
            scenario = 2

        else:
            phrase_count = re.findall("Sorry, there are no companies" +
                " matching", results_page_text)
            phrase_count = len(phrase_count)

            if phrase_count > 0:
                scenario = 0

            else:
                scenario = 1

        return scenario

    def get_companies_list(self, search_page, 
        per_limit = None):

        # Obtain the list of companies from a single
        # company search page. The companies are
        # presented as tuples in the form
        # (glassdoor_id, company_name, url).

        scenario = self.get_scenario(search_page)

        if scenario == 1:
            gd_id = self.get_gd_id_from_menu(search_page)
            comp_name = self.get_comp_name_from_menu(search_page)
            url = self.get_overview_url_from_menu(search_page)

            companies_list = [(gd_id, comp_name, url)]

        elif scenario == 2:
            gd_ids = self.get_gd_ids_from_search(search_page)
            comp_names = self.get_comp_names_from_search(search_page)
            urls = self.get_comp_urls_from_search(search_page)

            companies_list = list(zip(gd_ids, comp_names, urls))

        else:
            companies_list = []

        if isinstance(per_limit, int):
            if len(companies_list) >= per_limit:
                companies_list = companies_list[0: per_limit]

        return companies_list

    def get_url_base(self, company_page):

        url_base = company_page.find('a',
                   class_ = re.compile("eiCell cell.+?" +
                            self._aspect[1]))["href"]
        url_base = 'https://www.glassdoor.ca' + url_base + \
            '?sort.sortType=RD&sort.ascending=true'
        return url_base

    def get_url_n_th_page(self, url_base, page):

        head = re.search("^.+(?=.htm|.html)",url_base)[0]
        page_type = re.search("(.htm|.html)",url_base)[0]
        page_url = head + "_P" + str(page) + page_type

        return page_url

    def get_max_data_points(self, company_page):

        max_data_points = re.search("(?<=\"" + self._aspect[0] + 
            "Count\":)\d+", company_page.text)[0]
        max_data_points = int(max_data_points)
        return max_data_points

    def get_start_page(self, comp_id):

        query = 'select dp_count from _comp_meta ' + \
            f'where comp_id = "{comp_id}" ' + \
            f'and aspect = "{self._aspect[1]}";'
        self._sqlcursor.execute(query)
        query_result = self._sqlcursor.fetchone()

        start_page = 1

        if query_result != None:

            count = query_result[0]

            if count == None:
                start_page = 1
            else:
                start_page = (count //
                    self._max_data_points_per_page) + 1

        return start_page

    def get_search_urls_per_page(self, search_page):

        supp = search_page.find_all('div',
            class_="JobInfoStyle__jobTitle strong")
        supp = ["https://www.glassdoor.ca" + \
                each.find('a')['href'] for each in supp]
        return supp

    def get_search_result_urls(self, url_base,
        max_data_points = 1, start_page = 1):

        search_result_urls = []
        max_pages = ((max_data_points - 1) // 
            self._max_data_points_per_page) + 1

        if start_page > max_pages:
            start_page = 1

        for page_num in range(start_page, max_pages + 1):
            page_url = self.get_url_n_th_page(url_base, page_num)
            search_result_urls.append(page_url)

        return search_result_urls

    def simplify_search_keyword(self, keyword):

        keyword = keyword.lower()
        keyword = keyword.replace(' ', '+')
        return keyword

    def get_comp_search_url(self, keyword = "", location = ""):

        keyword = self.simplify_search_keyword(keyword)

        if location != "":
            best_match = self.get_loc_best_matches(location)
            loc_t = best_match[0]['locationType']
            loc_id = best_match[0]['locationId']

        search_url = f'https://www.glassdoor.ca/Reviews/' + \
            'company-reviews.htm?' + \
            f'typedKeyword={keyword}' + \
            f'&sc.keyword={keyword}' + \
            f'&locT={loc_t}' + \
            f'&locId={loc_id}'

        return search_url

    def get_data_point_urls(self, urls_list):
        
        urls_list = [(None, url) for url in urls_list]
        
        return urls_list

    def get_company_name(self, company_page):

        company_name = company_page.find("h1", 
            class_=" strong tightAll")
        company_name = company_name.text
        company_name = company_name.strip()
        return company_name

    def get_comps_from_criteria(self, per_limit = None):

        criteria_list = import_csv_list(self._criteria_list_dir)
        all_comps_list = []

        # Create progressbar
        prog_bar = pb.ProgressBar(prefix = "Companies search: ")
        criteria_list = prog_bar(criteria_list)

        for criteria in wait_rem(criteria_list, self._avg_wait):

            search_url = self.get_comp_search_url(
                criteria[0], criteria[1])

            results_page = self._session.get(search_url, 
                as_soup = True)
            all_comps_list += self.get_companies_list(results_page,
                per_limit)
            all_comps_list = remove_duplicates(all_comps_list)

        return all_comps_list
    
    # Methods for metadata

    def date_str_to_date(self, date_str):

        # Given a date string with the format of
        # 'd month, y' or 'month d, y', where 'month' is 
        # the month's name, convert the string into 
        # the corresponding datetime.date object.

        date_str = re.sub(',', '', date_str)

        date_str = date_str.split(' ')
        year = int(date_str[-1])
        for i in range(0, 2):
            try:
                day = int(date_str[i])
            except ValueError:
                month = month_str_to_num(date_str[i])

        # Use datetime.date to ensure proper zero padding.
        output = datetime.date(year, month, day)
        output = str(output)

        return output

    def get_last_update_date(self, aspect_page):
        return None

    def get_aspect_meta(self, aspect_page):

        meta = {}
        meta['last_update'] = self.get_last_update_date(
            aspect_page)

        return meta

    def check_for_update(self, comp_id, new_meta):

        # This method assumes there is update whenever 
        # there is ambiguity in the last update date.
        # This ensures that no data is left uncrawled.

        try:
            query = 'select last_update from _comp_meta ' + \
                f'where id = "{comp_id}_{self._aspect[1]}";'
            self._sqlcursor.execute(query)
            query_result = self._sqlcursor.fetchone()

            new_update_date = new_meta['last_update']

            if None in [query_result, new_update_date]:
                # query_result == None indicates that
            	# the queried id does not exist.

                pass

            elif len(query_result) > 0 and \
                query_result[0] != None:
                # len(query_result) == 0 indicates that
            	# the queried id does exist 
            	# but the corresponding last_update value
            	# is equal to NULL (ie. does not exist).

                old_update_date = query_result[0]
                new_update_date = datetime.datetime.strptime(
                    new_update_date, '%Y-%m-%d').date()
                
                if old_update_date >= new_update_date:
                    raise NoNewUpdate

        except KeyError as e:
            pass

        except TypeError as e:
            raise e

    # Methods for data extraction

    def get_comp_id(self, dp_soup = None):

        return self._current_company[0]

    def get_crawl_datetime(self, dp_soup = None):

        output = dt.now()
        output = output.strftime('%Y-%m-%d %H:%M:%S')

        return output

    def split_city_prov_state(self, loc):

        loc_split = re.split(", ", loc)
        city = ", ".join(loc_split[:-1])
        city_short = loc_split[0]
        province_state = loc_split[-1]

        output = {
            'city': city,
            'city_short': city_short,
            'province_state': province_state
        }

        return output

    def get_location_details(self, data_on_page):

        # Extract location details from data_on_page.
        # Override as necessary.

        return []

    def get_dp_soups_on_page(self, aspect_soup, table_def = None):

        # Get all data points on a page and return each
        # one of them as a BeautifulSoup object.

        return [aspect_soup]

    def get_and_insert_data(self, aspect_soup):

        for table_def in sorted(self._table_defs,
            key = lambda x: x['insert_order']):

            table_name = table_def['table_name']

            if table_name not in self._dp_counts:
                self._dp_counts[table_name] = 0

            dp_soups = self.get_dp_soups_on_page(aspect_soup, table_def)
            data = self.get_data_for_table(dp_soups, table_def)

            self.insert_rows(data, table_name,
                skip_on_dup_key = self._skip_ext_dps)

            self._dp_counts[table_name] += len(data)

    def get_data_for_table(self, dp_soups, table_def):

        dims = [each[0] for each in table_def['columns']]
        data_on_page = []

        for dp_soup in dp_soups:

            try:
                dps = self.get_each_dim(dp_soup, dims,
                    prefix = table_def['prefix'])
                data_on_page += dps

            except (requests.exceptions.HTTPError, TypeError,
                NoDataPoint) as e:
                # Data probably does not exist.
                pass

        return data_on_page

    # Crawling sequences

    def get_each_dim(self, dp_soup, dims, prefix):

        keys = []
        values = []

        # Compute

        for dim in dims:

            try:

                # Adapt to scenarios where a single page contains
                # multiple data points and are difficult to
                # be partitioned into separate self-contained soups.

                value = getattr(self, prefix + dim)(dp_soup)

                if not isinstance(value, list):
                    value = [value]

                keys.append(dim)
                values.append(value)

            except (AttributeError, TypeError, KeyError, ValueError):
                # Data for dimension probably does not exist.
                pass

        # Organize
        values = list(zip(*values))
        output = [dict(zip(keys, each)) for each in values]

        return output

    def crawl_aspect_urls(self, urls_list):

        urls_bar = pb.ProgressBar(prefix = \
            f"{self._aspect[1].capitalize()}: ")
        urls_list = urls_bar(urls_list)

        for url in wait_rem(urls_list, self._avg_wait):

            try:
                # Crawl
                aspect_soup = self._session.get(url[1],
                    as_soup = True)
                self.get_and_insert_data(aspect_soup)

            except FinalSearchPage:
                break

            except requests.exceptions.HTTPError:
                # Probably no data available.
                pass

    def crawl_company(self, company):

        try:

            # Preparation
            self.prepare_to_crawl()
            self._current_company = company

            # Skip companies if applicable.
            if self._skip_exc_comps:
                self.check_if_excluded(company)

            if self._skip_ext_comps:
                self.check_if_existing(company)

            # Obtain basic info about the company.
            company_page = self._session.get(company[2], as_soup = True)
            url_base = self.get_url_base(company_page)
            max_data_points = self.get_max_data_points(company_page)
            aspect_page = self._session.get(url_base, as_soup = True)

            # Skip ahead if applicable.
            self.check_data_exist_for_asp(aspect_page)
            new_meta = self.get_aspect_meta(aspect_page)

            if self._skip_by_update:
                self.check_for_update(company[0], new_meta)

            if self._skip_head_data:
                start_page = self.get_start_page(company[0])
            else:
                start_page = 1

            # Generate the list of URLs that return search results
            # of the aspect of interest.
            search_result_urls = self.get_search_result_urls(
                url_base, max_data_points, start_page)

            # Go through additional "layers" if necessary.
            # Some aspects, such as jobs, provides only hyperlinks
            # to the detailed information for each data point.
            # Acquire those data point IDs and URLs here.
            data_point_urls = self.get_data_point_urls(
                search_result_urls)

            # Skip all data points that already exist in the database.
            if self._skip_ext_dps:
                data_point_urls = self.skip_data_before_crawl(
                    data_point_urls)

            # Start crawling from aspect URLs.
            if len(data_point_urls) > 0:
                self.crawl_aspect_urls(data_point_urls)
                self._crawled_companies_count += 1

            # Insert the latest page update date
            self.insert_company_meta(company[0], new_meta)

        except (NoDataForAspect, NoNewUpdate,
                SkipExcludedCompany, SkipExistingCompany):
            pass

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                # The company page does not exist.
                pass

        finally:

            self.refresh_dp_count()

    def prepare_to_crawl(self):

        self.create_database()
        self._sqlcursor.execute("SET character_set_client = utf8mb4;")
        self._sqlcursor.execute(f"use {self._db_name};")
        self.create_tables(self._table_defs)
        self.create_tables(self._extra_table_defs)
        self.refresh_dp_count()