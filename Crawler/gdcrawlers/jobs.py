# ==========
# Imports

from .crawler import *
from assists.webnavi import *


# ==========
# Classes

class JobsCrawler(Crawler):

    def __init__(self, param_dict = None, 
        mysql_auth = None, session_params = None):

        Crawler.__init__(self, param_dict, 
            mysql_auth = mysql_auth, 
            session_params = session_params)

        self._aspect = ('job', 'jobs')
        self._max_data_points_per_page = 40

        self._table_defs += [
            {
                'table_name': 'jobs',
                'prefix': 'get_',
                'insert_order': 1,
                'columns': [
                    ('id', 'char(30) PRIMARY KEY'),
                    ('crawl_datetime', 'datetime'),
                    ('comp_id', 'char(30)'),
                    ('is_job_expired', 'tinyint(1)'),
                    ('job_post_date', 'date'),
                    ('title', 'text'),
                    ('url', 'text'),
                    ('emp_type', 'text'),
                    ('salary_curr', 'text'),
                    ('valid_thru', 'date'),
                    ('industry', 'text'),
                    ('loc_country', 'text'),
                    ('loc_locality', 'text'),
                    ('loc_region', 'text'),
                    ('loc_lati', 'float'),
                    ('loc_longi', 'float'),
                    ('category', 'text'),
                    ('description', 'longtext')
                ]
            }
        ]

    # ==========
    # Methods

    # Override

    def get_data_point_urls(self, url_list):

        job_ids = []
        job_urls = []

        for url in wait_rem(url_list, self._avg_wait):

            try:
                search_result_soup = self._session.get(
                    url, as_soup = True)
                ids_on_page = self.get_job_ids_on_page(
                    search_result_soup)
                job_ids += ids_on_page
                job_urls += [self.get_job_url(each) for each 
                            in ids_on_page]
            
            except requests.exceptions.HTTPError:
                # Server connection error.
                # There may be no data point url
                # beoynd this point
                # Proceed with existing urls.
                break

        data_point_urls = list(zip(job_ids, job_urls))

        return data_point_urls

    def get_last_update_date(self, page_soup):

        try:
            update_date = page_soup.find('span', 
                class_ = 'lastUpdated')
            update_date = re.search('(?<=Updated ).+', 
                update_date.text)[0]
            update_date = self.date_str_to_date(
                update_date)

        except TypeError:
            update_date = None

        return update_date

    def check_data_exist_for_asp(self, aspect_page):

        count = self.get_max_data_points(aspect_page)
        
        if count == 0 \
        or "There are currently no open jobs at" in aspect_page.text:
            
            raise NoDataForAspect

    # Methods for extracting info from search result pages.

    def get_job_ids_on_page(self, search_result_soup):

        try:
            ids = search_result_soup.find('div',
                id="EmployerJobs",
                class_='module jobScopeWrapper smallPort noPadBot')
            ids = ids.find_all('li',
                class_="jl")
            ids = [each['data-id'] for each in ids]
            
        except AttributeError as ae:
            ids = []

        finally:
            return ids        

    def get_job_url(self, job_id):

        return "https://www.glassdoor.ca/partner/" + \
        "jobListing.htm?ao=85944&jobListingId=" + job_id

    # Extract data from each job page.

    def get_job_meta(self, job_page):

        job_meta = job_page.find('script').text
        job_meta = re.sub("(\t|\n)","",job_meta)
        job_meta = re.search("(?<=\'job\' : ).+(?=,'test')",
            job_meta)[0]
        job_meta = literal_eval(job_meta)

        return job_meta

    def get_script(self, job_page):

        script = job_page.find('script',
            type = "application/ld+json").text
        script = self.cleanup(script)

        return script

    def get_script_dict(self, job_page, key, next_key = ""):
        
        script_dict = self.get_script(job_page)
        script_dict = re.search("(?<=" + key + "\": )" + 
            ".+?(?=,\"" + next_key + ")", script_dict)[0]
        script_dict = literal_eval(script_dict)

        return script_dict

    def get_id(self, job_page):

        job_id = job_page.find('div', class_='jobViewHeader')['id']
        job_id = job_id.replace("JD_", "")

        return job_id

    def get_is_job_expired(self, job_page):

        phrase = re.search("(?<=\'expired\' : )\'\w+?\'", 
                            job_page.text)[0]
        job_expired = (phrase == 'true')

        return job_expired

    def get_job_post_date(self, job_page):

        job_post_date = re.search("(?<=\"datePosted\": \")" + 
            ".+?(?=\",)", job_page.text)[0]

        return job_post_date

    def get_title(self, job_page):

        title = job_page.find('div', class_='jobViewJobTitleWrap')
        title = title.find('h2').text

        return title

    def get_url(self, job_page):

        url = re.search("(?<=\"url\": \")" + 
            ".+?(?=\",)", job_page.text)[0]

        return url

    def get_emp_type(self, job_page):

        emp_type = re.search("(?<=\"employmentType\": \")" + 
            ".*?(?=\",)", job_page.text)[0] 

        return emp_type

    def get_salary_curr(self, job_page):

        salary_curr = re.search("(?<=\"salaryCurrency\": \")" + 
            ".+?(?=\",)", job_page.text)[0] 

        return salary_curr

    def get_valid_thru(self, job_page):

        valid_thru = re.search("(?<=\"validThrough\": \")" + 
            ".+?(?=\",)", job_page.text)[0] 

        return valid_thru

    def get_industry(self, job_page):

        industry = re.search("(?<=\"industry\": \")" + 
            ".+?(?=\",)", job_page.text)[0] 

        return industry

    def get_loc_country(self, job_page):

        script = self.get_script(job_page)
        loc_country = re.search("(?<=\"addressCountry\")" + 
            ".+?(?=\})", script)[0]
        loc_country = re.search("(?<=\"name\" : \")" + 
            ".+?(?=\")", loc_country)[0]

        return loc_country

    def get_loc_locality(self, job_page):

        script = self.get_script(job_page)
        loc_locality = re.search("(?<=\"addressLocality\": \")" + 
            ".+?(?=\")", script)[0]

        return loc_locality

    def get_loc_region(self, job_page):

        script = self.get_script(job_page)
        loc_region = re.search("(?<=\"addressRegion\": \")" + 
                             ".+?(?=\")", script)[0]

        return loc_region

    def get_loc_lati(self, job_page):

        script = self.get_script(job_page)
        loc_lati = re.search("(?<=\"latitude\": \")" + 
                             ".+?(?=\")", script)[0]
        loc_lati = float(loc_lati)

        return loc_lati

    def get_loc_longi(self, job_page):

        script = self.get_script(job_page)
        loc_longi = re.search("(?<=\"longitude\": \")" + 
                             ".+?(?=\")", script)[0]
        loc_longi = float(loc_longi)

        return loc_longi

    def get_category(self, job_page):

        script = self.get_script(job_page)

        try:
            occu_list = re.search("(?<=\"occupationalCategory\"" + 
                " : )\[.+?\](?=\,)", script)[0]
            occu_list = literal_eval(occu_list)
            category = occu_list[1]

            return category

        except TypeError:
            # Data probably does not exist.
            pass
        
    def get_description(self, job_page):

        script = self.get_script(job_page)
        description = re.search("(?<=\"description\": \")" + 
            ".+(?=\"\})", script)[0]

        return description