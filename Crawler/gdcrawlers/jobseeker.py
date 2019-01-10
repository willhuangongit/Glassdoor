# ==========
# Imports

from .crawler import *
from assists.localio import import_csv_list
from assists.utilfunc import wait_rem, remove_duplicates


# ==========
# Classes

class JobSeeker(Crawler):

    def __init__(self, param_dict = None, 
        mysql_auth = None, session_params = None):

        Crawler.__init__(self, param_dict, 
            mysql_auth = mysql_auth, 
            session_params = session_params)

        self._aspect = ('job', 'jobs')
        self._max_data_points_per_page = 30


    # ==========
    # Methods

    # Override

    def get_url_n_th_page(self, url_base, page):

        head = re.search("^.+(?=.htm|.html)",url_base)[0]
        page_type = re.search("(.htm|.html)",url_base)[0]
        page_url = head + "_IP" + str(page) + page_type

        return page_url

    # Methods for searching jobs directly.

    def get_job_search_url(self, job = "", location = ""):
        
        if job == location == "":
            job_search_url = "https://www.glassdoor.ca/Job/" + \
            "independent-jobs-SRCH_KO0,11.htm"
        
        else:
            long_name = ""
            long_name_len = 0
            job_len = 0
            loc_id = ""
            loc_lower = 0
            loc_upper = 0
            job_lower = 0
            job_upper = 0
            loc_range = ""
            job_range = ""

            if location != "":

                location = location.replace(" ", "-")

                best_match = self.get_loc_best_matches(location)[0]

                long_name = best_match['longName']
                long_name = re.search("^.+?(?=,|\()", long_name)[0]
                long_name = long_name.strip()
                long_name = long_name.replace(" ", "-")

                long_name_len = len(long_name)

                loc_id = "IC" + str(best_match['locationId'])

                loc_upper = long_name_len
                job_lower = loc_upper + 1

                loc_range = f"IL.{loc_lower},{loc_upper}"

            else:
                job_lower = 0

            if job != "":

                job = job.replace(" ", "-")
                job_len = len(job)
                job_upper = job_lower + job_len
                job_range = f"KO{job_lower},{job_upper}"


            head = "https://www.glassdoor.ca/Job/"

            body = [long_name, job, "jobs-"]
            body = [each for each in body if each != ""]
            body = "-".join(body)

            tail = ["SRCH", loc_range, loc_id, job_range]
            tail  =[each for each in tail if each != ""]
            tail = "_".join(tail)

            job_search_url = head + body + tail + ".htm"
        
        return job_search_url

    def get_jobs_count(self, page_soup):

        jobs_count = re.search("(?<=\'totalSearchResults\': \")" + 
            "\d+?(?=\")", page_soup.text)[0]
        jobs_count = int(jobs_count)

        return jobs_count

    def get_job_search_urls(self, job = "", location = ""):

        url_base = self.get_job_search_url(job, location)
        base_page = self._session.get(url_base, as_soup = True)
        max_data_points = self.get_jobs_count(base_page)
        max_pages = ((max_data_points - 1) // 
            self._max_data_points_per_page) + 1

        job_search_urls = []

        for i in range(max_pages):
            url = self.get_url_n_th_page(url_base, i + 1)
            job_search_urls.append(url)

        return job_search_urls

    def get_job_search_meta(self, page_soup):

        search_meta = re.sub("(\n|\t)","", page_soup.text)
        search_meta = re.search("(?<=\'search\':).+?(?=,'employer')", 
                                search_meta)[0]
        search_meta = search_meta.strip()
        search_meta = literal_eval(search_meta)

        return search_meta

    def get_job_ids(self, page_soup):

        search_meta = self.get_job_search_meta(page_soup)
        job_ids = search_meta['jobIds']

        return job_ids

    def get_job_titles(self, page_soup):

        job_names = page_soup.find_all('li', class_ = 'jl')
        job_names = [each.find('div', class_ = 'flexbox') 
            for each in job_names]
        job_names = [each.text for each in job_names]

        return job_names

    def get_comp_ids(self, page_soup):

        company_ids = page_soup.find_all('li', class_="jl")
        company_ids = [each["data-emp-id"] for each in company_ids]

        return company_ids

    def get_comp_names(self, page_soup):

        company_names = page_soup.find_all('td', class_ = 'company')
        company_names = [each.text for each in company_names]

        return company_names

    def get_overview_url(self, company_id, company_name):

        company_name = company_name.replace(" ","-")
        ov_url = "https://www.glassdoor.ca/Overview/" + \
            f"Working-at-{company_name}-EI_IE{company_id}.htm"

        return ov_url

    def get_ov_urls(self, page_soup):
        
        company_ids = self.get_comp_ids(page_soup)
        company_names = self.get_comp_names(page_soup)

        pairs = list(zip(company_ids, company_names))

        ov_urls = []
        for pair in pairs:
            ov_urls.append(self.get_overview_url(pair[0], pair[1]))

        return ov_urls

    def get_jobs_table_from_page(self, page_soup):
        
        job_ids = self.get_job_ids(page_soup)
        job_titles = self.get_job_titles(page_soup)
        company_ids = self.get_comp_ids(page_soup)
        company_names = self.get_comp_names(page_soup)
        ov_urls = [self.get_overview_url(company_id, company_name) 
                   for (company_id, company_name) 
                   in list(zip(company_ids, company_names))]
        
        job_table = list(zip(job_ids, job_titles, company_ids, 
                            company_names, ov_urls))

        return job_table

    def get_jobs_table(self, job, location):

        job_search_urls = self.get_job_search_urls(job, location)
        jobs_table = []

        for url in job_search_urls:
            begi_time = time.time()
            page_soup = self._session.get(url, as_soup = True)
            jobs_table += self.get_jobs_table_from_page(page_soup)
            end_time = time.time()
            wait_remaining(begin_time, end_time, self._avg_wait)

        return jobs_table

    # Special methods

    def get_comps_from_criteria(self, per_limit = None):
       
        criteria_list = import_csv_list(self._criteria_list_dir)
        comps_list = []

        prog_bar = pb.ProgressBar(prefix = "Companies search: ")
        criteria_list = prog_bar(criteria_list)

        for criteria in criteria_list:
                
            job = criteria[0]
            location = criteria[1]
            job_search_urls = self.get_job_search_urls(job, location)

            comps_list += self.get_companies_list(job_search_urls, 
                per_limit)
            comps_list = remove_duplicates(comps_list)

        return comps_list

    def get_companies_list(self, job_search_urls, 
        per_limit = None):
        
        results = []

        for url in wait_rem(job_search_urls, self._avg_wait):

            page_soup = self._session.get(url, as_soup = True)
            company_ids = self.get_comp_ids(page_soup)
            company_names = self.get_comp_names(page_soup)
            company_urls = self.get_ov_urls(page_soup)
            tuples = list(zip(company_ids, company_names,
                company_urls))
            results += tuples
            results = remove_duplicates(results)

            if isinstance(per_limit, int):
                if len(results) >= per_limit:
                    results = results[0: per_limit]
                    break

        return results