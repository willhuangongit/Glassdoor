# ==========
# Imports

from bs4 import BeautifulSoup
import progressbar as pb
import requests

from .crawler import *
from assists.utilfunc import *


# =========
# Classes

class OverviewCrawler(Crawler):

    def __init__(self, param_dict = None, 
        mysql_auth = None, session_params = None):

        Crawler.__init__(self, param_dict, 
            mysql_auth = mysql_auth, 
            session_params = session_params)

        self._aspect = ('overview', 'overviews')

        self._table_defs += [
            {
                'table_name': 'overviews',
                'prefix': 'get_',
                'insert_order': 7,
                'columns': [
                    ('comp_id', 'char(30) PRIMARY KEY'),
                    ('crawl_datetime', 'datetime'),
                    ('company_name', 'text'),
                    ('website', 'text'),
                    ('size', 'text'),
                    ('year_founded', 'int(11)'),
                    ('type_of_org', 'text'),
                    ('industry', 'text'),
                    ('revenue', 'text'),
                    ('about', 'longtext'),
                    ('mission', 'mediumtext'),
                    ('wwfu', 'mediumtext')
                ]
            },
            {
                'table_name': "hqs",
                'prefix': 'get_hq_',
                'insert_order': 6,
                'columns': [
                    ('id', 'char(30) PRIMARY KEY'),
                    ('comp_id', 'char(30)'),
                    ('loc_id', 'char(30)')
                ]
            },
            {
                'table_name': "locations",
                'prefix': 'get_loc_',
                'insert_order': 5,
                'columns': [
                    ('id', 'char(30) PRIMARY KEY'),
                    ('comp_id', 'char(30)'),
                    ('loc_id', 'char(30)'),
                    ('rating', 'float')
                ]
            },
            {
                'table_name': "location_details",
                'prefix': 'get_locd_',
                'insert_order': 4,
                'columns': [
                    ('id', 'char(30) PRIMARY KEY'),
                    ('city', 'char(100)'),
                    ('province_state', 'char(50)'),
                    ('country', 'char(50)')
                ]
            },
            {
                'table_name': "competitors",
                'prefix': 'get_cpt_',
                'insert_order': 3,
                'columns': [
                    ('id', 'char(200) PRIMARY KEY'),
                    ('comp_id', 'char(30)'),
                    ('competitor', 'char(150)')
                ]
            },
            {
                'table_name': "awards",
                'prefix': 'get_awd_',
                'insert_order': 2,
                'columns': [
                    ('award', 'text'), # No PK
                    ('comp_id', 'char(30)')
                ]
            },
            {
                'table_name': "glassdoor_awards",
                'prefix': 'get_gdawd_',
                'insert_order': 1,
                'columns': [
                    ('award', 'text'), # No PK
                    ('comp_id', 'char(30)')
                ]
            }
        ]

    # ==========
    # Methods

    # Override

    @count_method_change
    def skip_data_before_crawl(self, urls_list):

        self._sqlcursor.execute(
            f"select distinct comp_id from {self._aspect};")
        ext_list = self._sqlcursor.fetchall()
        ext_list = [each[0] for each in ext_list]
        urls_list = [each for each in urls_list 
            if not each[0] in ext_list] 

        return urls_list

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

            company_page = self._session.get(company[2], 
                as_soup = True)

            # Use a progress bar just for consistency
            # with other crawlers, even though there
            # is only one data point...

            urls_bar = pb.ProgressBar(prefix= \
                f"{self._aspect[0].capitalize()}: ")
            comps_list = urls_bar([company_page])

            for each in comps_list:
                self.get_and_insert_data(each)
                self._crawled_companies_count += 1

        except (AttributeError, NoDataForAspect, NoNewUpdate,
                SkipExcludedCompany, SkipExistingCompany):
            # No data for table
            pass

        except requests.exceptions.HTTPError:
            # Data probably does not exist.
            pass

    def get_dp_soups_on_page(self, aspect_soup, table_def = None):

        if table_def['table_name'] == 'overviews':
            output = [aspect_soup]

        elif table_def['table_name'] == 'hqs':

            hqs_tag = aspect_soup.find("label", text = "Headquarters")
            output = []

            if hqs_tag != None:
                output = [each for each in hqs_tag.next_siblings]

        elif table_def['table_name'] == 'locations':

            locs = aspect_soup.find("div", class_ = "module toggleable")
            output = []

            if locs != None:
                locs = locs.find_all("li")
                for each in locs:
                    if 'class' in each.attrs:
                        if 'strong' in each.attrs['class']:
                            continue
                    output.append(each)

        elif table_def['table_name'] == 'location_details':

            hqs_tag = aspect_soup.find("label", text="Headquarters")
            locs = aspect_soup.find("div", class_ = "module toggleable")
            hqs = []
            locations = []

            if hqs_tag != None:
                hqs = [BeautifulSoup(each.text, 'lxml')
                       for each in hqs_tag.next_siblings]

            if locs != None:
                locations = locs.find_all("li")
                locations = [BeautifulSoup(each.a.text, 'lxml')
                    for each in locations]

            output = hqs + locations

            return output

        elif table_def['table_name'] == 'competitors':

            cpts = aspect_soup.find("label", text = "Competitors")

            if cpts != None:
                cpts = cpts.next_sibling
                cpts = cpts.text
                cpts = cpts.strip()
                cpts = re.split(", ", cpts)
                output = [BeautifulSoup(each, 'lxml') for each in cpts]
            else:
                output = []

        elif table_def['table_name'] == 'awards':
            output = aspect_soup.find_all("div", class_ = "award")

        elif table_def['table_name'] == 'glassdoor_awards':
            gd_awards = aspect_soup.find("h3", text = "Glassdoor Awards")

            if gd_awards != None:
                gd_awards = gd_awards.next_sibling
                output = gd_awards.find_all("p", class_="noMargTop noMargBot")
            else:
                output = []

        return output

    # Misc methods

    def get_gd_id_from_url(self, url):

        output = re.search("(?<=EI_IE)\d+?(?=\.)",url)[0]
        return output

    # Methods for table 'overviews'

    def get_website(self, company_page):

        website = company_page.find('span', class_="value website")
        website = website.find("a", class_="link")['href']

        return website

    def get_size(self, company_page):

        size = company_page.find("label", text="Size")
        size = size.next_sibling
        size = size.text
        size = size.strip()

        return size

    def get_year_founded(self, company_page):

        year_founded = company_page.find("label", text="Founded")
        year_founded = year_founded.next_sibling
        year_founded = year_founded.text
        year_founded = year_founded.strip()

        return year_founded

    def get_type_of_org(self, company_page):

        type_of_org = company_page.find("label", text="Type")
        type_of_org = type_of_org.next_sibling
        type_of_org = type_of_org.text
        type_of_org = type_of_org.strip()

        return type_of_org

    def get_industry(self, company_page):

        industry = company_page.find("label", text="Industry")
        industry = industry.next_sibling
        industry = industry.text
        industry = industry.strip()

        return industry

    def get_revenue(self, company_page):

        revenue = company_page.find("label", text="Revenue")
        revenue = revenue.next_sibling
        revenue = revenue.text
        revenue = revenue.strip()

        return revenue

    def get_about(self, company_page):

        about = company_page.find("div",
            class_ = "margTop empDescription")
        about = about.attrs['data-full']
        about = about.strip()
        about = re.sub("(<br/>)+", "/r/n", about)

        return about

    def get_mission(self, company_page):

        mission = company_page.find("p",
            attrs = {
                "class": "tightBot",
                "data-full": re.compile("Mission:")})
        mission = mission.attrs['data-full']
        mission = mission.strip()
        mission = re.sub("(<b>)+", "", mission)
        mission = re.sub("(</b>)+", "", mission)

        return mission

    def get_wwfu(self, company_page):

        # "Why Work for Us"

        why_work_for_us = ""

        wwfu = company_page.find("div",
                                 class_="whyWorkForUsModule module")
        wwfu = wwfu.find_all(
            "div", attrs={'class': 'overviewSectionBody'})

        for each in wwfu:
            each = [line for line in each.strings]
            each = "\r\n".join(each)
            why_work_for_us += each
            why_work_for_us += "\r\n==========\r\n"

        return why_work_for_us

    # Methods for table 'hqs'

    def get_hq_id(self, dp_soup):

        hq = dp_soup.text
        loc_data = self.get_loc_best_matches(hq)
        loc_id = str(loc_data[0]['realId'])
        output = f'{self._current_company[0]}_{loc_id}'

        return output

    def get_hq_comp_id(self, dp_soup):

        return self.get_comp_id()

    def get_hq_loc_id(self, dp_soup):

        hq = dp_soup.text
        loc_data = self.get_loc_best_matches(hq)
        output = str(loc_data[0]['realId'])

        return output

    # Methods for table 'locations'

    def get_loc_id(self, dp_soup):

        loc_full = self.get_loc_name_full(dp_soup)
        loc_data = self.get_loc_best_matches(loc_full)
        loc_id = str(loc_data[0]['realId'])
        output = f'{self._current_company[0]}_{loc_id}'

        return output

    def get_loc_comp_id(self, dp_soup):

        return self.get_comp_id()

    def get_loc_loc_id(self, dp_soup):

        loc_full = self.get_loc_name_full(dp_soup)
        loc_data = self.get_loc_best_matches(loc_full)
        output = str(loc_data[0]['realId'])

        return output

    def get_loc_rating(self, dp_soup):

        rating = dp_soup.find("a",
            href = re.compile("/Location/"))
        rating = rating.find("span",
            class_ = re.compile("compactStars"))
        rating = rating.text

        if rating == "n/a":
            rating = None
        else:
            rating = float(rating)

        return rating

    def get_loc_name_full(self, loc_soup):

        loc_name = loc_soup.find("a",
            href = re.compile("/Location/"))
        loc_name = loc_name.text

        return loc_name

    # Methods for table 'location_details'

    def get_locd_id(self, dp_soup):
        loc = dp_soup.text
        loc_data = self.get_loc_best_matches(loc)
        output = loc_data[0]['locationId']
        return output

    def get_locd_city(self, dp_soup):

        hq = dp_soup.text
        loc_split = self.split_city_prov_state(hq)
        output = loc_split['city']

        return output

    def get_locd_province_state(self, dp_soup):

        hq = dp_soup.text
        loc_split = self.split_city_prov_state(hq)
        output = loc_split['province_state']

        return output

    def get_locd_country(self, dp_soup):

        hq = dp_soup.text
        loc_data = self.get_loc_best_matches(hq)
        output = str(loc_data[0]['countryName'])

        return output

    # Methods for table 'competitors'

    def get_cpt_id(self, dp_soup):

        cpt = dp_soup.text
        output = f'{self._current_company[0]}_{cpt}'

        return output

    def get_cpt_comp_id(self, dp_soup = None):

        return self.get_comp_id(dp_soup)

    def get_cpt_competitor(self, dp_soup):

        output = dp_soup.text

        return output

    # Methods for table 'awards'

    def get_awd_award(self, dp_soup):

        return dp_soup.text

    def get_awd_comp_id(self, dp_soup = None):

        return self.get_comp_id(dp_soup)

    # Methods for table 'glassdoor_awards'

    def get_gdawd_award(self, dp_soup):

        award = dp_soup.text
        output = award.replace("\xa0", " ")

        return output

    def get_gdawd_comp_id(self, dp_soup = None):

        return self.get_comp_id(dp_soup)