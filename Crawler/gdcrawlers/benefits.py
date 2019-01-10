# ==========
# Imports
from .crawler import *


# ==========
# Classes

class BenefitsCrawler(Crawler):

    def __init__(self, param_dict = None, 
        mysql_auth = None, session_params = None):

        Crawler.__init__(self, param_dict, 
            mysql_auth = mysql_auth, 
            session_params = session_params)

        self._aspect = ('benefit', 'benefits')
        self._max_data_points_per_page = 10

        self._table_defs += [
            {
                'table_name': 'benefits',
                'prefix': 'get_',
                'insert_order': 1,
                'columns': [
                    ('id', 'char(30) PRIMARY KEY'),
                    ('crawl_datetime', 'datetime'),
                    ('comp_id', 'char(30)'),
                    ('review_date', 'date'),
                    ('author_emp_status', 'char(30)'),
                    ('author_emp_title', 'char(150)'),
                    ('city', 'char(50)'),
                    ('province_state', 'char(50)'),
                    ('comment', 'text'),
                    ('helpful', 'int(11)'),
                    ('emp_comm', 'text')
                ]
            }
        ]

    # ==========
    # Methods

    # Override

    def get_last_update_date(self, aspect_page):

        try:

            # This approach assumes that the latest benefit 
            # review is the second entry on the list.
            # The first entry is the "Most Commented"
            # benefit review.

            dates = aspect_page.find_all('li',
                'benefitReview hreview ')

            if len(dates) == 1:
                date = dates[0]
            else:
                date = dates[1]

            date = date.find('div', 'dtreviewed minor date')
            date = date.text
            date = date.strip()
            date = self.date_str_to_date(date)

            return date

        except AttributeError:
            # No benefit review available
            pass

    def check_data_exist_for_asp(self, aspect_page):

        count = self.get_max_data_points(aspect_page)
        if count == 0 \
        or "We don't have any benefit information for" \
        in aspect_page.text:

            raise NoDataForAspect

    def get_dp_soups_on_page(self, page_soup, table_def = None):

        if "We don't have any benefit information for" \
        in page_soup.text:

            items_on_page = []
            
        else:

            section_soup = page_soup.find('div', 
                id = "BenefitComments")
            items_on_page = section_soup.find_all('li',
                class_ = re.compile("benefitReview hreview "))

            if len(items_on_page) == 0:

                # Quit the iterative search when there is no more
                # benefits review to find.

                raise FinalSearchPage

        return items_on_page

    def get_url_n_th_page(self, url_base, page_num):

        head = re.search("^.+(?=.htm|.html)",url_base)[0]
        page_type = re.search("(.htm|.html)",url_base)[0]
        page_url = head + "_IP" + str(page_num) + page_type

        return page_url

    def get_location_details(self, data_on_page):

        output = []

        for data in data_on_page:

            city = data['city']
            province_state = data['province_state']
            loc = f'{city}, {province_state}'
            loc_data = self.get_loc_best_matches(loc)[0]
            loc_id = loc_data['realId']
            country = loc_data['countryName']

            row = {
                'id': loc_id,
                'city': city,
                'province_state': province_state,
                'country': country
            }

            if row not in output:
                output.append(row)

        return output

    # Methods for extracting data from each data point.

    def get_id(self, data_point):
        benefit_ID = data_point['data-benefit-id']
        return benefit_ID

    def get_rating(self, data_point):
        rating = data_point.find('span', class_="value-title")
        rating = rating['title']
        rating = float(rating)
        rating = int(rating)
        return rating

    def get_review_date(self, data_point):
        review_date = data_point.find('div',
            class_ = "dtreviewed minor date")
        review_date = review_date.text
        review_date = self.date_str_to_date(review_date)
        return review_date

    def get_author_emp_title(self, data_point):
        emp_title = data_point.find('span',
            class_ = "authorInfo minor cell middle")
        emp_title = emp_title.find('span', class_ = "reviewer")
        emp_title = emp_title.text
        emp_title = emp_title.strip()
        return emp_title
        
    def get_author_outer(self, data_point):
        emp_title = self.get_author_emp_title(data_point)    
        emp_title = self.esc_for_regex(emp_title)
        full_desc = data_point.find('span',
            class_ = "authorInfo minor cell middle").text
        outer = re.split(f'\s+?{emp_title}\s+?in ', full_desc)
        outer = [each.strip() for each in outer]
        return outer
        
    def get_author_emp_status(self, data_point):
        outer = self.get_author_outer(data_point)
        return outer[0]
        
    def get_city(self, data_point):


        outer = self.get_author_outer(data_point)
        location = outer[1]
        location = re.split(", ", location)
        city = ", ".join(location[:-1])

        return city
        
    def get_province_state(self, data_point):
        outer = self.get_author_outer(data_point)
        location = outer[1]
        location = re.split(", ", location)
        province_state = location[-1]
        return province_state

    def get_comment(self, data_point):
        comment = data_point.find('p',
            class_ = re.compile("^description "))
        comment = comment.text
        comment = comment.strip()
        return comment

    def get_helpful(self, data_point):
        helpful = data_point.find('span',
            class_ = re.compile("^block voteHelpful"))
        helpful = helpful['data-count']
        helpful = int(helpful)
        return helpful

    def get_emp_comm(self, data_point):
        emp_comm = data_point.find('p',
            class_ = "commentText quoteText")
        emp_comm = emp_comm.text
        return emp_comm