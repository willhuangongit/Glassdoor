# =========
from .crawler import *
import re


# ===========
class ReviewsCrawler(Crawler):

    def __init__(self, param_dict = None, 
        mysql_auth = None, session_params = None):

        Crawler.__init__(self, param_dict, 
            mysql_auth = mysql_auth, 
            session_params = session_params)

        self._aspect = ('review', 'reviews')
        self._max_data_points_per_page = 10

        self._table_defs += [
            {
                'table_name': 'reviews',
                'prefix': 'get_',
                'insert_order': 1,
                'columns': [
                    ('id', 'char(30) PRIMARY KEY'),
                    ('crawl_datetime', 'datetime'),
                    ('comp_id', 'char(30)'),
                    ('review_date', 'date'),
                    ('review_title', 'text'),
                    ('overall_stars', 'int(11)'),
                    ('work_life_balance', 'int(11)'),
                    ('culture_values', 'int(11)'),
                    ('career_ops', 'int(11)'),
                    ('comp_benefits', 'int(11)'),
                    ('emp_status', 'text'),
                    ('reviewer', 'text'),
                    ('recommend', 'text'),
                    ('outlook', 'longtext'),
                    ('recom_pos', 'mediumtext'),
                    ('background', 'mediumtext'),
                    ('pros', 'text'),
                    ('cons', 'text'),
                    ('adv_mngnt', 'text'),
                    ('helpful', 'int(11)')
                ]
            }
        ]

    # ==========
    # Methods

    # Override

    def get_last_update_date(self, page_soup):

        try:
            update_date = page_soup.find('div', 
                class_ = 'module filterableContents')
            update_date = update_date.find('div', 
                class_ = 'aside middle minor hideHH')
            update_date = re.search('(?<=Updated ).+' , 
                update_date.text)[0]
            update_date = self.date_str_to_date(update_date)

        except AttributeError:
            update_date = None

        return update_date

    def check_data_exist_for_asp(self, aspect_page):

        count = self.get_max_data_points(aspect_page)
        if count == 0 \
        or "We don't have any reviews for" in aspect_page.text:
            raise NoDataForAspect

    def get_dp_soups_on_page(self, aspect_soup, table_def = None):
        dp_soups = aspect_soup.find_all('li', 
            class_ = " empReview cf ")
        return dp_soups

    # Methods for extracting data from a single review page.

    def get_id(self, review):
        
        review_id = review.find("span",
            class_ = re.compile("^flagContent $"))["data-id"]
        return review_id

    def get_review_date(self, review):

        review_date = review.find("time",
                        class_="date subtle small")['datetime']
        return review_date

    def get_review_title(self, review):

        review_title = review.find("span",
                       class_=re.compile("^summary $"))
        review_title = review_title.text[1:-1]
        return review_title

    def get_overall_stars(self, review):

        overall_stars = review.find("span",
                        class_=re.compile("^value-title$"))
        overall_stars = int(overall_stars["title"][:-2])
        return overall_stars

    def get_work_life_balance(self, review):

        work_life_balance = review.find("div",text="Work/Life Balance")
        work_life_balance = work_life_balance.next_sibling["title"]
        work_life_balance = int(work_life_balance[:-2])
        return work_life_balance

    def get_culture_values(self, review):

        culture_values = review.find("div",text="Culture & Values")
        culture_values = culture_values.next_sibling["title"]
        culture_values = int(culture_values[:-2])
        return culture_values

    def get_career_ops(self, review):

        career_ops = review.find("div",text="Career Opportunities")
        career_ops = career_ops.next_sibling["title"]
        career_ops = int(career_ops[:-2])
        return career_ops

    def get_comp_benefits(self, review):

        comp_benefits = review.find("div",text="Comp & Benefits")
        comp_benefits = comp_benefits.next_sibling["title"]
        comp_benefits = int(comp_benefits[:-2])
        return comp_benefits

    def get_jt_rv(self, review):

        return review.find("span",
                        class_="authorJobTitle reviewer").text

    def get_emp_status(self, review):

        jt_rv = self.get_jt_rv(review)
        emp_status = re.search(".+?(?= - )",jt_rv)[0]
        return emp_status

    def get_reviewer(self, review):

        jt_rv = self.get_jt_rv(review)
        reviewer = re.search("(?<= - ).+",jt_rv)[0]
        return reviewer

    def get_recom_ratings(self, review):

        recom_ratings = review.find("div",
            class_="flex-grid recommends")
        recom_ratings = recom_ratings.find_all("div",
                        class_="tightLt col span-1-3")
        recom_ratings = [each.text for each in recom_ratings]

        return recom_ratings

    def get_recommend(self, review):
        
        recom_ratings = self.get_recom_ratings(review)
        recom = None
        for text in recom_ratings:
            if "Doesn't Recommend" in text:
                recom = "Does Not Recommend"
                break

            elif 'Recommend' in text:
                recom = 'Recommend'
                break

        return recom

    def get_outlook(self, review):

        recom_ratings = self.get_recom_ratings(review)
        outlook = None

        for each in recom_ratings:
            if 'Outlook' in each:
                outlook = each
                break

        return outlook

    def get_recom_pos(self, review):
        recom_ratings = self.get_recom_ratings(review)
        recom_pos = None
        for each in recom_ratings:
            if 'of CEO' in each:
                recom_pos = each
                break
        return recom_pos

    def get_background(self, review):

        background = review.find("p",class_=" tightBot mainText").text
        return background

    def get_pros(self, review):

        pros = review.find("p",
               class_=" pros mainText truncateThis wrapToggleStr").text
        return pros

    def get_cons(self, review):

        cons = review.find("p",
                class_=" cons mainText truncateThis wrapToggleStr").text
        return cons

    def get_adv_mngnt(self, review):

        adv_mngnt = review.find("p",
            class_=" adviceMgmt mainText truncateThis" + 
            " wrapToggleStr").text
        return adv_mngnt

    def get_helpful(self, review):

        try:
            helpful = review.find("span",
                       class_=re.compile("block voteHelpful.*"))["data-count"]
        except TypeError:
            helpful = 0
        finally:
            return helpful