# Module for extracting photo info.

# ==========
# Imports
from .crawler import *


# =========
# Classes

class PhotosCrawler(Crawler):

    def __init__(self, param_dict = None, 
        mysql_auth = None, session_params = None):

        Crawler.__init__(self, param_dict, 
            mysql_auth = mysql_auth, 
            session_params = session_params)

        self._aspect = ('photo', 'photos')
        self._max_data_points_per_page = 18

        self._table_defs += [
            {
                'table_name': 'photos',
                'prefix': 'get_',
                'insert_order': 1,
                'columns': [
                    ('id', 'char(30) PRIMARY KEY'),
                    ('crawl_datetime', 'datetime'),
                    ('comp_id', 'char(30)'),
                    ('photo_name', 'text'),
                    ('photo_url', 'text')
                ]
            }
        ]


    # ==========
    # Methods

    # Override

    def get_dp_soups_on_page(self, aspect_soup, table_def = None):
        photo_grid = aspect_soup.find('div', class_="photoGrid")
        dp_soups = photo_grid.find_all('li')
        return dp_soups

    def check_data_exist_for_asp(self, aspect_page):

        count = self.get_max_data_points(aspect_page)
        if count == 0 \
        or "haven't posted any photos yet" in aspect_page.text:
            raise NoDataForAspect

    # Methods for extracting data about photos posted.

    def get_id(self, photo_soup):

        photo_id = photo_soup.find('figure')['data-id']

        return photo_id

    def get_photo_name(self, photo):

        photo_name = self.get_photo_url(photo)
        photo_name = re.search("(?<=/)[^/]*?$", photo_name)[0]
        
        return photo_name

    def get_photo_url(self, photo):

        photo_url = photo.find('img',
            class_ = re.compile("^companyPhoto"))
        photo_url = photo_url['data-original']
        photo_url = photo_url.replace('/lst/','/l/')

        return photo_url