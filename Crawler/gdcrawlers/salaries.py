# ==========
# Imports

from .crawler import *
from assists.utilfunc import wait_rem


# ==========
# Classes

class SalariesCrawler(Crawler):

    def __init__(self, param_dict = None, 
        mysql_auth = None, session_params = None):

        Crawler.__init__(self, param_dict, 
            mysql_auth = mysql_auth, 
            session_params = session_params)

        self._aspect = ('salary', 'salaries')
        self._max_data_points_per_page = 20

        self._table_defs += [
            {
                'table_name': 'salaries',
                'insert_order': 1,
                'columns':[
                    ('id', 'char(200) PRIMARY KEY'),
                    ('crawl_datetime', 'datetime'),
                    ('comp_id', 'char(30)'),
                    ('position_name', 'text'),
                    ('update_datetime', 'datetime'),
                    ('time_unit', 'text'),
                    ('currency', 'text'),
                    ('description', 'text'),
                    ('title_id', 'text'),
                    ('emp_status', 'text'),
                    ('count', 'int(11)'),
                    ('total_count', 'int(11)'),
                    ('base_pay_count', 'int(11)'),
                    ('base_pay_min', 'float'),
                    ('base_pay_max', 'float'),
                    ('base_pay_mean', 'float'),
                    ('base_pay_median', 'float'),
                    ('total_pay_count', 'int(11)'),
                    ('total_pay_min', 'float'),
                    ('total_pay_max', 'float'),
                    ('total_pay_mean', 'float'),
                    ('total_pay_median', 'float'),
                    ('cash_bonus_total_count', 'int(11)'),
                    ('cash_bonus_total_min', 'float'),
                    ('cash_bonus_total_max', 'float'),
                    ('cash_bonus_total_mean', 'float'),
                    ('cash_bonus_total_median', 'float'),
                    ('cash_bonus_count', 'int(11)'),
                    ('cash_bonus_min', 'float'),
                    ('cash_bonus_max', 'float'),
                    ('cash_bonus_mean', 'float'),
                    ('cash_bonus_median', 'float'),
                    ('stock_bonus_count', 'int(11)'),
                    ('stock_bonus_min', 'float'),
                    ('stock_bonus_max', 'float'),
                    ('stock_bonus_mean', 'float'),
                    ('stock_bonus_median', 'float'),
                    ('profit_sharing_count', 'int(11)'),
                    ('profit_sharing_min', 'float'),
                    ('profit_sharing_max', 'float'),
                    ('profit_sharing_mean', 'float'),
                    ('profit_sharing_median', 'float'),
                    ('sales_comm_count', 'int(11)'),
                    ('sales_comm_min', 'float'),
                    ('sales_comm_max', 'float'),
                    ('sales_comm_mean', 'float'),
                    ('sales_comm_median', 'float')
                ]
            }
        ]

    # ==========
    # Methods

    # Override

    def get_data_point_urls(self, search_result_urls):

        data_point_urls = []

        try:
        
            for url in wait_rem(search_result_urls, self._avg_wait):

                search_page = self._session.get(url, as_soup = True)

                if "We don't have any salaries for" \
                    in search_page.text:
                    raise FinalSearchPage

                else:
                    data_point_urls += self.get_urls_on_page(
                        search_page)

        finally:
            return data_point_urls

    def get_dp_soups_on_page(self, page_soup, table_def = None):

        return [page_soup]

    def get_last_update_date(self, page_soup):

        page_text = page_soup.text

        if "We don't have any salaries for" in page_text:
            return None

        else:

            try:
                update_date = re.search('(?<=\"mostRecent"\:\")' + 
                    '.+?(?=T)', page_text)[0]
                update_date = update_date.split('-')
                update_date = [int(each) for each in update_date]
                
                # Note that datetime.date is not JSON-
                # serializable.

                update_date = datetime.date(*update_date)
                update_date = str(update_date)

            except TypeError:
                update_date = None

            return update_date

    def check_data_exist_for_asp(self, aspect_page):

        count = self.get_max_data_points(aspect_page)
        if count == 0 \
        or "We don't have any salaries for" in aspect_page.text:

            raise NoDataForAspect

    def get_data_for_table(self, dp_soups, table_def):

        dims = [each[0] for each in table_def['columns']]
        data_on_page = []

        for dp_soup in dp_soups:

            try:

                page_type = self.get_page_type(dp_soup)

                if page_type == 1:
                    prefix = 'get_ft_'
                else:
                    prefix = 'get_pt_'

                dp = self.get_each_dim(dp_soup, dims,
                    prefix = prefix)
                data_on_page += dp

            except (requests.exceptions.HTTPError, TypeError):
                print()
                # Data probably does not exist.
                pass

        return data_on_page

    # Page navigation methods

    def get_page_type(self, page):

        key_phrase = "Glassdoor has salaries, wages, tips, bonuses, " + \
            "and hourly pay based upon employee reports and estimates." 
        
        if key_phrase not in page.text:
            # Full-time position
            page_type = 1
        
        else:
            # Non-full-time position.
            page_type = 2

        return page_type

    def get_urls_on_page(self, search_page):
        
        tags = search_page.find_all('div',
            class_ = 'JobInfoStyle__jobTitle strong')
        tags = [each.find('a') for each in tags]

        ids = [tag.text for tag in tags]
        urls = ["https://www.glassdoor.ca" + 
            tag['href'] for tag in tags]

        urls_on_page = list(zip(ids, urls))

        return urls_on_page

    # Intermediate methods for full-time data columns

    def get_ft_main_data(self, page_soup):

        try:
            main_data = page_soup.find('script',
                                       type='application/ld+json').text
            main_data = literal_eval(main_data)
            return main_data

        except AttributeError:
            # Page probably does not exist.
            raise requests.exceptions.HTTPError

    def get_ft_base_pay(self, page_soup):

        base_pay = re.search("(?<=.results.0.basePayStatistics\":)" +
            ".+?\"SalaryStatistics\"}",page_soup.text)[0]
        base_pay = literal_eval(base_pay)
        base_pay.pop('__typename')

        return base_pay

    def get_ft_total_pay(self, page_soup):
        total_pay = re.search("(?<=.results.0." +
            "totalCompStatistics\":)" +
            ".+?\"SalaryStatistics\"}",page_soup.text)[0]
        total_pay = literal_eval(total_pay)
        total_pay.pop('__typename')
        return total_pay

    def get_ft_cash_bonus_total(self, page_soup):
        try:
            cash_bonus_total = re.search("(?<=.results.0." +
                "totalCashBonusStatistics\":)" +
                ".+?\"SalaryStatistics\"}",
                page_soup.text)[0]
            cash_bonus_total = literal_eval(cash_bonus_total)
            cash_bonus_total.pop('__typename')
        except TypeError:
            cash_bonus_total = None
        finally:
            return cash_bonus_total

    def get_ft_cash_bonus(self, page_soup):
        try:
            cash_bonus = re.search("(?<=.results.0." +
                "cashBonusStatistics\":).+?\"SalaryStatistics\"}",
                page_soup.text)[0]
            cash_bonus = literal_eval(cash_bonus)
            cash_bonus.pop('__typename')
        except TypeError:
            cash_bonus = None
        finally:
            return cash_bonus

    def get_ft_stock_bonus(self, page_soup):
        try:
            stock_bonus = re.search("(?<=.results.0." +
                "stockBonusStatistics\":).+?\"SalaryStatistics\"}",
                page_soup.text)[0]
            stock_bonus = literal_eval(stock_bonus)
            stock_bonus.pop('__typename')
        except TypeError:
            stock_bonus = None
        finally:
            return stock_bonus

    def get_ft_profit_sharing(self, page_soup):
        try:
            profit_sharing = re.search("(?<=.results.0." +
                "profitSharingStatistics\":).+?\"SalaryStatistics\"}",
                page_soup.text)[0]
            profit_sharing = literal_eval(profit_sharing)
            profit_sharing.pop('__typename')
        except TypeError:
            profit_sharing = None
        finally:
            return profit_sharing

    def get_ft_sales_comm(self, page_soup):
        try:
            sales_comm = re.search("(?<=.results.0." +
                "salesCommissionStatistics\":)" +
                ".+?\"SalaryStatistics\"}",
                page_soup.text)[0]
            sales_comm = literal_eval(sales_comm)
            sales_comm.pop('__typename')
        except TypeError:
            sales_comm = None
        finally:
            return sales_comm

    # Methods for full-time data columns.

    def get_ft_id(self, page_soup):

        output = f'{self._current_company[0]}_' + \
            f'{self.get_ft_position_name(page_soup)}'

        return output

    def get_ft_crawl_datetime(self, page_soup):

        return self.get_crawl_datetime()

    def get_ft_comp_id(self, page_soup):

        return self.get_comp_id()

    def get_ft_position_name(self, page_soup):

        main_data = self.get_ft_main_data(page_soup)

        if 'name' in main_data.keys():
            output = main_data['name']

        else:
            output = re.search("(?<=\"filter.jobTitleFTS\":\")" +
                               ".+?(?=\")", page_soup.text)[0]
            output = str(output)

        return output

    def get_ft_update_datetime(self, page_soup):

        update_datetime = re.search("(?<=\"mostRecent\":\")" +
            ".+?(?=\")", page_soup.text)[0]
        update_datetime = re.sub("T"," ",update_datetime)

        return update_datetime

    def get_ft_time_unit(self, page_soup):

        output = self.get_ft_main_data(page_soup)
        output = output['estimatedSalary'][0]['unitText']

        return output

    def get_ft_currency(self, page_soup):

        main_data = self.get_ft_main_data(page_soup)
        output = main_data['estimatedSalary'][0]['currency']

        return output

    def get_ft_emp_status(self, page_soup):

        return 'FULL_TIME'

    def get_ft_description(self, page_soup):
        main_data = self.get_ft_main_data(page_soup)
        description = main_data['description']
        return description

    def get_ft_base_pay_count(self, page_soup):

        base_pay = self.get_ft_base_pay(page_soup)
        return base_pay['count']

    def get_ft_base_pay_min(self, page_soup):

        base_pay = self.get_ft_base_pay(page_soup)
        return base_pay['min']

    def get_ft_base_pay_max(self, page_soup):

        base_pay = self.get_ft_base_pay(page_soup)
        return base_pay['max']

    def get_ft_base_pay_mean(self, page_soup):

        base_pay = self.get_ft_base_pay(page_soup)
        return base_pay['mean']

    def get_ft_base_pay_median(self, page_soup):

        base_pay = self.get_ft_base_pay(page_soup)
        return base_pay['median']

    def get_ft_total_pay_count(self, page_soup):

        total_pay = self.get_ft_total_pay(page_soup)
        return total_pay['count']

    def get_ft_total_pay_min(self, page_soup):

        total_pay = self.get_ft_total_pay(page_soup)
        return total_pay['min']

    def get_ft_total_pay_max(self, page_soup):

        total_pay = self.get_ft_total_pay(page_soup)
        return total_pay['max']

    def get_ft_total_pay_mean(self, page_soup):

        total_pay = self.get_ft_total_pay(page_soup)
        return total_pay['mean']

    def get_ft_total_pay_median(self, page_soup):

        total_pay = self.get_ft_total_pay(page_soup)
        return total_pay['median']

    def get_ft_cash_bonus_total_count(self, page_soup):

        data = self.get_ft_cash_bonus_total(page_soup)

        if data == None:
            return None
        else:
            return data['count']

    def get_ft_cash_bonus_total_min(self, page_soup):

        data = self.get_ft_cash_bonus_total(page_soup)

        if data == None:
            return None
        else:
            return data['min']

    def get_ft_cash_bonus_total_max(self, page_soup):

        data = self.get_ft_cash_bonus_total(page_soup)

        if data == None:
            return None
        else:
            return data['max']

    def get_ft_cash_bonus_total_mean(self, page_soup):

        data = self.get_ft_cash_bonus_total(page_soup)

        if data == None:
            return None
        else:
            return data['mean']

    def get_ft_cash_bonus_total_median(self, page_soup):

        data = self.get_ft_cash_bonus_total(page_soup)

        if data == None:
            return None
        else:
            return data['median']

    def get_ft_cash_bonus_count(self, page_soup):

        data = self.get_ft_cash_bonus(page_soup)

        if data == None:
            return None
        else:
            return data['count']

    def get_ft_cash_bonus_min(self, page_soup):

        data = self.get_ft_cash_bonus(page_soup)

        if data == None:
            return None
        else:
            return data['min']

    def get_ft_cash_bonus_max(self, page_soup):

        data = self.get_ft_cash_bonus(page_soup)

        if data == None:
            return None
        else:
            return data['max']

    def get_ft_cash_bonus_mean(self, page_soup):

        data = self.get_ft_cash_bonus(page_soup)

        if data == None:
            return None
        else:
            return data['mean']

    def get_ft_cash_bonus_median(self, page_soup):

        data = self.get_ft_cash_bonus(page_soup)

        if data == None:
            return None
        else:
            return data['median']

    def get_ft_stock_bonus_count(self, page_soup):

        data = self.get_ft_stock_bonus(page_soup)

        if data == None:
            return None
        else:
            return data['count']

    def get_ft_stock_bonus_min(self, page_soup):

        data = self.get_ft_stock_bonus(page_soup)

        if data == None:
            return None
        else:
            return data['min']

    def get_ft_stock_bonus_max(self, page_soup):

        data = self.get_ft_stock_bonus(page_soup)

        if data == None:
            return None
        else:
            return data['max']

    def get_ft_stock_bonus_mean(self, page_soup):

        data = self.get_ft_stock_bonus(page_soup)

        if data == None:
            return None
        else:
            return data['mean']

    def get_ft_stock_bonus_median(self, page_soup):

        data = self.get_ft_stock_bonus(page_soup)

        if data == None:
            return None
        else:
            return data['median']

    def get_ft_profit_sharing_count(self, page_soup):

        data = self.get_ft_profit_sharing(page_soup)

        if data == None:
            return None
        else:
            return data['count']

    def get_ft_profit_sharing_min(self, page_soup):

        data = self.get_ft_profit_sharing(page_soup)

        if data == None:
            return None
        else:
            return data['min']

    def get_ft_profit_sharing_max(self, page_soup):

        data = self.get_ft_profit_sharing(page_soup)

        if data == None:
            return None
        else:
            return data['max']

    def get_ft_profit_sharing_mean(self, page_soup):

        data = self.get_ft_profit_sharing(page_soup)

        if data == None:
            return None
        else:
            return data['mean']

    def get_ft_profit_sharing_median(self, page_soup):

        data = self.get_ft_profit_sharing(page_soup)

        if data == None:
            return None
        else:
            return data['median']

    def get_ft_sales_comm_count(self, page_soup):

        data = self.get_ft_sales_comm(page_soup)

        if data == None:
            return None
        else:
            return data['count']

    def get_ft_sales_comm_min(self, page_soup):

        data = self.get_ft_sales_comm(page_soup)

        if data == None:
            return None
        else:
            return data['min']

    def get_ft_sales_comm_max(self, page_soup):

        data = self.get_ft_sales_comm(page_soup)

        if data == None:
            return None
        else:
            return data['max']

    def get_ft_sales_comm_mean(self, page_soup):

        data = self.get_ft_sales_comm(page_soup)

        if data == None:
            return None
        else:
            return data['mean']

    def get_ft_sales_comm_median(self, page_soup):

        data = self.get_ft_sales_comm(page_soup)

        if data == None:
            return None
        else:
            return data['median']


    # Methods for part-time data.

    def get_pt_id(self, page_soup):

        output = self.get_pt_position_name(page_soup)
        output = [f'{self._current_company[0]}_{each}' for each in output]

        return output

    def get_pt_crawl_datetime(self, page_soup):

        ids = self.get_position_names(page_soup)
        n = len(ids)
        output = [self.get_crawl_datetime(page_soup)] * n

        return output

    def get_pt_comp_id(self, page_soup):

        ids = self.get_pt_position_name(page_soup)
        n = len(ids)
        output = [self._current_company] * n

        return output

    def get_pt_position_name(self, page_soup):

        output = page_soup.find('div',
            class_ = re.compile('^salaryList$'))
        output = output.find_all('div',
            id = re.compile("^SalaryRowStyle__"))
        output = [each.find('a').text for each in output]

        return output

    def get_pt_update_datetime(self, page_soup):

        text = page_soup.text

        # This assumes that the first result returned is the
        # correct value.

        update_datetime = re.search("(?<=\"mostRecent\":\")" +
            ".+?(?=\")", text)[0]
        update_datetime = re.sub("T"," ",update_datetime)

        titles_count = len(self.get_pt_position_name(
            page_soup))
        update_datetime = [update_datetime] * \
            titles_count

        return update_datetime

    def get_pt_time_unit(self, page_soup):

        time_unit = re.findall("(?<=\"payPeriod\":\").+?(?=\")",
            page_soup.text)

        return time_unit

    def get_pt_currency(self, page_soup):

        output = re.findall("(?<=\"code\":\").+?(?=\")",
            page_soup.text)

        return output

    def get_pt_title_id(self, page_soup):

        title_ids = re.findall("(?<=\"JobTitle:)\d+?(?=\")",
            page_soup.text)
        title_ids = [int(each) for each in title_ids]

        # Assume that the odd-indexed entries are
        # successive duplicates.
        # Do not use "remove duplicates", as two job titles
        # may possibly have

        title_ids = title_ids[0::2]
        return title_ids

    def get_pt_emp_status(self, page_soup):
        emp_status = re.findall("(?<=\"employmentStatus\":\")" + 
            ".+?(?=\")", page_soup.text)
        return emp_status

    def get_pt_count(self, page_soup):
        salary_counts = re.findall("(?<=\"count\":)\d+?(?=,)", 
            page_soup.text)
        salary_counts = [int(each) for each in salary_counts]
        return salary_counts

    def get_pt_total_count(self, page_soup):
        emp_total_count = re.findall("(?<=\"employerTotalCount\":)" + 
            "\d+?(?=,)", page_soup.text)
        emp_total_count = [int(each) for each in emp_total_count]
        return emp_total_count

    def get_pt_base_pay_min(self, page_soup):
        min_base_pay = re.findall("(?<=\"minBasePay\":).+?(?=,)", 
            page_soup.text)
        min_base_pay = [float(each) for each in min_base_pay]
        return min_base_pay

    def get_pt_base_pay_median(self, page_soup):
        median_base_pay = re.findall("(?<=\"medianBasePay\":).+?(?=,)", 
            page_soup.text)
        median_base_pay = [float(each) for each in median_base_pay]
        return median_base_pay

    def get_pt_base_pay_max(self, page_soup):
        max_base_pay = re.findall("(?<=\"maxBasePay\":).+?(?=,)", 
            page_soup.text)
        max_base_pay = [float(each) for each in max_base_pay]
        return max_base_pay