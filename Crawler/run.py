# ==========
# Imports

from assists.utilfunc import *
from parameters import *
from assists.webnavi import *

# ==========
# Functions

def sum_crawled_dp_count(aspect):

    list_name = aspect + '_crawlers'
    exec(f"global {list_name}")
    crawlers_list = eval(list_name)
    result = sum((crawler.crawled_data_points_count 
        for crawler in crawlers_list))
    return result

def create_crawlers(crawler_switches):

    crawlers = {}

    for switch in crawler_switches:

        if switch[1]:

            # Create the crawlers.
            class_name = switch[0].title() + 'Crawler'

            exec(f'from gdcrawlers.{switch[0]} import {class_name}')
            exec(f'crawlers["{switch[0]}"] = ' + \
                f'{class_name}(' + \
                f'param_dict = {switch[0]}_params, ' + \
                f'mysql_auth = mysql_auth, ' + \
                f'session_params = session_params)')

    return crawlers

def run_crawlers(crawlers_dict, companies_list):

    try:
        for company in companies_list:

            print("==========")
            print(f"Crawling data for {company[1]}.")

            for crawler in crawlers_dict.values():
                crawler.crawl_company(company)

    finally:
        for crawler in crawlers_dict.values():
            crawler.close()

def count_crawls(crawlers_dict):

    sums = {}

    if crawlers_dict == None:
        print("No data crawled.")

    else:
        for crawler in crawlers_dict.values():

            dp_counts = crawler.dp_counts

            for pair in dp_counts.items():
                if pair[0] in sums:
                    sums[pair[0]] += pair[1]
                else:
                    sums[pair[0]] = pair[1]

        sums = [(item[0], item[1]) for item in sums.items()]
        sums.sort(key = lambda x: x[0])
        total = sum([each[1] for each in sums])

        print("Total number of data points obtained by table:")

        for pair in sums:
            print(f"\t{pair[0]}: {'{:,}'.format(pair[1])}")

        print(f"\n\tAll: {'{:,}'.format(total)}")


# ==========
# Main Thread

if __name__ == "__main__":

    time_begin = time.time()

    print("==========\n" + 
        "Initiate Glassdoor crawling. Please wait.")

    try:

        # Search for companies according to 
        # search criteria.

        companies_list = []

        if search_mode == 1:

            from gdcrawlers.crawler import Crawler
            print("Crawling based on company keywords.")
            seeker = Crawler(
                param_dict = compseeker_params,
                mysql_auth = mysql_auth,
                session_params = session_params)

        elif search_mode == 2:

            from gdcrawlers.jobseeker import JobSeeker
            print("Crawling based on job search keywords.")
            seeker = JobSeeker(
                param_dict = jobseeker_params,
                mysql_auth = mysql_auth,
                session_params = session_params)
        
        companies_list = seeker.get_comps_from_criteria(per_limit)

        # Create crawlers.

        if len(companies_list) > 0:        
            crawlers_dict = create_crawlers(crawler_switches)    
            run_crawlers(crawlers_dict, companies_list)
        else:
            crawlers_dict = None
            print("No company to crawl for.\n")

    finally:

        print("==========\n" + 
            "The program ends. \n\n")

        print("< Summary >\n\n")

        # Calculations

        # Time
        time_end = time.time()
        time_diff = time_end - time_begin

        # Displays        
        print("Program total run time: " + 
            f"{time.strftime('%H:%M:%S', time.gmtime(time_diff))}\n" +
            f'Companies examined: {len(companies_list)}\n')

        count_crawls(crawlers_dict)