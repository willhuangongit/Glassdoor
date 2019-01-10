# =========
# Imports

# import re

from .crawler import *


# =========
# Classes

class InterviewsCrawler(Crawler):

    def __init__(self, param_dict = None, 
        mysql_auth = None, session_params = None):

        Crawler.__init__(self, param_dict, 
            mysql_auth = mysql_auth, 
            session_params = session_params)

        self._aspect = ('interview', 'interviews')
        self._max_data_points_per_page = 10

        self._table_defs += [
            {
                'table_name': 'interviews',
                'prefix': 'get_int_',
                'insert_order': 3,
                'columns':   [
                    ('id', 'char(30) PRIMARY KEY'),
                    ('comp_id', 'char(30)'),
                    ('crawl_datetime', 'datetime'),
                    ('update_datetime', 'datetime'),
                    ('interview_title', 'text'),
                    ('author_desc', 'text'),
                    ('author_loc', 'text'),
                    ('offer', 'text'),
                    ('exp_rating', 'text'),
                    ('difficulty', 'text'),
                    ('det_application', 'text'),
                    ('det_interview', 'text'),
                    ('det_negotiation', 'text'),
                    ('helpful', 'int(11)'),
                    ('emp_comm', 'text')
                ]
            },
            {
                'table_name': 'interview_questions',
                'prefix': 'get_q_',
                'insert_order': 1,
                'columns':   [
                    ('id', 'char(30) PRIMARY KEY'),
                    ('crawl_datetime', 'datetime'),
                    ('interview_id', 'char(30)'),
                    ('comp_id', 'char(30)'),
                    ('question', 'text'),
                    ('answers_count', 'int(11)'),
                    ('answers_url', 'text')
                ],
            },
            {
                'table_name': 'interview_answers',
                'prefix': 'get_a_',
                'insert_order': 2,
                'columns':[
                    ('id', 'char(30) PRIMARY KEY'),
                    ('crawl_datetime', 'datetime'),
                    ('comp_id', 'char(30)'),
                    ('question_id', 'char(30)'),
                    ('answer_date', 'date'),
                    ('author', 'char(200)'),
                    ('helpful', 'int(11) DEFAULT 0'),
                    ('answer', 'text')
                ]
            }
        ]

    # ==========
    # Methods

    # Override

    def get_dp_soups_on_page(self, aspect_soup, table_def = None):

        if table_def['table_name'] in ['interviews',
            'interview_questions']:

            dp_soups = aspect_soup.find_all('li',
                class_ = ' empReview cf ')

        elif table_def['table_name'] == 'interview_answers':

            urls = self.get_q_answers_url(aspect_soup)

            dp_soups = []

            for url in urls:

                soup = self._session.get(url, as_soup=True)

                if not self.get_a_ans_removed(soup):
                    dp_soups.append(soup)


        return dp_soups

    def get_last_update_date(self, page_soup):

        try:
            update_date = page_soup.find('div', 
                class_ = 'module interviewStats')
            update_date = update_date.find('div', 
                class_ = 'cell middle alignRt noWrap minor hideHH')
            update_date = re.search('(?<=Updated ).+', 
                update_date.text)[0]
            update_date = self.date_str_to_date(update_date)

        except (TypeError, AttributeError):
            update_date = None

        return update_date

    def check_data_exist_for_asp(self, aspect_page):

        count = self.get_max_data_points(aspect_page)
        if count == 0 \
        or "We don't have any interview reviews for" in aspect_page.text:
            raise NoDataForAspect

    # Intermediate methods for interviews

    def get_interview_outcomes(self, each_interview):

        int_outcomes = each_interview.find('div',
            class_ = "interviewOutcomes")
        int_outcomes = int_outcomes.find_all('div',
            class_ = "tightLt col span-1-3")
        int_outcomes = [each.text for each in int_outcomes]

        return int_outcomes

    # Methods for interviews

    def get_int_id(self, each_interview):

        review_id = each_interview.find("span",
            class_ = re.compile("block voteHelpful"))
        review_id = review_id['data-id']
        return review_id

    def get_int_crawl_datetime(self, soup):
        return self.get_crawl_datetime()

    def get_int_comp_id(self, soup):
        return self._current_company[0]

    def get_int_update_datetime(self, each_interview):

        update_datetime = each_interview.find('span',
            class_ = "dtreviewed")
        update_datetime = update_datetime.text
        update_datetime = re.sub('(PDT|PST)', '',
            update_datetime)
        update_datetime = update_datetime.strip()
        return update_datetime
     
    def get_int_interview_title(self, each_interview):

        interview_title = each_interview.find('span',
            class_ = "reviewer")
        interview_title = interview_title.text
        interview_title = interview_title.strip()
        return interview_title

    def get_int_author_desc(self, each_interview):

        author_desc = each_interview.find('div',
            class_ = "author minor")
        author_desc = author_desc.text
        author_desc = author_desc.strip()
        return author_desc

    def get_int_author_loc(self, each_interview):

        author_loc = each_interview.find('span',
            class_ = " authorLocation ")
        author_loc = author_loc.text
        author_loc = author_loc.strip()
        return author_loc

    def get_int_offer(self, each_interview):
        int_outcomes = self.get_interview_outcomes(each_interview)
        offer = None
        for each in int_outcomes:
            if "Offer" in each:
                offer = each
                break
        return offer

    def get_int_exp_rating(self, each_interview):
        int_outcomes = self.get_interview_outcomes(each_interview)
        exp_rating = None
        for each in int_outcomes:
            if "Experience" in each:
                exp_rating = each
                break
        return exp_rating

    def get_int_difficulty(self, each_interview):
        int_outcomes = self.get_interview_outcomes(each_interview)
        difficulty = None
        for each in int_outcomes:
            if "Interview" in each:
                difficulty = each
                break
        return difficulty

    def get_int_det_application(self, each_interview):
        det_appli = each_interview.find('p',
            class_ = re.compile("applicationDetails"))
        det_appli = det_appli.text
        return det_appli

    def get_int_det_interview(self, each_interview):
        det_interview = each_interview.find('p',
            class_ = re.compile("^interviewDetails"))
        det_interview = det_interview.text
        return det_interview

    def get_int_det_negotiation(self, each_interview):

        det_negotiation = each_interview.find('p',
            class_ = re.compile("^interviewNegotiationDetails"))
        det_negotiation = det_negotiation.text
        return det_negotiation

    def get_int_helpful(self, each_interview):

        helpful = each_interview.find('div',
            class_ = re.compile("helpfulBtn"))
        helpful = helpful.find('span',class_ = "count")
        helpful = helpful.text

        try:
            helpful = re.search("(?<=\()\d+?(?=\))",helpful)[0]
            helpful = int(helpful)

        except TypeError:
            helpful = 0

        finally:
            return helpful

    def get_int_emp_comm(self, each_interview):

        # Employer comments.

        emp_comm = each_interview.find('div',
            class_ = re.compile("empRepComment"))
        emp_comm = emp_comm.find("p",class_ = "commentText quoteText")
        emp_comm = emp_comm.text
        return emp_comm

    # Intermediate methods for table 'interview_questions'

    def get_qs_from_int(self, dp_int):

        output = dp_int.find_all('span',
            class_ = re.compile("^interviewQuestion "))

        return output

    def get_q_each_answers_url(self, dp_q):

        answers_url = dp_q.find('a',
            class_ = "questionResponse")
        answers_url = answers_url['href']
        answers_url = "https://www.glassdoor.ca" + answers_url

        return answers_url

    def get_q_qs_count(self, dp_int):
        return len(self.get_qs_from_int(dp_int))

    # Methods for questions

    def get_q_id(self, dp_int):

        q_tags = self.get_qs_from_int(dp_int)
        output = [self.get_q_each_answers_url(each)
            for each in q_tags]
        output = [re.search("(?<=QTN_)\d+(?=.htm)", each)[0]
            for each in output]

        return output

    def get_q_crawl_datetime(self, dp_int):

        n = self.get_q_qs_count(dp_int)
        output = [self.get_crawl_datetime()] * n

        return output

    def get_q_interview_id(self, dp_int):

        n = self.get_q_qs_count(dp_int)
        review_id = dp_int.find("span",
            class_ = re.compile("block voteHelpful"))
        review_id = review_id['data-id']
        output = [review_id] * n

        return output

    def get_q_comp_id(self, dp_int):

        n = self.get_q_qs_count(dp_int)
        output = [self._current_company[0]] * n

        return output

    def get_q_question(self, dp_int):

        q_tags = self.get_qs_from_int(dp_int)
        output = [str(each.contents[0]) for each in q_tags]
        output = [re.sub(' \\xa0 $', '', each).strip()
            for each in output]

        return output

    def get_q_answers_count(self, dp_int):

        qs = self.get_qs_from_int(dp_int)
        output = []

        for q in qs:

            response = q.find('a',
                class_ = "questionResponse").text

            if response == 'Answer Question':
                count = 0

            else:
                count = re.search("\d+?(?= Answer)",
                    response)[0]
                count = int(count.strip())

            output.append(count)
        
        return output

    def get_q_answers_url(self, dp_int):

        qs = self.get_qs_from_int(dp_int)
        output = [self.get_q_each_answers_url(each) for each in qs]

        return output

    # Intermediate methods for table 'interview_answers'

    def get_a_ans_removed(self, dp_ans):

        answers = self.get_ans_soups(dp_ans)
        removed = ["This post has been removed."
            in each.text for each in answers]

        return removed

    def include_ans(self, ans):

        subtag = ans.find('p')
        output = True

        if subtag != None:
            if 'class' in subtag.attrs:
                if 'postRemoved' in subtag.attrs['class']:
                    output = False

        return output

    def get_ans_soups(self, dp_ans):

        soups = dp_ans.find_all('div', id = 'InterviewQuestionAnswers')
        output = [each for each in soups if self.include_ans(each)]

        return output

    def get_ans_count(self, dp_ans):
        output = len(self.get_ans_soups(dp_ans))
        return output

    def get_answer_soups_from_url(self, each_answers_url):

        ans_page_soup = self._session.get(
            each_answers_url, as_soup = True)
        answer_soups = ans_page_soup.find('div',
            id = "InterviewQuestionAnswers")
        answer_soups = answer_soups.find_all('div',
            class_ = "comment tbl fill last")

        return answer_soups

    # Methods for table 'interview_answers'

    def get_a_id(self, dp_ans):

        answers = self.get_ans_soups(dp_ans)
        output = [each.find('span', attrs =
            {'data-disp-type': 'interview'})['data-id']
            for each in answers]

        return output

    def get_a_crawl_datetime(self, dp_ans):

        n = len(self.get_ans_soups(dp_ans))
        output = [self.get_crawl_datetime(dp_ans)] * n

        return output

    def get_a_comp_id(self, dp_ans):

        n = len(self.get_ans_soups(dp_ans))
        output = [self.get_crawl_datetime(dp_ans)] * n

        return output

    def get_a_question_id(self, dp_ans):

        n = len(self.get_ans_soups(dp_ans))

        output = dp_ans.find('span',
            attrs={'data-disp-type': 'interview question'})['data-id']

        return output

    def get_a_answer_date(self, dp_ans):

        answers = self.get_ans_soups(dp_ans)
        output = []

        for answer in answers:

            date = answer.find('div',
                class_ = re.compile('^authorJobTitle'))
            date = date.text
            date = re.search('(?= on )\d{4}-\d{2}-\d{2}',
                date)[0]
            date = date.strip()
            output.append(date)

        return output
   
    def get_a_author(self, dp_ans):

        answers = self.get_ans_soups(dp_ans)
        output = []

        for answer in answers:
            author = answer.find('div',
                class_ = re.compile('^authorJobTitle'))
            author = author.text
            author = re.search('.+(?= on )', author)[0]
            author = author.strip()
            output.append(author)

        return output

    def get_a_helpful(self, dp_ans):

        answers = self.get_ans_soups(dp_ans)
        output = []

        for answer in answers:

            helpful = answer.find('div',
                id = re.compile('^helpfulCount'))
            helpful = helpful.text
            helpful = helpful.strip()
            helpful = int(helpful)
            output.append(helpful)

        return output

    def get_a_answer(self, dp_ans):

        answers = self.get_ans_soups(dp_ans)
        output = []

        for answer in answers:

            comment = answer.find('p',
                class_ = re.compile('^commentText'))
            comment = comment.text
            comment = comment.strip()
            output.append(comment)

        return output