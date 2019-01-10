# ==========
# Import modules
from bs4 import BeautifulSoup
#from .localio import *
from .utilfunc import wait_rem, msg

import re, requests


# ===========
# Classes

# Custom exceptions

class ServerSuspicion(Exception):
    def __init__(self):
        Exception.__init__(self, "Server identifies session as " + 
            "suspicious and has blocked access.")

class LoginUnsuccessful(Exception):
    def __init__(self):
        Exception.__init__(self, "Login unsuccessful.")



# Custom object classes

class Session(requests.sessions.Session):
    
    # Default attributes
    def __init__(self, param_dict = None):

        self._session = requests.session()
        self._headers = {}
        self._conn_tries = 1
        self._login_tries = 1
        self._avg_wait = 0
        self._mute_mode = 1

        if param_dict != None:
            for key, value in param_dict.items():
                setattr(self, '_' + key, value)


    # ==========
    # Methods

    # Override

    def get(self, target_url, as_soup = False, **kwargs):

        # Override the original get method in requests.session.
        
        if 'headers' not in kwargs:
            kwargs['headers'] = self._headers

        for conn_try in wait_rem(range(1, self._conn_tries + 1), 
            self._avg_wait):

            try:

                page = self._session.get(target_url, **kwargs)

                # self.check_connection_status(page)
                page.raise_for_status()
                self.is_suspicious(page, **kwargs)

                # Convert to BeautifulSoup object if desired.

                if as_soup:
                    page = BeautifulSoup(page.text, 
                        self._soup_decoder)

                return page
                break

            except requests.exceptions.HTTPError as httpe:

                if conn_try < self._conn_tries:

                    conn_try += 1
                    msg("\tAttempt to connect with server again.\n" +
                        f"\tConnection attempt: {conn_try}", 
                        self._mute_mode)
                    
                else:

                    msg("\tMaximum retries has been reached, but" + 
                        "\tconnection error still persists.\n" + 
                        "\tAbort attempt to connect.",
                        self._mute_mode)

                    raise httpe

    # Custom methods

    def is_login_successful(self):

        test_page = self.get(self._homepage_url, 
            headers = self._headers)

        successful =  "\'state\' : \'open\'" in test_page.text
        if successful:
            msg("Login successful.", self._mute_mode)
            return successful
        else:
            raise LoginUnsuccessful

    def is_suspicious(self, page = None, **kwargs):

        # Allow for on-demand re-check
        if page == None:
            page = self.get(self._homepage_url, **kwargs)

        key_phrase = "We have been receiving some suspicious " + \
        "activity from you or someone sharing your internet " + \
        "network. Please help us keep Glassdoor safe by " + \
        "verifying that you're a real person."

        suspicious = key_phrase in page.text

        if suspicious:
            raise ServerSuspicion

    def get_token(self, token_page):
        token = re.search("(?<=\"gdToken\":\").+?(?=\",)",
            token_page.text)[0]
        return token

    def login(self):

        login_page = self.get(self._login_url)
        self._gdToken = self.get_token(login_page)

        # Send POST to loginAjax.htm to request login.
        # login_post_url can be figured out by using 
        # "Preserve log" under the "Network" tab in Chrome.
        msg("Send login POST to server.", self._mute_mode)
        payload = {"username": self._username,
                   "password": self._password,
                   "gdToken": self._gdToken,
                   "postLoginUrl": ""}

        for login_try in wait_rem(range(1, self._login_tries + 1), 
            self._avg_wait):

            try:
                msg(f"Login attempt: {login_try}", self._mute_mode)
                self._session.post(self._login_post_url, 
                                   data = payload,
                                   headers = self._headers)
                self.is_login_successful()
                break

            except LoginUnsuccessful as lu:
                if login_try < self._login_tries:
                    msg("Try to login again.", self._mute_mode)
                    login_try += 1
                else:
                    msg("Abort login attempt", self._mute_mode)
                    raise lu