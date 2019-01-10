# ==========
from calendar import month_name, month_abbr
from collections import OrderedDict
from datetime import datetime as dt
import re
import time


# =========
# Functions

def msg(string, mute_mode = 0, mute_type = 0, 
    print_args = [], print_kwargs = {}):
    
    # Display messages selectively depending on
    # parameter setting.
    # This allows the user to determine how detailed
    # the program's messages displayed should be.

    if mute_mode in [0, mute_type]:
        print(string, *print_args, **print_kwargs)

def remove_duplicates(my_list):

    # Remove duplicates from a list.

    my_list = list(OrderedDict.fromkeys(my_list))
    return my_list

def get_now_datetime():
    now_datetime = dt.now()
    now_datetime = now_datetime.strftime('%Y-%m-%d %H:%M:%S')
    return now_datetime


def month_str_to_num(month_str, as_str = False):
    
    # month_str: a string representing a month.
    # eg. January, Jan, etc.
    # If no result matches a proper month
    # representation, then return None.
    # as_str: return string if true, else
    # return int.
    
    month_str = month_str.title()
    result = None
    
    if len(month_str) == 3:
        pool = month_abbr
    else:
        pool = month_name
    
    for i in range(1, 13):
        if month_str == pool[i]:
            result = i
            if as_str:
                result = str(result)
            break

    return result





# ==========
# Wrappers for iterators and generators

def wait_rem(iter_gen, max_wait = 0):

    # For each iteration over some given iterator (or
    # generator) iter_gen, if the iteration ends before 
    # max_wait seconds, then wait for the remaining 
    # seconds until max_wait seconds have elapsed.
    # This function can help crawlers' loops slow down
    # and prevent possible server blocks.

    for each in iter_gen:
        begin_time = time.time()
        yield each
        end_time = time.time()
        elapsed_time = end_time - begin_time
        remaining = max(max_wait - elapsed_time, 0)
        time.sleep(remaining)


# ==========
# Decorators

def count_method_change(function):

    # Count the number of elements added or removed 
    # from an object with length attribute.
    # This is for class methods only.

    def wrapper(self, *args, **kwargs):

        for each in args:

            try:
                before_count = len(each)
                break

            except TypeError as te:
                # This accounts for cases where the leading arguments
                # don't have any length attribute.
                # msg(f'    {te}')
                if each == args[-1]:
                    # Raise TypeError if no entry
                    # has length attribute.
                    raise te
                else:
                    continue

        output = function(self, *args, **kwargs)
        after_count = len(output)
        change = after_count - before_count

        if 'mute_mode' in kwargs:
            mute_mode = kwargs['mute_mode']
        else:
            mute_mode = 0

        if change > 0:
            msg(f"\tNumber of elements added: {abs(change)}", 
                mute_mode)
        elif change < 0:
            msg(f"\tNumber of elements removed: {abs(change)}", 
                mute_mode)
        else:
            msg("\tThe number of elements stays the same.", 
                mute_mode)

        return output

    return wrapper