# ==========
from os import walk, makedirs, listdir
from os.path import exists, isfile, join, splitext
import csv, json, os

# ==========
# Functions

def this_module_name():

    # Obtain the current module name.
    
    return next(walk("."))[2][0]

def standardize_path(path):

    # Supplement a given local path with the forward slash if 
    # it does not already have one.

    if path == "":
        path = "/"
    elif path[-1] != "/":
        path += "/"
    return path

def import_csv_list(list_dir, discard_header = False, 
    encoding = 'utf-8'):

    # Obtain the CSV file that lists all the company search criteria 
    # the user wants to process.

    with open(list_dir, encoding = encoding, 
        newline='',) as csvfile:

        list_reader = csv.reader(csvfile, delimiter=',')

        # Ignore the first row if discard_header is True.
        if discard_header:
            list_reader.next()

        # Obtain each row if it is non-empty.
        search_list = [row for row in list_reader if row != []]

    return search_list

def import_json_database(file_name, root_dir, encoding = 'utf-8'):

    # Import the database to overwrite.

    # Starndardize in case this function is used individually.
    root_dir = standardize_path(root_dir)

    # Create the path where the database is stored
    # if it does not already exist.
    if not exists(root_dir):
        makedirs(root_dir)

    # Attempt to open the file.
    file_dir = root_dir + str(file_name) + '.json'

    if not exists(file_dir):
        database = {}
        write_result(file_name, database, root_dir)
    
    else:
        with open(root_dir + str(file_name) + '.json',
                  "r", encoding = encoding) as jsonFile:

            database = json.load(jsonFile)
            jsonFile.close()

    return database

def write_result(file_name, database, target_dir, encoding = 'utf-8'):

    target_dir = standardize_path(target_dir)

    if not exists(target_dir):
        makedirs(target_dir)

    with open(str(target_dir) + str(file_name) + '.json', 
        'w', encoding = encoding) as output_file:
        try:
            json.dump(database, output_file)
        finally:
            output_file.close()

def get_files_list(file_dir):

    # Obtain the list of files (and files only) from a given directory.
    # File name and extension are separated.

    files_list = [splitext(file) for file in listdir(file_dir) 
                  if isfile(join(file_dir, file))]
    return files_list    

def get_dirs_list(file_dir):

    # Obtain the list of sub-directories (and sub-directories only) in a 
    # given directory.

    dirs_list = [splitext(file) for file in listdir(file_dir) 
                  if not isfile(join(file_dir, file))]
    return dirs_list   

def is_file_in_dir(file_dir, name, ext = None):

    # Determine if a file exists in a directory.

    files_list = get_files_list(file_dir)
    
    # Match name only.
    if ext == None:
        files_list = [(each[0], None) for each in files_list]

    else:
        # offset
        if ext[0] != ".":
            ext = "." + ext

    exist = (name, ext) in files_list
    return exist

def is_subdir_in_dir(directory, subdir):

    # Determine if a sub-directory exists in a directory.

    dirs_list = get_dirs_list(directory)
    exist = (subdir,'') in dirs_list
    return exist

