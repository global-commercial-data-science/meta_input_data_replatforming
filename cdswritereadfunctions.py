import pickle
import json



# Define function to read a list from a file or return an empty string
def list_from_file(filepath):
    """
    Takes a filepath as an input
    :param filepath: Filepath for file containing list
    :return: If file exists returns list from file, otherwise and empty list
    """
    try:
        with open(filepath, 'rb') as f:
            return pickle.load(f)
    except FileNotFoundError:
        return []


# ----------------------------------------------------------------------------------------------------------------------
# Define function to pickle output to file
def write_to_temp(filepath, output):
    """
    Writes output to filepath in pickle format
    :param filepath: File path for temp file - string
    :param output: Data to write to file
    """
    with open(filepath, 'wb') as f:
        pickle.dump(output, f)


# ----------------------------------------------------------------------------------------------------------------------
def load_input_file(input_file_path):
    """
    Function to load inputs from a json formatted input file
    :param input_file_path: File path the json input file - string
    :return: inputs as a JSON object - JSON object
    """
    with open(input_file_path, 'r') as f:
        return json.load(f)


# ----------------------------------------------------------------------------------------------------------------------
def write_json_to_file(file_path, json_object):
    """
    Write out a json object to the given filepath as a json file
    :param file_path: File path to write file - string
    :param json_object: JSON object to write - JSON object
    :return: None
    """
    with open(file_path, 'w') as f:
        json.dump(json_object, f, indent=2)

