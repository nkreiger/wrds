############## utility functions for basic assistance ################

def read_line(fp):
    """
    Returns string read line by line

    :param fp: file
    :return:
    """

    return fp.readlines();

def read_from(path, list=True):
    """
    Function to read a .txt file into a string

    :param path: string
    :return content: string
    """

    with open(path) as fp:
        content = read_line(fp)

    if (list):
        return content

    return ''.join(content)

