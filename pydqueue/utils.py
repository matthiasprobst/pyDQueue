"""Utility module"""


class BColors:
    """Color class to color text"""
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'


def make_italic(string):
    """make string italic"""
    return f'\x1B[3m{string}\x1B[0m'


def make_bold(string):
    """make string bold"""
    return f"{BColors.BOLD}{string}{BColors.ENDC}"


def warningtext(string):
    """make string orange"""
    return f"{BColors.WARNING}{string}{BColors.ENDC}"


def failtext(string):
    """make string red"""
    return f"{BColors.FAIL}{string}{BColors.ENDC}"


def oktext(string):
    """make string green"""
    return f"{BColors.OKGREEN}{string}{BColors.ENDC}"
