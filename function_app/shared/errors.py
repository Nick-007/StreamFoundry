# function_app/shared/errors.py

class CmdError(Exception):
    pass

class ConfigError(Exception): 
    pass

class BadMessageError(Exception):
    pass