from configparser import ConfigParser
from decouple import config


def get_database_config(filename=config('file_db'), section='postgresql'):
    config_ = ConfigParser()  
    config_.read(filename)  

    db = {}
    if config_.has_section(section):
        params = config_.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))
    return db
