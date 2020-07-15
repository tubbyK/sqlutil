from configparser import ConfigParser, RawConfigParser

def read_config(filename:str, section:str):
    parser = ConfigParser()
    parser.read(filename)
    config = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            config[param[0]] = param[1]
    else:
        raise Exception(f'Section {section} not found in the {filename} file')
    return config

def add_section(filename:str, section:str, data:{}):
    config = RawConfigParser()
    config.add_section(section)
    for key, value in data.items():
        config.set(section, key, value)
    with open(filename, 'a+') as cfgfile:
        config.write(cfgfile)