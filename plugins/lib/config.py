import yaml
import os
def read_credential(conf_file):
    # return '', ''
    with open(conf_file, 'r') as stream:
        try:
            return yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc)