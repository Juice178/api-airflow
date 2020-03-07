import yaml
import os
def get_info(conf_file, **context):
    print('---------------------')
    print(os.listdir())
    with open(conf_file, 'r') as stream:
        try:
            print(yaml.safe_load(stream))
        except yaml.YAMLError as exc:
            print(exc)