import traceback
from datetime import date
import yaml


def get_today_formatted_date() -> str:
    return date.today().strftime("%Y-%m-%d")


def get_traceback(o_exception: Exception) -> str:
    s_lines = traceback.format_exception(
        type(o_exception), o_exception, o_exception.__traceback__
    )
    return "".join(s_lines)


def dict_parse(input_dict: dict, factory_dict: dict) -> dict:
    """Parse a dictionary containing elements in yaml file.

    Args:
        dictionnary (dict): _description_
        factory_dict (dict): _description_

    Returns:
        dict: _description_
    """

    def get(element, factory_dict):
        if isinstance(element, dict):
            return {
                key: get(value, factory_dict=factory_dict)
                for key, value in element.items()
            }
        if isinstance(element, list):
            return [get(item, factory_dict) for item in element]
        if isinstance(element, (str, int, float, bool)):
            return factory_dict.get(element, element)

    return {
        key: get(value, factory_dict=factory_dict) for key, value in input_dict.items()
    }


def read_yaml_file(path_config: str) -> dict:
    """Parse a yaml file and return a dict object.

    Args:
        path_config (str): path to yaml file.

    Returns:
        dict: dictionary containing elements in yaml file.
    """
    with open(path_config) as conf:
        return yaml.load(conf, yaml.FullLoader)


def parse_config(config_path: str, factory_dict: dict = {}) -> dict:
    config: dict = read_yaml_file(config_path)
    return dict_parse(config, factory_dict=factory_dict)
