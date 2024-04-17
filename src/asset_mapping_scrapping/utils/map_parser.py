from bs4 import BeautifulSoup
import pandas as pd
import requests
import re
import ast
from asset_mapping_scrapping.utils.global_vars import HEADERS


def parse_google_map(s_url):
    print(f"Parsing `{s_url}`")
    o_response = requests.get(s_url, headers=HEADERS)
    o_soup = BeautifulSoup(o_response.content, features="html.parser")
    s_script = o_soup.find("script").text
    l_content = ast.literal_eval(
        re.findall("initEmbed\((.*)\);", s_script)[0].replace("null", "None")
    )
    if (l_content[21][3] is not None):
        s_address_1 = l_content[21][3][13]
        s_address_2 = l_content[21][3][0][1]
        s_address = (
            s_address_1
            if s_address_1 is not None and len(s_address_1) > len(s_address_2)
            else s_address_2
        )
        f_latitude = l_content[21][3][0][2][0]
        f_longitude = l_content[21][3][0][2][1]
        return s_address, f_latitude, f_longitude
    else:
        s_address = l_content[21][5][0]

        f_latitude = l_content[21][0][0][2]
        f_longitude = l_content[21][0][0][1]
        return s_address, f_latitude, f_longitude


def decode_ggmaps_url(ggmaps_url: str) -> dict:
    """Extracts the latitude and longitude data from a url pointing to google maps, in its contracted
    format. Ex: http://goo.gl/maps/cfD5s

    Args:
        ggmaps_url (str): google maps url in contracted format.

    Returns:
        dict: dictionary with keys latitude and longitude
    """
    r = requests.get(ggmaps_url, headers=HEADERS)
    if r.status_code == 200:
        redirection_url = r.url
        match = re.search(
            r"[-+]?([1-8]?\d(\.\d+)?|90(\.0+)?),\s*[-+]?(180(\.0+)?|((1[0-7]\d)|([1-9]?\d))(\.\d+)?)",
            redirection_url,
        )
        if match:
            latitude, longitude = match.group().split(",")
            latitude = float(latitude)
            longitude = float(longitude)
        else:
            latitude, longitude = None, None
    else:
        latitude, longitude = None, None
    return {"latitude": latitude, "longitude": longitude}
