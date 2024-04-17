from typing import List, Dict
from bs4 import BeautifulSoup
import pandas as pd
import requests
from asset_mapping_scrapping.scrapper.scrapper_base import Scrapper, ScrapperFactory
from asset_mapping_scrapping.utils.global_vars import HEADERS

from dataclasses import dataclass
import logging

logger = logging.getLogger("VerboseLogger")


@dataclass
@ScrapperFactory.register_handler()
class AryadutaHotelGroup(Scrapper):
    sector: str = "Real Estate"
    company_name: str = "Aryaduta Hotel Group"
    subtype: str = "hotel"
    type: str = "hospitality"

    def __post_init__(self):
        return super().__post_init__()
    
    def _get_list_assets(self, url: str) -> List[Dict[str, str]]:
        response = requests.get(url, headers=HEADERS)
        asset_list = response.json()
        data = []
        asset_list = asset_list['data']['items']
        for asset in asset_list:
            properties = asset["properties"][0]
            asset_name = properties["name"].strip()
            address = properties["address"].strip()
            city = properties['city']['name'].strip()
            data.append(
                {
                    "asset_name": asset_name,
                    "address": address,
                    "city": city,
                    "country": "",
                    "state": "",
                }
            )
        return data

    def get_data_from_main_page(self, url: str) -> pd.DataFrame:
        data = self._get_list_assets(url)
        df_final: pd.DataFrame = pd.DataFrame(data)

        return df_final.assign(
            sector=self.sector,
            company_name=self.company_name,
            subtype=self.subtype,
            type=self.type
        )
