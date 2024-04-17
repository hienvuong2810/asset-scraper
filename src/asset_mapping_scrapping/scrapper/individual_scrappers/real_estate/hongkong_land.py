from typing import Tuple, List, Dict
from bs4 import BeautifulSoup
import pandas as pd
import requests
from asset_mapping_scrapping.scrapper.scrapper_base import Scrapper, ScrapperFactory
from dataclasses import dataclass, field
import logging

logger = logging.getLogger("VerboseLogger")


@dataclass
@ScrapperFactory.register_handler()
class HongkongLand(Scrapper):
    sector: str = "Real Estate"
    company_name: str = "Hongkong Land"

    def __post_init__(self):
        return super().__post_init__()


    def _convert_to_subtype(self, arr: List) -> str:
        result = [item.split("___")[1].replace("_", " ").title() for item in arr]
        return ','.join(result)
    def _get_list_assets(self, url: str) -> pd.DataFrame:
        response = requests.get(url)
        response = response.json()["items"]
        result = []
        for item in response:
            item = item["elements"]
            subtype = self._convert_to_subtype(item["property_categories"]["value"])
            result.append({
                "asset_name": item["name"]["value"],
                "subtype": subtype,
                "latitude": item["google_latitude"]["value"],
                "longitude": item["google_longitude"]["value"],
                "status": "Operating" if "Investment Properties" in subtype else "Under development"
            })
        return pd.DataFrame(result)
    def get_data_from_main_page(self, url: str) -> pd.DataFrame:
        asset_list = self._get_list_assets(url)
        df_final: pd.DataFrame = pd.DataFrame(asset_list)
        return df_final.assign(
            sector=self.sector,
            company_name=self.company_name
        )
