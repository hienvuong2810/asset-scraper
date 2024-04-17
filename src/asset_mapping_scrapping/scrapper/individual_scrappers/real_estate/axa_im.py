from typing import List, Dict
import pandas as pd
import requests
from asset_mapping_scrapping.scrapper.scrapper_base import Scrapper, ScrapperFactory
from dataclasses import dataclass, field
import logging

logger = logging.getLogger("VerboseLogger")

@dataclass
@ScrapperFactory.register_handler()
class AXAIM(Scrapper):
    sector: str = "Real Estate"
    company_name: str = "AXA IM Alts"
    paths = ["/production/residential-map", "/production/axa-core", "/production/logistics"]

    def __post_init__(self):
        return super().__post_init__()
    
    def _get_payload(self, path) -> Dict[str, str]:
        object_data = {
          "operationName": "Map",
          "query": "query Map($url: String) {\n  map(url: $url) {\n    header\n    description\n    properties: children {\n      items {\n        ... on Asset {\n          id\n          name\n          url\n          filters {\n            name\n            __typename\n          }\n          assetType\n          acquisitionDate\n          addressLine1\n          bannerText\n          caseStudyUrl\n          city {\n            ... on City {\n              country\n              name\n              __typename\n            }\n            __typename\n          }\n          description\n          energyRating\n          images {\n            url\n            media {\n              name\n              __typename\n            }\n            __typename\n          }\n          isInDevelopment\n          latitude\n          longitude\n          zipCode\n          squareMeters\n          type {\n            ... on AXACore {\n              surfaceArea\n              __typename\n            }\n            ... on Logistics {\n              clearHeight\n              floorLoadingCapacity\n              floorLoadingCapacitySQM\n              grossLettableArea\n              numberOfDockDoors\n              __typename\n            }\n            ... on ResidentialMap {\n              assetManager\n              numberOfUnits\n              __typename\n            }\n            ... on AAI {\n              investmentDate\n              valueOfInvestment\n              installedCapacity\n              numberOfUsers\n              fleet\n              networkSize\n              __typename\n            }\n            ... on ASI {\n              investmentDate\n              valueOfInvestment\n              numberOfBeds\n              squareMetres\n              __typename\n            }\n            ... on ASIS {\n              investmentDate\n              valueOfInvestment\n              numberOfBeds\n              squareMetres\n              __typename\n            }\n            __typename\n          }\n          __typename\n        }\n        __typename\n      }\n      __typename\n    }\n    __typename\n  }\n  locations: allCity {\n    items {\n      name\n      country\n      latitude\n      longitude\n      id\n      __typename\n    }\n    __typename\n  }\n  tags: allFilter {\n    items {\n      name\n      id\n      __typename\n    }\n    __typename\n  }\n}",
          "variables": {
            "url": path
          }
        } 

        return object_data

    def _get_list_assets(self, url) -> List[Dict[str, str]]:
        data = []

        for path in self.paths:
            object_data = self._get_payload(path)
            response = requests.post(url, json = object_data , headers={"Umb-Project-Alias": "axa-interactive-map"  })
            asset_list = response.json()
            items = asset_list["data"]["map"]["properties"]["items"]
            for item in items:
                id = item["id"].strip()
                asset_name = item["name"].strip()
                area = item["squareMeters"]
                unit = "sqm"
                address = item["addressLine1"].strip()
                city = item["city"]["name"].strip()
                country = item["city"]["country"].strip()
                latitude = item["latitude"]
                longitude = item["longitude"]
                subtype = item["assetType"].strip()
                if area == 0.0:
                    area = None
                data.append(
                {
                    "id": id,
                    "asset_name": asset_name,
                    "area": area,
                    "unit": unit,
                    "address": address,
                    "city": city,
                    "country": country,
                    "latitude": latitude,
                    "longitude": longitude,
                    "subtype": subtype,
                }
            )
        return data

    def get_data_from_main_page(self, url: str) -> pd.DataFrame:
        data = self._get_list_assets(url)
        df_final: pd.DataFrame = pd.DataFrame(data)
        
        return df_final.assign(
            sector=self.sector,
            company_name=self.company_name
        )
