import numpy as np
import requests
import pandas as pd
import datetime
import os
from typing import Union, Literal
import pandera as pa
from bs4 import BeautifulSoup
from dataclasses import dataclass
from abc import abstractmethod
from rich.progress import track
import yaml
from pandera.dtypes import DateTime
from asset_mapping_scrapping.scrapper.schema import sector_schema_mapping
from asset_mapping_scrapping.utils.export import export_source
import logging

logger = logging.getLogger("VerboseLogger")


@dataclass
class Scrapper:
    """Abstract class that serves as mother class of scrappers for the
    asset mapping project. The class offers a structure where it is easy to get data
    from a main portfolio page, and dive in specific asset pages. It enforces that the
    output dataframe has a specific format.
    """

    s3_bucket_name: str = "vuong"
    s3_base_path: str = "app_data/asset_mapping/input_data/"
    mode: Literal["dev", "prod"] = "prod"
    _schema: Union[pa.DataFrameSchema, dict] = None

    def __post_init__(self):
        self.source_name = self.__class__.__name__

    @property
    def schema(self) -> Union[pa.DataFrameSchema, dict]:
        """Pandera schema to assess whether the dataframe has a correct format, only allowed columns etc.

        Returns:
            pa.Schema: Pandera schema to assess whether the dataframe has a correct format,
            only allowed columns etc.
        """
        if self._schema is None:
            if isinstance(self.sector, list):
                self._schema: dict = {s: sector_schema_mapping[s] for s in self.sector}
            else:
                self._schema: pa.DataFrameSchema = sector_schema_mapping[self.sector]
        return self._schema

    def _generate_urls(self, **kwargs) -> list[str]:
        """This function is used to generate urls from a base url. It can be useful for websites requesting some API,
        and rendering a 'next page' parameter. This function needs to be called in get_data_from_main_page.

        Returns:
            list[str]: list of urls.
        """
        pass

    @abstractmethod
    def get_data_from_main_page(self, url: Union[str, list]) -> pd.DataFrame:
        """Function in charge of getting the main page of a portfolio. Such a page provides the broad
        picture of all assets mentionned on the site, but may not contain specific asset detail.

        Examples: https://www.clsholdings.com/our-portfolio#/mapview/all/

        Args:
            url (Union[str, list]): url(s) of the page(s)

        Returns:
            pd.DataFrame: dataframe containing the data.
        """
        pass

    @abstractmethod
    def get_data_from_asset_page(self, asset_url: str) -> pd.DataFrame:
        """Optional function in charge of scrapping an individual asset page.

        Args:
            asset_url (str): url of the asset page

        Returns:
            pd.DataFrame: dataframe containing specific asset data
        """
        pass

    def export_to_s3(self, df: pd.DataFrame) -> None:
        """Creates the s3 key and exports the dataframe to this key.

        Args:
            df (pd.DataFrame): dataframe resulting from the scrapping.
        """
        path: str = os.path.join(
            self.s3_base_path,
            self.source_name,
            f"{self.source_name}_{str(datetime.date.today())}.csv",
        )
        if self.mode == "dev":
            logging.info("Dev mode activated. No export to s3.")
            return
        export_source(df, self.source_name, self.s3_base_path, self.s3_bucket_name)
    def __call__(self, url: Union[str, list]):
        """Call of the class in charge of scrapping some main page. If column 'asset_url' is present in output of
        `self.get_data_from_main_page`, then `self.get_data_from_asset_page` is called on individual asset pages.
        Results are merged onto the dataframe resulting from the main page.

        Args:
            url (Union[str, list]): input url or urls (sometimes websites have portfolios on different pages, but pages have
            same structure).
        """
        try:
            if isinstance(url, list) and len(url) == 1:
                url = url[0]
            base_df: pd.DataFrame = self.get_data_from_main_page(url)
        except Exception as e:
            logger.error(f"Error on getting data from main page on {url}.")
            logger.exception(e)
        if "asset_url" in base_df.columns:
            asset_df: pd.DataFrame = pd.DataFrame()
            for i, row in track(base_df.iterrows(), total=len(base_df)):
                try:
                    asset_df = pd.concat(
                        [
                            asset_df,
                            self.get_data_from_asset_page(row["asset_url"]).assign(
                                asset_url=row["asset_url"]
                            ),
                        ]
                    )
                except:
                    logger.error(
                        f"Error on getting data from asset page on {row['asset_url']}."
                    )

            base_df = base_df.merge(asset_df, on="asset_url", how="left").drop(
                columns="asset_url"
            )
        if isinstance(self.schema, pa.DataFrameSchema):
            self.schema.validate(base_df)
        else:
            for sector, schema in self.schema.items():
                schema.validate(
                    base_df.query("sector == @sector").filter(
                        list(schema.columns.keys())
                    )
                )
        print(base_df.to_string())
        # self.export_to_s3(base_df)


class ScrapperFactory:
    scrapper_mapping = {}

    @classmethod
    def register_handler(cls):
        """Method to record a class as a new scrapper that inherits from
        the Scrapper class.
        """

        def wrapper(handler_cls):
            cls.scrapper_mapping[handler_cls.__name__] = handler_cls
            return handler_cls

        return wrapper

    @classmethod
    def get_handler(cls, output_type):
        return cls.scrapper_mapping[output_type]
