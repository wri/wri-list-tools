import pandas as pd
import json
import os
from collections.abc import Mapping
from pandas import Series
from typing import Union

from .list_utils import read_job_output


def expand_dic_col(col: Series, key_names: Union[str, list[str]], value_name: str):
    """Expand a pandas column that contains a dictionary into a de-normalized table"""
    key_names = key_names if isinstance(key_names, list) else [key_names]
    head, *tail = key_names

    df = col.apply(pd.Series)
    df = pd.melt(df, ignore_index=False, var_name=head, value_name=value_name)
    df = df.set_index([head], append=True)

    if len(tail) == 0:
        return df
    else:
        col = df[value_name].dropna()  # NA values propogate as 0 values, undersirable
        return expand_dic_col(col, tail, value_name)


class ExpandedDataFrame(Mapping):
    index_cols = []
    """List of columns used as index on read"""

    nested_cols = {}
    """Mapping from column name containing nested values to list of key names"""

    def __init__(self, df, index_cols=None):
        if (type(df) == pd.DataFrame):
            self.df = df
        elif (type(df) == str):
            df = read_job_output(df)
        else:
            raise ValueError(f"Can't handle {df}")

        # read JSON cols from str to dict
        json_cols = list(self.nested_cols)
        df[json_cols] = df[json_cols].applymap(
            lambda v: json.loads(v) if type(v) == str else v
        )
        # reset the DataFrame index to what we expect
        index_cols = index_cols or self.index_cols
        df = df.set_index(index_cols)

        # cache for previously resolved columns
        self._resolved_col_df = {}
        self.df = df
        self.attributes_df = df.drop(self.nested_cols, axis=1).sort_index()

    def __eq__(self, other):
        return (
            self.__class__ == other.__class__ and
            self.attributes_df.equals(other.attributes_df) and
            all([self[col].equals(other[col]) for col in self.nested_cols])
        )

    def __len__(self):
        return len(self.df.columns)

    def __iter__(self):
        return self.df.columns.__iter__()

    def __getitem__(self, name):
        if name in self._resolved_col_df:
            return self._resolved_col_df[name]
        elif name in self.nested_cols:
            flat_df = expand_dic_col(self.df[name], self.nested_cols[name], name)
            self._resolved_col_df[name] = flat_df
            return flat_df
        elif name in self.df:
            return self.df[name]
        else:
            raise KeyError(name)

    def to_csv_dir(self, dir_name: str):
        """Save all expanded dataframes as CSV files"""
        if not os.path.exists(dir_name):
            os.makedirs(dir_name)
        nested_cols = list(self.nested_cols)
        self.attributes_df.to_csv(os.path.join(dir_name, "attributes.csv"))

        for name in nested_cols:
            path = os.path.join(dir_name, name + ".csv")
            print(path)
            df = self[name].dropna().sort_index()
            df.to_csv(path)
        print("DONE!")

    def to_gpkg(self, path: str, input_df):
        def attribute_name(col):
            if type(col) == tuple:
                return ":".join(col)
            else:
                return col

        def feature_id(index):
            pairs = [f"{k}={v}" for k, v in zip(self.index_cols, index)]
            return ":".join(pairs)

        def join_for_gpkg(df):
            join_df = input_df.join(df)
            join_df.index = join_df.index.to_flat_index()
            join_df = join_df.rename(index=feature_id, columns=attribute_name)
            return join_df

        for col in list(self.nested_cols):
            print(col)
            col_keys = list(reversed(self.nested_cols[col]))
            df = self[col].reset_index().pivot(index=self.index_cols, columns=col_keys, values=col)
            if (len(df) > 0):
                # can't write empty dataframe
                join_df = join_for_gpkg(df)
                join_df.to_file(path, driver="GPKG", layer=col, index=True)

        join_df = join_for_gpkg(self.attributes_df)
        join_df.to_file(path, driver="GPKG", layer="attributes", index=True)

    @classmethod
    def read_csv(cls, path):
        if path[:5] == "s3://":
            df = pd.read_csv(f"{path}/final/*.csv", sep="\t", index_col=cls.index_col)
        else:
            df = pd.read_csv(path, sep="\t", index_col=cls.index_col)
        json_cols = list(cls.nested_cols)
        df[json_cols] = df[json_cols].applymap(
            lambda v: json.loads(v) if type(v) == str else v
        )
        return cls(df)


class ForestChangeDiagnostic(ExpandedDataFrame):
    """ForestChangeDiagnostic job output from https://github.com/wri/gfw_forest_loss_geotrellis"""

    index_cols = ["list_id", "location_id"]
    nested_cols = {
        "tree_cover_loss_total_yearly": ["year"],
        "tree_cover_loss_primary_forest_yearly": ["year"],
        "tree_cover_loss_peat_yearly": ["year"],
        "tree_cover_loss_intact_forest_yearly": ["year"],
        "tree_cover_loss_protected_areas_yearly": ["year"],
        "tree_cover_loss_sea_landcover_yearly": ["landcover", "year"],
        "tree_cover_loss_idn_landcover_yearly": ["landcover", "year"],
        "tree_cover_loss_soy_yearly": ["year"],
        "tree_cover_loss_idn_legal_yearly": ["legal", "year"],
        "tree_cover_loss_idn_forest_moratorium_yearly": ["year"],
        "tree_cover_loss_prodes_yearly": ["year"],
        "tree_cover_loss_prodes_wdpa_yearly": ["year"],
        "tree_cover_loss_prodes_primary_forest_yearly": ["year"],
        "tree_cover_loss_brazil_biomes_yearly": ["year"],
        "sea_landcover_area": ["landcover"],
        "idn_landcover_area": ["landcover"],
        "commodity_value_forest_extent": ["year"],
        "commodity_value_peat": ["year"],
        "commodity_value_protected_areas": ["year"],
        "commodity_threat_deforestation": ["year"],
        "commodity_threat_peat": ["year"],
        "commodity_threat_protected_areas": ["year"],
        "commodity_threat_fires": ["year"],
    }


class Dashboard(ExpandedDataFrame):
    """Dashboard job output from https://github.com/wri/gfw_forest_loss_geotrellis"""

    index_cols = ["location_id", "gadm_id"]
    nested_cols = {
        "glad_alerts_daily": ["date"],
        "glad_alerts_weekly": ["week"],
        "glad_alerts_monthly": ["month"],
    }
