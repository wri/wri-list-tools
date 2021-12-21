import glob
import os

import pandas as pd
from geopandas import GeoDataFrame
from shapely import wkb


def read_list_tsv(path: str) -> GeoDataFrame:
    """Read input TSV file into GeoDataFrame
    Reads the WKB from the 'geom' column into 'geometry' column.
    """
    df = pd.read_csv(path, sep="\t", index_col=["list_id", "location_id"])
    gdf = GeoDataFrame(df, geometry=df["geom"].apply(wkb.loads, hex=True), crs=4326)
    return gdf.drop("geom", axis=1)


def write_list_tsv(gdf: GeoDataFrame, path: str):
    """Write GeoDataFrame to TSV file
    Write 'geometry' column as 'geom' using WKB"""
    df = gdf.copy()
    df["geom"] = df["geometry"].apply(wkb.dumps, hex=True)
    df = df.drop("geometry", axis=1)
    df.to_csv(path, sep="\t")


def read_job_output(path: str) -> GeoDataFrame:
    """Read job output produced by 'wri/gfw_forest_loss_geotrellis' into GeoDataFrame

    Parameters
    ----------
    path: str
        Directory of the job output or CSV file path.
        Path can be local or S3 URI.
    """
    if path[:5] == "s3://":
        df = pd.read_csv(f"{path}/final/*.csv", sep="\t")
    elif os.path.isdir(path):
        s = os.path.join(path, "final", "*.csv")
        files = glob.glob(s)
        df = pd.DataFrame()
        for f in files:
            fdf = pd.read_csv(f, sep="\t")
            df = df.append(fdf)
    else:
        df = pd.read_csv(path, sep="\t")
    return df
