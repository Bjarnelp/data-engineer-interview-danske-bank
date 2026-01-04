import polars as pl


class DataLoader:
    def __init__(self, path: str) -> None:
        self.path = path

    def load_data(self) -> pl.DataFrame:
        assert self.path, "Data path must be provided."
        assert isinstance(self.path, str), "Data path must be a string."

        # Get data type from file extension
        if self.path.endswith(".parquet"):
            return pl.read_parquet(self.path)
        elif self.path.endswith(".csv"):
            return pl.read_csv(self.path)
        else:
            raise ValueError(
                "Unsupported file format. Please use .parquet or .csv files."
            )

    def write_data_as_parquet(self, df: pl.DataFrame, output_path: str) -> None:
        df.write_parquet(output_path)


if __name__ == "__main__":
    data_loader = DataLoader("./data/yellow_tripdata_2025-06.parquet")
    tripdata = data_loader.load_data()

    tripdata.head(5).show()
