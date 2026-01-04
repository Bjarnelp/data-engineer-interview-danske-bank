import polars as pl
import os


class DataLoader:
    def __init__(self, path: str) -> None:
        self.path = path

    def load_data(self) -> pl.DataFrame:
        if not self.path:
            raise ValueError("Data path must be provided.")
        if not isinstance(self.path, str):
            raise TypeError("Data path must be a string.")

        # Get data type from file extension
        if self.path.endswith(".parquet"):
            return pl.read_parquet(self.path)
        elif self.path.endswith(".csv"):
            return pl.read_csv(self.path)
        else:
            raise ValueError(
                "Unsupported file format. Please use .parquet or .csv files."
            )

    def save_data(self, df: pl.DataFrame, output_path: str, file_name: str) -> None:
        if not output_path:
            raise ValueError("Output path must be provided.")
        if not isinstance(output_path, str):
            raise TypeError("Output path must be a string.")

        # Extract path from output path if it includes a file name
        if os.path.basename(output_path) != "":
            output_path = os.path.dirname(output_path)

        if not os.path.exists(output_path):
            os.makedirs(output_path)

        if not file_name:
            raise ValueError("File name must be provided.")
        if not isinstance(file_name, str):
            raise TypeError("File name must be a string.")
        if not file_name.endswith(".parquet"):
            raise ValueError("File name must end with .parquet extension.")

        try:
            df.write_parquet(f"{output_path}/{file_name}")
        except Exception as exc:
            raise RuntimeError(
                f"Failed to write DataFrame to parquet file at '{output_path}': {exc}"
            ) from exc


if __name__ == "__main__":
    data_loader = DataLoader("./data/yellow_tripdata_2025-06.parquet")
    tripdata = data_loader.load_data()

    tripdata.head(5).show()

    output_dir = "./output/source/"
    file_name = "tripdata_sample.parquet"
    data_loader.save_data(tripdata.head(1000), output_dir, file_name)
