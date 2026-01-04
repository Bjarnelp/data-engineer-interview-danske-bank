import polars as pl
import os
import pathlib


class DataIO:
    def __init__(self, path: str) -> None:
        self.path = path

    def load_data(self) -> pl.DataFrame:
        if not self.path:
            raise ValueError("Data path must be provided.")
        if not isinstance(self.path, str):
            raise TypeError("Data path must be a string.")

        self.path = pathlib.Path(self.path).as_posix()

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

        if not file_name:
            raise ValueError("File name must be provided.")
        if not isinstance(file_name, str):
            raise TypeError("File name must be a string.")
        if not file_name.endswith(".parquet"):
            raise ValueError("File name must end with .parquet extension.")

        output_path = pathlib.Path(output_path).as_posix()

        pathlib.Path(output_path).mkdir(parents=True, exist_ok=True)

        full_output_path = f"{output_path}/{file_name}"
        if os.path.isfile(full_output_path):
            raise FileExistsError(f"File already exists at '{full_output_path}'.")

        try:
            df.write_parquet(full_output_path)
        except Exception as exc:
            raise RuntimeError(
                f"Failed to write DataFrame to parquet file at '{full_output_path}': {exc}"
            ) from exc


if __name__ == "__main__":
    data_io = DataIO("./data/yellow_tripdata_2025-06.parquet")
    tripdata = data_io.load_data()

    tripdata.head(5).show()

    output_dir = "./output/source/"
    file_name = "tripdata_sample.parquet"
    data_io.save_data(tripdata.head(1000), output_dir, file_name)
