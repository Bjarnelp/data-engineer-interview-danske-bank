import polars as pl
import os
import pathlib


class DataIO:
    def __init__(self, input_path: str, output_path: str):
        self.input_path = input_path
        self.output_path = output_path

    def load_data(self, file_name: str) -> pl.DataFrame:
        if not self.input_path:
            raise ValueError("Data path must be provided.")
        if not isinstance(self.input_path, str):
            raise TypeError("Data path must be a string.")
        if not os.path.exists(self.input_path):
            raise FileNotFoundError(f"Data path '{self.input_path}' does not exist.")

        self.input_path = pathlib.Path(self.input_path).as_posix()

        full_input_path = f"{self.input_path}/{file_name}"

        # Get data type from file extension
        if full_input_path.endswith(".parquet"):
            return pl.read_parquet(full_input_path)
        elif full_input_path.endswith(".csv"):
            return pl.read_csv(full_input_path)
        else:
            raise ValueError(
                "Unsupported file format. Please use .parquet or .csv files."
            )

    def save_data(self, df: pl.DataFrame, file_name: str) -> None:
        if not self.output_path:
            raise ValueError("Output path must be provided.")
        if not isinstance(self.output_path, str):
            raise TypeError("Output path must be a string.")

        if not file_name:
            raise ValueError("File name must be provided.")
        if not isinstance(file_name, str):
            raise TypeError("File name must be a string.")
        if not file_name.endswith(".parquet"):
            raise ValueError("File name must end with .parquet extension.")

        output_path = pathlib.Path(self.output_path).as_posix()

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
    data_io = DataIO(input_path="./data/", output_path="./output/raw/")
    tripdata = data_io.load_data(file_name="yellow_tripdata_2025-06.parquet")

    tripdata.head(5).show()

    data_io.save_data(tripdata.head(1000), file_name="tripdata_sample.parquet")
