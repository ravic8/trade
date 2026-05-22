from pathlib import Path

import pandas as pd


class ParquetStore:
    def __init__(self, root: Path | str = "data") -> None:
        self.root = Path(root)

    def write_frame(self, name: str, df: pd.DataFrame) -> Path:
        path = self.root / f"{name}.parquet"
        path.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(path, index=False)
        return path

    def read_frame(self, name: str) -> pd.DataFrame:
        return pd.read_parquet(self.root / f"{name}.parquet")
