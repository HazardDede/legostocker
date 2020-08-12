"""Module to retrieve official LEGO set identifications from APIs / Databases."""
import asyncio
from typing import Optional

import pandas as pd
from pydantic import BaseModel  # pylint: disable=no-name-in-module
from typeguard import typechecked

LegoSetID = int


class LegoSet(BaseModel):
    """Represents an official LEGO set with properties like ID, name and year."""
    set_id: LegoSetID
    set_name: str
    release_year: Optional[int] = None
    part_count: Optional[int] = None


class LegoSetStore:
    """Interface definition for a LEGO set store."""

    async def by_id(self, set_id: LegoSetID) -> Optional[LegoSet]:
        """
        Searches the store for the passed LEGO id. If the set is not found None is returned.

        Args:
          set_id (LegoSetID): The id of the LEGO set to search for.

        Returns:
          A `LegoSet` entity if the set was found; otherwise None (absence).
        """
        raise NotImplementedError()


class RebrickableStore(LegoSetStore):
    """Concrete implementation of a LEGO set store using the rebrickable set database."""
    # DOWNLOAD_URI = "https://cdn.rebrickable.com/media/downloads/sets.csv.gz?1597223281.4982338"

    DATASET_COL_ID = "set_num"
    DATASET_COL_NAME = "name"
    DATASET_COL_REL_YEAR = "year"
    DATASET_COL_PART_COUNT = "num_parts"

    @typechecked
    def __init__(self, sets_frame: pd.DataFrame):
        """
        Initializer. Don't call this directly. Use one of the factory methods.

        Args:
          sets_csv (str): Path to the csv file that contains all released LEGO sets (downloaded from rebrickable.com)
        """
        self.sets_frame = sets_frame

    @classmethod
    async def from_csv(cls, csv_file: str) -> 'RebrickableStore':
        """
        Factory method: Creates a new instance of the `RebrickableStore` by
        fetching the data from a csv soure already downloaded from rebrickable.com.

        Args:
          csv_file (str): The path to the csv file.

        Returns:
          An instance of a `RebrickableStore`.
        """
        def _sync():
            return pd.read_csv(csv_file)

        loop = asyncio.get_event_loop()
        sets_frame = await loop.run_in_executor(None, _sync)
        return cls(sets_frame)

    async def _pdrow_to_set(self, row: pd.Series) -> LegoSet:
        assert isinstance(row, pd.Series)
        assert isinstance(row[self.DATASET_COL_ID], str)

        return LegoSet(
            set_id=int(row[self.DATASET_COL_ID].split('-')[0]),
            set_name=str(row[self.DATASET_COL_NAME]),
            release_year=int(row[self.DATASET_COL_REL_YEAR]),
            part_count=int(row[self.DATASET_COL_PART_COUNT])
        )

    async def by_id(self, set_id: LegoSetID) -> Optional[LegoSet]:
        set_id = LegoSetID(set_id)
        # We need to use startswith because id's have a -1|-2 suffix
        searched_set = self.sets_frame[self.sets_frame[self.DATASET_COL_ID].str.startswith(str(set_id))]
        if len(searched_set) <= 0:
            return None
        if len(searched_set) > 0:
            searched_set = searched_set.iloc[0]
        return await self._pdrow_to_set(searched_set)
