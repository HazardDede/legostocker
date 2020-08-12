"""Main entry point."""

import asyncio
from sets import LegoSetID, RebrickableStore

import fire


class Store:
    """Lego Set Store entrypoint."""
    def __init__(self, csv_path: str = "~/Downloads/sets.csv"):
        self.csv_path = csv_path
        self._loop = asyncio.get_event_loop()
        self._store = self._loop.run_until_complete(RebrickableStore.from_csv(csv_path))

    def by_id(self, set_id: LegoSetID):
        """Retrieve a lego set by its id."""
        lego_set = self._loop.run_until_complete(self._store.by_id(set_id))
        if lego_set is None:
            print(f"No lego set {set_id} was found in the database")
            return
        print(f"Found: {lego_set}")


if __name__ == '__main__':
    fire.Fire(Store)
