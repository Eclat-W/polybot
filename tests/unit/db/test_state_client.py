import asyncio

from polybot.db.state_client import StateClient


class FakeStore:
    def __init__(self) -> None:
        self.connected = False
        self.saved = None

    async def connect(self) -> None:
        self.connected = True

    async def close(self) -> None:
        self.connected = False

    async def get_strategy_config(self, name: str):
        return {"name": name, "enabled": True, "shadow": False}

    async def save_strategy_config(self, name: str, enabled: bool, config: dict, shadow: bool = False) -> None:
        self.saved = {"name": name, "enabled": enabled, "config": config, "shadow": shadow}


class FakeRequester:
    def __init__(self, address: str) -> None:
        self.address = address

    async def open(self) -> None:
        raise ConnectionRefusedError("state service unavailable")

    async def close(self) -> None:
        return None

    async def request(self, message: dict):
        raise AssertionError("NNG request should not be used when falling back to SQLite")


def test_state_client_falls_back_to_sqlite_when_nng_is_unavailable(monkeypatch):
    fake_store = FakeStore()

    monkeypatch.setattr("polybot.db.state_client.NNGRequester", FakeRequester)
    monkeypatch.setattr("polybot.db.state_client.SQLiteStore", lambda: fake_store)

    async def run_test() -> None:
        client = StateClient()
        await client.connect()

        config = await client.get_strategy_config("arbitrage")
        await client.save_strategy_config("arbitrage", True, {"mode": "paper"}, shadow=True)

        assert config == {"name": "arbitrage", "enabled": True, "shadow": False}
        assert fake_store.saved == {
            "name": "arbitrage",
            "enabled": True,
            "config": {"mode": "paper"},
            "shadow": True,
        }
        assert client._use_local_store is True

        await client.close()

    asyncio.run(run_test())
