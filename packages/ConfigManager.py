import pickle

class ConfigManager:
    def __init__(self):
        self._path = "packages/config/config.pkl"
        self._config = self._load_config()

    def _load_config(self):
        try:
            with open(self._path, "rb") as f:
                config = pickle.load(f) or {}
            return config
        except EOFError or FileNotFoundError:
            return {"credentials": []}

    def get_config(self):
        return self._config

    def _save_config(self):
        with open(self._path, "rb") as f:
            pickle.dump(self._config, f)

    def add_credentials(self, credentials):
        if "credentials" in self._config.keys():
            self._config["credentials"].append(credentials)
        else:
            self._config["credentials"] = [credentials]
