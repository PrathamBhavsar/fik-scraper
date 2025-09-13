class Config:
    def __init__(self, config_file='config.json'):
        import json
        
        with open(config_file, 'r') as file:
            self.config = json.load(file)

    def get_base_url(self):
        return self.config.get('base_url')

    def get_xpaths(self):
        return self.config.get('xpaths', [])