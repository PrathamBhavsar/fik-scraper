class Scraper:
    def __init__(self, config):
        self.config = config
        self.xpaths = config.get("xpaths", [])
        self.crawler = None  # Placeholder for the crawl4AI crawler instance

    def initialize_crawler(self):
        from crawl4AI import Crawler
        self.crawler = Crawler()

    def add_xpath(self, xpath):
        if xpath not in self.xpaths:
            self.xpaths.append(xpath)

    def remove_xpath(self, xpath):
        if xpath in self.xpaths:
            self.xpaths.remove(xpath)

    def scrape(self):
        if not self.crawler:
            self.initialize_crawler()

        self.crawler.open(self.config["base_url"])
        self.crawler.wait(5)  # Wait for elements to load

        scraped_data = {}
        for xpath in self.xpaths:
            data = self.crawler.scrape(xpath)
            scraped_data[xpath] = data

        return scraped_data

    def print_scraped_data(self, data):
        for xpath, value in data.items():
            print(f"XPath: {xpath}, Data: {value}")