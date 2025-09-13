from config import Config
from scraper import Scraper

def main():
    # Load configuration
    config = Config('config.json')
    base_url = config.get_base_url()
    xpaths = config.get_xpaths()

    # Initialize the scraper
    scraper = Scraper(base_url, xpaths)

    # Start the scraping process
    scraper.scrape()

if __name__ == "__main__":
    main()