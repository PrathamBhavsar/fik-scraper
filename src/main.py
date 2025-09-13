import asyncio
from config import Config
from scraper import Scraper

async def main():
    # Load configuration
    config = Config('config.json')
    base_url = config.get_base_url()
    xpaths = config.get_xpaths()
    
    # Initialize the scraper
    scraper = Scraper(base_url, xpaths)
    
    # Start the scraping process (now async)
    result = await scraper.scrape()
    
    if result:
        print("Scraping completed successfully!")
    else:
        print("Scraping failed!")

if __name__ == "__main__":
    asyncio.run(main())