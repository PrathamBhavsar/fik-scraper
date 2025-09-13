# FikFap API Scraper

## Overview
The FikFap API Scraper is a Python-based web scraping tool designed to extract video content from the FikFap API. It utilizes the `crawl4AI` package for efficient scraping and supports configurable XPaths for flexibility in data extraction.

## Project Structure
```
fik-scraper
├── src
│   ├── __init__.py
│   ├── main.py
│   ├── config.py
│   ├── scraper.py
│   └── utils.py
├── config.json
├── requirements.txt
└── README.md
```

## Installation

1. Clone the repository:
   ```
   git clone <repository-url>
   cd fik-scraper
   ```

2. Install the required packages:
   ```
   pip3 install -r requirements.txt
   ```

## Configuration
The configuration settings are stored in `config.json`. You can modify the base URL and the XPaths used for scraping as needed.

### Example `config.json`
```json
{
  "base_url": "https://api.fikfap.com",
  "xpaths": [
    "//div[@class='video-title']/text()",
    "//span[@class='views']/text()"
  ]
}
```

## Usage
To run the scraper, execute the following command:
```
python src/main.py
```

This will initialize the scraper, read the configuration, and start the scraping process based on the specified XPaths.

## Features
- Configurable XPaths for flexible data extraction.
- Utilizes the `crawl4AI` package for efficient web scraping.
- Organized project structure for maintainability.

## Contributing
Contributions are welcome! Please submit a pull request or open an issue for any enhancements or bug fixes.

## License
This project is licensed under the MIT License. See the LICENSE file for more details.