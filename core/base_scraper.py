"""
Base scraper class for FikFap API - Phase 2 Enhanced
Enhanced with data extraction capabilities
"""
import asyncio
import aiohttp
from typing import Dict, Any, Optional, List, Union, TYPE_CHECKING

if TYPE_CHECKING:
    from data.extractor import FikFapDataExtractor

from utils.logger import logger
from .config import config
from .exceptions import APIError, NetworkError
from data.models import VideoPost
from data.validator import DataValidator

class BaseScraper:
    """Enhanced base scraper with data extraction capabilities"""

    def __init__(self):
        self.session: Optional[aiohttp.ClientSession] = None
        self.base_url = config.api_base_url
        self.timeout = aiohttp.ClientTimeout(total=config.get('api.timeout', 30))
        self.max_retries = config.get('api.max_retries', 3)

        # Phase 2: Initialize data components
        self.extractor: Optional['FikFapDataExtractor'] = None
        self.validator = DataValidator()

    async def __aenter__(self):
        """Async context manager entry"""
        await self.start_session()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        await self.close_session()

    async def start_session(self):
        """Start HTTP session and initialize data extractor"""
        if not self.session:
            connector = aiohttp.TCPConnector(
                limit=config.get('download.concurrent_downloads', 5),
                verify_ssl=config.get('download.verify_ssl', True)
            )

            headers = {
                'User-Agent': config.get('download.user_agent', 'FikFap-Scraper/1.0')
            }

            self.session = aiohttp.ClientSession(
                connector=connector,
                timeout=self.timeout,
                headers=headers
            )

            # Phase 2: Initialize extractor with this scraper instance
            # Import here to avoid circular import
            from data.extractor import FikFapDataExtractor
            self.extractor = FikFapDataExtractor(self)

            logger.info("HTTP session and data extractor initialized")

    async def close_session(self):
        """Close HTTP session"""
        if self.session:
            await self.session.close()
            self.session = None
            self.extractor = None
            logger.info("HTTP session closed")

    async def make_request(self, url: str, method: str = 'GET', params: Optional[Dict[str, Any]] = None, **kwargs) -> Dict[str, Any]:
        """Make HTTP request with retry logic"""
        if not self.session:
            await self.start_session()

        for attempt in range(self.max_retries):
            try:
                # Add params to kwargs if provided
                if params:
                    kwargs['params'] = params

                async with self.session.request(method, url, **kwargs) as response:
                    if response.status == 200:
                        if response.content_type == 'application/json':
                            return await response.json()
                        else:
                            return {'content': await response.text()}
                    elif response.status == 429:
                        # Rate limited
                        wait_time = 2 ** attempt
                        logger.warning(f"Rate limited, waiting {wait_time}s before retry")
                        await asyncio.sleep(wait_time)
                        continue
                    else:
                        raise APIError(f"HTTP {response.status}: {await response.text()}")

            except asyncio.TimeoutError:
                if attempt == self.max_retries - 1:
                    raise NetworkError(f"Timeout after {self.max_retries} attempts")
                logger.warning(f"Timeout on attempt {attempt + 1}, retrying...")
                await asyncio.sleep(2 ** attempt)

            except Exception as e:
                if attempt == self.max_retries - 1:
                    raise NetworkError(f"Request failed: {str(e)}")
                logger.warning(f"Request failed on attempt {attempt + 1}: {e}")
                await asyncio.sleep(2 ** attempt)

        raise NetworkError("Max retries exceeded")

    def is_valid_video(self, video) -> bool:
        """Basic video validation"""
        return True

    def filter_qualities_by_codec(self, qualities: List[Dict[str, Any]], exclude_vp9: bool = False) -> List[Dict[str, Any]]:
        """Filter video qualities by codec preferences"""
        if not exclude_vp9:
            return qualities

        filtered_qualities = []
        for quality in qualities:
            is_vp9 = quality.get('is_vp9', False)
            codec = quality.get('codec', '').lower()

            # Skip VP9 codecs if exclusion is enabled
            if exclude_vp9 and (is_vp9 or 'vp9' in codec or 'vp09' in codec):
                logger.debug(f"Excluding VP9 quality: {quality.get('resolution')}")
                continue

            filtered_qualities.append(quality)

        return filtered_qualities

    def get_preferred_quality(self, qualities: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        """Get preferred video quality based on configuration"""
        if not qualities:
            return None

        preferred_resolutions = config.get('quality.preferred_qualities', ['1080p', '720p', '480p'])

        # Try to find preferred resolution in order
        for resolution in preferred_resolutions:
            for quality in qualities:
                if quality.get('resolution', '').lower() == resolution.lower():
                    return quality

        # If no preferred resolution found, return highest available
        sorted_qualities = sorted(qualities, key=lambda q: q.get('bandwidth', 0), reverse=True)
        return sorted_qualities[0] if sorted_qualities else None

    def get_download_summary(self, video) -> Dict[str, Any]:
        """Get download summary"""
        return {
            'title': getattr(video, 'label', 'Unknown'),
            'author': getattr(video.author, 'username', 'Unknown') if hasattr(video, 'author') and video.author else 'Unknown',
            'duration': getattr(video, 'duration', 0),
            'rating': getattr(video, 'explicitnessRating', 'UNKNOWN'),
            'total_qualities': len(getattr(video, 'availableQualities', [])),
            'has_vp9': getattr(video, 'has_vp9_qualities', False),
            'qualities': []
        }
