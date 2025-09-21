"""
FikFap Data Validator - Phase 2 Implementation
Comprehensive data validation using business logic and type checking
"""
import re
from typing import Dict, Any, List, Optional, Union
from datetime import datetime
from urllib.parse import urlparse

from core.config import config
from core.exceptions import ValidationError
from utils.logger import logger
from utils.helpers import is_valid_url

class DataValidator:
    """
    Comprehensive data validation for FikFap scraper
    Validates API responses, extracted data, and business logic constraints
    """

    def __init__(self):
        """Initialize validator with configuration"""
        self.logger = logger
        self.min_duration = config.get_filter('content.min_duration', 10)
        self.max_duration = config.get_filter('content.max_duration', 3600)
        self.min_views = config.get_filter('content.min_views', 100)
        self.blocked_users = set(config.get_filter('content.blocked_users', []))
        self.blocked_hashtags = set(config.get_filter('content.blocked_hashtags', []))
        self.allowed_ratings = config.get_filter('content.explicitness_ratings', ['FULLY_EXPLICIT'])

    def validate_raw_response(self, data: Dict[str, Any]) -> bool:
        """
        Validate raw API response structure and content

        Args:
            data: Raw API response dictionary

        Returns:
            bool: True if response is valid, False otherwise
        """
        try:
            if not isinstance(data, dict):
                self.logger.error("Response data must be a dictionary")
                return False

            if not data:
                self.logger.error("Response data is empty")
                return False

            # Check for common error indicators in API response
            if 'error' in data:
                error_msg = data.get('error', 'Unknown API error')
                self.logger.error(f"API error in response: {error_msg}")
                return False

            if 'status' in data and data['status'] != 'success':
                self.logger.error(f"API returned non-success status: {data.get('status')}")
                return False

            # Validate response has some meaningful content
            content_indicators = [
                'video', 'post', 'data', 'item', 'posts', 
                'postId', 'id', 'videoStreamUrl', 'streamUrl'
            ]

            if not any(key in data for key in content_indicators):
                self.logger.error("Response does not contain expected content indicators")
                return False

            self.logger.debug("Raw response validation passed")
            return True

        except Exception as e:
            self.logger.error(f"Error validating raw response: {e}")
            return False

    def validate_video_post(self, video_data: Dict[str, Any]) -> bool:
        """
        Comprehensive validation of video post data

        Args:
            video_data: Dictionary containing video post information

        Returns:
            bool: True if video data is valid, False otherwise
        """
        try:
            # Required fields validation
            required_fields = ['postId', 'videoStreamUrl', 'publishedAt']
            missing_fields = [field for field in required_fields if not video_data.get(field)]

            if missing_fields:
                self.logger.error(f"Missing required fields: {missing_fields}")
                return False

            # Validate post ID
            if not self._validate_post_id(video_data.get('postId')):
                return False

            # Validate video stream URL
            if not self._validate_stream_url(video_data.get('videoStreamUrl')):
                return False

            # Validate timestamps
            if not self._validate_timestamp(video_data.get('publishedAt')):
                return False

            # Validate content constraints
            if not self._validate_content_constraints(video_data):
                return False

            # Validate text fields
            if not self._validate_text_fields(video_data):
                return False

            # Validate numeric fields
            if not self._validate_numeric_fields(video_data):
                return False

            # Validate explicitness rating
            if not self._validate_explicitness_rating(video_data.get('explicitnessRating')):
                return False

            # Validate author data if present
            if video_data.get('author') and not self.validate_author_data(video_data['author']):
                return False

            # Validate hashtags
            if not self._validate_hashtags(video_data.get('hashtags', [])):
                return False

            self.logger.debug(f"Video post validation passed for ID: {video_data.get('postId')}")
            return True

        except Exception as e:
            self.logger.error(f"Error validating video post: {e}")
            return False

    def validate_author_data(self, author_data: Dict[str, Any]) -> bool:
        """
        Validate author/user data structure and content

        Args:
            author_data: Dictionary containing author information

        Returns:
            bool: True if author data is valid, False otherwise
        """
        try:
            if not isinstance(author_data, dict):
                self.logger.error("Author data must be a dictionary")
                return False

            # Required author fields
            required_fields = ['userId', 'username']
            missing_fields = [field for field in required_fields if not author_data.get(field)]

            if missing_fields:
                self.logger.error(f"Missing required author fields: {missing_fields}")
                return False

            # Validate user ID
            user_id = author_data.get('userId')
            if not user_id or not isinstance(user_id, str) or not user_id.strip():
                self.logger.error("Invalid user ID format")
                return False

            # Validate username
            username = author_data.get('username')
            if not self._validate_username(username):
                return False

            # Check blocked users
            if user_id in self.blocked_users or username.lower() in self.blocked_users:
                self.logger.warning(f"Author {username} is blocked")
                return False

            # Validate optional URL fields
            url_fields = ['thumbnailUrl', 'bannerUrl']
            for field in url_fields:
                if author_data.get(field) and not is_valid_url(author_data[field]):
                    self.logger.error(f"Invalid {field}: {author_data[field]}")
                    return False

            # Validate numeric fields
            numeric_fields = ['followerCount', 'followingCount', 'postCount']
            for field in numeric_fields:
                value = author_data.get(field, 0)
                if not isinstance(value, int) or value < 0:
                    self.logger.error(f"Invalid {field}: must be non-negative integer")
                    return False

            # Validate profile links
            profile_links = author_data.get('profileLinks', [])
            if profile_links and not self._validate_profile_links(profile_links):
                return False

            self.logger.debug(f"Author validation passed for: {username}")
            return True

        except Exception as e:
            self.logger.error(f"Error validating author data: {e}")
            return False

    def validate_m3u8_content(self, content: str) -> bool:
        """
        Validate M3U8 playlist content structure

        Args:
            content: M3U8 playlist content as string

        Returns:
            bool: True if content is valid M3U8, False otherwise
        """
        try:
            if not content or not isinstance(content, str):
                self.logger.error("M3U8 content must be non-empty string")
                return False

            content = content.strip()
            if not content:
                self.logger.error("M3U8 content is empty after stripping")
                return False

            # Check for M3U8 header
            lines = content.split('\n')
            if not lines[0].strip().startswith('#EXTM3U'):
                self.logger.error("M3U8 content missing required #EXTM3U header")
                return False

            # Validate basic M3U8 structure
            has_version = any('#EXT-X-VERSION' in line for line in lines)
            has_playlist_type = any('#EXT-X-PLAYLIST-TYPE' in line or '#EXT-X-STREAM-INF' in line for line in lines)

            if not (has_version or has_playlist_type):
                self.logger.error("M3U8 content appears to be malformed")
                return False

            # Check for at least one media segment or stream
            media_segments = [line for line in lines if not line.startswith('#') and line.strip()]
            if not media_segments:
                self.logger.error("M3U8 playlist contains no media segments")
                return False

            self.logger.debug("M3U8 content validation passed")
            return True

        except Exception as e:
            self.logger.error(f"Error validating M3U8 content: {e}")
            return False

    def validate_fragment_url(self, url: str) -> bool:
        """
        Validate video fragment URL

        Args:
            url: Fragment URL to validate

        Returns:
            bool: True if URL is valid fragment URL, False otherwise
        """
        try:
            if not is_valid_url(url):
                self.logger.error(f"Invalid fragment URL format: {url}")
                return False

            parsed = urlparse(url)

            # Check for valid scheme
            if parsed.scheme not in ['http', 'https']:
                self.logger.error(f"Invalid URL scheme: {parsed.scheme}")
                return False

            # Check for valid domain
            if not parsed.netloc:
                self.logger.error("URL missing domain")
                return False

            # Check for path
            if not parsed.path:
                self.logger.error("URL missing path")
                return False

            # Check for common video fragment file extensions
            valid_extensions = ['.m4s', '.ts', '.mp4', '.webm']
            path_lower = parsed.path.lower()

            if not any(path_lower.endswith(ext) for ext in valid_extensions):
                # Allow if path contains fragment patterns
                if not any(pattern in path_lower for pattern in ['segment', 'fragment', 'chunk']):
                    self.logger.warning(f"Fragment URL has unusual extension: {url}")

            self.logger.debug(f"Fragment URL validation passed: {url}")
            return True

        except Exception as e:
            self.logger.error(f"Error validating fragment URL: {e}")
            return False

    def validate_quality_data(self, quality_data: Dict[str, Any]) -> bool:
        """
        Validate video quality information

        Args:
            quality_data: Dictionary containing quality information

        Returns:
            bool: True if quality data is valid, False otherwise
        """
        try:
            # Required fields
            if not quality_data.get('resolution'):
                self.logger.error("Quality data missing resolution")
                return False

            if not quality_data.get('playlist_url'):
                self.logger.error("Quality data missing playlist URL")
                return False

            # Validate resolution format
            resolution = quality_data['resolution']
            if not re.match(r'^\d+[px]?$', resolution.lower()):
                self.logger.error(f"Invalid resolution format: {resolution}")
                return False

            # Validate playlist URL
            if not is_valid_url(quality_data['playlist_url']):
                self.logger.error(f"Invalid playlist URL: {quality_data['playlist_url']}")
                return False

            # Validate optional numeric fields
            numeric_fields = ['bandwidth', 'bitrate', 'width', 'height']
            for field in numeric_fields:
                if field in quality_data:
                    value = quality_data[field]
                    if value is not None and (not isinstance(value, int) or value < 0):
                        self.logger.error(f"Invalid {field}: must be non-negative integer")
                        return False

            # Validate FPS
            if 'fps' in quality_data and quality_data['fps'] is not None:
                fps = quality_data['fps']
                if not isinstance(fps, (int, float)) or fps <= 0 or fps > 120:
                    self.logger.error(f"Invalid FPS value: {fps}")
                    return False

            self.logger.debug(f"Quality validation passed for: {resolution}")
            return True

        except Exception as e:
            self.logger.error(f"Error validating quality data: {e}")
            return False

    def _validate_post_id(self, post_id: Any) -> bool:
        """Validate post ID format and value"""
        try:
            if not isinstance(post_id, int):
                # Try converting to int
                post_id = int(post_id)

            if post_id <= 0:
                self.logger.error(f"Post ID must be positive integer: {post_id}")
                return False

            return True

        except (ValueError, TypeError):
            self.logger.error(f"Invalid post ID format: {post_id}")
            return False

    def _validate_stream_url(self, url: str) -> bool:
        """Validate video stream URL"""
        if not is_valid_url(url):
            self.logger.error(f"Invalid stream URL: {url}")
            return False

        # Check for M3U8 format
        if not url.lower().endswith('.m3u8'):
            self.logger.warning(f"Stream URL is not M3U8 format: {url}")

        return True

    def _validate_timestamp(self, timestamp: Any) -> bool:
        """Validate timestamp format"""
        try:
            if isinstance(timestamp, datetime):
                return True

            if isinstance(timestamp, str):
                # Try parsing common formats
                formats = ['%Y-%m-%dT%H:%M:%S.%fZ', '%Y-%m-%dT%H:%M:%SZ', '%Y-%m-%d %H:%M:%S']
                for fmt in formats:
                    try:
                        datetime.strptime(timestamp, fmt)
                        return True
                    except ValueError:
                        continue

            self.logger.error(f"Invalid timestamp format: {timestamp}")
            return False

        except Exception as e:
            self.logger.error(f"Error validating timestamp: {e}")
            return False

    def _validate_content_constraints(self, video_data: Dict[str, Any]) -> bool:
        """Validate content against business logic constraints"""
        try:
            # Duration constraints
            duration = video_data.get('duration')
            if duration is not None:
                if not isinstance(duration, int) or duration < self.min_duration or duration > self.max_duration:
                    self.logger.warning(f"Duration {duration}s outside allowed range ({self.min_duration}-{self.max_duration}s)")
                    return False

            # View count constraints
            views = video_data.get('viewsCount', 0)
            if views < self.min_views:
                self.logger.warning(f"View count {views} below minimum {self.min_views}")
                return False

            # Blocked user check
            user_id = video_data.get('userId', '')
            if user_id in self.blocked_users:
                self.logger.warning(f"Content from blocked user: {user_id}")
                return False

            return True

        except Exception as e:
            self.logger.error(f"Error validating content constraints: {e}")
            return False

    def _validate_text_fields(self, video_data: Dict[str, Any]) -> bool:
        """Validate text field formats and content"""
        try:
            # Validate label/title
            label = video_data.get('label', '')
            if not label or not isinstance(label, str) or not label.strip():
                self.logger.error("Video label is required and cannot be empty")
                return False

            if len(label) > 200:
                self.logger.error(f"Video label too long: {len(label)} characters")
                return False

            # Validate description
            description = video_data.get('description')
            if description is not None:
                if not isinstance(description, str):
                    self.logger.error("Description must be string")
                    return False

                if len(description) > 2000:
                    self.logger.error(f"Description too long: {len(description)} characters")
                    return False

            # Validate required string fields
            string_fields = ['mediaId', 'bunnyVideoId', 'userId']
            for field in string_fields:
                value = video_data.get(field, '')
                if not value or not isinstance(value, str) or not value.strip():
                    self.logger.error(f"Required field {field} is empty or invalid")
                    return False

            return True

        except Exception as e:
            self.logger.error(f"Error validating text fields: {e}")
            return False

    def _validate_numeric_fields(self, video_data: Dict[str, Any]) -> bool:
        """Validate numeric field formats and ranges"""
        try:
            numeric_fields = ['viewsCount', 'likesCount', 'score']

            for field in numeric_fields:
                value = video_data.get(field, 0)

                if not isinstance(value, int):
                    try:
                        value = int(value)
                    except (ValueError, TypeError):
                        self.logger.error(f"Invalid {field}: must be integer")
                        return False

                if field in ['viewsCount', 'likesCount'] and value < 0:
                    self.logger.error(f"Invalid {field}: must be non-negative")
                    return False

            return True

        except Exception as e:
            self.logger.error(f"Error validating numeric fields: {e}")
            return False

    def _validate_explicitness_rating(self, rating: str) -> bool:
        """Validate explicitness rating"""
        try:
            if not rating:
                rating = 'UNKNOWN'

            valid_ratings = ['FULLY_EXPLICIT', 'PARTIALLY_EXPLICIT', 'NOT_EXPLICIT', 'UNKNOWN']

            if rating not in valid_ratings:
                self.logger.error(f"Invalid explicitness rating: {rating}")
                return False

            # Check against allowed ratings
            if rating not in self.allowed_ratings and rating != 'UNKNOWN':
                self.logger.warning(f"Content rating {rating} not in allowed list")
                return False

            return True

        except Exception as e:
            self.logger.error(f"Error validating explicitness rating: {e}")
            return False

    def _validate_username(self, username: str) -> bool:
        """Validate username format"""
        try:
            if not username or not isinstance(username, str):
                self.logger.error("Username is required and must be string")
                return False

            username = username.strip()

            if not username:
                self.logger.error("Username cannot be empty")
                return False

            if len(username) < 1 or len(username) > 50:
                self.logger.error(f"Username length invalid: {len(username)} characters")
                return False

            # Check username format (alphanumeric, underscore, hyphen, dot)
            if not re.match(r'^[a-zA-Z0-9_.-]+$', username):
                self.logger.error(f"Username contains invalid characters: {username}")
                return False

            return True

        except Exception as e:
            self.logger.error(f"Error validating username: {e}")
            return False

    def _validate_hashtags(self, hashtags: List[str]) -> bool:
        """Validate hashtags list"""
        try:
            if not isinstance(hashtags, list):
                self.logger.error("Hashtags must be a list")
                return False

            for tag in hashtags:
                if not isinstance(tag, str):
                    self.logger.error(f"Hashtag must be string: {tag}")
                    return False

                clean_tag = tag.strip().lstrip('#').lower()

                if len(clean_tag) > 50:
                    self.logger.error(f"Hashtag too long: {clean_tag}")
                    return False

                if clean_tag in self.blocked_hashtags:
                    self.logger.warning(f"Blocked hashtag found: {clean_tag}")
                    return False

            return True

        except Exception as e:
            self.logger.error(f"Error validating hashtags: {e}")
            return False

    def _validate_profile_links(self, profile_links: List[Dict[str, Any]]) -> bool:
        """Validate profile links structure"""
        try:
            if not isinstance(profile_links, list):
                self.logger.error("Profile links must be a list")
                return False

            for link in profile_links:
                if not isinstance(link, dict):
                    self.logger.error("Profile link must be dictionary")
                    return False

                if 'platform' not in link or 'url' not in link:
                    self.logger.error("Profile link missing required fields")
                    return False

                if not isinstance(link['platform'], str) or not link['platform'].strip():
                    self.logger.error("Platform name must be non-empty string")
                    return False

                if not is_valid_url(link['url']):
                    self.logger.error(f"Invalid profile link URL: {link['url']}")
                    return False

            return True

        except Exception as e:
            self.logger.error(f"Error validating profile links: {e}")
            return False

    def validate_download_job_data(self, job_data: Dict[str, Any]) -> bool:
        """Validate download job data structure"""
        try:
            required_fields = ['jobId', 'postId', 'videoUrl', 'outputPath']
            missing_fields = [field for field in required_fields if not job_data.get(field)]

            if missing_fields:
                self.logger.error(f"Missing required job fields: {missing_fields}")
                return False

            # Validate job ID
            if not isinstance(job_data['jobId'], str) or not job_data['jobId'].strip():
                self.logger.error("Job ID must be non-empty string")
                return False

            # Validate post ID
            if not self._validate_post_id(job_data['postId']):
                return False

            # Validate video URL
            if not is_valid_url(job_data['videoUrl']):
                self.logger.error(f"Invalid video URL: {job_data['videoUrl']}")
                return False

            # Validate output path
            output_path = job_data['outputPath']
            if not isinstance(output_path, str) or not output_path.strip():
                self.logger.error("Output path must be non-empty string")
                return False

            # Validate progress if present
            if 'progress' in job_data:
                progress = job_data['progress']
                if not isinstance(progress, (int, float)) or progress < 0 or progress > 100:
                    self.logger.error(f"Invalid progress value: {progress}")
                    return False

            return True

        except Exception as e:
            self.logger.error(f"Error validating download job data: {e}")
            return False
