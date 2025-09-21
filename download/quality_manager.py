"""
Quality Manager for FikFap Scraper - Phase 3
Advanced quality filtering, selection, and management
"""
import asyncio
from typing import List, Dict, Any, Optional, Tuple, Set
from urllib.parse import urlparse
import re

from core.config import config
from core.exceptions import QualityNotFoundError, ValidationError
from data.models import VideoQuality, VideoCodec, VideoPost
from utils.logger import logger

class QualityManager:
    """
    Advanced quality management system for video downloads

    Features:
    - Intelligent quality filtering by codec, resolution, and bandwidth
    - VP9 detection and exclusion
    - Quality ranking and selection algorithms
    - Bandwidth-based optimization
    - Resolution constraint enforcement
    """

    def __init__(self):
        """Initialize quality manager with configuration"""
        self.logger = logger

        # Load configuration settings
        self.exclude_vp9 = config.get_filter('codecs.exclude_vp9', False)
        self.vp9_patterns = config.get_filter('codecs.vp9_patterns', ['vp9_', 'vp09.'])
        self.preferred_codecs = config.get_filter('codecs.preferred_codecs', ['avc1', 'h264'])

        # Quality constraints
        self.min_resolution = config.get_filter('quality.min_resolution', '240p')
        self.max_resolution = config.get_filter('quality.max_resolution', '1080p')
        self.excluded_resolutions = set(config.get_filter('quality.exclude_resolutions', []))

        # Bandwidth constraints
        self.min_bandwidth = config.get('quality.min_bandwidth', 100000)
        self.max_bandwidth = config.get('quality.max_bandwidth', 10000000)

        # Selection preferences
        self.preferred_qualities = config.get('quality.preferred_qualities', ['1080p', '720p', '480p'])
        self.auto_select_best = config.get('quality.auto_select_best', False)
        self.download_all_qualities = config.get('quality.download_all_qualities', True)

        # Resolution ranking for intelligent selection
        self.resolution_ranks = self._build_resolution_ranking()

        self.logger.info(f"Quality Manager initialized - VP9 exclusion: {self.exclude_vp9}")

    def _build_resolution_ranking(self) -> Dict[str, int]:
        """Build resolution ranking for quality comparison"""
        standard_resolutions = {
            '144p': 144, '240p': 240, '360p': 360, '480p': 480,
            '720p': 720, '1080p': 1080, '1440p': 1440, '2160p': 2160, '4320p': 4320
        }

        # Add custom resolutions from config
        for resolution in self.preferred_qualities:
            if resolution not in standard_resolutions:
                # Extract numeric value from resolution string
                match = re.match(r'(\d+)', resolution)
                if match:
                    standard_resolutions[resolution] = int(match.group(1))

        return standard_resolutions

    async def filter_qualities(self, qualities: List[VideoQuality]) -> List[VideoQuality]:
        """
        Apply comprehensive quality filtering

        Args:
            qualities: List of available video qualities

        Returns:
            Filtered list of video qualities
        """
        if not qualities:
            self.logger.warning("No qualities provided for filtering")
            return []

        self.logger.info(f"Filtering {len(qualities)} qualities with current preferences")

        filtered_qualities = []

        for quality in qualities:
            # Check VP9 exclusion
            if self.exclude_vp9 and self._is_vp9_quality(quality):
                self.logger.debug(f"Excluding VP9 quality: {quality.resolution}")
                continue

            # Check resolution constraints
            if not self._meets_resolution_constraints(quality):
                self.logger.debug(f"Quality {quality.resolution} doesn't meet resolution constraints")
                continue

            # Check bandwidth constraints
            if not self._meets_bandwidth_constraints(quality):
                self.logger.debug(f"Quality {quality.resolution} doesn't meet bandwidth constraints")
                continue

            # Check codec preferences
            if not self._meets_codec_preferences(quality):
                self.logger.debug(f"Quality {quality.resolution} doesn't meet codec preferences")
                continue

            filtered_qualities.append(quality)
            self.logger.debug(f"Accepted quality: {quality.resolution} ({quality.codec.value})")

        self.logger.info(f"Filtered to {len(filtered_qualities)} acceptable qualities")
        return filtered_qualities

    def select_qualities_for_download(self, qualities: List[VideoQuality]) -> List[VideoQuality]:
        """
        Select which qualities to download based on preferences

        Args:
            qualities: List of filtered video qualities

        Returns:
            Selected qualities for download
        """
        if not qualities:
            raise QualityNotFoundError("No qualities available for selection")

        self.logger.info(f"Selecting qualities from {len(qualities)} available options")

        if self.download_all_qualities:
            self.logger.info("Download all qualities mode - selecting all filtered qualities")
            return self._rank_qualities(qualities)

        if self.auto_select_best:
            best_quality = self._select_best_quality(qualities)
            self.logger.info(f"Auto-selected best quality: {best_quality.resolution}")
            return [best_quality]

        # Select based on preferred qualities list
        selected = self._select_preferred_qualities(qualities)

        if not selected:
            # Fallback: select the best available quality
            self.logger.warning("No preferred qualities found, falling back to best available")
            selected = [self._select_best_quality(qualities)]

        self.logger.info(f"Selected {len(selected)} qualities for download")
        return selected

    def _select_preferred_qualities(self, qualities: List[VideoQuality]) -> List[VideoQuality]:
        """Select qualities matching user preferences"""
        selected = []

        for preferred_res in self.preferred_qualities:
            for quality in qualities:
                if quality.resolution.lower() == preferred_res.lower():
                    selected.append(quality)
                    break

        return selected

    def _select_best_quality(self, qualities: List[VideoQuality]) -> VideoQuality:
        """Select the single best quality based on ranking algorithm"""
        if len(qualities) == 1:
            return qualities[0]

        ranked_qualities = self._rank_qualities(qualities)
        return ranked_qualities[0]

    def _rank_qualities(self, qualities: List[VideoQuality]) -> List[VideoQuality]:
        """
        Rank qualities using sophisticated scoring algorithm

        Scoring factors:
        - Resolution (higher is better)
        - Bandwidth efficiency
        - Codec preference
        - Configuration preferences
        """
        def calculate_score(quality: VideoQuality) -> float:
            score = 0.0

            # Resolution score (40% weight)
            resolution_score = self._get_resolution_score(quality.resolution)
            score += resolution_score * 0.4

            # Codec preference score (30% weight)
            codec_score = self._get_codec_score(quality.codec)
            score += codec_score * 0.3

            # Bandwidth efficiency score (20% weight)
            bandwidth_score = self._get_bandwidth_score(quality.bandwidth or 0)
            score += bandwidth_score * 0.2

            # User preference bonus (10% weight)
            preference_score = self._get_preference_score(quality.resolution)
            score += preference_score * 0.1

            return score

        # Sort by score (descending)
        ranked = sorted(qualities, key=calculate_score, reverse=True)

        # Log ranking details
        self.logger.debug("Quality ranking:")
        for i, quality in enumerate(ranked[:5]):  # Show top 5
            score = calculate_score(quality)
            self.logger.debug(f"  {i+1}. {quality.resolution} ({quality.codec.value}) - Score: {score:.2f}")

        return ranked

    def _get_resolution_score(self, resolution: str) -> float:
        """Calculate resolution-based score (0-100)"""
        resolution_value = self.resolution_ranks.get(resolution.lower(), 0)
        if resolution_value == 0:
            # Try to extract numeric value
            match = re.match(r'(\d+)', resolution)
            if match:
                resolution_value = int(match.group(1))

        # Normalize to 0-100 scale (1080p = 100)
        max_resolution = 1080
        return min(100.0, (resolution_value / max_resolution) * 100)

    def _get_codec_score(self, codec: VideoCodec) -> float:
        """Calculate codec preference score (0-100)"""
        codec_scores = {
            VideoCodec.H264: 100,
            VideoCodec.AVC1: 95,
            VideoCodec.HEVC: 85,
            VideoCodec.VP9: 70 if not self.exclude_vp9 else 0,
            VideoCodec.VP09: 70 if not self.exclude_vp9 else 0,
            VideoCodec.UNKNOWN: 50
        }

        return codec_scores.get(codec, 50)

    def _get_bandwidth_score(self, bandwidth: int) -> float:
        """Calculate bandwidth efficiency score (0-100)"""
        if bandwidth <= 0:
            return 50  # Neutral score for unknown bandwidth

        # Optimal bandwidth range (good quality without excessive size)
        optimal_min = 1000000  # 1 Mbps
        optimal_max = 5000000  # 5 Mbps

        if optimal_min <= bandwidth <= optimal_max:
            return 100
        elif bandwidth < optimal_min:
            return (bandwidth / optimal_min) * 100
        else:
            # Penalize excessively high bandwidth
            excess = bandwidth - optimal_max
            penalty = (excess / optimal_max) * 30
            return max(70, 100 - penalty)

    def _get_preference_score(self, resolution: str) -> float:
        """Calculate user preference bonus score (0-100)"""
        try:
            index = [q.lower() for q in self.preferred_qualities].index(resolution.lower())
            # Higher score for higher preference position
            return 100 - (index * 20)
        except ValueError:
            return 50  # Neutral score for non-preferred resolutions

    def _is_vp9_quality(self, quality: VideoQuality) -> bool:
        """Check if quality uses VP9 codec"""
        # Check explicit VP9 flag
        if quality.is_vp9:
            return True

        # Check codec enum
        if quality.codec in [VideoCodec.VP9, VideoCodec.VP09]:
            return True

        # Check URL patterns
        playlist_url = str(quality.playlist_url)
        return any(pattern.lower() in playlist_url.lower() for pattern in self.vp9_patterns)

    def _meets_resolution_constraints(self, quality: VideoQuality) -> bool:
        """Check if quality meets resolution constraints"""
        resolution = quality.resolution.lower()

        # Check excluded resolutions
        if resolution in [r.lower() for r in self.excluded_resolutions]:
            return False

        # Check min/max resolution constraints
        current_value = self.resolution_ranks.get(resolution, 0)
        min_value = self.resolution_ranks.get(self.min_resolution.lower(), 0)
        max_value = self.resolution_ranks.get(self.max_resolution.lower(), 9999)

        return min_value <= current_value <= max_value

    def _meets_bandwidth_constraints(self, quality: VideoQuality) -> bool:
        """Check if quality meets bandwidth constraints"""
        if not quality.bandwidth:
            return True  # Allow unknown bandwidth

        return self.min_bandwidth <= quality.bandwidth <= self.max_bandwidth

    def _meets_codec_preferences(self, quality: VideoQuality) -> bool:
        """Check if quality meets codec preferences"""
        # If VP9 is excluded and this is VP9, reject
        if self.exclude_vp9 and self._is_vp9_quality(quality):
            return False

        # If we have specific codec preferences, check them
        if self.preferred_codecs:
            codec_str = quality.codec.value.lower()
            return any(pref.lower() in codec_str for pref in self.preferred_codecs)

        return True

    async def analyze_quality_distribution(self, video_post: VideoPost) -> Dict[str, Any]:
        """
        Analyze the distribution of available qualities

        Args:
            video_post: Video post with available qualities

        Returns:
            Analysis report dictionary
        """
        qualities = video_post.availableQualities

        if not qualities:
            return {"error": "No qualities available for analysis"}

        analysis = {
            "total_qualities": len(qualities),
            "resolutions": [],
            "codecs": {},
            "bandwidth_range": {"min": 0, "max": 0, "avg": 0},
            "vp9_count": 0,
            "h264_count": 0,
            "recommended": None
        }

        bandwidths = []

        for quality in qualities:
            # Resolution analysis
            analysis["resolutions"].append(quality.resolution)

            # Codec analysis
            codec_name = quality.codec.value
            analysis["codecs"][codec_name] = analysis["codecs"].get(codec_name, 0) + 1

            # VP9 vs H264 counting
            if self._is_vp9_quality(quality):
                analysis["vp9_count"] += 1
            elif quality.codec in [VideoCodec.H264, VideoCodec.AVC1]:
                analysis["h264_count"] += 1

            # Bandwidth analysis
            if quality.bandwidth:
                bandwidths.append(quality.bandwidth)

        # Bandwidth statistics
        if bandwidths:
            analysis["bandwidth_range"]["min"] = min(bandwidths)
            analysis["bandwidth_range"]["max"] = max(bandwidths)
            analysis["bandwidth_range"]["avg"] = sum(bandwidths) // len(bandwidths)

        # Get recommended qualities
        try:
            filtered_qualities = await self.filter_qualities(qualities)
            if filtered_qualities:
                selected_qualities = self.select_qualities_for_download(filtered_qualities)
                analysis["recommended"] = [
                    {
                        "resolution": q.resolution,
                        "codec": q.codec.value,
                        "bandwidth": q.bandwidth,
                        "is_vp9": q.is_vp9
                    }
                    for q in selected_qualities[:3]  # Top 3 recommendations
                ]
        except Exception as e:
            analysis["recommendation_error"] = str(e)

        self.logger.info(f"Quality analysis completed: {analysis['total_qualities']} qualities, "
                        f"{analysis['vp9_count']} VP9, {analysis['h264_count']} H.264")

        return analysis

    def get_quality_summary(self, qualities: List[VideoQuality]) -> str:
        """Get human-readable summary of qualities"""
        if not qualities:
            return "No qualities available"

        summary_parts = []

        # Group by resolution
        resolution_counts = {}
        codec_counts = {}

        for quality in qualities:
            resolution_counts[quality.resolution] = resolution_counts.get(quality.resolution, 0) + 1
            codec_counts[quality.codec.value] = codec_counts.get(quality.codec.value, 0) + 1

        # Build summary
        resolutions = sorted(resolution_counts.keys(), 
                           key=lambda x: self.resolution_ranks.get(x.lower(), 0), 
                           reverse=True)

        summary_parts.append(f"{len(qualities)} qualities available")
        summary_parts.append(f"Resolutions: {', '.join(resolutions)}")

        if codec_counts:
            codecs = [f"{codec} ({count})" for codec, count in codec_counts.items()]
            summary_parts.append(f"Codecs: {', '.join(codecs)}")

        return " | ".join(summary_parts)
