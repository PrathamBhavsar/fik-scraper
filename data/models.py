"""
Enhanced data models for FikFap Scraper with comprehensive validation
Phase 4: Complete with storage and file management models
"""
from typing import Optional, List, Dict, Any, Union, Literal
from datetime import datetime
from pydantic import BaseModel, HttpUrl, Field, model_validator, validator
from enum import Enum
from pathlib import Path
import re

class ExplicitnessRating(str, Enum):
    """Explicitness rating enumeration"""
    FULLY_EXPLICIT = "FULLY_EXPLICIT"
    PARTIALLY_EXPLICIT = "PARTIALLY_EXPLICIT"
    NOT_EXPLICIT = "NOT_EXPLICIT"
    UNKNOWN = "UNKNOWN"

class VideoCodec(str, Enum):
    """Video codec enumeration"""
    H264 = "h264"
    AVC1 = "avc1"
    VP9 = "vp9"
    VP09 = "vp09"
    HEVC = "hevc"
    UNKNOWN = "unknown"

class DownloadStatus(str, Enum):
    """Download job status enumeration"""
    PENDING = "pending"
    DOWNLOADING = "downloading"
    COMPLETED = "completed"
    FAILED = "failed"
    PAUSED = "paused"
    CANCELLED = "cancelled"

class ProcessingStatus(str, Enum):
    """Processing status for metadata tracking"""
    NEW = "new"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"
    DUPLICATE = "duplicate"

class StorageFormat(str, Enum):
    """Storage format enumeration"""
    MP4 = "mp4"
    MKV = "mkv"
    AVI = "avi"
    MOV = "mov"
    M3U8 = "m3u8"

class ProfileLink(BaseModel):
    """Profile link model for author social links"""
    platform: str = Field(..., min_length=1, description="Social media platform name")
    url: HttpUrl = Field(..., description="Profile URL")
    verified: bool = Field(default=False, description="Whether the profile is verified")

    @validator('platform')
    def validate_platform(cls, v):
        """Validate platform name"""
        if not v or not v.strip():
            raise ValueError("Platform name cannot be empty")
        return v.strip().lower()

class Author(BaseModel):
    """Enhanced author/user model with comprehensive validation"""
    userId: str = Field(..., min_length=1, description="Unique user identifier")
    username: str = Field(..., min_length=1, max_length=50, description="Username")
    displayName: Optional[str] = Field(None, max_length=100, description="Display name")
    isVerified: bool = Field(default=False, description="Verification status")
    isPartner: bool = Field(default=False, description="Partner program status")
    isPremium: bool = Field(default=False, description="Premium account status")
    description: Optional[str] = Field(None, max_length=500, description="User bio/description")
    thumbnailUrl: Optional[HttpUrl] = Field(None, description="Profile picture URL")
    bannerUrl: Optional[HttpUrl] = Field(None, description="Profile banner URL")
    followerCount: int = Field(default=0, ge=0, description="Number of followers")
    followingCount: int = Field(default=0, ge=0, description="Number of following")
    postCount: int = Field(default=0, ge=0, description="Number of posts")
    profileLinks: List[ProfileLink] = Field(default_factory=list, description="Social media links")
    joinedAt: Optional[datetime] = Field(None, description="Account creation date")
    lastActiveAt: Optional[datetime] = Field(None, description="Last activity timestamp")

    @validator('username')
    def validate_username(cls, v):
        """Validate username format"""
        if not v or not v.strip():
            raise ValueError("Username cannot be empty")
        username = v.strip()
        if not re.match(r'^[a-zA-Z0-9_.-]+$', username):
            raise ValueError("Username contains invalid characters")
        return username

    @validator('description')
    def validate_description(cls, v):
        """Clean and validate description"""
        if v:
            cleaned = re.sub(r'\s+', ' ', v.strip())
            cleaned = re.sub(r'[\x00-\x1f\x7f-\x9f]', '', cleaned)
            return cleaned if cleaned else None
        return v

class VideoQuality(BaseModel):
    """Video quality information with codec detection"""
    resolution: str = Field(..., description="Video resolution (e.g., '1080p', '720p')")
    width: Optional[int] = Field(None, gt=0, description="Video width in pixels")
    height: Optional[int] = Field(None, gt=0, description="Video height in pixels")
    codec: VideoCodec = Field(default=VideoCodec.UNKNOWN, description="Video codec")
    bandwidth: Optional[int] = Field(None, ge=0, description="Bandwidth in bits per second")
    fps: Optional[float] = Field(None, gt=0, le=120, description="Frames per second")
    bitrate: Optional[int] = Field(None, ge=0, description="Video bitrate")
    playlist_url: HttpUrl = Field(..., description="M3U8 playlist URL for this quality")
    is_vp9: bool = Field(default=False, description="Whether this is a VP9 codec stream")
    audio_codec: Optional[str] = Field(None, description="Audio codec information")

    @validator('resolution')
    def validate_resolution(cls, v):
        """Validate resolution format"""
        if not re.match(r'^\d+[px]?$', v.lower()):
            raise ValueError("Invalid resolution format")
        return v.lower()

    @validator('codec', pre=True)
    def detect_codec(cls, v, values):
        """Auto-detect codec from various sources"""
        if isinstance(v, str):
            v_lower = v.lower()
            if 'vp9' in v_lower or 'vp09' in v_lower:
                return VideoCodec.VP9
            elif 'h264' in v_lower or 'avc1' in v_lower:
                return VideoCodec.H264
            elif 'hevc' in v_lower or 'h265' in v_lower:
                return VideoCodec.HEVC
        return v or VideoCodec.UNKNOWN

    @model_validator(mode='before')
    @classmethod
    def set_vp9_flag(cls, values):
        if isinstance(values, dict):
            codec = values.get('codec')
            if codec and ('vp9' in str(codec).lower() or 'vp09' in str(codec)):
                values['is_vp9'] = True
        return values

class VideoPost(BaseModel):
    """Enhanced main video post model"""
    postId: int = Field(..., gt=0, description="Unique post identifier")
    mediaId: str = Field(..., min_length=1, description="Media identifier")
    bunnyVideoId: str = Field(..., min_length=1, description="Bunny CDN video identifier")
    userId: str = Field(..., min_length=1, description="Author user identifier")
    label: str = Field(..., min_length=1, max_length=200, description="Video title/label")
    description: Optional[str] = Field(None, max_length=2000, description="Video description")
    videoStreamUrl: HttpUrl = Field(..., description="Main video stream URL (M3U8)")
    thumbnailUrl: Optional[HttpUrl] = Field(None, description="Video thumbnail URL")
    duration: Optional[int] = Field(None, gt=0, description="Duration in seconds")
    viewsCount: int = Field(default=0, ge=0, description="Number of views")
    likesCount: int = Field(default=0, ge=0, description="Number of likes")
    score: int = Field(default=0, description="Content score/rating")
    explicitnessRating: ExplicitnessRating = Field(default=ExplicitnessRating.UNKNOWN)
    publishedAt: datetime = Field(..., description="Publication timestamp")
    isBunnyVideoReady: bool = Field(default=False, description="Video ready status")
    hashtags: List[str] = Field(default_factory=list, description="Associated hashtags")
    author: Optional[Author] = Field(None, description="Post author information")
    availableQualities: List[VideoQuality] = Field(default_factory=list)

    @validator('label')
    def clean_label(cls, v):
        """Clean and validate video label"""
        if not v or not v.strip():
            raise ValueError("Label cannot be empty")
        cleaned = re.sub(r'\s+', ' ', v.strip())
        cleaned = re.sub(r'[\x00-\x1f\x7f-\x9f]', '', cleaned)
        return cleaned

    @validator('hashtags', pre=True)
    def clean_hashtags(cls, v):
        """Clean and validate hashtags"""
        if not isinstance(v, list):
            return []
        cleaned_tags = []
        for tag in v:
            if isinstance(tag, str) and tag.strip():
                clean_tag = tag.strip().lstrip('#').lower()
                if clean_tag and len(clean_tag) <= 50:
                    cleaned_tags.append(clean_tag)
        return list(set(cleaned_tags))

    @property
    def has_vp9_qualities(self) -> bool:
        """Check if any quality uses VP9 codec"""
        return any(q.is_vp9 for q in self.availableQualities)

class DownloadJob(BaseModel):
    """Download job tracking"""
    jobId: str = Field(..., min_length=1, description="Unique job identifier")
    postId: int = Field(..., gt=0, description="Associated post ID")
    videoUrl: HttpUrl = Field(..., description="Source video URL")
    outputPath: str = Field(..., min_length=1, description="Output directory path")
    status: DownloadStatus = Field(default=DownloadStatus.PENDING)
    progress: float = Field(default=0.0, ge=0.0, le=100.0, description="Progress percentage")
    createdAt: datetime = Field(default_factory=datetime.now)
    completedAt: Optional[datetime] = Field(None)
    errorMessage: Optional[str] = Field(None)

    @property
    def is_completed(self) -> bool:
        return self.status == DownloadStatus.COMPLETED

class SystemStatus(BaseModel):
    """System monitoring status"""
    diskSpaceGb: float = Field(..., ge=0)
    memoryUsagePercent: float = Field(..., ge=0, le=100)
    cpuUsagePercent: float = Field(..., ge=0, le=100)
    activeDownloads: int = Field(default=0, ge=0)
    lastUpdate: datetime = Field(default_factory=datetime.now)

    @property
    def is_healthy(self) -> bool:
        """Check if system is in healthy state"""
        return (
            self.diskSpaceGb > 1.0 and
            self.memoryUsagePercent < 90 and
            self.cpuUsagePercent < 95
        )

# Phase 4: Storage & File Management Models

class StorageMetadata(BaseModel):
    """Metadata for stored files and downloads"""
    postId: int = Field(..., gt=0)
    mediaId: str = Field(...)
    title: str = Field(...)
    author: str = Field(...)
    authorId: str = Field(...)
    publishedAt: datetime = Field(...)
    downloadedAt: datetime = Field(default_factory=datetime.now)
    fileSize: int = Field(default=0, ge=0)
    filePath: str = Field(...)
    fileName: str = Field(...)
    quality: str = Field(...)
    resolution: str = Field(...)
    codec: VideoCodec = Field(default=VideoCodec.UNKNOWN)
    duration: Optional[int] = Field(None, ge=0)
    format: StorageFormat = Field(default=StorageFormat.MP4)
    checksum: Optional[str] = Field(None)
    tags: List[str] = Field(default_factory=list)
    processingStatus: ProcessingStatus = Field(default=ProcessingStatus.NEW)
    downloadJobId: Optional[str] = Field(None)

class DirectoryStructure(BaseModel):
    """Directory structure information"""
    postId: int = Field(..., gt=0)
    basePath: str = Field(...)
    authorPath: Optional[str] = Field(None)
    datePath: Optional[str] = Field(None)
    postPath: str = Field(...)
    m3u8Path: str = Field(...)
    qualityPaths: Dict[str, str] = Field(default_factory=dict)
    metadataPath: str = Field(...)
    
    def get_full_path(self, quality: Optional[str] = None) -> Path:
        """Get full path for storage"""
        if quality and quality in self.qualityPaths:
            return Path(self.qualityPaths[quality])
        return Path(self.postPath)

class ProcessingRecord(BaseModel):
    """Record of processing attempts"""
    postId: int = Field(..., gt=0)
    processingId: str = Field(...)
    status: ProcessingStatus = Field(default=ProcessingStatus.NEW)
    startedAt: datetime = Field(default_factory=datetime.now)
    completedAt: Optional[datetime] = Field(None)
    attempts: int = Field(default=1, ge=1)
    lastError: Optional[str] = Field(None)
    downloadJobs: List[str] = Field(default_factory=list)
    storedFiles: List[str] = Field(default_factory=list)
    
    @property
    def is_completed(self) -> bool:
        return self.status == ProcessingStatus.COMPLETED
    
    @property
    def is_failed(self) -> bool:
        return self.status == ProcessingStatus.FAILED

class DiskUsageInfo(BaseModel):
    """Disk usage information"""
    totalGb: float = Field(..., ge=0)
    usedGb: float = Field(..., ge=0) 
    freeGb: float = Field(..., ge=0)
    usagePercent: float = Field(..., ge=0, le=100)
    path: str = Field(...)
    lastChecked: datetime = Field(default_factory=datetime.now)
    
    @property
    def is_low_space(self) -> bool:
        """Check if disk space is low"""
        return self.usagePercent > 85.0 or self.freeGb < 1.0

class CleanupSummary(BaseModel):
    """Summary of cleanup operations"""
    operation: str = Field(...)
    filesRemoved: int = Field(default=0, ge=0)
    bytesFreed: int = Field(default=0, ge=0)
    directoriesRemoved: int = Field(default=0, ge=0)
    errors: List[str] = Field(default_factory=list)
    duration: float = Field(default=0.0, ge=0)
    timestamp: datetime = Field(default_factory=datetime.now)
    
    @property
    def success_rate(self) -> float:
        """Calculate success rate percentage"""
        total_operations = self.filesRemoved + self.directoriesRemoved + len(self.errors)
        if total_operations == 0:
            return 100.0
        return ((total_operations - len(self.errors)) / total_operations) * 100

# Export all models
__all__ = [
    'ExplicitnessRating', 'VideoCodec', 'DownloadStatus', 'ProcessingStatus', 'StorageFormat',
    'ProfileLink', 'Author', 'VideoQuality', 'VideoPost', 'DownloadJob', 'SystemStatus',
    'StorageMetadata', 'DirectoryStructure', 'ProcessingRecord', 'DiskUsageInfo', 'CleanupSummary'
]