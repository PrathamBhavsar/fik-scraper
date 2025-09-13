# FikFap API Scraper - Technical Specification

## General Overview
Scraping system to download video content from the FikFap API, with support for multiple video qualities and formats.

## Base Configuration
- **API URL**: `api.fikfap.com`
- **Backend**: Python
- **Execution**: Compatible with cronjobs for automation

## Main Features

### 1. Video Download
- Complete video download in M3U8 format
- Support for all available qualities
- Download of master playlists and individual video fragments

### 2. Content Filtering and Processing

#### Codec Filtering
- **VP9 exclusion option**: Filter streams containing the `vp09` codec in their specifications
- **Automatic detection**: The system automatically identifies VP9 streams using:
  - `vp9_` prefix in playlist paths (e.g., `vp9_1080p/video.m3u8`)
  - `vp09.xx.xx.xx` codec in `#EXT-X-STREAM-INF` tags
- **Selective compatibility**: Option to download only H.264/AVC1 formats if required

#### M3U8 Playlist Processing
- **Path restructuring**: Original playlist paths are reorganized according to the defined folder structure
- **Transformation example**:
  ```
  # Original path in playlist:
  vp9_1080p/video.m3u8

  # Reorganized to:
  m3u8/vp9_1080p/video.m3u8
  ```
- **Reference update**: All internal references in playlists are updated to maintain link integrity

#### Master Playlist Analysis
The system processes playlists containing both VP9 and H.264 streams:

```m3u8
# VP9 Streams (CODECS="vp09.xx.xx.xx")
vp9_1080p/video.m3u8  # Resolution 1078x1920, 60fps
vp9_720p/video.m3u8   # Resolution 718x1280, 60fps
vp9_480p/video.m3u8   # Resolution 480x854, 30fps

# H.264/AVC1 Streams (CODECS="avc1.xxxxx")
1080p/video.m3u8      # Resolution 1078x1920, 60fps
720p/video.m3u8       # Resolution 718x1280, 60fps
480p/video.m3u8       # Resolution 480x854, 30fps
```

### 3. Metadata Storage
Each downloaded video includes a `data.json` file with all the information from the API's `video` object:

```json
{
  "postId": 1264563,
  "label": "Car fuck",
  "score": 4442,
  "likesCount": 4562,
  "userId": "dde5de56-e3df-4a9a-aa41-7706cde28e1d",
  "mediaId": "82018641-9671-4c60-b638-f8f295d2b3b8",
  "duration": null,
  "viewsCount": 283114,
  "bunnyVideoId": "d70a1994-5564-4a34-a133-ef80826cca05",
  "isBunnyVideoReady": true,
  "videoStreamUrl": "https://vz-5d293dac-178.b-cdn.net/...",
  "thumbnailStreamUrl": "https://vz-5d293dac-178.b-cdn.net/...",
  "publishedAt": "2025-07-22T00:07:39.339Z",
  "explicitnessRating": "FULLY_EXPLICIT",
  "author": {
    "userId": "dde5de56-e3df-4a9a-aa41-7706cde28e1d",
    "username": "VibeWithMommy",
    "isVerified": true,
    "isPartner": true,
    "description": "Home of the Hottest Kinkiest MILF!...",
    "thumbnailUrl": "https://fikfap-media-prod.b-cdn.net/...",
    "profileLinks": [...]
  },
  "hashtags": [...]
}
```

## File Structure

Each downloaded video is organized into the following directory structure:

```
283472/
├── data.json                 # Video metadata
└── m3u8/                    # Video files
    ├── playlist.m3u8        # Main playlist
    ├── 240p/                # 240p quality (H.264)
    │   ├── video.m3u8
    │   ├── video1.m4s
    │   └── video2.m4s
    ├── 360p/                # 360p quality (H.264)
    │   ├── video.m3u8
    │   ├── video1.m4s
    │   └── video2.m4s
    ├── 480p/                # 480p quality (H.264)
    │   ├── video.m3u8
    │   ├── video1.m4s
    │   └── video2.m4s
    ├── 720p/                # 720p quality (H.264)
    │   ├── video.m3u8
    │   ├── video1.m4s
    │   └── video2.m4s
    ├── 1080p/               # 1080p quality (H.264)
    │   ├── video.m3u8
    │   ├── video1.m4s
    │   └── video2.m4s
    ├── vp9_240p/            # 240p quality (VP9) *
    │   ├── video.m3u8
    │   ├── video1.m4s
    │   └── video2.m4s
    ├── vp9_360p/            # 360p quality (VP9) *
    ├── vp9_480p/            # 480p quality (VP9) *
    ├── vp9_720p/            # 720p quality (VP9) *
    └── vp9_1080p/           # 1080p quality (VP9) *
        ├── video.m3u8
        ├── video1.m4s
        └── video2.m4s
```

**Note**: Directories marked with `*` are optional and can be excluded based on configuration.

## Technical Features

### Automation
- **Cronjob compatible**: The system can run automatically using scheduled tasks
- **Batch processing**: Ability to process multiple videos sequentially

### File Management
- **ID-based organization**: Each video is stored in a unique directory identified by its `postId`
- **Metadata preservation**: All API information is retained in JSON format
- **Consistent structure**: Uniform organization to facilitate access and further processing

## Recommended Configurations

### For Development
- Include all formats (H.264 + VP9) for complete testing
- Detailed logging for debugging

### For Production
- Exclude VP9 formats if not needed (space saving)
- Implement retry logic for failed downloads
- Configure automatic cleanup of temporary files

###
- You must make sure that each video quality downloaded in .hls fragments is complete, otherwise it will give errors when we play it.

###
- You can download videos from your browser or with IDM, whichever you find most efficient.

###
- We should consider an administration UI, an alert to stop the scraper if the VPS disk is full.

**
then we will move all the fragments in order to an FTP, do you have any suggestions, should we integrate it into the scraper or is it better to do it manually?