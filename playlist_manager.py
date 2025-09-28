
import os
import asyncio
from pathlib import Path
from typing import Dict, List, Optional, Any

class PlaylistManager:
    """Handles playlist creation and management"""
    
    @staticmethod
    def create_custom_playlist(m3u8_dir: Path, original_playlist_path: Path = None) -> Dict[str, Any]:
        """
        Create custom playlist.m3u8 based on directories with init.mp4 files
        Replaces the downloaded playlist with a custom one
        """
        try:
            print("\n📝 Creating custom playlist based on available init.mp4 files...")
            
            # Read original playlist if exists (for reference, but we'll create our own)
            original_content = ""
            if original_playlist_path and original_playlist_path.exists():
                with open(original_playlist_path, 'r', encoding='utf-8') as f:
                    original_content = f.read()
            
            # Find all quality directories that have init.mp4
            available_qualities = []
            
            # Check each subdirectory in m3u8_dir
            for subdir in m3u8_dir.iterdir():
                if not subdir.is_dir():
                    continue
                
                # Skip audio directory
                if subdir.name == "audio":
                    continue
                
                # Check if init.mp4 exists in this directory
                init_file = subdir / "init.mp4"
                if init_file.exists() and init_file.stat().st_size > 0:
                    # Check if video.m3u8 also exists
                    video_playlist = subdir / "video.m3u8"
                    if video_playlist.exists():
                        quality_info = PlaylistManager._parse_quality_info(subdir.name, original_content)
                        if quality_info:
                            quality_info['path'] = subdir.name
                            available_qualities.append(quality_info)
                            print(f"  ✓ Found {subdir.name} with init.mp4")
                else:
                    print(f"  ✗ Skipping {subdir.name} - no init.mp4")
            
            if not available_qualities:
                print("❌ No qualities with init.mp4 found")
                return {"success": False, "reason": "No qualities with init.mp4"}
            
            # Sort qualities by resolution (highest first)
            available_qualities.sort(key=lambda x: x.get('resolution_height', 0), reverse=True)
            
            # Create custom playlist content
            playlist_lines = ["#EXTM3U", "#EXT-X-VERSION:4", ""]
            
            # Add audio media definition if audio directory exists with init.mp4
            audio_dir = m3u8_dir / "audio"
            audio_init = audio_dir / "init.mp4"
            if audio_dir.exists() and audio_init.exists():
                playlist_lines.append('#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID="audio",NAME="English",DEFAULT=YES,AUTOSELECT=YES,LANGUAGE="en",URI="audio/audio.m3u8"')
                playlist_lines.append("")
            
            # Add each quality stream
            for quality in available_qualities:
                # Create stream info line
                stream_info = PlaylistManager._create_stream_info_line(quality)
                playlist_lines.append(stream_info)
                
                # Add the path to video.m3u8
                playlist_lines.append(f"{quality['path']}/video.m3u8")
                playlist_lines.append("")
            
            # Write custom playlist
            custom_playlist_path = m3u8_dir / "playlist.m3u8"
            custom_content = '\n'.join(playlist_lines).strip() + '\n'
            
            with open(custom_playlist_path, 'w', encoding='utf-8') as f:
                f.write(custom_content)
            
            print(f"✅ Custom playlist created with {len(available_qualities)} qualities")
            print(f"   Saved to: {custom_playlist_path}")
            
            return {
                "success": True,
                "playlist_path": str(custom_playlist_path),
                "qualities_included": [q['path'] for q in available_qualities],
                "total_qualities": len(available_qualities)
            }
            
        except Exception as e:
            print(f"❌ Error creating custom playlist: {e}")
            return {"success": False, "error": str(e)}
    
    @staticmethod
    def _parse_quality_info(dir_name: str, original_playlist_content: str) -> Optional[Dict[str, Any]]:
        """
        Parse quality information from directory name and original playlist
        """
        quality_info = {
            'path': dir_name,
            'resolution_height': 0,
            'bandwidth': 1000000,  # Default
            'average_bandwidth': 900000,  # Default
            'codecs': 'avc1.64001f,mp4a.40.2',  # Default H264 codecs
            'resolution': '720x1280',  # Default
            'frame_rate': '30.000',
            'audio': 'audio'
        }
        
        # Determine if VP9 or H264
        is_vp9 = dir_name.startswith('vp9_')
        
        # Extract resolution
        resolution_map = {
            '240p': (240, '196x352', 'avc1.64000d', 'vp09.00.11.08.00.01.01.01.00', 400000, 350000),
            '360p': (360, '357x640', 'avc1.64001e', 'vp09.00.21.08.00.01.01.01.00', 700000, 650000),
            '480p': (480, '476x854', 'avc1.64001f', 'vp09.00.30.08.00.01.01.01.00', 1000000, 900000),
            '720p': (720, '714x1280', 'avc1.64001f', 'vp09.00.31.08.00.01.01.01.00', 1800000, 1600000),
            '1080p': (1080, '1072x1920', 'avc1.640028', 'vp09.00.40.08.00.01.01.01.00', 4000000, 3700000),
        }
        
        # Find matching resolution
        for res_name, (height, dimensions, h264_codec, vp9_codec, bandwidth, avg_bandwidth) in resolution_map.items():
            if res_name in dir_name.lower():
                quality_info['resolution_height'] = height
                quality_info['resolution'] = dimensions
                quality_info['bandwidth'] = bandwidth
                quality_info['average_bandwidth'] = avg_bandwidth
                
                if is_vp9:
                    quality_info['codecs'] = f"{vp9_codec},mp4a.40.2"
                else:
                    quality_info['codecs'] = f"{h264_codec},mp4a.40.2"
                    quality_info['video_range'] = 'SDR'
                break
        
        # Try to find exact values from original playlist if available
        if original_playlist_content and dir_name in original_playlist_content:
            lines = original_playlist_content.split('\n')
            for i, line in enumerate(lines):
                if dir_name + '/video.m3u8' in line or (dir_name in line and i > 0):
                    # Check previous line for stream info
                    if i > 0 and lines[i-1].startswith('#EXT-X-STREAM-INF'):
                        stream_line = lines[i-1]
                        # Parse actual values
                        if 'BANDWIDTH=' in stream_line:
                            bandwidth_match = stream_line.split('BANDWIDTH=')[1].split(',')[0]
                            quality_info['bandwidth'] = int(bandwidth_match)
                        if 'AVERAGE-BANDWIDTH=' in stream_line:
                            avg_match = stream_line.split('AVERAGE-BANDWIDTH=')[1].split(',')[0]
                            quality_info['average_bandwidth'] = int(avg_match)
                        if 'RESOLUTION=' in stream_line:
                            res_match = stream_line.split('RESOLUTION=')[1].split(',')[0]
                            quality_info['resolution'] = res_match
                        if 'CODECS=' in stream_line:
                            codec_match = stream_line.split('CODECS="')[1].split('"')[0]
                            quality_info['codecs'] = codec_match
                        break
        
        return quality_info
    
    @staticmethod
    def _create_stream_info_line(quality_info: Dict[str, Any]) -> str:
        """
        Create #EXT-X-STREAM-INF line for playlist
        """
        parts = [
            f"BANDWIDTH={quality_info['bandwidth']}",
            f"AVERAGE-BANDWIDTH={quality_info['average_bandwidth']}",
        ]
        
        # Add video range for H264
        if quality_info.get('video_range'):
            parts.append(f"VIDEO-RANGE={quality_info['video_range']}")
        
        parts.extend([
            f'CODECS="{quality_info["codecs"]}"',
            f"RESOLUTION={quality_info['resolution']}",
            f"FRAME-RATE={quality_info['frame_rate']}",
            f'AUDIO="{quality_info["audio"]}"'
        ])
        
        return f"#EXT-X-STREAM-INF:{','.join(parts)}"


# Add this method to the VideoDownloaderOrganizer class
async def download_and_organize_post_with_custom_playlist(self, post_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Enhanced version that creates custom playlist after downloading
    """
    # First, run the original download process
    result = await self.original_download_and_organize_post(post_data)
    
    if result.get("success"):
        # After successful download, create custom playlist
        post_id = str(post_data.get("postId", "unknown"))
        m3u8_dir = self.base_download_path / post_id / "m3u8"
        
        if m3u8_dir.exists():
            # Create custom playlist based on init.mp4 availability
            playlist_result = PlaylistManager.create_custom_playlist(
                m3u8_dir,
                m3u8_dir / "playlist.m3u8"
            )
            
            result["custom_playlist"] = playlist_result
            
            if playlist_result.get("success"):
                print(f"✅ Custom playlist created for post {post_id}")
                print(f"   Included qualities: {', '.join(playlist_result['qualities_included'])}")
            else:
                print(f"⚠️ Could not create custom playlist for post {post_id}")
    
    return result