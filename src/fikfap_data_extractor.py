import json
import re
from datetime import datetime
from typing import Dict, List, Any, Optional
from pathlib import Path

class FikFapDataExtractor:
    """
    Extracts post data from FikFap API responses and formats them into structured JSON
    """

    def __init__(self, api_capture_file: str = None):
        self.api_capture_file = api_capture_file
        self.raw_api_data = None
        self.extracted_posts = []

        if api_capture_file:
            self.load_api_data(api_capture_file)

    def load_api_data(self, file_path: str):
        """Load the API capture JSON file"""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                self.raw_api_data = json.load(f)
            print(f"✅ Loaded API data from: {file_path}")
            print(f"📊 Found {len(self.raw_api_data.get('api_responses', []))} API responses")
        except Exception as e:
            print(f"❌ Error loading API data: {e}")

    def identify_post_endpoints(self) -> List[Dict]:
        """Identify API endpoints that contain post data"""
        if not self.raw_api_data:
            return []

        post_endpoints = []

        for response in self.raw_api_data.get('api_responses', []):
            url = response.get('url', '')

            # Look for endpoints that likely contain post data
            if any(pattern in url for pattern in [
                '/posts', '/cached-high-quality', '/hashtags/', 
                'godpussy', 'trending', 'for-you'
            ]):
                if response.get('json') and response.get('status') == 200:
                    post_endpoints.append(response)

        print(f"🎯 Found {len(post_endpoints)} endpoints with post data")
        return post_endpoints

    def extract_posts_from_response(self, response_data: Dict) -> List[Dict]:
        """Extract individual posts from a single API response"""
        posts = []
        json_data = response_data.get('json')

        if not json_data:
            return posts

        # Handle different response structures
        if isinstance(json_data, list):
            # Direct array of posts
            posts.extend(self._process_post_array(json_data))
        elif isinstance(json_data, dict):
            # Look for posts in various nested structures
            for key, value in json_data.items():
                if key in ['posts', 'data', 'results', 'items'] and isinstance(value, list):
                    posts.extend(self._process_post_array(value))
                elif isinstance(value, list) and len(value) > 0:
                    # Check if this array contains post-like objects
                    if self._looks_like_post_array(value):
                        posts.extend(self._process_post_array(value))

        return posts

    def _looks_like_post_array(self, arr: List) -> bool:
        """Check if an array contains post-like objects"""
        if not arr or not isinstance(arr[0], dict):
            return False

        first_item = arr[0]
        post_indicators = ['postId', 'label', 'author', 'userId', 'mediaId', 'videoStreamUrl']

        return any(key in first_item for key in post_indicators)

    def _process_post_array(self, posts_array: List[Dict]) -> List[Dict]:
        """Process an array of raw post objects"""
        processed_posts = []

        for raw_post in posts_array:
            if isinstance(raw_post, dict):
                processed_post = self._extract_post_data(raw_post)
                if processed_post:
                    processed_posts.append(processed_post)

        return processed_posts

    def _extract_post_data(self, raw_post: Dict) -> Optional[Dict]:
        """Extract and structure data from a single raw post object - CUSTOMIZE THIS METHOD"""
        try:
            # Basic post info
            post_data = {
                'postId': self._safe_get(raw_post, 'postId'),
                'label': self._safe_get(raw_post, 'label', ''),
                'score': self._safe_get(raw_post, 'score', 0),
                'likesCount': self._safe_get(raw_post, 'likesCount', 0),
                'userId': self._safe_get(raw_post, 'userId'),
                'mediaId': self._safe_get(raw_post, 'mediaId'),
                'duration': self._safe_get(raw_post, 'duration'),
                'viewsCount': self._safe_get(raw_post, 'viewsCount', 0),
                'bunnyVideoId': self._safe_get(raw_post, 'bunnyVideoId'),
                'isBunnyVideoReady': self._safe_get(raw_post, 'isBunnyVideoReady', False),
                'videoStreamUrl': self._safe_get(raw_post, 'videoStreamUrl'),
                'thumbnailStreamUrl': self._safe_get(raw_post, 'thumbnailStreamUrl'),
                'publishedAt': self._safe_get(raw_post, 'publishedAt'),
                'explicitnessRating': self._safe_get(raw_post, 'explicitnessRating'),
            }

            # Author information
            author_data = raw_post.get('author', {})
            if author_data:
                post_data['author'] = self._extract_author_data(author_data)

            # Hashtags
            hashtags_data = raw_post.get('hashtags', [])
            if hashtags_data:
                post_data['hashtags'] = self._extract_hashtags_data(hashtags_data)

            # Additional fields that might be useful
            additional_fields = [
                'createdAt', 'updatedAt', 'inCollectionsCount', 
                'commentsCount', 'sexualOrientation', 'uploadMethod'
            ]

            for field in additional_fields:
                if field in raw_post:
                    post_data[field] = raw_post[field]

            return post_data

        except Exception as e:
            print(f"⚠️ Error extracting post data: {e}")
            return None

    def _extract_author_data(self, author: Dict) -> Dict:
        """Extract author information - CUSTOMIZE THIS METHOD"""
        return {
            'userId': self._safe_get(author, 'userId'),
            'username': self._safe_get(author, 'username', ''),
            'isVerified': self._safe_get(author, 'isVerified', False),
            'isPartner': self._safe_get(author, 'isPartner', False),
            'description': self._safe_get(author, 'description', ''),
            'thumbnailUrl': self._safe_get(author, 'thumbnailUrl'),
            'countPosts': self._safe_get(author, 'countPosts', 0),
            'countIncomingLikes': self._safe_get(author, 'countIncomingLikes', 0),
            'countIncomingFollows': self._safe_get(author, 'countIncomingFollows', 0),
            'countTotalViews': self._safe_get(author, 'countTotalViews', 0),
            'profileLinks': self._safe_get(author, 'profileLinks', [])
        }

    def _extract_hashtags_data(self, hashtags: List[Dict]) -> List[Dict]:
        """Extract hashtags information - CUSTOMIZE THIS METHOD"""
        extracted_hashtags = []

        for hashtag in hashtags:
            if isinstance(hashtag, dict):
                extracted_hashtags.append({
                    'hashtagId': self._safe_get(hashtag, 'hashtagId'),
                    'label': self._safe_get(hashtag, 'label', ''),
                    'description': self._safe_get(hashtag, 'description', ''),
                    'countPosts': self._safe_get(hashtag, 'countPosts', 0),
                    'countFollows': self._safe_get(hashtag, 'countFollows', 0)
                })

        return extracted_hashtags

    def _safe_get(self, data: Dict, key: str, default=None):
        """Safely get a value from dictionary"""
        return data.get(key, default)

    def extract_all_posts(self) -> List[Dict]:
        """Main method to extract all posts from loaded API data"""
        if not self.raw_api_data:
            print("❌ No API data loaded")
            return []

        print("🔍 Starting post extraction...")

        # Find all endpoints with post data
        post_endpoints = self.identify_post_endpoints()

        all_posts = []

        for i, endpoint in enumerate(post_endpoints, 1):
            url = endpoint.get('url', '')
            print(f"[{i}/{len(post_endpoints)}] Processing: {url}")

            posts = self.extract_posts_from_response(endpoint)
            all_posts.extend(posts)
            print(f"  ➤ Extracted {len(posts)} posts")

        # Remove duplicates based on postId
        unique_posts = self._remove_duplicates(all_posts)

        self.extracted_posts = unique_posts

        print(f"\n✅ Extraction completed!")
        print(f"📊 Total unique posts extracted: {len(unique_posts)}")

        return unique_posts

    def _remove_duplicates(self, posts: List[Dict]) -> List[Dict]:
        """Remove duplicate posts based on postId"""
        seen_ids = set()
        unique_posts = []

        for post in posts:
            post_id = post.get('postId')
            if post_id and post_id not in seen_ids:
                seen_ids.add(post_id)
                unique_posts.append(post)

        duplicates_removed = len(posts) - len(unique_posts)
        if duplicates_removed > 0:
            print(f"🔄 Removed {duplicates_removed} duplicate posts")

        return unique_posts

    def filter_posts(self, posts: List[Dict] = None, **filters) -> List[Dict]:
        """
        Filter posts based on criteria - CUSTOMIZE THIS METHOD

        Available filters:
        - min_score: minimum score
        - min_likes: minimum likes
        - min_views: minimum views
        - verified_authors_only: only verified authors
        - partner_authors_only: only partner authors
        - explicitness_rating: specific explicitness rating
        - hashtag_labels: list of hashtag labels to include
        """
        if posts is None:
            posts = self.extracted_posts

        filtered_posts = posts

        # Apply filters
        if 'min_score' in filters:
            filtered_posts = [p for p in filtered_posts if p.get('score', 0) >= filters['min_score']]

        if 'min_likes' in filters:
            filtered_posts = [p for p in filtered_posts if p.get('likesCount', 0) >= filters['min_likes']]

        if 'min_views' in filters:
            filtered_posts = [p for p in filtered_posts if p.get('viewsCount', 0) >= filters['min_views']]

        if filters.get('verified_authors_only'):
            filtered_posts = [p for p in filtered_posts if p.get('author', {}).get('isVerified', False)]

        if filters.get('partner_authors_only'):
            filtered_posts = [p for p in filtered_posts if p.get('author', {}).get('isPartner', False)]

        if 'explicitness_rating' in filters:
            filtered_posts = [p for p in filtered_posts if p.get('explicitnessRating') == filters['explicitness_rating']]

        if 'hashtag_labels' in filters:
            hashtag_labels = filters['hashtag_labels']
            filtered_posts = [
                p for p in filtered_posts 
                if any(
                    h.get('label', '').lower() in [hl.lower() for hl in hashtag_labels]
                    for h in p.get('hashtags', [])
                )
            ]

        print(f"🔽 Filtered from {len(posts)} to {len(filtered_posts)} posts")
        return filtered_posts

    def sort_posts(self, posts: List[Dict] = None, by: str = 'score', reverse: bool = True) -> List[Dict]:
        """
        Sort posts by specified criteria - CUSTOMIZE THIS METHOD

        Available sorting options:
        - score, likesCount, viewsCount, publishedAt
        """
        if posts is None:
            posts = self.extracted_posts

        def get_sort_value(post):
            if by == 'publishedAt':
                # Handle datetime sorting
                date_str = post.get(by)
                if date_str:
                    try:
                        return datetime.fromisoformat(date_str.replace('Z', '+00:00'))
                    except:
                        return datetime.min
                return datetime.min
            else:
                return post.get(by, 0)

        sorted_posts = sorted(posts, key=get_sort_value, reverse=reverse)
        print(f"📊 Sorted {len(sorted_posts)} posts by {by} ({'desc' if reverse else 'asc'})")

        return sorted_posts

    def save_extracted_data(self, posts: List[Dict] = None, filename: str = None) -> str:
        """Save extracted posts to JSON file"""
        if posts is None:
            posts = self.extracted_posts

        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"extracted_posts_{timestamp}.json"

        # Prepare the output data
        output_data = {
            'extraction_info': {
                'timestamp': datetime.now().isoformat(),
                'total_posts': len(posts),
                'source_file': self.api_capture_file
            },
            'posts': posts
        }

        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(output_data, f, indent=2, ensure_ascii=False, default=str)

            print(f"💾 Saved {len(posts)} posts to: {filename}")
            return filename

        except Exception as e:
            print(f"❌ Error saving data: {e}")
            return None

    def get_extraction_summary(self, posts: List[Dict] = None) -> Dict:
        """Get summary statistics of extracted posts"""
        if posts is None:
            posts = self.extracted_posts

        if not posts:
            return {'total_posts': 0}

        # Calculate statistics
        scores = [p.get('score', 0) for p in posts]
        likes = [p.get('likesCount', 0) for p in posts]
        views = [p.get('viewsCount', 0) for p in posts]

        # Count by explicitness rating
        explicitness_counts = {}
        for post in posts:
            rating = post.get('explicitnessRating', 'UNKNOWN')
            explicitness_counts[rating] = explicitness_counts.get(rating, 0) + 1

        # Count verified authors
        verified_authors = sum(1 for p in posts if p.get('author', {}).get('isVerified', False))
        partner_authors = sum(1 for p in posts if p.get('author', {}).get('isPartner', False))

        # Get unique hashtags
        all_hashtags = set()
        for post in posts:
            for hashtag in post.get('hashtags', []):
                all_hashtags.add(hashtag.get('label', ''))

        summary = {
            'total_posts': len(posts),
            'score_stats': {
                'min': min(scores) if scores else 0,
                'max': max(scores) if scores else 0,
                'avg': sum(scores) / len(scores) if scores else 0
            },
            'likes_stats': {
                'min': min(likes) if likes else 0,
                'max': max(likes) if likes else 0,
                'avg': sum(likes) / len(likes) if likes else 0
            },
            'views_stats': {
                'min': min(views) if views else 0,
                'max': max(views) if views else 0,
                'avg': sum(views) / len(views) if views else 0
            },
            'explicitness_breakdown': explicitness_counts,
            'verified_authors': verified_authors,
            'partner_authors': partner_authors,
            'unique_hashtags': len(all_hashtags),
            'top_hashtags': list(all_hashtags)[:20]  # First 20
        }

        return summary

    def print_summary(self, posts: List[Dict] = None):
        """Print a formatted summary of extracted posts"""
        summary = self.get_extraction_summary(posts)

        print("\n" + "="*80)
        print("📊 EXTRACTION SUMMARY")
        print("="*80)
        print(f"Total Posts: {summary['total_posts']}")

        if summary['total_posts'] > 0:
            print(f"\n📈 SCORE STATS:")
            print(f"  Min: {summary['score_stats']['min']:,}")
            print(f"  Max: {summary['score_stats']['max']:,}")
            print(f"  Avg: {summary['score_stats']['avg']:,.1f}")

            print(f"\n❤️ LIKES STATS:")
            print(f"  Min: {summary['likes_stats']['min']:,}")
            print(f"  Max: {summary['likes_stats']['max']:,}")
            print(f"  Avg: {summary['likes_stats']['avg']:,.1f}")

            print(f"\n👀 VIEWS STATS:")
            print(f"  Min: {summary['views_stats']['min']:,}")
            print(f"  Max: {summary['views_stats']['max']:,}")
            print(f"  Avg: {summary['views_stats']['avg']:,.1f}")

            print(f"\n🔞 EXPLICITNESS BREAKDOWN:")
            for rating, count in summary['explicitness_breakdown'].items():
                print(f"  {rating}: {count}")

            print(f"\n👤 AUTHORS:")
            print(f"  Verified: {summary['verified_authors']}")
            print(f"  Partners: {summary['partner_authors']}")

            print(f"\n🏷️ HASHTAGS:")
            print(f"  Unique hashtags: {summary['unique_hashtags']}")
            if summary['top_hashtags']:
                print(f"  Top tags: {', '.join(summary['top_hashtags'][:10])}")

        print("="*80)


# Example usage and customization guide
if __name__ == "__main__":
    # Basic usage
    extractor = FikFapDataExtractor("your_api_capture_file.json")
    posts = extractor.extract_all_posts()

    # Filter posts
    high_quality_posts = extractor.filter_posts(
        min_score=5000,
        min_likes=3000,
        verified_authors_only=True
    )

    # Sort posts
    top_posts = extractor.sort_posts(high_quality_posts, by='viewsCount', reverse=True)

    # Save results
    extractor.save_extracted_data(top_posts, "high_quality_posts.json")
    extractor.print_summary(top_posts)