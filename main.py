#!/usr/bin/env python3
"""
Main script to run TikTok hashtag crawler and video metadata extractor in sequence.
"""

import sys
import os
import argparse
from pathlib import Path
import re

# Import from hashtag_crawler
from hashtag_crawler import HashtagCrawler, crawl_with_requests, SELENIUM_AVAILABLE as CRAWLER_SELENIUM_AVAILABLE

# Import from video_metadata_extractor
from video_metadata_extractor import TikTokVideoMetadataExtractor, load_links_from_json, SELENIUM_AVAILABLE as EXTRACTOR_SELENIUM_AVAILABLE


def export_filtered_videos_to_excel(videos, hashtag, base_output_path=None, excel_path=None):
    """Filter videos by hashtag (case-insensitive) and export to an Excel file."""
    if not hashtag:
        return
    
    normalized_target = hashtag.lstrip('#').lower()
    if not normalized_target:
        print("Filter hashtag is empty after stripping '#'. Skipping export.")
        return
    
    filtered = []
    for video in videos or []:
        if not isinstance(video, dict):
            continue
        tags = video.get('hashtags') or []
        normalized_tags = [
            str(tag).lstrip('#').lower()
            for tag in tags
            if isinstance(tag, str)
        ]
        if normalized_target in normalized_tags:
            filtered.append(video)
    
    if not filtered:
        print(f"No videos found containing hashtag '#{normalized_target}'. Skipping Excel export.")
        return
    
    try:
        from openpyxl import Workbook
    except ImportError:
        print("openpyxl is required to export Excel files. Install with: pip install openpyxl")
        return
    
    safe_tag = re.sub(r'[^A-Za-z0-9_]+', '_', normalized_target)
    if excel_path:
        excel_file = Path(excel_path)
    else:
        base_path = Path(base_output_path) if base_output_path else Path.cwd() / "filtered_metadata.json"
        excel_file = base_path.with_name(f"{base_path.stem}_{safe_tag}_filtered.xlsx")
    
    wb = Workbook()
    ws = wb.active
    ws.title = "Filtered Videos"
    
    headers = [
        "url", "username", "title", "description", "like_count",
        "comment_count", "share_count", "view_count", "archive_count",
        "hashtags", "error"
    ]
    ws.append(headers)
    
    for video in filtered:
        row = [
            video.get('url'),
            video.get('username'),
            video.get('title'),
            video.get('description'),
            video.get('like_count'),
            video.get('comment_count'),
            video.get('share_count'),
            video.get('view_count'),
            video.get('archive_count'),
            ', '.join(video.get('hashtags', [])),
            video.get('error'),
        ]
        ws.append(row)
    
    excel_file.parent.mkdir(parents=True, exist_ok=True)
    wb.save(excel_file)
    print(f"\nFiltered videos exported to: {excel_file}")


def main():
    parser = argparse.ArgumentParser(
        description='TikTok Hashtag Crawler and Metadata Extractor - Full Pipeline',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Crawl hashtag and extract metadata (default hashtag: nhuabinhminh)
  python main.py

  # Crawl specific hashtag with max videos limit
  python main.py myhashtag --max-videos 50

  # Run in headless mode with custom threads
  python main.py --headless --threads 5

  # Skip crawling, only extract metadata from existing file
  python main.py --skip-crawl --input tiktok_myhashtag_videos.json
        """
    )
    
    # Hashtag crawler arguments
    parser.add_argument('hashtag', nargs='?', default='nhuabinhminh',
                       help='Hashtag to crawl (default: nhuabinhminh)')
    parser.add_argument('--max-videos', type=int, default=None,
                       help='Max videos to collect from hashtag')
    parser.add_argument('--skip-crawl', action='store_true',
                       help='Skip crawling, only extract metadata from existing input file')
    parser.add_argument('--crawler-output', type=str, default=None,
                       help='Output file for crawler results (default: tiktok_{hashtag}_videos.json)')
    parser.add_argument('--no-fallback', action='store_true',
                       help='Disable automatic fallback warnings in crawler')
    parser.add_argument('--method', choices=['selenium', 'api'], default='selenium',
                       help='Crawling method to use (default: selenium)')
    
    # Metadata extractor arguments
    parser.add_argument('--input', '-i', type=str, default=None,
                       help='Input JSON file with video links (auto-generated from crawler if not provided)')
    parser.add_argument('--output', '-o', type=str, default=None,
                       help='Output JSON file for metadata (default: {input_file}_metadata.json)')
    parser.add_argument('--headless', action='store_true',
                       help='Run browser in headless mode (for both crawler and extractor)')
    parser.add_argument('--delay', type=float, default=2.0,
                       help='Delay between requests in seconds (default: 2.0)')
    parser.add_argument('--threads', '-t', type=int, default=5,
                       help='Number of parallel threads for metadata extraction (default: 5)')
    parser.add_argument('--no-threading', action='store_true',
                       help='Disable multi-threading in metadata extractor')
    parser.add_argument('--limit', type=int, default=None,
                       help='Limit number of videos to process in metadata extraction')
    parser.add_argument('--finalize', action='store_true',
                       help='After extraction, retry failed extractions in single-threaded mode')
    parser.add_argument('--filter-excel', type=str, default=None,
                       help='Optional Excel file path for filtered results')
    
    args = parser.parse_args()
    
    # Check if Selenium is available
    if not CRAWLER_SELENIUM_AVAILABLE and not args.skip_crawl:
        print("ERROR: undetected-chromedriver not installed!")
        print("Install with: pip install undetected-chromedriver selenium")
        return 1
    
    if not EXTRACTOR_SELENIUM_AVAILABLE:
        print("ERROR: Selenium packages not available for metadata extractor!")
        print("Install with: pip install undetected-chromedriver selenium")
        return 1
    
    crawler_output_file = None
    input_file = args.input
    
    # Step 1: Run hashtag crawler (unless skipped)
    if not args.skip_crawl:
        print("=" * 80)
        print("STEP 1: Crawling TikTok Hashtag")
        print("=" * 80)
        
        # Determine crawler output file
        if args.crawler_output:
            crawler_output_file = args.crawler_output
        else:
            crawler_output_file = f"tiktok_{args.hashtag}_videos.json"
        
        try:
            if args.method == 'api':
                video_links = crawl_with_requests(args.hashtag)
            else:
                crawler = HashtagCrawler(
                    headless=False,
                    auto_fallback=not args.no_fallback
                )
                video_links = crawler.crawl_hashtag(args.hashtag, args.max_videos)
            
            # Save crawler results
            import json
            import time
            result = {
                'hashtag': args.hashtag,
                'total_videos': len(video_links),
                'video_links': video_links,
                'crawl_timestamp': time.strftime('%Y-%m-%d %H:%M:%S'),
                'method': args.method
            }
            
            with open(crawler_output_file, 'w', encoding='utf-8') as f:
                json.dump(result, f, indent=2, ensure_ascii=False)
            
            print(f"\n✓ Crawler complete! Found {len(video_links)} videos")
            print(f"  Results saved to: {crawler_output_file}")
            
            if video_links:
                print(f"\n  First 5 video links:")
                for i, link in enumerate(video_links[:5], 1):
                    print(f"    {i}. {link}")
            
            # Use crawler output as input for metadata extractor
            input_file = crawler_output_file
            
        except Exception as e:
            print(f"\n✗ Error during crawling: {e}")
            import traceback
            traceback.print_exc()
            return 1
    else:
        # Skip crawling, use provided input file
        if not input_file:
            print("ERROR: --input is required when using --skip-crawl")
            parser.print_help()
            return 1
        
        if not os.path.exists(input_file):
            print(f"ERROR: Input file not found: {input_file}")
            return 1
        
        print("=" * 80)
        print("STEP 1: Skipped (using existing input file)")
        print("=" * 80)
        print(f"Using input file: {input_file}")
    
    # Step 2: Run metadata extractor
    print("\n" + "=" * 80)
    print("STEP 2: Extracting Video Metadata")
    print("=" * 80)
    
    # Determine metadata extractor output file
    if args.output:
        output_file = args.output
    else:
        input_path = Path(input_file)
        output_file = str(input_path.parent / f"{input_path.stem}_metadata.json")
    
    try:
        # Load video links
        print(f"\nLoading video links from: {input_file}")
        video_links = load_links_from_json(input_file)
        print(f"Loaded {len(video_links)} video links")
        
        if args.limit:
            video_links = video_links[:args.limit]
            print(f"Limited to {len(video_links)} videos")
        
        if not video_links:
            print("No video links found in input file!")
            return 1
        
        # Create extractor instance
        extractor = TikTokVideoMetadataExtractor(
            headless=args.headless,
            delay=args.delay,
            num_threads=args.threads
        )
        
        # Extract metadata using the threaded approach or sequential
        if args.no_threading:
            # Use sequential processing (extract_from_links will save automatically)
            print("\nUsing sequential processing (single thread)...")
            all_metadata = extractor.extract_from_links(
                video_links,
                output_file,
                use_threading=False
            )
            # Results are already saved by extract_from_links, skip manual save
            save_results = False
        else:
            save_results = True
            # Use threaded approach
            num_threads = args.threads
            chunk_size = len(video_links) // num_threads
            remainder = len(video_links) % num_threads
            
            chunks = []
            start_idx = 0
            for i in range(num_threads):
                current_chunk_size = chunk_size + (1 if i < remainder else 0)
                end_idx = start_idx + current_chunk_size
                chunks.append(video_links[start_idx:end_idx])
                start_idx = end_idx
            
            print(f"\nDividing {len(video_links)} videos into {num_threads} threads:")
            for i, chunk in enumerate(chunks, 1):
                print(f"  Thread {i}: {len(chunk)} videos")
            
            # Import threading components
            from threading import Lock, Thread
            import time
            
            # Create shared results list and lock
            all_metadata = []
            results_lock = Lock()
            
            # Create and start threads
            threads = []
            for thread_id in range(1, num_threads + 1):
                if len(chunks[thread_id - 1]) > 0:
                    from video_metadata_extractor import _process_chunk_in_thread
                    thread = Thread(
                        target=_process_chunk_in_thread,
                        args=(
                            chunks[thread_id - 1],
                            thread_id,
                            args.headless,
                            args.delay,
                            all_metadata,
                            results_lock
                        )
                    )
                    threads.append(thread)
                    thread.start()
                    time.sleep(1.5)  # Delay between thread starts
            
            # Wait for all threads to complete
            print(f"\nWaiting for all {len(threads)} threads to complete...")
            for thread in threads:
                thread.join()
            
            print(f"\nAll threads completed. Total results: {len(all_metadata)}")
        
        # Save results (only if using threading, sequential mode saves automatically)
        if save_results:
            extractor.save_results(all_metadata, output_file)
        
        # Print summary
        print(f"\n{'='*80}")
        print("Extraction Summary:")
        print(f"{'='*80}")
        print(f"Total videos processed: {len(all_metadata)}")
        successful = sum(1 for m in all_metadata if not m.get('error'))
        print(f"Successfully extracted: {successful}")
        print(f"Errors: {len(all_metadata) - successful}")
        print(f"\nResults saved to: {output_file}")
        
        # Show sample
        if all_metadata:
            print("\nSample extracted data:")
            sample = [m for m in all_metadata if not m.get('error')][:3]
            for m in sample:
                title = m.get('title') or 'N/A'
                title_display = title[:50] + '...' if isinstance(title, str) and len(title) > 50 else title
                print(f"  - @{m.get('username', 'N/A')}: {title_display}")
                print(f"    Likes: {m.get('like_count', 'N/A')}, Views: {m.get('view_count', 'N/A')}, Archive: {m.get('archive_count', 'N/A')}")
        
        # Finalize: retry errors if requested
        if args.finalize:
            print("\n" + "=" * 80)
            print("STEP 3: Finalizing (Retrying Failed Extractions)")
            print("=" * 80)
            extractor.finalize_and_retry_errors(output_file)
        
        print("\n" + "=" * 80)
        print("✓ Pipeline Complete!")
        print("=" * 80)
        print(f"  Crawler output: {input_file}")
        print(f"  Metadata output: {output_file}")
        
        if args.filter_excel:
            export_filtered_videos_to_excel(
                all_metadata,
                args.hashtag,
                base_output_path=output_file,
                excel_path=args.filter_excel
            )
        
    except FileNotFoundError:
        print(f"ERROR: Input file not found: {input_file}")
        return 1
    except Exception as e:
        print(f"ERROR: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0


if __name__ == '__main__':
    sys.exit(main())

