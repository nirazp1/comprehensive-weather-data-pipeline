#!/usr/bin/env python3
"""
Generate 5 Years of Historical Data and Statistics
Creates comprehensive 5-year datasets and generates statistics.
"""
import sys
from pathlib import Path
from datetime import datetime, timedelta, timezone

# Add project root and src to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(project_root / "src"))

from src.data_generator import generate_millions_of_records
from src.statistics import load_all_data, generate_statistics_report
from src.config import settings

def main():
    print("\n" + "="*70)
    print("🌐 5-YEAR HISTORICAL DATA GENERATOR & STATISTICS")
    print("="*70)
    print()
    
    # Configuration
    num_cities = 30  # All major American cities including Cincinnati
    years = 5
    days = years * 365
    frequency = "hourly"
    
    print(f"Configuration:")
    print(f"  Cities: {num_cities} (American cities, Cincinnati first!)")
    print(f"  Years: {years} years")
    print(f"  Days: {days:,} days")
    print(f"  Frequency: {frequency}")
    print(f"  Expected Records: {num_cities * days * 24:,} ({num_cities * days * 24 / 1_000_000:.2f} million)")
    print()
    
    # Output directory
    output_dir = settings.processed_data_dir / "5year_historical"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    print(f"Output Directory: {output_dir}")
    print()
    
    # Confirm
    response = input("Generate 5 years of data? This may take a while. (yes/no): ")
    if response.lower() not in ['yes', 'y']:
        print("Cancelled.")
        return
    
    print("\n🚀 Starting data generation...\n")
    
    # Progress callback
    start_time = datetime.now()
    last_update = 0
    
    def progress_callback(current: int, total: int):
        nonlocal last_update
        if current - last_update >= 100000 or current == total:
            progress = (current / total * 100) if total > 0 else 0
            elapsed = (datetime.now() - start_time).total_seconds()
            rate = current / elapsed if elapsed > 0 else 0
            remaining = (total - current) / rate if rate > 0 else 0
            
            print(f"\r🔄 Progress: {current:,} / {total:,} ({progress:.1f}%) | "
                  f"Rate: {rate:,.0f} rec/s | ETA: {remaining/60:.1f} min", end="", flush=True)
            last_update = current
    
    # Generate data
    try:
        total_records, saved_files = generate_millions_of_records(
            num_cities=num_cities,
            days=days,
            frequency=frequency,
            output_dir=output_dir,
            progress_callback=progress_callback
        )
        
        elapsed = (datetime.now() - start_time).total_seconds()
        
        print("\n" + "="*70)
        print("✅ DATA GENERATION COMPLETE!")
        print("="*70)
        print(f"  Total Records: {total_records:,}")
        print(f"  Files Created: {len(saved_files):,}")
        print(f"  Time Elapsed: {elapsed/60:.2f} minutes")
        print(f"  Generation Rate: {total_records/elapsed:,.0f} records/second")
        print()
        
        # Generate statistics
        print("📊 Generating statistics...")
        print()
        
        df = load_all_data(output_dir)
        
        if not df.empty:
            stats_file = generate_statistics_report(df, output_dir / "statistics")
            
            print("\n" + "="*70)
            print("✅ STATISTICS GENERATED!")
            print("="*70)
            print(f"  Statistics File: {stats_file}")
            print(f"  Summary File: {stats_file.parent / stats_file.name.replace('.json', '_summary.txt')}")
            print()
            
            # Show sample statistics for Cincinnati
            from src.statistics import calculate_city_statistics
            cinci_stats = calculate_city_statistics(df, "Cincinnati")
            
            if cinci_stats:
                print("⭐ CINCINNATI STATISTICS (5 Years):")
                print("-" * 70)
                print(f"  Total Records: {cinci_stats.get('total_records', 0):,}")
                if 'temperature' in cinci_stats:
                    temp = cinci_stats['temperature']
                    print(f"  Average Temperature: {temp.get('mean', 0):.2f}°C")
                    print(f"  Temperature Range: {temp.get('min', 0):.2f}°C to {temp.get('max', 0):.2f}°C")
                if 'seasonal_temperature' in cinci_stats:
                    print(f"  Seasonal Averages:")
                    for season, data in cinci_stats['seasonal_temperature'].items():
                        print(f"    {season}: {data.get('mean', 0):.2f}°C")
                print()
        else:
            print("⚠️  No data loaded for statistics")
        
        print("="*70)
        print("✅ Complete!")
        print()
        print("💡 Next Steps:")
        print(f"  • View statistics: {output_dir / 'statistics'}")
        print(f"  • Use Streamlit app: streamlit run web/streamlit_app.py")
        print(f"  • Analyze data: python3 -c \"from src.statistics import *; ...\"")
        print()
        
    except KeyboardInterrupt:
        print("\n\n❌ Generation interrupted by user.")
        sys.exit(1)
    except Exception as e:
        print(f"\n\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()

