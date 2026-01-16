import sys
import pandas as pd  # <--- Added this import
from utils.file_handler import read_sales_data, save_clean_data, save_enriched_data, save_report
from utils.data_processor import (
    parse_transactions,
    validate_and_filter,
    generate_sales_report,
    enrich_sales_data
)
from utils.api_handler import fetch_all_products, create_product_mapping

def get_user_filters(parsed_data):
    """
    Handles User Interaction for filtering.
    """
    # Calculate stats for display
    regions = sorted(list(set(t['Region'] for t in parsed_data if t['Region'])))
    amounts = [t['Quantity'] * t['UnitPrice'] for t in parsed_data]
    min_amt = min(amounts) if amounts else 0
    max_amt = max(amounts) if amounts else 0

    print(f"\n   [3/10] Filter Options Available:")
    print(f"   Regions: {', '.join(regions)}")
    print(f"   Amount Range: ${min_amt:,.2f} - ${max_amt:,.2f}")
    
    choice = input("\n   Do you want to filter data? (y/n): ").strip().lower()
    
    if choice == 'y':
        print("\n   --- Enter Filter Criteria (Press Enter to skip) ---")
        
        # Region Input
        region_input = input("   Region: ").strip()
        region_filter = region_input if region_input else None
        
        # Min Amount Input
        min_input = input("   Min Amount: ").strip()
        try:
            min_filter = float(min_input) if min_input else None
        except ValueError:
            print("   (Invalid number, ignoring min amount)")
            min_filter = None
            
        # Max Amount Input
        max_input = input("   Max Amount: ").strip()
        try:
            max_filter = float(max_input) if max_input else None
        except ValueError:
            print("   (Invalid number, ignoring max amount)")
            max_filter = None
            
        return region_filter, min_filter, max_filter
    
    return None, None, None

def main():
    # Define paths
    INPUT_PATH = 'data/sales_data.txt'
    OUTPUT_REPORT_PATH = 'output/sales_report.txt'
    OUTPUT_DATA_PATH = 'output/cleaned_sales_data.csv'
    OUTPUT_ENRICHED_PATH = 'data/enriched_sales_data.txt'

    print("="*40)
    print("      SALES ANALYTICS SYSTEM")
    print("="*40 + "\n")

    try:
        # --- STEP 1 & 2: READ DATA ---
        print("   [1/10] Reading sales data...")
        raw_lines = read_sales_data(INPUT_PATH)
        if not raw_lines:
            print("   X Error: No data found. Exiting.")
            return
        print(f"   ✓ Successfully read {len(raw_lines)} transactions")

        # --- STEP 3: PARSE DATA ---
        print("\n   [2/10] Parsing and cleaning data...")
        parsed_data = parse_transactions(raw_lines)
        print(f"   ✓ Parsed {len(parsed_data)} records")

        # --- STEP 4 & 5: USER INTERACTION (FILTERS) ---
        region_filter, min_filter, max_filter = get_user_filters(parsed_data)

        # --- STEP 6 & 7: VALIDATE & FILTER ---
        print("\n   [4/10] Validating transactions...")
        valid_data, invalid_count, summary = validate_and_filter(
            parsed_data, 
            region=region_filter, 
            min_amount=min_filter, 
            max_amount=max_filter
        )
        print(f"   ✓ Valid: {len(valid_data)} | Invalid: {invalid_count}")
        
        if summary['filtered_by_region'] > 0 or summary['filtered_by_amount'] > 0:
            print(f"   ✓ Filtered out: {summary['filtered_by_region'] + summary['filtered_by_amount']} records")

        if not valid_data:
            print("\n   X Error: No valid data left after processing. Exiting.")
            return

        # --- STEP 8: ANALYSIS ---
        print("\n   [5/10] Analyzing sales data...")
        print("   ✓ Analysis complete")

        # --- STEP 9: API FETCH ---
        print("\n   [6/10] Fetching product data from API...")
        api_products = fetch_all_products()
        product_map = {}
        if api_products:
            product_map = create_product_mapping(api_products)
            print(f"   ✓ Fetched {len(api_products)} products")
        else:
            print("   ! Warning: API fetch failed. Continuing without enrichment.")

        # --- STEP 10: ENRICHMENT ---
        print("\n   [7/10] Enriching sales data...")
        enriched_data = enrich_sales_data(valid_data, product_map)
        
        enriched_count = sum(1 for t in enriched_data if t.get('API_Match'))
        if valid_data:
            percent = (enriched_count / len(valid_data)) * 100
            print(f"   ✓ Enriched {enriched_count}/{len(valid_data)} transactions ({percent:.1f}%)")

        # --- STEP 11: SAVE ENRICHED DATA ---
        print("\n   [8/10] Saving enriched data...")
        save_enriched_data(enriched_data, OUTPUT_ENRICHED_PATH)
        print(f"   ✓ Saved to: {OUTPUT_ENRICHED_PATH}")

        # --- STEP 12: GENERATE REPORT ---
        print("\n   [9/10] Generating report...")
        generate_sales_report(valid_data, enriched_data, OUTPUT_REPORT_PATH)
        
        # --- FIX: Convert list to DataFrame before saving CSV ---
        df_clean = pd.DataFrame(valid_data)
        save_clean_data(df_clean, OUTPUT_DATA_PATH)
        
        print(f"   ✓ Report saved to: {OUTPUT_REPORT_PATH}")

        # --- FINAL SUCCESS ---
        print("\n   [10/10] Process Complete!")
        print("="*40)

    except Exception as e:
        print(f"\n   X CRITICAL ERROR: {e}")
        # import traceback
        # traceback.print_exc()

if __name__ == "__main__":
    main()