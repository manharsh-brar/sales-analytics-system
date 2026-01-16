import pandas as pd
import datetime
import os

# ==========================================
#          TASK 1: DATA PROCESSING
# ==========================================

def parse_transactions(raw_lines):
    cleaned_data = []
    for line in raw_lines:
        parts = line.strip().split('|')
        if len(parts) != 8:
            continue
            
        t_id, date, p_id, p_name, qty_str, price_str, c_id, region = parts
        clean_p_name = p_name.replace(',', ' ')
        try:
            qty = int(qty_str.replace(',', ''))
            price = float(price_str.replace(',', ''))
        except ValueError:
            continue

        row = {
            'TransactionID': t_id, 'Date': date, 'ProductID': p_id,
            'ProductName': clean_p_name, 'Quantity': qty, 'UnitPrice': price,
            'CustomerID': c_id, 'Region': region
        }
        cleaned_data.append(row)
    return cleaned_data

def validate_and_filter(transactions, region=None, min_amount=None, max_amount=None):
    valid_pool = []
    invalid_count = 0
    for t in transactions:
        is_valid = (t['Quantity'] > 0 and t['UnitPrice'] > 0 and
                    t['TransactionID'].startswith('T') and t['ProductID'].startswith('P') and
                    t['CustomerID'].startswith('C') and t['Region'].strip() != '')
        if is_valid:
            valid_pool.append(t)
        else:
            invalid_count += 1
            
    filtered_list = []
    filter_region_count = 0
    filter_amount_count = 0
    
    for t in valid_pool:
        if region and t['Region'] != region:
            filter_region_count += 1
            continue
        amount = t['Quantity'] * t['UnitPrice']
        if min_amount is not None and amount < min_amount:
            filter_amount_count += 1
            continue
        if max_amount is not None and amount > max_amount:
            filter_amount_count += 1
            continue
        filtered_list.append(t)

    summary = {
        'total_input': len(transactions), 'invalid': invalid_count,
        'filtered_by_region': filter_region_count, 'filtered_by_amount': filter_amount_count,
        'final_count': len(filtered_list)
    }
    return filtered_list, invalid_count, summary

# ==========================================
#          TASK 2: ANALYTICS
# ==========================================

def calculate_total_revenue(transactions):
    return sum(t['Quantity'] * t['UnitPrice'] for t in transactions)

def region_wise_sales(transactions):
    stats = {}
    total_revenue = calculate_total_revenue(transactions)
    for t in transactions:
        r = t['Region']
        sale = t['Quantity'] * t['UnitPrice']
        if r not in stats: stats[r] = {'total_sales': 0.0, 'transaction_count': 0}
        stats[r]['total_sales'] += sale
        stats[r]['transaction_count'] += 1
        
    for r in stats:
        sales = stats[r]['total_sales']
        stats[r]['percentage'] = round((sales / total_revenue) * 100, 2) if total_revenue > 0 else 0.0
            
    return dict(sorted(stats.items(), key=lambda item: item[1]['total_sales'], reverse=True))

def top_selling_products(transactions, n=5):
    product_stats = {}
    for t in transactions:
        p_name = t['ProductName']
        qty = t['Quantity']
        revenue = qty * t['UnitPrice']
        if p_name not in product_stats: product_stats[p_name] = {'qty': 0, 'revenue': 0.0}
        product_stats[p_name]['qty'] += qty
        product_stats[p_name]['revenue'] += revenue
        
    result_list = [(k, v['qty'], v['revenue']) for k, v in product_stats.items()]
    return sorted(result_list, key=lambda x: x[1], reverse=True)[:n]

def customer_analysis(transactions):
    customer_stats = {}
    for t in transactions:
        c_id = t['CustomerID']
        amt = t['Quantity'] * t['UnitPrice']
        prod = t['ProductName']
        if c_id not in customer_stats:
            customer_stats[c_id] = {'total_spent': 0.0, 'purchase_count': 0, 'products_bought': set()}
        customer_stats[c_id]['total_spent'] += amt
        customer_stats[c_id]['purchase_count'] += 1
        customer_stats[c_id]['products_bought'].add(prod)
        
    for c_id, stats in customer_stats.items():
        stats['avg_order_value'] = round(stats['total_spent'] / stats['purchase_count'], 2) if stats['purchase_count'] > 0 else 0.0
        stats['products_bought'] = list(stats['products_bought'])
        
    return dict(sorted(customer_stats.items(), key=lambda item: item[1]['total_spent'], reverse=True))

def daily_sales_trend(transactions):
    date_stats = {}
    for t in transactions:
        date = t['Date']
        revenue = t['Quantity'] * t['UnitPrice']
        c_id = t['CustomerID']
        if date not in date_stats:
            date_stats[date] = {'revenue': 0.0, 'transaction_count': 0, 'unique_customers_set': set()}
        date_stats[date]['revenue'] += revenue
        date_stats[date]['transaction_count'] += 1
        date_stats[date]['unique_customers_set'].add(c_id)
        
    final_stats = {}
    for date in sorted(date_stats.keys()):
        stats = date_stats[date]
        final_stats[date] = {
            'revenue': stats['revenue'],
            'transaction_count': stats['transaction_count'],
            'unique_customers': len(stats['unique_customers_set'])
        }
    return final_stats

def find_peak_sales_day(transactions):
    daily_stats = daily_sales_trend(transactions)
    if not daily_stats: return None
    peak_date = max(daily_stats, key=lambda d: daily_stats[d]['revenue'])
    return (peak_date, daily_stats[peak_date]['revenue'], daily_stats[peak_date]['transaction_count'])

def low_performing_products(transactions, threshold=10):
    product_stats = {}
    for t in transactions:
        p_name = t['ProductName']
        qty = t['Quantity']
        revenue = qty * t['UnitPrice']
        if p_name not in product_stats: product_stats[p_name] = {'qty': 0, 'revenue': 0.0}
        product_stats[p_name]['qty'] += qty
        product_stats[p_name]['revenue'] += revenue
        
    result_list = [(k, v['qty'], v['revenue']) for k, v in product_stats.items() if v['qty'] < threshold]
    return sorted(result_list, key=lambda x: x[1])

# ==========================================
#          TASK 3: API ENRICHMENT
# ==========================================

def enrich_sales_data(transactions, product_mapping):
    enriched_list = []
    for t in transactions:
        row = t.copy()
        try:
            numeric_id = int(row['ProductID'].upper().replace('P', ''))
        except ValueError:
            numeric_id = -1
            
        if numeric_id in product_mapping:
            info = product_mapping[numeric_id]
            row.update({'API_Category': info['category'], 'API_Brand': info['brand'], 'API_Rating': info['rating'], 'API_Match': True})
        else:
            row.update({'API_Category': None, 'API_Brand': None, 'API_Rating': None, 'API_Match': False})
        enriched_list.append(row)
    return enriched_list

# ==========================================
#          TASK 4: REPORT GENERATION
# ==========================================

def generate_sales_report(transactions, enriched_transactions, output_file='output/sales_report.txt'):
    """
    Generates a comprehensive formatted text report.
    """
    if not transactions:
        print("No transactions to report.")
        return

    # --- CALCULATIONS ---
    total_rev = calculate_total_revenue(transactions)
    total_txns = len(transactions)
    avg_order_val = total_rev / total_txns if total_txns > 0 else 0
    
    dates = [t['Date'] for t in transactions]
    date_range = f"{min(dates)} to {max(dates)}" if dates else "N/A"
    
    region_stats = region_wise_sales(transactions)
    top_prods = top_selling_products(transactions, n=5)
    
    cust_stats = customer_analysis(transactions)
    # Get top 5 customers
    top_custs = list(cust_stats.items())[:5]
    
    daily_stats = daily_sales_trend(transactions)
    peak_day = find_peak_sales_day(transactions)
    low_prods = low_performing_products(transactions, threshold=10)
    
    # Enrichment Stats
    total_enriched = len(enriched_transactions)
    success_count = sum(1 for t in enriched_transactions if t.get('API_Match') == True)
    success_rate = (success_count / total_enriched * 100) if total_enriched > 0 else 0
    missing_prods = list(set(t['ProductName'] for t in enriched_transactions if t.get('API_Match') == False))[:5]

    # --- REPORT BUILDING ---
    lines = []
    lines.append("="*50)
    lines.append(f"{'SALES ANALYTICS REPORT':^50}")
    lines.append(f"{'Generated: ' + datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'):^50}")
    lines.append(f"{'Records Processed: ' + str(total_txns):^50}")
    lines.append("="*50 + "\n")

    # 2. OVERALL SUMMARY
    lines.append("OVERALL SUMMARY")
    lines.append("-" * 50)
    lines.append(f"Total Revenue:       ${total_rev:,.2f}")
    lines.append(f"Total Transactions:  {total_txns}")
    lines.append(f"Average Order Value: ${avg_order_val:,.2f}")
    lines.append(f"Date Range:          {date_range}\n")

    # 3. REGION-WISE PERFORMANCE
    lines.append("REGION-WISE PERFORMANCE")
    lines.append("-" * 65)
    lines.append(f"{'Region':<15} {'Sales':<15} {'% of Total':<15} {'Transactions':<15}")
    lines.append("-" * 65)
    for reg, data in region_stats.items():
        lines.append(f"{reg:<15} ${data['total_sales']:<14,.2f} {data['percentage']:<14}% {data['transaction_count']:<15}")
    lines.append("")

    # 4. TOP 5 PRODUCTS
    lines.append("TOP 5 PRODUCTS")
    lines.append("-" * 65)
    lines.append(f"{'Rank':<5} {'Product Name':<25} {'Quantity':<10} {'Revenue':<15}")
    lines.append("-" * 65)
    for idx, (name, qty, rev) in enumerate(top_prods, 1):
        lines.append(f"{idx:<5} {name:<25} {qty:<10} ${rev:,.2f}")
    lines.append("")

    # 5. TOP 5 CUSTOMERS
    lines.append("TOP 5 CUSTOMERS")
    lines.append("-" * 65)
    lines.append(f"{'Rank':<5} {'Customer ID':<15} {'Total Spent':<15} {'Orders':<10}")
    lines.append("-" * 65)
    for idx, (cid, data) in enumerate(top_custs, 1):
        lines.append(f"{idx:<5} {cid:<15} ${data['total_spent']:<14,.2f} {data['purchase_count']:<10}")
    lines.append("")
    
    # 6. DAILY SALES TREND
    lines.append("DAILY SALES TREND (Last 5 Days)")
    lines.append("-" * 65)
    lines.append(f"{'Date':<15} {'Revenue':<15} {'Txns':<10} {'Unique Cust':<15}")
    lines.append("-" * 65)
    for date, data in list(daily_stats.items()):
        lines.append(f"{date:<15} ${data['revenue']:<14,.2f} {data['transaction_count']:<10} {data['unique_customers']:<15}")
    lines.append("")

    # 7. PRODUCT PERFORMANCE ANALYSIS
    lines.append("PRODUCT PERFORMANCE ANALYSIS")
    lines.append("-" * 50)
    if peak_day:
        lines.append(f"Best Selling Day:        {peak_day[0]} (${peak_day[1]:,.2f})")
    
    if low_prods:
        low_names = ", ".join([p[0] for p in low_prods[:3]])
        lines.append(f"Low Performing Products: {low_names}...")
    else:
        lines.append("Low Performing Products: None")
        
    lines.append("Avg Txn Value per Region:")
    for reg, data in region_stats.items():
        avg_reg = data['total_sales'] / data['transaction_count'] if data['transaction_count'] > 0 else 0
        lines.append(f"  - {reg}: ${avg_reg:,.2f}")
    lines.append("")

    # 8. API ENRICHMENT SUMMARY
    lines.append("API ENRICHMENT SUMMARY")
    lines.append("-" * 50)
    lines.append(f"Total Products Enriched: {total_enriched}")
    lines.append(f"Successful Matches:      {success_count}")
    lines.append(f"Success Rate:            {success_rate:.2f}%")
    if missing_prods:
        lines.append(f"Missing Products (Sample): {', '.join(missing_prods)}")
    lines.append("="*50)

    # WRITE TO FILE
    try:
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write("\n".join(lines))
        print(f"Detailed report successfully saved to {output_file}")
    except Exception as e:
        print(f"Error saving report: {e}")

# ==========================================
#          MAIN WRAPPER
# ==========================================

def process_data(raw_lines):
    parsed_data = parse_transactions(raw_lines)
    valid_data, _, summary = validate_and_filter(parsed_data)
    
    print("\n--- PROCESSING SUMMARY ---")
    print(summary)
    
    df = pd.DataFrame(valid_data)
    if not df.empty:
        df['TotalSale'] = df['Quantity'] * df['UnitPrice']
        
    return df