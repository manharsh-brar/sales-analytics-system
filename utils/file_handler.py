import os

def read_sales_data(filename):
    """
    Reads sales data handling encoding issues.
    """
    if not os.path.exists(filename):
        print(f"Error: The file '{filename}' was not found.")
        return []

    encodings_to_try = ['utf-8', 'latin-1', 'cp1252']
    
    for encoding in encodings_to_try:
        try:
            with open(filename, 'r', encoding=encoding) as f:
                lines = f.readlines()
            # Skip header (lines[1:]) and ignore empty lines
            clean_lines = [line.strip() for line in lines[1:] if line.strip()]
            return clean_lines
        except UnicodeDecodeError:
            continue
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            return []
            
    print("Error: Could not read the file.")
    return []

def save_report(report_text, output_path):
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, 'w') as f:
        f.write(report_text)
    print(f"Report successfully saved to {output_path}")

def save_clean_data(df, output_path):
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_csv(output_path, index=False)
    print(f"Cleaned data successfully saved to {output_path}")

# --- NEW FUNCTION FOR TASK 3.2 ---
def save_enriched_data(enriched_transactions, filename='data/enriched_sales_data.txt'):
    """
    Saves enriched transactions back to file.
    """
    if not enriched_transactions:
        print("No enriched data to save.")
        return

    # Define the exact column order required
    header = [
        "TransactionID", "Date", "ProductID", "ProductName", 
        "Quantity", "UnitPrice", "CustomerID", "Region", 
        "API_Category", "API_Brand", "API_Rating", "API_Match"
    ]
    
    try:
        # Ensure the directory exists
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        
        with open(filename, 'w', encoding='utf-8') as f:
            # Write Header
            f.write('|'.join(header) + '\n')
            
            # Write Rows
            for row in enriched_transactions:
                line_parts = []
                for col in header:
                    val = row.get(col)
                    # Handle None values by converting to empty string or string 'None'
                    # The screenshot implies strict pipe separation
                    if val is None:
                        line_parts.append("None") # Or "" depending on preference, usually "None" or empty
                    else:
                        line_parts.append(str(val))
                
                f.write('|'.join(line_parts) + '\n')
                
        print(f"Enriched data successfully saved to {filename}")
        
    except Exception as e:
        print(f"Error saving enriched data: {e}")