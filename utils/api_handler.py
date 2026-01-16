import requests

def fetch_all_products():
    """
    Fetches all products from DummyJSON API.
    """
    url = "https://dummyjson.com/products?limit=100"
    
    try:
        print(f"Connecting to {url}...")
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        
        data = response.json()
        products = data.get('products', [])
        
        print(f"Success! Fetched {len(products)} products.")
        return products
        
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
        return []

def create_product_mapping(api_products):
    """
    Creates a mapping of product IDs to product info.
    Returns: dictionary mapping product IDs to info.
    """
    mapping = {}
    
    for p in api_products:
        # Extract the ID to use as the key
        p_id = p.get('id')
        
        if p_id is not None:
            # Create the dictionary value with specific fields
            mapping[p_id] = {
                'title': p.get('title'),
                'category': p.get('category'),
                'brand': p.get('brand'),
                'rating': p.get('rating')
            }
            
    return mapping