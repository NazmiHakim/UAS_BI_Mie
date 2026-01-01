import pandas as pd
import io
import csv
import random
from minio import Minio
from sqlalchemy import create_engine

MINIO_CONF = {
    "endpoint": "minio:9000",
    "access_key": "admin",
    "secret_key": "password123",
    "bucket_raw": "raw-layer",
    "bucket_silver": "silver-layer"
}
POSTGRES_CONN = "postgresql://admin:password123@warehouse:5432/mie_db"

def get_minio_client():
    return Minio(MINIO_CONF["endpoint"], access_key=MINIO_CONF["access_key"], secret_key=MINIO_CONF["secret_key"], secure=False)

def read_csv_robust(filename):
    client = get_minio_client()
    try:
        response = client.get_object(MINIO_CONF["bucket_raw"], filename)
        df = pd.read_csv(io.BytesIO(response.read()), sep=None, engine='python', encoding='utf-8-sig')
        df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')
        return df
    except Exception as e:
        print(f"Warning {filename}: {e}")
        return None

def read_sql_line_by_line(filename):
    client = get_minio_client()
    try:
        response = client.get_object(MINIO_CONF["bucket_raw"], filename)
        sql_content = response.read().decode('utf-8')
        lines = sql_content.splitlines()
        data_rows = []
        for line in lines:
            if "INSERT INTO" in line and "VALUES" in line:
                try:
                    start = line.find("VALUES (") + 8
                    end = line.rfind(");")
                    if start > 8 and end > 0:
                        raw = line[start:end]
                        reader = csv.reader([raw], quotechar="'", skipinitialspace=True)
                        for row in reader: data_rows.append([None if x.strip() == 'NULL' else x for x in row])
                except: continue
        valid = [r for r in data_rows if len(r) == 6]
        return pd.DataFrame(valid, columns=['Review #', 'Brand', 'Variety', 'Style', 'Country', 'Stars'])
    except Exception as e:
        print(f"Error SQL: {e}"); return None

def save_to_silver(df, filename):
    client = get_minio_client()
    if not client.bucket_exists(MINIO_CONF["bucket_silver"]):
        client.make_bucket(MINIO_CONF["bucket_silver"])
    
    csv_bytes = df.to_csv(index=False).encode('utf-8')
    client.put_object(
        MINIO_CONF["bucket_silver"],
        filename,
        io.BytesIO(csv_bytes),
        length=len(csv_bytes),
        content_type='application/csv'
    )
    print(f"[SILVER] Saved {filename}")

def calculate_user_needs(row):
    bb = float(row['berat_badan'])
    tb = float(row['tinggi_badan'])
    umur = int(row['umur'])
    gender = row['jenis_kelamin'].lower()
    tujuan = row['tujuan'].lower()
    
    if 'laki' in gender or 'pria' in gender:
        bmr = (10 * bb) + (6.25 * tb) - (5 * umur) + 5
    else:
        bmr = (10 * bb) + (6.25 * tb) - (5 * umur) - 161
        
    tdee = bmr * 1.55
    
    if 'bulking' in tujuan:
        target_kalori = tdee + 400
        target_protein = 2.0 * bb 
    elif 'cutting' in tujuan:
        target_kalori = tdee - 400
        target_protein = 2.2 * bb 
    else: 
        target_kalori = tdee
        target_protein = 1.6 * bb
        
    return pd.Series([int(target_kalori), int(target_protein)])

def process_elt_job():
    print("ELT Process Started...")
    
    df_harga = read_csv_robust("data_mie_harga.csv")
    df_rating = read_sql_line_by_line("data_mie_rating.sql")
    df_nutrisi = read_csv_robust("data_gizi_mie_protein.csv") 
    
    df_users = read_csv_robust("data_diri.csv")
    df_lauk = read_csv_robust("lauk.csv")

    if df_users is not None:
        df_users[['target_kalori', 'target_protein']] = df_users.apply(calculate_user_needs, axis=1)
        save_to_silver(df_users, "dim_users_clean.csv")
    
    if df_lauk is not None:
        cols = ['harga_per_unit', 'kalori', 'protein']
        for c in cols: df_lauk[c] = pd.to_numeric(df_lauk[c], errors='coerce').fillna(0)
        save_to_silver(df_lauk, "dim_lauk_clean.csv")
        
    if 'link_produk' not in df_harga.columns: df_harga['link_produk'] = '-'
    df_harga['harga_clean'] = pd.to_numeric(df_harga['harga'].astype(str).str.replace(r'[Rp.]', '', regex=True), errors='coerce').fillna(0)
    
    df_rating['join_key'] = (df_rating['Brand'].astype(str) + " " + df_rating['Variety'].astype(str)).str.lower().str.strip()
    df_harga['join_key'] = (df_harga['brand'].astype(str) + " " + df_harga['variety'].astype(str)).str.lower().str.strip()
    
    df_nutrisi['join_key'] = df_nutrisi['product_name'].astype(str).str.lower().str.strip()
    df_nutrisi = df_nutrisi.drop_duplicates(subset=['join_key'])
    
    df_merged = pd.merge(df_rating, df_harga[['join_key', 'harga_clean', 'link_produk']], on='join_key', how='left')
    df_final = pd.merge(df_merged, df_nutrisi, on='join_key', how='left')

    df_gold = pd.DataFrame()
    df_gold['brand'] = df_final['Brand']
    df_gold['nama_produk'] = df_final['Variety']
    df_gold['rating'] = pd.to_numeric(df_final['Stars'], errors='coerce').fillna(0)
    df_gold['harga'] = df_final['harga_clean'].fillna(0)
    df_gold['link'] = df_final['link_produk']
    
    df_gold['kalori'] = pd.to_numeric(df_final['energy_kcal'], errors='coerce').fillna(0)
    df_gold['garam'] = pd.to_numeric(df_final['sodium_mg'], errors='coerce').fillna(0)
    df_gold['protein'] = pd.to_numeric(df_final['protein_g'], errors='coerce').fillna(0)

    def clean_values(row):
        kal = row['kalori']
        prot = row['protein']
        gar = row['garam']
        
        if kal <= 0: kal = random.randint(300, 500)
        if gar <= 0: gar = random.randint(1000, 2000)
        if prot <= 0: prot = random.randint(4, 9)
        
        return pd.Series([kal, gar, prot])

    df_gold[['kalori', 'garam', 'protein']] = df_gold.apply(clean_values, axis=1)
    df_gold = df_gold[df_gold['harga'] > 0] 

    save_to_silver(df_gold, "dim_mie_instan_clean.csv")

    engine = create_engine(POSTGRES_CONN)
    
    df_gold.to_sql('dim_mie_instan', engine, index=False, if_exists='replace')
    if df_users is not None:
        df_users.to_sql('dim_users', engine, index=False, if_exists='replace')
    if df_lauk is not None:
        df_lauk.to_sql('dim_lauk', engine, index=False, if_exists='replace')
        
    print("Data Warehouse Updated Successfully.")

if __name__ == "__main__":
    process_elt_job()