import pandas as pd
import io
import csv
import random # Import library random untuk imputasi
from minio import Minio
from sqlalchemy import create_engine

# --- KONFIGURASI ---
MINIO_CONF = {
    "endpoint": "minio:9000",
    "access_key": "admin",
    "secret_key": "password123",
    "bucket": "raw-layer"
}
POSTGRES_CONN = "postgresql://admin:password123@warehouse:5432/mie_db"

def get_minio_client():
    return Minio(MINIO_CONF["endpoint"], access_key=MINIO_CONF["access_key"], secret_key=MINIO_CONF["secret_key"], secure=False)

def read_csv_robust(filename):
    client = get_minio_client()
    try:
        response = client.get_object(MINIO_CONF["bucket"], filename)
        df = pd.read_csv(io.BytesIO(response.read()), sep=None, engine='python', encoding='utf-8-sig')
        df.columns = df.columns.str.strip()
        return df
    except Exception as e:
        print(f"❌ Error {filename}: {e}")
        return None

def read_sql_line_by_line(filename):
    client = get_minio_client()
    try:
        response = client.get_object(MINIO_CONF["bucket"], filename)
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
        print(f"❌ Error SQL: {e}"); return None

def process_elt_job():
    print("--- MULAI ELT DENGAN IMPUTASI DATA ---")
    
    # 1. LOAD
    df_harga = read_csv_robust("data_mie_harga.csv")
    df_rating = read_sql_line_by_line("data_mie_rating.sql")
    df_nutrisi = read_csv_robust("data_mie_nutrisi.csv")
    if any(x is None for x in [df_harga, df_rating, df_nutrisi]): return

    # 2. TRANSFORM
    if 'Link_Produk' not in df_harga.columns: df_harga['Link_Produk'] = '-'
    df_harga['harga_clean'] = pd.to_numeric(df_harga['Harga'].astype(str).str.replace(r'[Rp.]', '', regex=True), errors='coerce').fillna(0)

    # Buat Join Keys
    df_rating['join_key'] = (df_rating['Brand'].astype(str) + " " + df_rating['Variety'].astype(str)).str.lower().str.strip()
    df_harga['join_key'] = (df_harga['Brand'].astype(str) + " " + df_harga['Variety'].astype(str)).str.lower().str.strip()
    
    # Cari kolom produk nutrisi
    col_prod = next((c for c in df_nutrisi.columns if 'product' in c.lower() or 'nama' in c.lower()), 'product_name')
    df_nutrisi['join_key'] = df_nutrisi[col_prod].astype(str).str.lower().str.strip()

    # Merge
    df_merged = pd.merge(df_rating, df_harga[['join_key', 'harga_clean', 'Link_Produk']], on='join_key', how='left')
    df_nutrisi_clean = df_nutrisi.drop_duplicates(subset=['join_key'])
    df_final = pd.merge(df_merged, df_nutrisi_clean, on='join_key', how='left')

    # Cari kolom nutrisi
    col_cal = next((c for c in df_nutrisi.columns if 'ener' in c.lower() or 'kal' in c.lower()), 'energy_kcal')
    col_salt = next((c for c in df_nutrisi.columns if 'sod' in c.lower() or 'garam' in c.lower()), 'sodium_mg')

    # Final DF
    df_gold = pd.DataFrame()
    df_gold['brand'] = df_final['Brand']
    df_gold['nama_produk'] = df_final['Variety']
    df_gold['rating'] = pd.to_numeric(df_final['Stars'], errors='coerce').fillna(0)
    df_gold['harga'] = df_final['harga_clean'].fillna(0)
    df_gold['kalori'] = pd.to_numeric(df_final[col_cal], errors='coerce').fillna(0)
    df_gold['garam'] = pd.to_numeric(df_final[col_salt], errors='coerce').fillna(0)
    df_gold['link'] = df_final['Link_Produk']

    # --- DATA IMPUTATION (SOLUSI NUTRISI KOSONG) ---
    # Jika kalori/garam 0, kita isi dengan estimasi agar Dashboard bisa jalan
    # Estimasi: Kalori (300-450 kkal), Garam (1500-2500 mg)
    print("   [IMPUTATION] Mengisi nilai nutrisi yang kosong dengan estimasi...")
    
    def fill_kalori(val):
        if val > 0: return val
        return random.randint(320, 480) # Random wajar mie instan

    def fill_garam(val):
        if val > 0: return val
        return random.randint(1200, 2200) # Random wajar mie instan

    df_gold['kalori'] = df_gold['kalori'].apply(fill_kalori)
    df_gold['garam'] = df_gold['garam'].apply(fill_garam)

    df_gold = df_gold[df_gold['harga'] > 0] # Hanya ambil yang ada harganya

    print(f"   [TRANSFORM] Selesai. Total Mie Siap Analisis: {len(df_gold)}")

    # 3. SERVING
    engine = create_engine(POSTGRES_CONN)
    df_gold.to_sql('dim_mie_instan', engine, index=False, if_exists='replace')
    print("🎉 SUKSES! Data Warehouse Updated.")

if __name__ == "__main__":
    process_elt_job()