import streamlit as st
import pandas as pd
from sqlalchemy import create_engine

# ==========================================
# 1. KONEKSI & LOAD DATA
# ==========================================
DB_CONN = "postgresql://admin:password123@warehouse:5432/mie_db"

@st.cache_data # Cache agar tidak query berulang-ulang setiap klik
def load_data():
    engine = create_engine(DB_CONN)
    
    # Ambil Data Mie
    df_mie = pd.read_sql("SELECT * FROM dim_mie_instan", engine)
    
    # Ambil Data Referensi Gizi (untuk Data Binding)
    try:
        df_ref = pd.read_sql("SELECT * FROM ref_batas_gizi", engine)
    except:
        df_ref = pd.DataFrame() # Fallback jika tabel belum ada
        
    return df_mie, df_ref

df_mie, df_ref = load_data()

# ==========================================
# 2. LOGIKA DATA BINDING (REFERENSI GIZI)
# ==========================================
def get_batas_ref(parameter_name, default_val):
    if not df_ref.empty:
        filtered = df_ref[df_ref['parameter'].str.contains(parameter_name, case=False, na=False)]
        if not filtered.empty:
            return float(filtered.iloc[0]['batas_harian_dewasa'])
    return default_val

std_kalori_pria = get_batas_ref("Laki", 2500.0)
std_kalori_wanita = get_batas_ref("Perempuan", 2000.0)
std_garam = get_batas_ref("Garam", 2000.0)

# ==========================================
# 3. SIDEBAR (INPUT & SWITCHES)
# ==========================================
st.set_page_config(page_title="Mie Sehat & Hemat", layout="wide")
st.title("Rekomendasi Mie Instan")
st.markdown("Sistem Preskriptif dengan **Dynamic Data Binding**.")

st.sidebar.header("Konfigurasi User")

# --- FITUR BARU: MODE YOLO (ABAIKAN KESEHATAN) ---
# Jika dicentang, filter kalori & garam akan dimatikan
ignore_health = st.sidebar.checkbox("⚠️ Abaikan Batas Kesehatan (Mode YOLO)", value=False)

if ignore_health:
    st.sidebar.warning("Filter Kalori & Garam dinonaktifkan!")

st.sidebar.divider()

# --- FITUR 1: GENDER SWITCH (DATA BINDING) ---
gender = st.sidebar.radio("Jenis Kelamin", ["Laki-laki", "Perempuan"])

if gender == "Laki-laki":
    default_kalori = std_kalori_pria
else:
    default_kalori = std_kalori_wanita

# --- INPUT BATASAN ---
st.sidebar.subheader("Batasan Harian")

# Budget selalu aktif
budget_user = st.sidebar.number_input("Budget (Rp)", 1000, 50000, 5000, 500)

# Input Kalori & Garam (Disable jika Mode YOLO aktif agar user sadar fiturnya mati)
disabled_status = ignore_health 
sisa_kalori = st.sidebar.number_input("Sisa Kalori (kkal)", 0.0, 4000.0, default_kalori, 50.0, disabled=disabled_status)
sisa_garam = st.sidebar.number_input("Sisa Garam (mg)", 0.0, 4000.0, std_garam, 100.0, disabled=disabled_status)

# --- FITUR 2: SWITCH MSG ---
st.sidebar.subheader("Preferensi Diet")
no_msg = st.sidebar.toggle("Hindari MSG", value=False)
st.sidebar.caption("Jika aktif, memprioritaskan mie 'Sehat/Natural'.")

# --- FITUR 3: MODE HEMAT VS NORMAL ---
st.sidebar.subheader("Mode Rekomendasi")
mode_pilih = st.sidebar.selectbox(
    "Prioritas Pemilihan:",
    ["Mode Hemat (Termurah)", "Mode Sultan (Termahal/Premium)"]
)

# ==========================================
# 4. ENGINE PRESKRIPTIF
# ==========================================

# A. Filter Budget (Selalu Aktif)
df_filtered = df_mie[df_mie['harga'] <= budget_user].copy()

# B. Filter Kesehatan (Kondisional - UPDATE DISINI)
# Hanya filter jika Mode YOLO MATI (False)
if not ignore_health:
    df_filtered = df_filtered[
        (df_filtered['kalori'] <= sisa_kalori) & 
        (df_filtered['garam'] <= sisa_garam)
    ]
    status_kesehatan = "✅ Filter Kesehatan Aktif"
else:
    status_kesehatan = "⚠️ Filter Kesehatan DIABAIKAN"

# C. Filter MSG Logic
if no_msg:
    keywords_sehat = ['lemonilo', 'fitmee', 'ladang', 'ashitaki', 'natural', 'vegan', 'sehat']
    pattern = '|'.join(keywords_sehat)
    df_filtered = df_filtered[df_filtered['nama_produk'].str.contains(pattern, case=False) | df_filtered['brand'].str.contains(pattern, case=False)]

# D. Sorting Logic
if mode_pilih == "Mode Hemat (Termurah)":
    df_rekomendasi = df_filtered.sort_values(by=['harga', 'rating'], ascending=[True, False])
    pesan_mode = "Mencari mie **paling murah** yang masuk kriteria."
else:
    df_rekomendasi = df_filtered.sort_values(by=['harga', 'rating'], ascending=[False, False])
    pesan_mode = "Mencari mie **paling premium (mahal)** yang masuk kriteria."

# ==========================================
# 5. TAMPILAN DASHBOARD
# ==========================================

# KPI Metrics
col1, col2, col3, col4 = st.columns(4)
col1.metric("Gender", gender)
col2.metric("Limit Kalori", f"{int(sisa_kalori)} kkal", help="Diabaikan jika mode YOLO aktif")
col3.metric("Limit Garam", f"{int(sisa_garam)} mg", help="Diabaikan jika mode YOLO aktif")
col4.metric("Opsi Tersedia", f"{len(df_rekomendasi)} mie")

st.divider()
st.info(f"**Status:** {pesan_mode} | {status_kesehatan}")

if not df_rekomendasi.empty:
    top = df_rekomendasi.iloc[0]
    
    st.success(f"**Rekomendasi Terbaik: {top['brand']} - {top['nama_produk']}**")
    
    c1, c2, c3 = st.columns(3)
    c1.metric("Harga", f"Rp {int(top['harga']):,}")
    c2.metric("Kalori", f"{int(top['kalori'])} kkal")
    c3.metric("Garam", f"{int(top['garam'])} mg")
    
    with st.expander("Lihat Detail & Analisis"):
        st.write(f"**Alasan Rekomendasi:** Masuk budget Rp {budget_user:,}. Kandungan: Garam {top['garam']}mg, Kalori {top['kalori']}kkal.")
        if top['link'] != '-':
            st.markdown(f"[Link Pembelian]({top['link']})")

    st.subheader("Alternatif Lainnya")
    st.dataframe(
        df_rekomendasi[['brand', 'nama_produk', 'harga', 'rating', 'kalori', 'garam']].head(10),
        use_container_width=True,
        hide_index=True
    )
else:
    st.error("Tidak ditemukan mie yang sesuai kriteria.")
    if not ignore_health:
        st.warning("Coba aktifkan **'Abaikan Batas Kesehatan'** di sidebar jika ingin melihat lebih banyak opsi.")