import streamlit as st
import pandas as pd
import random
from sqlalchemy import create_engine

DB_CONN = "postgresql://admin:password123@warehouse:5432/mie_db"

@st.cache_data 
def load_data():
    engine = create_engine(DB_CONN)
    try:
        df_mie = pd.read_sql("SELECT * FROM dim_mie_instan", engine)
        df_users = pd.read_sql("SELECT * FROM dim_users", engine)
        df_lauk = pd.read_sql("SELECT * FROM dim_lauk", engine)
    except Exception as e:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    return df_mie, df_users, df_lauk

df_mie, df_users, df_lauk = load_data()

st.set_page_config(page_title="GymBro Noodle Plan", layout="wide")
st.title("GymBro Noodle Planner")
st.markdown("Sistem Preskriptif Mie Instan dengan **Personalisasi Fitness Goal**.")

st.sidebar.header("1. Pilih Profil Anda")

if not df_users.empty:
    user_list = df_users['nama'].unique()
    selected_user = st.sidebar.selectbox("Siapa yang makan?", user_list)
    
    curr_user = df_users[df_users['nama'] == selected_user].iloc[0]
    
    st.sidebar.divider()
    st.sidebar.subheader("Profil Fisik")
    col_a, col_b = st.sidebar.columns(2)
    col_a.metric("BB", f"{curr_user['berat_badan']} kg")
    col_b.metric("TB", f"{curr_user['tinggi_badan']} cm")
    
    user_goal = str(curr_user['tujuan']).upper()
    user_pref_protein = str(curr_user['jenis_protein']).capitalize()
    target_kalori = curr_user['target_kalori']
    target_protein = curr_user['target_protein']
    
    st.sidebar.info(f"**Goal:** {user_goal}")
    st.sidebar.info(f"**Pref. Protein:** {user_pref_protein}")
    
    st.sidebar.markdown("### Kebutuhan Harian")
    st.sidebar.progress(100, text=f"Target Kalori: {target_kalori} kkal")
    st.sidebar.progress(100, text=f"Target Protein: {int(target_protein)} gram")

else:
    st.error("Data User belum tersedia. Jalankan script ETL terlebih dahulu.")
    st.stop()

st.sidebar.divider()
st.sidebar.header("2. Konteks Makan")
budget = st.sidebar.number_input("Budget Makan Ini (Rp)", 5000, 50000, 15000, step=1000)
porsi_harian = st.sidebar.slider("Alokasi Gizi Makan Ini (%)", 20, 50, 30) / 100

meal_cal_target = target_kalori * porsi_harian
meal_prot_target = target_protein * porsi_harian

st.subheader(f"Rekomendasi Meal Plan untuk {selected_user}")
st.caption(f"Target Meal Ini: Kalori ~{int(meal_cal_target)} kkal | Protein ~{int(meal_prot_target)}g | Budget Rp {budget:,}")

available_mie = df_mie[df_mie['harga'] < budget].copy()

if available_mie.empty:
    st.error("Budget terlalu kecil, tidak ada mie yang bisa dibeli!")
else:
    
    goal_lower = user_goal.lower()
    
    if 'bulking' in goal_lower:
        st.caption("**Bulking** (Mencari Protein Tertinggi)")
        top_mie = available_mie.sort_values(by=['protein', 'rating'], ascending=[False, False]).head(5)
        
    elif 'cutting' in goal_lower:
        st.caption("**Cutting** (Mencari Kalori Terendah)")
        top_mie = available_mie.sort_values(by=['kalori', 'protein', 'rating'], ascending=[True, False, False]).head(5)
        
    else:
        st.caption("**Maintenance** (Rating Terbaik & Hemat)")
        top_mie = available_mie.sort_values(by=['rating', 'harga'], ascending=[False, True]).head(5)
    
    count_top = len(top_mie)
    if count_top > 0:
        pick_idx = random.randint(0, min(2, count_top - 1))
        best_mie = top_mie.iloc[pick_idx]
    else:
        best_mie = available_mie.iloc[0]
    
    sisa_uang = budget - best_mie['harga']
    gap_protein = meal_prot_target - best_mie['protein']
    gap_kalori = meal_cal_target - best_mie['kalori']
    
    c_main, c_budget = st.columns([3, 1])
    with c_main:
        st.markdown(f"### Utama: {best_mie['brand']} - {best_mie['nama_produk']}")
        st.write(f"Rating: {best_mie['rating']} | Rp {int(best_mie['harga']):,} | {int(best_mie['kalori'])} kkal | **{int(best_mie['protein'])}g Protein**")
    with c_budget:
        st.metric("Sisa Budget", f"Rp {int(sisa_uang):,}")

    st.divider()
    
    st.subheader("Optimalisasi Gizi")
    
    pref_str = str(curr_user['jenis_protein']).lower().strip()
    
    if not df_lauk.empty:
        df_lauk['jenis'] = df_lauk['jenis'].str.lower().str.strip()
        
        if 'nabati' in pref_str:
            allowed_types = ['nabati', 'sayur', 'minuman', 'tambahan']
            filter_msg = "Filter Aktif: Menu Nabati & Sayuran (No Meat)"
        elif 'hewani' in pref_str:
            allowed_types = ['hewani', 'olahan', 'sayur', 'minuman', 'tambahan']
            filter_msg = "Filter Aktif: Fokus Protein Hewani"
        else:
            allowed_types = df_lauk['jenis'].unique()
            filter_msg = "Filter: Semua Jenis"
            
        df_lauk_filtered = df_lauk[df_lauk['jenis'].isin(allowed_types)].copy()
        
        if not df_lauk_filtered.empty:
            st.caption(filter_msg)
            
            df_lauk_affordable = df_lauk_filtered[df_lauk_filtered['harga_per_unit'] <= sisa_uang].copy()
            
            rekomendasi_lauk = []
            total_lauk_prot = 0
            
            if gap_protein > 0 and not df_lauk_affordable.empty:
                if 'ppi' not in df_lauk_affordable.columns:
                     df_lauk_affordable['ppi'] = df_lauk_affordable['protein'] / df_lauk_affordable['harga_per_unit']

                df_lauk_sorted = df_lauk_affordable.sort_values('ppi', ascending=False)
                
                current_money = sisa_uang

                for index, row in df_lauk_sorted.iterrows():
                    if current_money >= row['harga_per_unit']:
                        max_qty = int(current_money // row['harga_per_unit'])
                        qty = min(max_qty, 3)
                        
                        if qty > 0:
                            rekomendasi_lauk.append({
                                'item': row['nama_item'],
                                'qty': qty,
                                'total_harga': qty * row['harga_per_unit'],
                                'total_prot': qty * row['protein'],
                                'jenis': row['jenis']
                            })
                            current_money -= (qty * row['harga_per_unit'])
                            total_lauk_prot += (qty * row['protein'])
                    
                    if total_lauk_prot >= gap_protein:
                        break

            if rekomendasi_lauk:
                st.info("Saran Sistem: Tambahkan lauk ini agar target tercapai:")
                
                cols = st.columns(len(rekomendasi_lauk))
                for idx, item in enumerate(rekomendasi_lauk):
                    with cols[idx]:
                        st.success(f"**{item['qty']}x {item['item']}**")
                        st.caption(f"Rp {int(item['total_harga']):,} | +{int(item['total_prot'])}g Protein")

                final_prot = best_mie['protein'] + total_lauk_prot
                st.write(f"**Total Protein:** {int(final_prot)}g (Target: {int(meal_prot_target)}g)")
                
                if final_prot >= meal_prot_target:
                    st.success("Target Protein Tercapai!")
                else:
                    st.warning(f"Kurang {int(meal_prot_target - final_prot)}g. Budget habis.")
            else:
                if sisa_uang < 1000:
                    st.warning("Uang sisa tidak cukup untuk beli lauk tambahan.")
                elif gap_protein <= 0:
                    st.success("Protein mie sudah cukup tinggi. Tidak butuh lauk tambahan.")
                else:
                    st.write("Tidak ada rekomendasi lauk yang cocok dengan budget & preferensi.")
        else:
            st.error("Data lauk tidak ditemukan untuk preferensi ini.")
    else:
        st.write("Data Lauk Kosong.")

with st.expander("Lihat Data Referensi Lauk"):
    st.dataframe(df_lauk)