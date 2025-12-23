import os
from minio import Minio
from minio.error import S3Error

def upload_to_datalake():
    # ---------------------------------------------------------
    # 1. KONFIGURASI KONEKSI
    # ---------------------------------------------------------
    client = Minio(
        "minio:9000",
        access_key="admin",
        secret_key="password123",
        secure=False
    )

    bucket_name = "raw-layer"

    # ---------------------------------------------------------
    # 2. PERSIAPAN BUCKET
    # ---------------------------------------------------------
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)
        print(f"[INFO] Bucket '{bucket_name}' berhasil dibuat.")
    else:
        print(f"[INFO] Bucket '{bucket_name}' ditemukan.")

    # ---------------------------------------------------------
    # 3. DAFTAR FILE (⚠️ WAJIB ADA)
    # ---------------------------------------------------------
    files_to_upload = [
        "data_mie_harga.csv",
        "data_mie_nutrisi.csv",
        "data_mie_rating.sql",
        "data_batas_gizi.csv"
    ]

    # ---------------------------------------------------------
    # 4. EKSEKUSI UPLOAD
    # ---------------------------------------------------------
    print("\n--- Memulai Proses Ingestion ke Data Lake ---")

    for file_name in files_to_upload:
        file_path = os.path.join("src", file_name)

        if os.path.exists(file_path):
            try:
                client.fput_object(
                    bucket_name,
                    file_name,
                    file_path,
                    content_type="application/octet-stream"
                )
                print(f"✅ Berhasil upload: {file_name}")
            except S3Error as err:
                print(f"❌ Gagal upload {file_name}. Error: {err}")
        else:
            print(f"⚠️  File TIDAK DITEMUKAN: {file_name} (Pastikan file ada di folder src)")

if __name__ == "__main__":
    try:
        upload_to_datalake()
        print("\n🎉 INGESTION SELESAI! Cek MinIO Browser untuk verifikasi.")
    except Exception as e:
        print(f"🔥 Terjadi Error Fatal: {e}")
