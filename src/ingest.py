import os
from minio import Minio
from minio.error import S3Error

def upload_to_datalake():
    client = Minio(
        "minio:9000",
        access_key="admin",
        secret_key="password123",
        secure=False
    )

    bucket_name = "raw-layer"

    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)
        print(f"Bucket '{bucket_name}' berhasil dibuat.")
    else:
        print(f"Bucket '{bucket_name}' ditemukan.")

    files_to_upload = [
        "data_mie_harga.csv",
        "data_mie_nutrisi.csv",
        "data_mie_rating.sql",
        "data_batas_gizi.csv"
    ]

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
                print(f"Berhasil upload: {file_name}")
            except S3Error as err:
                print(f"Gagal upload {file_name}. Error: {err}")
        else:
            print(f"File tidak ditemukan: {file_name}")

if __name__ == "__main__":
    try:
        upload_to_datalake()
        print("\nIngestion selesai, Cek MinIO Browser untuk verifikasi.")
    except Exception as e:
        print(f"Error : {e}")