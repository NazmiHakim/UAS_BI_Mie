echo "Menunggu Database & Storage siap (15 detik)"
sleep 15

echo "Memulai Ingestion Data"
python src/ingest.py

echo "Memulai Transformasi ELT"
python src/process_elt.py

echo "Mengaktifkan Cron Scheduler"
service cron start

echo "Menyalakan Dashboard Streamlit"
streamlit run src/app.py --server.port=8501 --server.address=0.0.0.0