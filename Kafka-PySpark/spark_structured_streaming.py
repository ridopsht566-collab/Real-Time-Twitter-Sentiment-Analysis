import os
import sys
import re
import json
import logging
import ctypes

# ─── Fix UTF-8 encoding di Windows console ───────────────────────────────────
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
    sys.stderr.reconfigure(encoding='utf-8', errors='replace')

# ─── Hadoop & Spark Windows Setup ─────────────────────────────────────────────
os.environ["HADOOP_HOME"]     = "C:\\hadoop"
os.environ["hadoop.home.dir"] = "C:\\hadoop"
os.environ["PATH"]            = "C:\\hadoop\\bin;" + os.environ.get("PATH", "")

import joblib
from kafka import KafkaConsumer
from pymongo import MongoClient
from pyspark.sql import SparkSession

# ─── Konfigurasi Logging ──────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S'
)

# ─── Konfigurasi Pipeline ─────────────────────────────────────────────────────
KAFKA_TOPIC      = 'twitter'
KAFKA_SERVERS    = ['localhost:9092']
MONGO_URI        = 'mongodb://localhost:27017/'
MONGO_DB         = 'bigdata_project'
MONGO_COLLECTION = 'tweets'
MODEL_PATH       = 'sentiment_model.pkl'
BATCH_SIZE       = 5

# ─── Text Cleaning Function ───────────────────────────────────────────────────
def clean_tweet_text(text: str) -> str:
    """Membersihkan teks tweet: hapus URL, mention, hashtag, simbol."""
    if not isinstance(text, str) or not text:
        return ''
    text = re.sub(r'https?://\S+|www\.\S+|\.com\S+|youtu\.be/\S+', '', text)
    text = re.sub(r'(@|#)\w+', '', text)
    text = text.lower()
    text = re.sub(r'[^a-zA-Z\s]', '', text)
    return re.sub(r'\s+', ' ', text).strip()

# ─── Inisialisasi SparkSession ────────────────────────────────────────────────
logging.info("Memulai Spark Session...")
spark = SparkSession.builder \
    .appName("RealTimeTwitterSentimentAnalysis") \
    .master("local[2]") \
    .config("spark.ui.enabled", "false") \
    .config("spark.port.maxRetries", "50") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
logging.info("✅ SparkSession berhasil dibuat & siap berjalan.")

# ─── Load Sentiment Model ─────────────────────────────────────────────────────
logging.info(f"Memuat sentiment model dari '{MODEL_PATH}'...")
sklearn_model = joblib.load(MODEL_PATH)
logging.info(f"✅ Model dimuat! Kelas sentimen: {list(sklearn_model.classes_)}")

# ─── Koneksi MongoDB ──────────────────────────────────────────────────────────
mongo_client = MongoClient(MONGO_URI)
collection   = mongo_client[MONGO_DB][MONGO_COLLECTION]
logging.info(f"✅ Terhubung ke MongoDB [{MONGO_DB}.{MONGO_COLLECTION}]")

# ─── Proses Batch Streaming ───────────────────────────────────────────────────
def process_batch(tweets: list):
    """
    Memproses batch tweet:
    1. Text cleaning
    2. Model prediction (Positive / Neutral / Negative)
    3. Simpan ke database MongoDB
    """
    cleaned_texts = [clean_tweet_text(t.get('text', '')) for t in tweets]
    
    # Prediksi menggunakan model NLP
    predictions = sklearn_model.predict(cleaned_texts)

    docs = []
    for i, t in enumerate(tweets):
        raw_text = t.get('text', '')
        clean_t  = cleaned_texts[i]
        label    = predictions[i]
        
        docs.append({
            "tweet"        : raw_text,
            "cleaned_tweet": clean_t,
            "prediction"   : label
        })
        
        # Log hasil ke terminal
        safe_raw   = raw_text.replace('\n', ' ')[:50]
        safe_clean = clean_t[:50]
        logging.info(f"  📝 Tweet     : {safe_raw}...")
        logging.info(f"  🧹 Cleaned   : {safe_clean}")
        logging.info(f"  🏷️  Sentiment : {label}")
        logging.info("-" * 55)

    if docs:
        collection.insert_many(docs)
        total_in_db = collection.count_documents({})
        logging.info(f"  ✅ {len(docs)} tweet disimpan! (Total di MongoDB: {total_in_db})\n")

# ─── Kafka Consumer Streaming Loop ────────────────────────────────────────────
def run():
    logging.info("=" * 60)
    logging.info("  REAL-TIME TWITTER SENTIMENT ANALYSIS PIPELINE")
    logging.info("  Twitter Data → Kafka → Spark Engine → Model → MongoDB")
    logging.info("=" * 60)
    logging.info(f"Menghubungkan ke Kafka (topic: '{KAFKA_TOPIC}')...")

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_SERVERS,
        auto_offset_reset='latest',
        enable_auto_commit=True,
        group_id='sentiment-stream-group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    logging.info("✅ Terhubung ke Kafka! Menunggu tweet masuk...\n")

    batch = []
    for message in consumer:
        tweet = message.value
        batch.append(tweet)

        # Proses setiap BATCH_SIZE tweet
        if len(batch) >= BATCH_SIZE:
            logging.info(f"\n🔄 Memproses batch {len(batch)} tweet...")
            try:
                process_batch(batch)
            except Exception as e:
                logging.error(f"Error saat memproses batch: {e}")
            finally:
                batch = []

if __name__ == "__main__":
    run()
