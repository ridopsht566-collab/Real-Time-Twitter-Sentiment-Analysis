import os
import re
import joblib
from pymongo import MongoClient

# ─── Koneksi MongoDB ──────────────────────────────────────────────────────────
client = MongoClient('mongodb://localhost:27017/')
db = client['bigdata_project']
collection = db['tweets']

# ─── Load Model NLP ───────────────────────────────────────────────────────────
MODEL_PATH = os.path.join(os.path.dirname(__file__), '..', '..', 'Kafka-PySpark', 'sentiment_model.pkl')
if not os.path.exists(MODEL_PATH):
    MODEL_PATH = 'sentiment_model.pkl'

try:
    model = joblib.load(MODEL_PATH)
except Exception as e:
    # fallback jika di root folder
    model = joblib.load('../Kafka-PySpark/sentiment_model.pkl')

def clean_text(text):
    if text is not None:
        text = re.sub(r'https?://\S+|www\.\S+|S+\.com\S+|youtu\.be/\S+', '', text)
        text = re.sub(r'(@|#)\w+', '', text)
        text = text.lower()
        text = re.sub(r'[^a-zA-Z\s]', '', text)
        text = re.sub(r'\s+', ' ', text).strip()
        return text
    else:
        return ''

def classify_text(text: str):
    preprocessed_tweet = clean_text(text)
    prediction = model.predict([preprocessed_tweet])[0]

    # Simpan dokumen ke MongoDB agar otomatis muncul di Live Tweets Feed
    tweet_doc = {
        "tweet": text,
        "cleaned_tweet": preprocessed_tweet,
        "prediction": prediction
    }
    collection.insert_one(tweet_doc)

    print("-> Tweet : ", text)
    print("-> Cleaned : ", preprocessed_tweet)
    print("-> Predicted Sentiment : ", prediction)
    print("-> ✅ Berhasil disimpan ke MongoDB (Live Feed)")
    print("/" * 50)

    return prediction