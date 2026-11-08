import time
import json
import random
import logging
from kafka import KafkaProducer

# ─── Konfigurasi Logging ──────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S'
)

KAFKA_TOPIC = 'twitter'
KAFKA_SERVERS = ['localhost:9092']

# ─── Dataset Komentar Sepak Bola (Football Tweets) ─────────────────────────────
FOOTBALL_TWEETS = [
    # Positive Sentiments
    {"id": "fb_101", "text": "What an incredible goal by Bellingham in the 90th minute! Absolute masterclass! 🔥⚽"},
    {"id": "fb_102", "text": "Permainan tim malam ini luar biasa solid! Umpan satu-dua cantik dan passing akurat banget."},
    {"id": "fb_103", "text": "Messi is truly the greatest of all time, that assist was out of this world! 🐐"},
    {"id": "fb_104", "text": "Kemenangan yang sangat pantas! Pertahanan kokoh dan serangan balik sangat mematikan. Bangga!"},
    {"id": "fb_105", "text": "Haaland hattrick again! Best striker in the world right now without a doubt! 🚀"},
    {"id": "fb_106", "text": "Kiper kita malam ini penyelamat sejati, refleks kelas dunia menepis penalti di menit akhir!"},
    {"id": "fb_107", "text": "What a comeback victory! 3 goals in 10 minutes, pure passion and team spirit! 👏"},
    {"id": "fb_108", "text": "Stadion bergemuruh malam ini, atmosfer suporter terbaik sepanjang musim!"},
    {"id": "fb_109", "text": "Beautiful football from Arsenal today, fluid attacking and great team chemistry."},
    {"id": "fb_110", "text": "Gol salto tadi layak memenangkan penghargaan Puskas Award musim ini!"},

    # Negative Sentiments
    {"id": "fb_201", "text": "Worst referee decision I have ever seen in the Champions League, completely ruined the match!"},
    {"id": "fb_202", "text": "Taktik pelatih malam ini sangat buruk dan membosankan, tidak ada kreativitas sama sekali."},
    {"id": "fb_203", "text": "Our defense was a complete disaster tonight, giving away 3 easy goals. Shameful performance."},
    {"id": "fb_204", "text": "Kecewa berat dengan penampilan striker baru, buang-buang peluang emas di depan gawang kosong."},
    {"id": "fb_205", "text": "The club spent $100M on this player and he cannot even complete a simple 5-yard pass. Horrible."},
    {"id": "fb_206", "text": "Wasit berat sebelah banget! Jelas-jelas pelanggaran keras di kotak penalti tapi dibiarkan."},
    {"id": "fb_207", "text": "Another terrible loss. The board needs to sack the manager immediately before it gets worse."},
    {"id": "fb_208", "text": "Pemain bermain tanpa semangat juang, cuma jalan kaki di lapangan padahal lagi tertinggal 0-2."},
    {"id": "fb_209", "text": "VAR is killing the joy of football, taking 5 minutes just to make the wrong call again."},
    {"id": "fb_210", "text": "Pertahanan kacau balau, kiper blunder fatal. Sangat memalukan bagi klub sebesar ini."},

    # Neutral Sentiments
    {"id": "fb_301", "text": "Official lineup for the derby match tonight has been announced by the manager."},
    {"id": "fb_302", "text": "Hasil pertandingan babak pertama berakhir dengan skor kacamata 0-0."},
    {"id": "fb_303", "text": "Transfer update: Fabrizio Romano confirms negotiations are currently underway between the clubs."},
    {"id": "fb_304", "text": "Jadwal pertandingan perempat final Liga Champions akan disiarkan langsung nanti malam pukul 02:00 WIB."},
    {"id": "fb_305", "text": "The match stats show 55% ball possession and 6 shots on target for the home team."},
    {"id": "fb_306", "text": "Daftar susunan 11 pemain pertama Timnas untuk pertandingan kualifikasi besok malam."},
    {"id": "fb_307", "text": "Full Time: The match ends in a 1-1 draw after 90 minutes of play."},
    {"id": "fb_308", "text": "Konferensi pers pra-pertandingan pelatih akan dimulai dalam 30 menit ke depan."},
    {"id": "fb_309", "text": "The referee for next Sunday's big match has been appointed by the football association."},
    {"id": "fb_310", "text": "Pemain pengganti nomor punggung 7 masuk menggantikan gelandang tengah di menit ke-65."}
]

def run_football_producer():
    logging.info("=" * 65)
    logging.info("  ⚽ FOOTBALL LIVE STREAMING PRODUCER (KAFKA)")
    logging.info("  Streaming Tweet & Komentar Sepak Bola ke Kafka Topic 'twitter'")
    logging.info("=" * 65)

    # Inisialisasi Kafka Producer
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    try:
        logging.info("Memulai streaming komentar bola (1 tweet / 2 detik). Tekan Ctrl+C untuk berhenti.\n")
        
        # Stream secara bergantian / acak terus menerus
        tweet_index = 0
        while True:
            # Ambil tweet bola secara berurutan atau acak
            tweet = FOOTBALL_TWEETS[tweet_index % len(FOOTBALL_TWEETS)]
            tweet_index += 1
            
            # Buat payload
            tweet_data = {
                "id": f"{tweet['id']}_{int(time.time())}",
                "text": tweet['text']
            }
            
            # Kirim ke Kafka
            producer.send(KAFKA_TOPIC, tweet_data)
            logging.info(f"📤 [FOOTBALL] Dikirim ke Kafka: {tweet_data['text']}")
            
            # Jeda waktu seolah-olah live stream
            time.sleep(2)
            
    except KeyboardInterrupt:
        logging.info("\nProducer bola dihentikan oleh user.")
    except Exception as e:
        logging.error(f"Error: {e}")
    finally:
        producer.close()
        logging.info("Kafka Producer ditutup.")

if __name__ == "__main__":
    run_football_producer()
