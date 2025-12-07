# Telegram Link Tracker Bot

Bot Telegram untuk melacak tautan dengan fitur analitik lengkap dan manajemen pengguna.

## Fitur Utama

- 🎯 Lacak klik pada tautan yang dibagikan
- 📊 Statistik klik terperinci
- 👥 Manajemen grup dan pengguna
- 📊 Ekspor data ke CSV
- 🔍 Pantau aktivitas pengguna
- 🔗 Dukungan deep linking
- 📱 Antarmuka pengguna yang interaktif

## Persyaratan Sistem

- Python 3.7+
- Pyrogram
- python-dotenv

## Instalasi

1. Clone repositori ini:
   ```bash
   git clone [URL_REPOSITORY]
   cd bot
   ```

2. Buat virtual environment (disarankan):
   ```bash
   python -m venv venv
   source venv/bin/activate  # Linux/Mac
   # atau
   .\venv\Scripts\activate  # Windows
   ```

3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

4. Buat file `.env` dan isi dengan konfigurasi Anda:
   ```env
   API_ID=your_api_id
   API_HASH=your_api_hash
   BOT_TOKEN=your_bot_token
   BOT_USERNAME=your_bot_username
   DB_PATH=link_tracker.db
   DATA_DB_PATH=data.db
   ```

## Cara Menggunakan

1. Mulai bot dengan perintah:
   ```bash
   python link_tracker_bot.py
   ```

2. Di Telegram, mulai chat dengan bot Anda dan gunakan perintah berikut:

   - `/start` - Mulai bot dan lihat menu utama
   - `/addlink` - Tambah tautan baru untuk dilacak
   - `/mylinks` - Lihat daftar tautan yang Anda lacak
   - `/export` - Ekspor statistik klik
   - `/activity` - Lihat aktivitas pengguna
   - `/deletelink` - Hapus tautan yang dilacak

## Database

Bot menggunakan dua database SQLite:
- `link_tracker.db` - Menyimpan data tautan dan statistik
- `data.db` - Menyimpan data pengguna dan grup

## Kontribusi

Kontribusi terbuka! Silakan buat issue atau pull request untuk fitur baru atau perbaikan bug.

## Lisensi

[MIT License](LICENSE)
