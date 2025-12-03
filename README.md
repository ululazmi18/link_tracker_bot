# Link Tracker Bot for Telegram

Bot Telegram untuk melacak tautan dengan fitur pelacakan klik, analitik, dan manajemen tautan yang mudah.

## 🚀 Fitur Utama

- **Pelacakan Tautan**: Buat tautan pendek yang dapat dilacak
- **Analitik Detail**: Lacak jumlah klik, lokasi, dan waktu akses
- **Manajemen Tautan**: Kelola tautan Anda dengan mudah
- **Ekspor Data**: Ekspor data klik ke format CSV
- **Pemantauan Aktivitas**: Lacak aktivitas pengguna di grup/channel
- **Multi-grup**: Dukungan untuk beberapa grup dan channel
- **Database SQLite**: Penyimpanan data yang andal

## 🛠️ Persyaratan

- Python 3.7+
- Pyrogram
- python-dotenv
- rich (untuk logging yang lebih baik)
- SQLite3 (terinstal secara default dengan Python)

## ⚙️ Instalasi

1. Clone repositori ini:
   ```bash
   git clone https://github.com/ululazmi18/link_tracker_bot.git
   cd link_tracker_bot
   ```

2. Buat environment virtual (disarankan):
   ```bash
   python -m venv venv
   .\venv\Scripts\activate  # Windows
   # Atau
   source venv/bin/activate  # Linux/Mac
   ```

3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

4. Buat file `.env` dengan konfigurasi berikut:
   ```env
   API_ID=your_api_id
   API_HASH=your_api_hash
   BOT_TOKEN=your_bot_token
   BOT_USERNAME=your_bot_username
   DB_PATH=link_tracker.db
   DATA_DB_PATH=data.db
   ```

## 🚀 Cara Menggunakan

1. Jalankan bot:
   ```bash
   python bot/link_tracker_bot.py
   ```

2. Mulai obrolan dengan bot Anda di Telegram dan gunakan perintah berikut:

   - `/start` - Memulai bot dan menampilkan menu bantuan
   - `/addlink` - Menambahkan tautan baru untuk dilacak
   - `/mylinks` - Melihat daftar tautan yang Anda kelola
   - `/export` - Mengekspor data klik ke CSV
   - `/activity` - Melihat aktivitas pengguna
   - `/deletelink` - Menghapus tautan yang dilacak

## 📊 Struktur Database

Bot menggunakan dua database SQLite:
1. `link_tracker.db` - Menyimpan informasi tautan dan pelacakan klik
2. `data.db` - Menyimpan data pengguna, grup, dan aktivitas

## 🤖 Perintah yang Tersedia

- **/start** - Memulai bot dan menampilkan menu bantuan
- **/addlink** - Tambahkan tautan baru untuk dilacak
- **/mylinks** - Lihat daftar tautan yang Anda kelola
- **/export** - Ekspor data klik ke CSV
- **/activity** - Lihat aktivitas pengguna
- **/deletelink** - Hapus tautan yang dilacak

## 📝 Catatan

- Bot ini menggunakan Pyrogram, pastikan Anda telah mengatur API ID dan hash dengan benar
- Database SQLite akan dibuat secara otomatis saat pertama kali dijalankan
- Pastikan untuk menjaga file database tetap aman dan rahasia

## 📄 Lisensi

Proyek ini dilisensikan di bawah [MIT License](LICENSE).

## 🤝 Berkontribusi

Kontribusi selalu diterima! Jangan ragu untuk membuka issue atau pull request.

---

Dibuat dengan ❤️ oleh [Ulul Azmi](https://github.com/ululazmi18)
