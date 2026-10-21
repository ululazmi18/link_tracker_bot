<div align="center">

# 🔗 Telegram Link Tracker Bot
### *Advanced Link Analytics, Click Tracking, & User Engagement Bot for Telegram*

[![Python](https://img.shields.io/badge/Python-3.8+-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://python.org)
[![Library](https://img.shields.io/badge/Library-Pyrogram-0088cc?style=for-the-badge&logo=telegram)](https://docs.pyrogram.org/)
[![Deploy](https://img.shields.io/badge/Deploy-Railway_%7C_Docker-0B0D0E?style=for-the-badge&logo=railway)](https://railway.app)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg?style=for-the-badge)](LICENSE)

</div>

---

## 🎯 Gambaran Umum (Overview)

**Telegram Link Tracker Bot** adalah bot analitik lengkap yang dirancang untuk memantau, melacak jumlah klik, dan mencatat metrik konversi tautan yang dibagikan ke dalam grup, channel, atau kampanye pemasaran Telegram.

---

## 🚀 Fitur Utama (Key Features)

- 🎯 **Pelacakan Klik Real-Time:** Menghasilkan tautan unik (*deep linking*) untuk melacak performa traffic.
- 📊 **Statistik Terperinci:** Menampilkan jumlah total klik, klik unik per pengguna, dan waktu akses.
- 👥 **Manajemen Pengguna & Grup:** Mengelompokkan statistik berdasarkan grup atau kampanye spesifik.
- 📥 **Ekspor Data ke CSV:** Unduh laporan statistik kapan saja dalam format CSV.
- 🐳 **Siap Deploy:** Dilengkapi dengan `Dockerfile` dan `Procfile` untuk deployment 1-klik di **Railway**, **Render**, atau **VPS**.

---

## 🛠️ Instalasi & Konfigurasi

1. **Clone Repositori:**
   ```bash
   git clone https://github.com/ululazmi18/link_tracker_bot.git
   cd link_tracker_bot
   ```

2. **Pasang Dependensi:**
   ```bash
   pip install -r requirements.txt
   ```

3. **Buat File Konfigurasi `.env`:**
   Salin `.env.example` menjadi `.env` dan isi token Anda:
   ```env
   API_ID=12345678
   API_HASH=abcdef1234567890abcdef1234567890
   BOT_TOKEN=123456789:ABCDEF...
   BOT_USERNAME=MyLinkTrackerBot
   ```

4. **Jalankan Bot:**
   ```bash
   python main.py
   ```

---

## 🐳 Deployment via Docker

```bash
docker build -t link-tracker-bot .
docker run -d --env-file .env link-tracker-bot
```

---

## 📄 Lisensi (License)

Didistribusikan di bawah Lisensi **MIT**. Dibuat oleh [**Ulul Azmi**](https://github.com/ululazmi18).
