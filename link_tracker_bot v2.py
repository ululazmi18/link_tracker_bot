
import os
import sys
import csv
import io
import re
import logging
import sqlite3

# Third-party imports
from pyrogram import Client, filters
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration
API_ID = os.getenv("API_ID")
API_HASH = os.getenv("API_HASH")
BOT_TOKEN = os.getenv("BOT_TOKEN")
BOT_USERNAME = os.getenv("BOT_USERNAME", "YourBotUsername")
DB_PATH = os.getenv("DB_PATH", "link_tracker.db")
DATA_DB_PATH = os.getenv("DATA_DB_PATH", "data.db")

# Validate Config
if not all([API_ID, API_HASH, BOT_TOKEN]):
    print("Missing API_ID, API_HASH, or BOT_TOKEN in environment variables.")
    print("Please create a .env file with these values.")
    sys.exit(1)

# Initialize SQLite Database
def init_database():
    """Initialize SQLite database with required tables."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Tables will only be created if they don't exist (Data Persistence Enabled)
    
    # Create links table
    # links = link_id, owner_id, username_target, owner_code, clicks
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS links (
            link_id TEXT PRIMARY KEY,
            owner_id INTEGER NOT NULL,
            username_target TEXT NOT NULL,
            owner_code TEXT NOT NULL,
            clicks INTEGER DEFAULT 0,
            group_username TEXT,
            group_id INTEGER
        )
    ''')

    # Migration: Ensure new columns exist if table was created previously
    try:
        cursor.execute("SELECT group_username FROM links LIMIT 1")
    except sqlite3.OperationalError:
        print("Migrating links table: adding group_username and group_id")
        cursor.execute("ALTER TABLE links ADD COLUMN group_username TEXT")
        cursor.execute("ALTER TABLE links ADD COLUMN group_id INTEGER")
    
    # Create click_stats table
    # click_stats = link_id, sumber, user_id, first_name, last_name, username, language_code
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS click_stats (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            link_id TEXT NOT NULL,
            sumber TEXT,
            user_id INTEGER,
            first_name TEXT,
            last_name TEXT,
            username TEXT,
            language_code TEXT,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (link_id) REFERENCES links(link_id)
        )
    ''')
    
    # Create user_activity table (keeping this as it helps with activity tracking, updated FK)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS user_activity (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            username TEXT,
            chat_id INTEGER NOT NULL,
            chat_title TEXT,
            chat_username TEXT,
            owner_code TEXT NOT NULL,
            link_id TEXT NOT NULL,
            message_text TEXT,
            message_id INTEGER,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (link_id) REFERENCES links(link_id)
        )
    ''')
    
    # Create indexes for better performance
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_owner_id ON links(owner_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_link_id ON click_stats(link_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_user_id ON click_stats(user_id)')
    
    conn.commit()
    conn.close()
    print(f"SQLite database initialized at {DB_PATH}")

def init_user_database():
    """Initialize data.db for users, groups, and members tracking."""
    conn = sqlite3.connect(DATA_DB_PATH)
    cursor = conn.cursor()
    
    # Create users table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            first_name TEXT,
            last_name TEXT,
            language_code TEXT,
            is_bot INTEGER DEFAULT 0,
            first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            interaction_count INTEGER DEFAULT 0
        )
    ''')
    
    # Create groups table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS groups (
            chat_id INTEGER PRIMARY KEY,
            chat_type TEXT,
            title TEXT,
            username TEXT,
            description TEXT,
            added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # Create members table (passive tracking)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS members (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            username TEXT,
            first_name TEXT,
            last_name TEXT,
            first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            message_count INTEGER DEFAULT 0,
            UNIQUE(chat_id, user_id)
        )
    ''')
    
    # Create indexes for faster lookups
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_username ON users(username)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_members_chat_id ON members(chat_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_members_user_id ON members(user_id)')
    
    conn.commit()
    conn.close()
    print(f"Data database initialized at {DATA_DB_PATH}")

# Initialize database on startup
try:
    init_database()
    init_user_database()
except Exception as e:
    print(f"Failed to initialize database: {e}")
    sys.exit(1)

# Initialize Pyrogram Client
app = Client(
    "link_tracker_bot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    in_memory=True,
)

# --- Helper Functions ---

def generate_owner_code() -> str:
    """Generate a random 3-character code (lowercase letters + digits)."""
    import random
    import string
    return ''.join(random.choices(string.ascii_lowercase + string.digits, k=3))

def sanitize_slug(text: str) -> str:
    """Sanitize text to be used as a slug."""
    slug = re.sub(r'[^a-zA-Z0-9\s-]', '', text)
    slug = re.sub(r'[\s-]+', '-', slug).strip('-').lower()
    return slug[:50]

def track_user(user):
    """Track user interaction in data.db."""
    if not user:
        return
    
    try:
        conn = sqlite3.connect(DATA_DB_PATH)
        cursor = conn.cursor()
        
        # Check if user exists
        cursor.execute('SELECT user_id FROM users WHERE user_id = ?', (user.id,))
        exists = cursor.fetchone()
        
        if exists:
            # Update existing user
            cursor.execute('''
                UPDATE users 
                SET username = ?, 
                    first_name = ?, 
                    last_name = ?, 
                    language_code = ?,
                    last_seen = CURRENT_TIMESTAMP,
                    interaction_count = interaction_count + 1
                WHERE user_id = ?
            ''', (user.username, user.first_name, user.last_name, user.language_code, user.id))
        else:
            # Insert new user
            cursor.execute('''
                INSERT INTO users (user_id, username, first_name, last_name, language_code, is_bot, interaction_count)
                VALUES (?, ?, ?, ?, ?, ?, 1)
            ''', (user.id, user.username, user.first_name, user.last_name, user.language_code, 1 if user.is_bot else 0))
        
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"Error tracking user: {e}")

def save_group_to_db(chat):
    """Save/update group information to data.db."""
    if not chat:
        return
    
    try:
        conn = sqlite3.connect(DATA_DB_PATH)
        cursor = conn.cursor()
        
        # Get chat type as string
        chat_type = str(chat.type).replace("ChatType.", "").lower() if chat.type else "unknown"
        
        # Save or update group
        cursor.execute('''
            INSERT OR REPLACE INTO groups (chat_id, chat_type, title, username, description, last_seen)
            VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ''', (chat.id, chat_type, chat.title, chat.username, chat.description))
        
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"Error saving group: {e}")

def save_member_to_db(chat_id: int, user):
    """Save/update member information to data.db (passive tracking)."""
    if not user:
        return
    
    try:
        conn = sqlite3.connect(DATA_DB_PATH)
        cursor = conn.cursor()
        
        # Check if member exists
        cursor.execute('SELECT id FROM members WHERE chat_id = ? AND user_id = ?', (chat_id, user.id))
        exists = cursor.fetchone()
        
        if exists:
            # Update existing member
            cursor.execute('''
                UPDATE members 
                SET username = ?, 
                    first_name = ?, 
                    last_name = ?,
                    last_seen = CURRENT_TIMESTAMP,
                    message_count = message_count + 1
                WHERE chat_id = ? AND user_id = ?
            ''', (user.username, user.first_name, user.last_name, chat_id, user.id))
        else:
            # Insert new member
            cursor.execute('''
                INSERT INTO members (chat_id, user_id, username, first_name, last_name, message_count)
                VALUES (?, ?, ?, ?, ?, 1)
            ''', (chat_id, user.id, user.username, user.first_name, user.last_name))
        
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"Error saving member: {e}")

def get_link_from_db(link_id: str):
    """Retrieve link from SQLite database."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    cursor.execute('SELECT * FROM links WHERE link_id = ?', (link_id,))
    row = cursor.fetchone()
    conn.close()
    
    if row:
        return dict(row)
    return None

async def get_username_supergroup(client: Client, username_target: str):
    result = await client.get_chat(username_target)
    
    username = None
    chat_id = None

    if str(result.type).split('.')[1] == 'CHANNEL':
        if result.linked_chat:
            username = result.linked_chat.username
            chat_id = result.linked_chat.id
    else:
        username = result.username
        chat_id = result.id
    
    return username, chat_id

def save_link_to_db(user_id: int, username_target: str, owner_code: str, group_username: str, group_id: int):
    """Save new link to SQLite database."""
    link_id = f"{username_target}-{owner_code}"
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute('''
        INSERT OR REPLACE INTO links (link_id, owner_id, username_target, owner_code, clicks, group_username, group_id)
        VALUES (?, ?, ?, ?, 0, ?, ?)
    ''', (link_id, user_id, username_target, owner_code, group_username, group_id))
    
    conn.commit()
    conn.close()
    return link_id

def log_click(link_id: str, user, source: str = None):
    """Log a click event to SQLite database."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Increment counter
    cursor.execute('UPDATE links SET clicks = clicks + 1 WHERE link_id = ?', (link_id,))
    
    # Log detail
    cursor.execute('''
        INSERT INTO click_stats (link_id, sumber, user_id, first_name, last_name, username, language_code)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    ''', (link_id, source, user.id, user.first_name, user.last_name, user.username, user.language_code))
    
    conn.commit()
    conn.close()

async def get_user_tracked_links(user_id: int, chat_username: str, chat_id: int):
    """Get tracked links that a user clicked for a specific chat (by username or ID)."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    # Logic:
    # 1. If we have both ID and Username, match either (handling username changes or ID stability).
    # 2. If only one is available, match that one.
    
    if chat_username and chat_id:
        cursor.execute('''
            SELECT DISTINCT l.link_id, l.owner_code, l.username_target
            FROM links l
            INNER JOIN click_stats cs ON l.link_id = cs.link_id
            WHERE cs.user_id = ? AND (l.group_id = ? OR LOWER(l.group_username) = LOWER(?))
        ''', (user_id, chat_id, chat_username.replace("@", "")))
    elif chat_username:
        cursor.execute('''
            SELECT DISTINCT l.link_id, l.owner_code, l.username_target
            FROM links l
            INNER JOIN click_stats cs ON l.link_id = cs.link_id
            WHERE cs.user_id = ? AND LOWER(l.group_username) = LOWER(?)
        ''', (user_id, chat_username.replace("@", "")))
    elif chat_id:
        cursor.execute('''
            SELECT DISTINCT l.link_id, l.owner_code, l.username_target
            FROM links l
            INNER JOIN click_stats cs ON l.link_id = cs.link_id
            WHERE cs.user_id = ? AND l.group_id = ?
        ''', (user_id, chat_id))
    else:
        # No identifier provided
        conn.close()
        return []
    
    results = [dict(row) for row in cursor.fetchall()]
    conn.close()
    
    return results

def log_user_activity(user_id: int, username: str, chat_id: int, chat_title: str, 
                      chat_username: str, owner_code: str, link_id: str, 
                      message_text: str, message_id: int):
    """Log user activity in a group/channel."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Truncate message text to avoid excessive storage (max 500 chars)
    truncated_message = message_text[:500] if message_text else None
    
    cursor.execute('''
        INSERT INTO user_activity 
        (user_id, username, chat_id, chat_title, chat_username, owner_code, link_id, message_text, message_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (user_id, username, chat_id, chat_title, chat_username, owner_code, link_id, truncated_message, message_id))
    
    conn.commit()
    conn.close()

# --- Conversation State ---
user_states = {}

# --- Bot Handlers ---

@app.on_message(filters.command("start"))
async def start_handler(client: Client, message: Message):
    """Handle /start command. Can be a normal start or a deep link redirect."""
    track_user(message.from_user)
    args = message.command
    
    if len(args) > 1:
        # Deep link usage: /start target-code OR /start target-code-source
        payload = args[1]
        
        # Parse payload
        parts = payload.split('-')
        
        if len(parts) < 2:
            await message.reply_text("❌ Invalid link format.")
            return

        # Basic parsing: last part is always code? No, structure is username-code[-source]
        # But username can contain hyphens? Telegram usernames cannot contain hyphens.
        # So split by hyphen is safe if username is strict.
        
        # Case 1: username-code (2 parts)
        # Case 2: username-code-source (3 parts or more?)
        # User said: usernametarget-kodeunikouner-usernamesumber
        # But wait, code is generated as {last4_userid}{2_random_chars}. User ID is numeric. 
        # So code might look like "1234ab".
        
        # Let's assume standard format: Part1=Username, Part2=Code, Part3+=Source
        # But if source is missing?
        # We need to find the "Code" part. It is the owner_code.
        
        target = parts[0]
        code = parts[1]
        source = "-".join(parts[2:]) if len(parts) > 2 else None
        
        # Construct link_id for lookup
        link_id = f"{target}-{code}"
        
        link_data = get_link_from_db(link_id)
        
        if link_data:
            username_target = link_data.get('username_target')
            original_link = f"https://t.me/{username_target}"
            
            # Log the click
            try:
                log_click(link_id, message.from_user, source)
            except Exception as e:
                print(f"Error logging click: {e}")
            
            await message.reply_text(
                f"🔗 **Redirecting to @{username_target}...**\n\n[Click here to open]({original_link})",
                disable_web_page_preview=True,
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton(f"Open @{username_target} 🚀", url=original_link)]
                ])
            )
        else:
            await message.reply_text("❌ Link not found or expired.")
            
    else:
        # Normal start
        await message.reply_text(
            "👋 **Welcome to Link Tracker Bot!**\n\n"
            "Use /addlink to create a tracked shortcut to a Group/Channel.\n"
            "Use /mylinks to view your links.\n"
            "Use /export to get click stats.\n"
            "Use /activity to get user activity logs.\n"
            "Use /deletelink to delete a tracked link.\n"
        )

@app.on_message(filters.command("addlink"))
async def add_link_handler(client: Client, message: Message):
    """Create a new tracked link."""
    track_user(message.from_user)
    user_id = message.from_user.id
    
    # Initialize state
    user_states[user_id] = {'step': 'waiting_target'}
    
    await message.reply_text(
        "🔗 **Send the Target Username**\n"
        "Example: `@username` or `t.me/username`\n\n"
        "Send /cancel to cancel."
    )

@app.on_message(filters.text & filters.private & ~filters.command(["start", "mylinks", "export", "addlink", "activity", "deletelink"]))
async def text_handler(client: Client, message: Message):
    """Handle text messages for conversation."""
    track_user(message.from_user)
    user_id = message.from_user.id
    
    if user_id not in user_states:
        return
    
    state = user_states[user_id]
    
    # Cancel handler
    if message.text == "/cancel":
        user_states.pop(user_id, None)
        await message.reply_text("❌ Cancelled.")
        return
    
    # Step 1: Waiting for Target
    if state['step'] == 'waiting_target':
        input_text = message.text.strip()
        
        # Extract username
        username_target = ""
        if "t.me/" in input_text:
            username_target = input_text.split("t.me/")[-1].split("/")[0].split("?")[0]
        else:
            username_target = input_text.replace("@", "").replace("https://", "")
        
        # Basic validation
        username_target = re.sub(r'[^a-zA-Z0-9_]', '', username_target)
        
        if not username_target:
             await message.reply_text("❌ Invalid username. Please send a valid Telegram username or link.")
             return

        owner_code = generate_owner_code()
        
        username, chat_id = await get_username_supergroup(client, username_target)
        # Save to DB
        try:
            link_id = save_link_to_db(
                user_id=user_id,
                username_target=username_target,
                owner_code=owner_code,
                group_username=username,
                group_id=chat_id
            )
        except Exception as e:
            print(f"DB Error: {e}")
            await message.reply_text("An error occurred while saving the link.")
            user_states.pop(user_id, None)
            return

        final_link = f"https://t.me/{BOT_USERNAME}?start={link_id}"
        # With source example
        example_source_link = f"{final_link}-fb"

        await message.reply_text(
            f"✅ **Link Created!**\n\n"
            f"🎯 **Target:** @{username_target}\n"
            f"🔗 **Referral Link:** \n   `{final_link}`\n"
            f"🔗 **With Source Example (e.g. fb):** \n   `{example_source_link}`\n"
        )
        
        # Clear state
        user_states.pop(user_id, None)

async def send_mylinks_menu(client: Client, chat_id: int, user_id: int, message_to_edit: Message = None):
    """Helper to send/edit the My Links menu."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    # Get unique targets and total clicks for each
    cursor.execute('''
        SELECT username_target, SUM(clicks) as total_clicks
        FROM links 
        WHERE owner_id = ?
        GROUP BY username_target
    ''', (user_id,))
    
    targets = [dict(row) for row in cursor.fetchall()]
    conn.close()

    if not targets:
        text = "You haven't created any links yet."
        if message_to_edit:
            await message_to_edit.edit_text(text)
        else:
            await client.send_message(chat_id, text)
        return

    # Show as buttons with click counts
    buttons = []
    for t in targets:
        display_text = f"@{t['username_target']} ({t['total_clicks']} clicks)"
        buttons.append([InlineKeyboardButton(display_text, callback_data=f"showlink_{t['username_target']}")])
    
    text = "📂 **Select a Target to view info:**"
    markup = InlineKeyboardMarkup(buttons)
    
    if message_to_edit:
        await message_to_edit.edit_text(text, reply_markup=markup)
    else:
        await client.send_message(chat_id, text, reply_markup=markup)

@app.on_message(filters.command("mylinks"))
async def mylinks_handler(client: Client, message: Message):
    """List all target usernames to select from."""
    track_user(message.from_user)
    user_id = message.from_user.id
    await send_mylinks_menu(client, message.chat.id, user_id)

@app.on_callback_query(filters.regex(r"^showlink_"))
async def show_link_callback(client: Client, callback_query):
    target = callback_query.data.split("_", 1)[1]
    user_id = callback_query.from_user.id
    
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT * FROM links 
        WHERE owner_id = ? AND username_target = ?
    ''', (user_id, target))
    
    link = cursor.fetchone()
    conn.close()
    
    if not link:
        await callback_query.answer("Link not found.", show_alert=True)
        return
        
    link_data = dict(link)
    link_id = link_data['link_id']
    username_target = link_data['username_target']
    
    final_link = f"https://t.me/{BOT_USERNAME}?start={link_id}"
    
    await callback_query.message.edit_text(
        f"🎯 **Target:** @{username_target}\n\n"
        f"🔗 **Link:** `{final_link}`\n"
        f"📊 **Total Clicks:** {link_data.get('clicks', 0)}\n\n"
        f"To track source, append `-somename` to the link.\n"
        f"Ex: `{final_link}-twitter`",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🔙 Back", callback_data="back_mylinks")]
        ])
    )

@app.on_callback_query(filters.regex(r"^back_mylinks"))
async def back_mylinks_callback(client: Client, callback_query):
    # Re-trigger mylinks logic using the helper
    # CORRECT FIX: Use callback_query.from_user.id for user_id
    await send_mylinks_menu(
        client, 
        callback_query.message.chat.id, 
        callback_query.from_user.id, 
        message_to_edit=callback_query.message
    )

@app.on_message(filters.command("export"))
async def export_handler(client: Client, message: Message):
    """Export click stats to CSV."""
    track_user(message.from_user)
    user_id = message.from_user.id
    
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    cursor.execute('SELECT * FROM links WHERE owner_id = ?', (user_id,))
    links = {row['link_id']: dict(row) for row in cursor.fetchall()}
    conn.close()
    
    if not links:
        await message.reply_text("No links found to export.")
        return

    # Create buttons
    buttons = []
    for doc_id, data in links.items():
        btn_text = f"@{data.get('username_target')} ({data.get('clicks')} clicks)"
        buttons.append([InlineKeyboardButton(btn_text, callback_data=f"export_{doc_id}")])

    await message.reply_text(
        "📊 **Select a link to export data:**",
        reply_markup=InlineKeyboardMarkup(buttons)
    )

@app.on_callback_query(filters.regex(r"^export_"))
async def export_callback(client: Client, callback_query):
    try:
        doc_id = callback_query.data.split("_", 1)[1]
        
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        cursor.execute('SELECT * FROM links WHERE link_id = ?', (doc_id,))
        link_row = cursor.fetchone()
        
        if not link_row or link_row['owner_id'] != callback_query.from_user.id:
            conn.close()
            await callback_query.answer("Link not found or access denied.", show_alert=True)
            return
        
        link_data = dict(link_row)

        await callback_query.message.edit_text("⏳ Generating CSV & Summary...")

        # Fetch click stats
        cursor.execute('''
            SELECT sumber, user_id, first_name, username, language_code, timestamp
            FROM click_stats 
            WHERE link_id = ? 
            ORDER BY timestamp DESC
        ''', (doc_id,))
        
        stats = cursor.fetchall()
        conn.close()
        
        if len(stats) == 0:
            await callback_query.message.edit_text("No clicks recorded for this link yet.")
            return
        
        # 1. GENERATE CSV (Unique Users with Enrichment)
        output_csv = io.StringIO()
        writer = csv.writer(output_csv)
        writer.writerow(['User ID', 'First Name', 'Username', 'Language', 'First Click', 'Join Status', 'Activity Count'])

        # Get unique users and their first click
        conn = sqlite3.connect(DB_PATH) # Re-open for this query
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute('''
            SELECT user_id, first_name, username, language_code, MIN(timestamp) as first_click
            FROM click_stats 
            WHERE link_id = ? 
            GROUP BY user_id
            ORDER BY first_click DESC
        ''', (doc_id,))
        
        unique_users = cursor.fetchall()
        
        # We also need source counts for the summary txt
        cursor.execute('SELECT sumber FROM click_stats WHERE link_id = ?', (doc_id,))
        all_clicks_sources = cursor.fetchall()
        
        source_counts = {}
        for row in all_clicks_sources:
            src = row['sumber'] or "None"
            source_counts[src] = source_counts.get(src, 0) + 1

        conn.close() 
        
        # Process each unique user for enrichment
        username_target = link_data.get('username_target')
        
        for user in unique_users:
            uid = user['user_id']
            
            # 1. Check Join Status
            join_status = "Unknown"
            try:
                # get_chat_member fails if target is a User or Bot is not Admin
                member = await client.get_chat_member(username_target, uid)
                join_status = member.status.name
            except Exception:
                join_status = "Not Joined"
                
            # 2. Check Activity Count
            conn_act = sqlite3.connect(DB_PATH)
            cursor_act = conn_act.cursor()
            cursor_act.execute('SELECT COUNT(*) FROM user_activity WHERE link_id = ? AND user_id = ?', (doc_id, uid))
            act_count = cursor_act.fetchone()[0]
            conn_act.close()
            
            writer.writerow([
                uid,
                user['first_name'],
                user['username'],
                user['language_code'],
                user['first_click'],
                join_status,
                act_count
            ])

        output_csv.seek(0)
        
        # 2. GENERATE SUMMARY TXT
        output_txt = io.StringIO()
        output_txt.write(f"📊 Click Summary for @{link_data.get('username_target')}\n")
        output_txt.write(f"Total Clicks: {len(all_clicks_sources)}\n")
        output_txt.write(f"Unique Users: {len(unique_users)}\n\n")
        output_txt.write("🔹 Clicks by Source:\n")
        
        # Sort sources by count DESC
        sorted_sources = sorted(source_counts.items(), key=lambda item: item[1], reverse=True)
        for src, count in sorted_sources:
            output_txt.write(f"- {src}: {count}\n")
            
        output_txt.seek(0)

        # SEND FILES
        
        # Send CSV
        filename_csv = f"clicks_{link_data.get('username_target')}.csv"
        bio_csv = io.BytesIO(output_csv.getvalue().encode('utf-8'))
        bio_csv.name = filename_csv
        
        await client.send_document(
            chat_id=callback_query.message.chat.id,
            document=bio_csv,
            caption=f"📊 Raw Data (Unique Users) for **@{link_data.get('username_target')}**"
        )
        
        # Send Summary
        filename_txt = f"summary_{link_data.get('username_target')}.txt"
        bio_txt = io.BytesIO(output_txt.getvalue().encode('utf-8'))
        bio_txt.name = filename_txt
        
        await client.send_document(
            chat_id=callback_query.message.chat.id,
            document=bio_txt,
            caption=f"📈 Summary Report"
        )
        
        await callback_query.message.delete()
        
    except Exception as e:
        print(f"Error in export handler: {e}")
        await callback_query.message.edit_text(f"❌ An error occurred: {str(e)[:100]}")



@app.on_message(filters.command("activity"))
async def activity_handler(client: Client, message: Message):
    """Export user activity data for tracked links."""
    try:
        track_user(message.from_user)
        user_id = message.from_user.id
        
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        cursor.execute('SELECT * FROM links WHERE owner_id = ?', (user_id,))
        links = {row['link_id']: dict(row) for row in cursor.fetchall()}
        conn.close()
        
        if not links:
            await message.reply_text("No links found.")
            return

        # Create buttons to select link
        buttons = []
        for doc_id, data in links.items():
            # Count activities for this link
            try:
                conn = sqlite3.connect(DB_PATH)
                cursor = conn.cursor()
                cursor.execute('SELECT COUNT(*) as count FROM user_activity WHERE link_id = ?', (doc_id,))
                activity_count = cursor.fetchone()[0]
                conn.close()
            except Exception as e:
                print(f"Error counting activity for {doc_id}: {e}")
                activity_count = 0
            
            btn_text = f"@{data.get('username_target')} ({activity_count} activities)"
            buttons.append([InlineKeyboardButton(btn_text, callback_data=f"activity_{doc_id}")])

        await message.reply_text(
            "📊 **Select a link to export activity data:**",
            reply_markup=InlineKeyboardMarkup(buttons)
        )
    except Exception as e:
        print(f"Error in activity handler: {e}")
        await message.reply_text("An error occurred. Please try again later.")

@app.on_callback_query(filters.regex(r"^activity_"))
async def activity_callback(client: Client, callback_query):
    """Handle activity export callback."""
    try:
        doc_id = callback_query.data.split("_", 1)[1]
        
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        cursor.execute('SELECT * FROM links WHERE link_id = ?', (doc_id,))
        link_row = cursor.fetchone()
        
        if not link_row or link_row['owner_id'] != callback_query.from_user.id:
            conn.close()
            await callback_query.answer("Link not found or access denied.", show_alert=True)
            return
        
        link_data = dict(link_row)

        await callback_query.message.edit_text("⏳ Generating activity CSV...")

        # Fetch activity data
        cursor.execute('''
            SELECT user_id, username, chat_id, chat_title, chat_username, 
                   owner_code, message_text, message_id, timestamp
            FROM user_activity 
            WHERE link_id = ?
            ORDER BY timestamp DESC
        ''', (doc_id,))
        
        activities = cursor.fetchall()
        conn.close()
        
        if len(activities) == 0:
            await callback_query.message.edit_text("No activity recorded for this link yet.")
            return
        
        # Create CSV
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(['User ID', 'Chat ID', 'Message ID', 'Owner Code', 'Timestamp', 
                         'Username', 'Chat Username', 'Chat Title', 'Message Preview'])

        for activity in activities:
            # Truncate message for preview
            message_preview = activity['message_text'][:100] if activity['message_text'] else ""
            
            writer.writerow([
                activity['user_id'],
                activity['chat_id'],
                activity['message_id'],
                activity['owner_code'],
                activity['timestamp'],
                activity['username'],
                activity['chat_username'],
                activity['chat_title'],
                message_preview,
            ])

        output.seek(0)

        # Send file
        filename = f"activity_{link_data.get('username_target')}.csv"
        bio = io.BytesIO(output.getvalue().encode('utf-8'))
        bio.name = filename
        
        caption_text = f"📊 Activity Export for **@{link_data.get('username_target')}**\\nTotal Activities: {len(activities)}"
        
        await client.send_document(
            chat_id=callback_query.message.chat.id,
            document=bio,
            caption=caption_text
        )
        
        await callback_query.message.delete()
    except Exception as e:
        print(f"Error in activity callback: {e}")
        await callback_query.message.edit_text("An error occurred generating the file.")

@app.on_message(filters.command("deletelink"))
async def deletelink_handler(client: Client, message: Message):
    """Delete a tracked link."""
    track_user(message.from_user)
    user_id = message.from_user.id
    
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    cursor.execute('SELECT * FROM links WHERE owner_id = ?', (user_id,))
    links = {row['link_id']: dict(row) for row in cursor.fetchall()}
    conn.close()
    
    if not links:
        await message.reply_text("You don't have any links to delete.")
        return

    # Create buttons to select link to delete
    buttons = []
    for doc_id, data in links.items():
        btn_text = f"@{data.get('username_target')} ({data.get('clicks')} clicks)"
        buttons.append([InlineKeyboardButton(btn_text, callback_data=f"delsel_{doc_id}")])

    await message.reply_text(
        "🗑 **Select a link to delete:**",
        reply_markup=InlineKeyboardMarkup(buttons)
    )

@app.on_callback_query(filters.regex(r"^delsel_"))
async def delete_select_callback(client: Client, callback_query):
    """Handle link selection for deletion - show confirmation."""
    doc_id = callback_query.data.split("_", 1)[1]
    
    # Verify ownership
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    cursor.execute('SELECT * FROM links WHERE link_id = ?', (doc_id,))
    link_row = cursor.fetchone()
    conn.close()
    
    if not link_row or link_row['owner_id'] != callback_query.from_user.id:
        await callback_query.answer("Link not found or access denied.", show_alert=True)
        return
    
    link_data = dict(link_row)
    
    # Show confirmation
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Yes, Delete", callback_data=f"delconf_{doc_id}")],
        [InlineKeyboardButton("❌ Cancel", callback_data=f"delcanc_{doc_id}")]
    ])
    
    await callback_query.message.edit_text(
        f"⚠️ **Confirm Deletion**\\n\\n"
        f"Are you sure you want to delete this link?\\n\\n"
        f"🎯 **Target:** @{link_data.get('username_target')}\\n"
        f"📊 **Clicks:** {link_data.get('clicks')}\\n\\n"
        f"This will also delete all click stats and activity logs for this link.",
        reply_markup=keyboard
    )

@app.on_callback_query(filters.regex(r"^delconf_"))
async def delete_confirm_callback(client: Client, callback_query):
    """Handle deletion confirmation."""
    doc_id = callback_query.data.split("_", 1)[1]
    
    # Verify ownership one more time
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    cursor.execute('SELECT * FROM links WHERE link_id = ?', (doc_id,))
    link_row = cursor.fetchone()
    
    if not link_row or link_row['owner_id'] != callback_query.from_user.id:
        conn.close()
        await callback_query.answer("Link not found or access denied.", show_alert=True)
        return
    
    link_data = dict(link_row)
    
    # Delete cascading: user_activity -> click_stats -> links
    cursor.execute('DELETE FROM user_activity WHERE link_id = ?', (doc_id,))
    cursor.execute('DELETE FROM click_stats WHERE link_id = ?', (doc_id,))
    cursor.execute('DELETE FROM links WHERE link_id = ?', (doc_id,))
    
    conn.commit()
    conn.close()
    
    await callback_query.message.edit_text(
        f"✅ **Link Deleted Successfully**\\n\\n"
        f"🎯 @{link_data.get('username_target')} has been removed.\\n"
        f"All associated data (clicks and activity logs) have also been deleted."
    )

@app.on_callback_query(filters.regex(r"^delcanc_"))
async def delete_cancel_callback(client: Client, callback_query):
    """Handle deletion cancellation."""
    await callback_query.message.edit_text("❌ Deletion cancelled.")

# --- Activity Monitoring Handlers ---
@app.on_message(filters.group & ~filters.bot & ~filters.service)
async def monitor_group_activity(client: Client, message: Message):
    """Monitor user activity in groups."""
    try:
        track_user(message.from_user)
        # Skip if no user
        if not message.from_user:
            return
        
        # Save group info (passive tracking)
        save_group_to_db(message.chat)
        
        # Save member info (passive tracking)
        save_member_to_db(message.chat.id, message.from_user)
        
        # Skip if no text (for activity logging)
        if not (message.text or message.caption):
            return
        
        user_id = message.from_user.id
        chat_id = message.chat.id
        chat_username = message.chat.username

        if not (chat_username or chat_id):
            # Cannot track without username since we rely on username_target
            return

        # Get tracked links for this user in this chat
        tracked_links = await get_user_tracked_links(user_id, chat_username, chat_id)
        if not tracked_links:
            return


        
        # Log activity for the first tracked link (or all if needed)
        for link in tracked_links:
            log_user_activity(
                user_id=user_id,
                username=message.from_user.username or "",
                chat_id=chat_id,
                chat_title=message.chat.title or "",
                chat_username=chat_username,
                owner_code=link['owner_code'],
                link_id=link['link_id'],
                message_text=message.text or message.caption,
                message_id=message.id
            )

    except Exception as e:
        print(f"Error monitoring group activity: {e}")


if __name__ == "__main__":
    print("Starting Link Tracker Bot...")
    app.run()

