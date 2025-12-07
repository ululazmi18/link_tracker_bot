import os
import sys
import csv
import io
import re
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
    
    # Create links table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS links (
            id TEXT PRIMARY KEY,
            owner_id INTEGER NOT NULL,
            original_link TEXT NOT NULL,
            slug TEXT NOT NULL,
            owner_code TEXT NOT NULL,
            alias TEXT,
            target_chat_id INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            clicks INTEGER DEFAULT 0
        )
    ''')
    
    # Migrate existing database: add target_chat_id column if it doesn't exist
    try:
        cursor.execute("SELECT target_chat_id FROM links LIMIT 1")
    except sqlite3.OperationalError:
        # Column doesn't exist, add it
        cursor.execute("ALTER TABLE links ADD COLUMN target_chat_id INTEGER")
        print("Added target_chat_id column to links table")
    
    # Create click_stats table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS click_stats (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            link_id TEXT NOT NULL,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            user_id INTEGER,
            first_name TEXT,
            username TEXT,
            language_code TEXT,
            FOREIGN KEY (link_id) REFERENCES links(id)
        )
    ''')
    
    # Create user_activity table for monitoring user interactions in groups/channels
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
            FOREIGN KEY (link_id) REFERENCES links(id)
        )
    ''')
    
    # Create indexes for better performance
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_owner_id ON links(owner_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_link_id ON click_stats(link_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_user_id ON click_stats(user_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_user_activity_user_id ON user_activity(user_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_user_activity_chat_id ON user_activity(chat_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_user_activity_link_id ON user_activity(link_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_user_activity_owner_code ON user_activity(owner_code)')
    
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
    logger.error(f"Failed to initialize database: {e}")
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

def generate_owner_code(user_id: int) -> str:
    """Generate a short code for the owner based on user_id."""
    import random
    import string
    suffix = ''.join(random.choices(string.ascii_lowercase + string.digits, k=2))
    return f"{str(user_id)[-4:]}{suffix}"

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
        logger.error(f"Error tracking user: {e}")

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
        logger.error(f"Error saving group: {e}")

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
        logger.error(f"Error saving member: {e}")

def get_link_from_db(slug: str, owner_code: str):
    """Retrieve link from SQLite database."""
    doc_id = f"{slug}-{owner_code}"
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    cursor.execute('SELECT * FROM links WHERE id = ?', (doc_id,))
    row = cursor.fetchone()
    conn.close()
    
    if row:
        return dict(row)
    return None

def save_link_to_db(user_id: int, original_link: str, slug: str, owner_code: str, alias: str = None, target_chat_id: int = None):
    """Save new link to SQLite database."""
    doc_id = f"{slug}-{owner_code}"
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute('''
        INSERT OR REPLACE INTO links (id, owner_id, original_link, slug, owner_code, alias, target_chat_id, clicks)
        VALUES (?, ?, ?, ?, ?, ?, ?, 0)
    ''', (doc_id, user_id, original_link, slug, owner_code, alias or slug, target_chat_id))
    
    conn.commit()
    conn.close()
    return doc_id

def log_click(doc_id: str, user):
    """Log a click event to SQLite database."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Increment counter
    cursor.execute('UPDATE links SET clicks = clicks + 1 WHERE id = ?', (doc_id,))
    
    # Log detail
    cursor.execute('''
        INSERT INTO click_stats (link_id, user_id, first_name, username, language_code)
        VALUES (?, ?, ?, ?, ?)
    ''', (doc_id, user.id, user.first_name, user.username, user.language_code))
    
    conn.commit()
    conn.close()

async def extract_chat_id_from_link(original_link: str):
    """Extract chat ID from Telegram link if possible."""
    try:
        if "t.me/" not in original_link:
            return None
        
        # Extract username from link
        username = original_link.split("t.me/")[-1].split("/")[0].split("?")[0].strip()
        
        if not username or username.startswith("+"):
            return None
        
        # Try to get chat info
        try:
            chat = await app.get_chat(username)
            return chat.id
        except Exception as e:
            logger.warning(f"Could not resolve chat for {username}: {e}")
            return None
    except Exception as e:
        logger.error(f"Error extracting chat ID: {e}")
        return None

async def check_user_membership(chat_id: int, user_id: int) -> str:
    """Check user's membership status in a chat."""
    try:
        member = await app.get_chat_member(chat_id, user_id)
        return member.status
    except Exception as e:
        return "Not Joined"

async def get_linked_chat(chat_id: int):
    """Get linked discussion group for a channel."""
    try:
        chat = await app.get_chat(chat_id)
        if not str(chat.type) == "ChatType.CHANNEL":
            if hasattr(chat, 'linked_chat') and chat.linked_chat:
                linked_chat = await app.get_chat(chat.linked_chat.id)
                return linked_chat
        else:
            return chat
    except:
        return None

async def get_user_tracked_links(user_id: int, chat_id: int):
    """Get tracked links that a user clicked for a specific chat."""
    chat = await get_linked_chat(chat_id)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    # Find links where:
    # 1. User clicked the link (in click_stats)
    # 2. Link points to the chat_id (target_chat_id matches)
    cursor.execute('''
        SELECT DISTINCT l.id, l.owner_code, l.slug
        FROM links l
        INNER JOIN click_stats cs ON l.id = cs.link_id
        WHERE cs.user_id = ? AND l.target_chat_id = ?
    ''', (user_id, chat.id))
    
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
        # Deep link usage: /start slug-owner_code
        payload = args[1]
        
        if '-' not in payload:
            await message.reply_text("Invalid link format.")
            return

        slug, owner_code = payload.rsplit('-', 1)
        
        link_data = get_link_from_db(slug, owner_code)
        
        if link_data:
            original_link = link_data.get('original_link')
            
            # Log the click
            try:
                log_click(link_data['id'], message.from_user)
            except Exception as e:
                logger.error(f"Error logging click: {e}")
            
            await message.reply_text(
                f"🔗 **Redirecting...**\n\n[Click here to open destination]({original_link})",
                disable_web_page_preview=True,
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("Open Link 🚀", url=original_link)]
                ])
            )
        else:
            await message.reply_text("❌ Link not found or expired.")
            
    else:
        # Normal start
        await message.reply_text(
            "👋 **Welcome to Link Tracker Bot!**\n\n"
            "Use /addlink to create a tracked shortcut.\n"
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
    user_states[user_id] = {'step': 'waiting_url', 'data': {}}
    
    await message.reply_text(
        "🔗 **Send the Destination Link**\n"
        "Can be a URL (https://...) or Telegram Username (@username).\n\n"
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
    
    # Step 1: Waiting for URL
    if state['step'] == 'waiting_url':
        target_link = message.text.strip()
        
        # Basic validation/normalization
        if target_link.startswith('@'):
            target_link = f"https://t.me/{target_link[1:]}"
        elif not target_link.startswith('http'):
            target_link = f"https://{target_link}"
        
        state['data']['target_link'] = target_link
        state['step'] = 'waiting_alias'
        
        await message.reply_text(
            "🏷 **Enter a Name/Alias for this link** (Optional)\n"
            "Send /skip to use the domain/slug as alias."
        )
    
    # Step 2: Waiting for Alias
    elif state['step'] == 'waiting_alias':
        alias = message.text.strip() if message.text != "/skip" else None
        target_link = state['data']['target_link']
        
        # Generate Slug
        default_slug = ""
        if "t.me/" in target_link:
            default_slug = target_link.split("t.me/")[-1].split("?")[0].strip('/')
        else:
            clean_url = target_link.replace("https://", "").replace("http://", "")
            default_slug = clean_url.replace('/', '-')

        slug = sanitize_slug(default_slug)
        if not slug:
            slug = "link"

        owner_code = generate_owner_code(message.from_user.id)
        
        # Try to extract chat ID from Telegram link
        target_chat_id = None
        if "t.me/" in target_link:
            target_chat_id = await extract_chat_id_from_link(target_link)
        
        # Save to DB
        try:
            doc_id = save_link_to_db(
                user_id=message.from_user.id,
                original_link=target_link,
                slug=slug,
                owner_code=owner_code,
                alias=alias,
                target_chat_id=target_chat_id
            )
        except Exception as e:
            logger.error(f"DB Error: {e}")
            await message.reply_text("An error occurred while saving the link.")
            user_states.pop(user_id, None)
            return

        # Construct Final Link
        full_slug = f"{slug}-{owner_code}"
        if len(full_slug) > 64:
            allowed_len = 64 - len(owner_code) - 1
            slug = slug[:allowed_len]
            full_slug = f"{slug}-{owner_code}"
            save_link_to_db(message.from_user.id, target_link, slug, owner_code, alias, target_chat_id)

        final_link = f"https://t.me/{BOT_USERNAME}?start={full_slug}"

        await message.reply_text(
            f"✅ **Link Created!**\n\n"
            f"🔖 **Alias:** {alias or slug}\n"
            f"🔗 **Referral Link:** \n   `{final_link}`\n"
            f"🔗 **Markdown Link for bot:** \n   `[{target_link}]({final_link})`\n"
            f"🎯 **Target:** {target_link}"
        )
        
        # Clear state
        user_states.pop(user_id, None)

@app.on_message(filters.command("mylinks"))
async def mylinks_handler(client: Client, message: Message):
    """List all links created by the user."""
    track_user(message.from_user)
    user_id = message.from_user.id
    
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    cursor.execute('SELECT * FROM links WHERE owner_id = ?', (user_id,))
    results = [dict(row) for row in cursor.fetchall()]
    conn.close()

    if not results:
        await message.reply_text("You haven't created any links yet.")
        return

    response = "📂 **Your Links:**\n\n"
    for link in results:
        slug = link.get('slug')
        owner_code = link.get('owner_code')
        target_link = link.get('original_link')
        short_link = f"https://t.me/{BOT_USERNAME}?start={slug}-{owner_code}"
        
        response += (
            f"🔹 **{link.get('alias')}**\n"
            f"   Referral Link: \n   `{short_link}`\n"
            f"   Markdown Link: \n   `[{target_link}]({short_link})`\n"
            f"   Target: {link.get('original_link')}\n"
            f"   Clicks: {link.get('clicks', 0)}\n\n"
        )
        
        if len(response) > 3500:
            await message.reply_text(response)
            response = ""

    if response:
        await message.reply_text(response)

@app.on_message(filters.command("export"))
async def export_handler(client: Client, message: Message):
    """Export click stats to CSV."""
    track_user(message.from_user)
    user_id = message.from_user.id
    
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    cursor.execute('SELECT * FROM links WHERE owner_id = ?', (user_id,))
    links = {row['id']: dict(row) for row in cursor.fetchall()}
    conn.close()
    
    if not links:
        await message.reply_text("No links found to export.")
        return

    # Create buttons to select link
    buttons = []
    for doc_id, data in links.items():
        btn_text = f"{data.get('alias')} ({data.get('clicks')} clicks)"
        buttons.append([InlineKeyboardButton(btn_text, callback_data=f"export_{doc_id}")])

    await message.reply_text(
        "📊 **Select a link to export data:**",
        reply_markup=InlineKeyboardMarkup(buttons)
    )

@app.on_callback_query(filters.regex(r"^export_"))
async def export_callback(client: Client, callback_query):
    doc_id = callback_query.data.split("_", 1)[1]
    
    # Verify ownership
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    cursor.execute('SELECT * FROM links WHERE id = ?', (doc_id,))
    link_row = cursor.fetchone()
    
    if not link_row or link_row['owner_id'] != callback_query.from_user.id:
        conn.close()
        await callback_query.answer("Link not found or access denied.", show_alert=True)
        return
    
    link_data = dict(link_row)
    target_chat_id = link_data.get('target_chat_id')

    await callback_query.message.edit_text("⏳ Generating CSV...")

    # Fetch click stats - get unique users only (remove duplicates)
    cursor.execute('''
        SELECT user_id, first_name, username, language_code, MIN(timestamp) as first_click
        FROM click_stats 
        WHERE link_id = ? 
        GROUP BY user_id
        ORDER BY first_click DESC
    ''', (doc_id,))
    
    stats = cursor.fetchall()
    conn.close()
    
    if len(stats) == 0:
        await callback_query.message.edit_text("No clicks recorded for this link yet.")
        return
    
    # Create CSV with join status and activity count
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(['User ID', 'First Name', 'Username', 'Language', 'First Click', 'Join Status', 'Activity Count'])

    # Check join status and activity count for each user
    for stat in stats:
        user_id = stat['user_id']
        join_status = ""
        
        # Check join status if we have target_chat_id
        if target_chat_id:
            join_status = await check_user_membership(target_chat_id, user_id)
        
        # Get activity count for this user on this link
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute('''
            SELECT COUNT(*) as count 
            FROM user_activity 
            WHERE link_id = ? AND user_id = ?
        ''', (doc_id, user_id))
        activity_count = cursor.fetchone()[0]
        conn.close()
        
        writer.writerow([
            stat['user_id'],
            stat['first_name'],
            stat['username'],
            stat['language_code'],
            stat['first_click'],
            join_status,
            activity_count
        ])

    output.seek(0)

    # Send file
    filename = f"clicks_{link_data.get('slug')}.csv"
    bio = io.BytesIO(output.getvalue().encode('utf-8'))
    bio.name = filename
    
    joined_count = sum(1 for stat in stats if target_chat_id)
    caption_text = f"📊 Export for **{link_data.get('alias')}**\nUnique Users: {len(stats)}"
    if target_chat_id:
        caption_text += "\n✅ Join status checked"
    
    await client.send_document(
        chat_id=callback_query.message.chat.id,
        document=bio,
        caption=caption_text
    )
    
    await callback_query.message.delete()

@app.on_message(filters.command("activity"))
async def activity_handler(client: Client, message: Message):
    """Export user activity data for tracked links."""
    track_user(message.from_user)
    user_id = message.from_user.id
    
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    cursor.execute('SELECT * FROM links WHERE owner_id = ?', (user_id,))
    links = {row['id']: dict(row) for row in cursor.fetchall()}
    conn.close()
    
    if not links:
        await message.reply_text("No links found.")
        return

    # Create buttons to select link
    buttons = []
    for doc_id, data in links.items():
        # Count activities for this link
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute('SELECT COUNT(*) as count FROM user_activity WHERE link_id = ?', (doc_id,))
        activity_count = cursor.fetchone()[0]
        conn.close()
        
        btn_text = f"{data.get('alias')} ({activity_count} activities)"
        buttons.append([InlineKeyboardButton(btn_text, callback_data=f"activity_{doc_id}")])

    await message.reply_text(
        "📊 **Select a link to export activity data:**",
        reply_markup=InlineKeyboardMarkup(buttons)
    )

@app.on_callback_query(filters.regex(r"^activity_"))
async def activity_callback(client: Client, callback_query):
    """Handle activity export callback."""
    doc_id = callback_query.data.split("_", 1)[1]
    
    # Verify ownership
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    cursor.execute('SELECT * FROM links WHERE id = ?', (doc_id,))
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
    filename = f"activity_{link_data.get('slug')}.csv"
    bio = io.BytesIO(output.getvalue().encode('utf-8'))
    bio.name = filename
    
    caption_text = f"📊 Activity Export for **{link_data.get('alias')}**\\nTotal Activities: {len(activities)}"
    
    await client.send_document(
        chat_id=callback_query.message.chat.id,
        document=bio,
        caption=caption_text
    )
    
    await callback_query.message.delete()

@app.on_message(filters.command("deletelink"))
async def deletelink_handler(client: Client, message: Message):
    """Delete a tracked link."""
    track_user(message.from_user)
    user_id = message.from_user.id
    
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    cursor.execute('SELECT * FROM links WHERE owner_id = ?', (user_id,))
    links = {row['id']: dict(row) for row in cursor.fetchall()}
    conn.close()
    
    if not links:
        await message.reply_text("You don't have any links to delete.")
        return

    # Create buttons to select link to delete
    buttons = []
    for doc_id, data in links.items():
        btn_text = f"{data.get('alias')} ({data.get('clicks')} clicks)"
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
    
    cursor.execute('SELECT * FROM links WHERE id = ?', (doc_id,))
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
        f"🔖 **Alias:** {link_data.get('alias')}\\n"
        f"🎯 **Target:** {link_data.get('original_link')}\\n"
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
    
    cursor.execute('SELECT * FROM links WHERE id = ?', (doc_id,))
    link_row = cursor.fetchone()
    
    if not link_row or link_row['owner_id'] != callback_query.from_user.id:
        conn.close()
        await callback_query.answer("Link not found or access denied.", show_alert=True)
        return
    
    link_data = dict(link_row)
    
    # Delete cascading: user_activity -> click_stats -> links
    cursor.execute('DELETE FROM user_activity WHERE link_id = ?', (doc_id,))
    cursor.execute('DELETE FROM click_stats WHERE link_id = ?', (doc_id,))
    cursor.execute('DELETE FROM links WHERE id = ?', (doc_id,))
    
    conn.commit()
    conn.close()
    
    await callback_query.message.edit_text(
        f"✅ **Link Deleted Successfully**\\n\\n"
        f"🔖 {link_data.get('alias')} has been removed.\\n"
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
        # Skip if no user
        if not message.from_user:
            return
        
        # Save group info (passive tracking)
        save_group_to_db(message.chat)
        
        # Save member info (passive tracking)
        save_member_to_db(message.chat.id, message.from_user)
        
        # Skip if no text (for activity logging)
        if not message.text:
            return
        
        user_id = message.from_user.id
        chat_id = message.chat.id
        
        # Get tracked links for this user in this chat
        tracked_links = await get_user_tracked_links(user_id, chat_id)
        if not tracked_links:
            return
        
        # Log activity for the first tracked link (or all if needed)
        for link in tracked_links:
            log_user_activity(
                user_id=user_id,
                username=message.from_user.username or "",
                chat_id=chat_id,
                chat_title=message.chat.title or "",
                chat_username=message.chat.username or "",
                owner_code=link['owner_code'],
                link_id=link['id'],
                message_text=message.text,
                message_id=message.id
            )
    except Exception as e:
        logger.error(f"Error monitoring group activity: {e}")


if __name__ == "__main__":
    print("Starting Link Tracker Bot...")
    app.run()

