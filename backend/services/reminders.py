"""
Reminders Service - SQLite Database
"""

import sqlite3
from datetime import datetime
from typing import List, Dict


class RemindersService:
    def __init__(self, db_path: str = "ravvyn.db"):
        self.db_path = db_path
        self._init_db()
    
    def _init_db(self):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS reminders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                message TEXT NOT NULL,
                datetime TEXT NOT NULL,
                status TEXT DEFAULT 'active',
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                user_id TEXT DEFAULT 'default'
            )
        ''')
        conn.commit()
        conn.close()
    
    def set_reminder(self, message: str, datetime_str: str, user_id: str = "default") -> Dict:
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO reminders (message, datetime, user_id) VALUES (?, ?, ?)",
            (message, datetime_str, user_id)
        )
        reminder_id = cursor.lastrowid
        conn.commit()
        conn.close()
        return {"id": reminder_id, "message": message, "datetime": datetime_str, "status": "active"}
    
    def list_reminders(self, user_id: str = "default") -> List[Dict]:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute(
            "SELECT * FROM reminders WHERE user_id = ? AND status = 'active' ORDER BY datetime",
            (user_id,)
        )
        rows = cursor.fetchall()
        conn.close()
        return [dict(row) for row in rows]
    
    def delete_reminder(self, reminder_id: int):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("DELETE FROM reminders WHERE id = ?", (reminder_id,))
        conn.commit()
        conn.close()
    
    def check_due_reminders(self, user_id: str = "default") -> List[Dict]:
        now = datetime.now().strftime("%Y-%m-%d %H:%M")
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute(
            "SELECT * FROM reminders WHERE datetime <= ? AND status = 'active' AND user_id = ?",
            (now, user_id)
        )
        rows = cursor.fetchall()
        
        # Mark as sent
        for row in rows:
            cursor.execute("UPDATE reminders SET status = 'sent' WHERE id = ?", (row['id'],))
        
        conn.commit()
        conn.close()
        return [dict(row) for row in rows]

