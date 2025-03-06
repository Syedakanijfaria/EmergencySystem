from flask import Flask, render_template
import sqlite3
from datetime import datetime

app = Flask(__name__)

@app.route("/")
def dashboard():
    conn = sqlite3.connect("events_log.db")
    cursor = conn.cursor()
    cursor.execute("SELECT topic, message, severity, timestamp FROM events ORDER BY timestamp DESC")
    events = cursor.fetchall()
    conn.close()

    #Formats timestamp
    formatted_events= [
        (event[0], event[1], event[2], datetime.strptime(event[3], "%Y-%m-%d %H:%M:%S").strftime("%d %b %Y, %I:%M:%p"))
        for event in events
    ]
    return render_template("dashboard.html", events=formatted_events)

if __name__ == "__main__":
    app.run(debug=True)
