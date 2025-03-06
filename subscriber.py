import sqlite3
from solace.messaging.messaging_service import MessagingService
from solace.messaging.resources.topic_subscription import TopicSubscription
from solace_config import solace_config
import threading
import heapq
import re
# Priority queue for dynamic event handling
event_queue = []
# Priority mapping based on severity
SEVERITY_PRIORITY = {
    "critical": 1,  # Highest priority
    "high": 2,
    "moderate": 3,
}
# Database setup
def setup_database():
    conn = sqlite3.connect("events_log.db")
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            topic TEXT,
            message TEXT,
            severity TEXT,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    conn.close()
# Save event to the database
def save_event_to_db(topic, message, severity):
    conn = sqlite3.connect("events_log.db")
    cursor = conn.cursor()
    cursor.execute("INSERT INTO events (topic, message, severity) VALUES (?, ?, ?)", (topic, message, severity))
    conn.commit()
    conn.close()
# Extract severity from message payload
def extract_severity(topic, message):
    # Parse severity from the message payload
    match = re.search(r"Severity: (\w+)", message)
    if match:
        severity = match.group(1).lower()  # Extracted severity (e.g., critical, high, moderate)
        if severity in SEVERITY_PRIORITY:
            return severity
    # Fallback to default severity if parsing fails
    return "moderate"
# Handle the incoming situation
def handle_situation(topic, message, severity):
    # Save to database
    save_event_to_db(topic, message, severity)
    # Log the event
    print(f"Handling situation: Topic: {topic}, Message: {message}, Severity: {severity}")
    # Send alerts for critical severity
    if severity == "critical":
        print(f"Simulated email sent:\nSubject: Critical Alert: {topic}\nMessage: {message}")
        print("Immediate action required for critical situation!")
# Process events from the priority queue
def process_event_queue():
    while True:
        if event_queue:
            _, topic, message, severity = heapq.heappop(event_queue)
            handle_situation(topic, message, severity)
# Subscribe to all topics dynamically
# Subscribe to all topics dynamically
def subscribe_to_topics():
    try:
        # Setup database
        setup_database()
        # Build and connect to the messaging service
        service = MessagingService.builder().from_properties(solace_config).build()
        service.connect()
        # Subscribe to all dynamic topics
        receiver = service.create_direct_message_receiver_builder().with_subscriptions(
            [TopicSubscription.of("carid/>")]  # Wrap in a list
        ).build()
        receiver.start()
        print("Subscribed to all dynamic topics under 'carid/'")
        # Start processing the event queue in a separate thread
        threading.Thread(target=process_event_queue, daemon=True).start()
        # Listen for messages in real-time
        while True:
            message = receiver.receive_message()
            topic = message.get_destination_name()
            payload = message.get_payload_as_string()
            # Extract severity
            severity = extract_severity(topic, payload)
            # Add to priority queue
            priority = SEVERITY_PRIORITY[severity]
            heapq.heappush(event_queue, (priority, topic, payload, severity))
    except Exception as e:
        print(f"Error in subscribing: {e}")
# Start subscribing
subscribe_to_topics()
