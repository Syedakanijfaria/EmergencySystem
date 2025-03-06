import time
from solace.messaging.messaging_service import MessagingService
from solace.messaging.resources.topic import Topic
from solace_config import solace_config

# Severity mapping
SEVERITY_MAP = {
    "1": "critical",
    "2": "high",
    "3": "moderate"
}

def publish_event(topic, message, severity="moderate"):
    try:
        # Build and connect to the messaging service
        service = MessagingService.builder().from_properties(solace_config).build()
        service.connect()

        # Create a Direct Message Publisher
        publisher = service.create_direct_message_publisher_builder().build()
        publisher.start()

        # Publish the message
        topic_object = Topic.of(topic)
        full_message = f"{message} | Severity: {severity}"
        publisher.publish(destination=topic_object, message=full_message)
        print(f"Published to {topic}: {full_message}")

        # Stop the publisher and disconnect the service
        publisher.terminate()
        service.disconnect()
    except Exception as e:
        print(f"Error in publishing: {e}")

# Dynamic publishing loop
while True:
    try:
        # Get dynamic inputs for topic structure
        carid = input("Enter the car ID (e.g., 2244, 33444): ").strip()
        emergency_type = input("Enter the emergency type (e.g., emergency, incident, accident): ").strip()
        situation = input("Enter the situation (e.g., medical, breakdown, injured, ambulance): ").strip()
        location = input("Enter the location (e.g., way, lane5, highway): ").strip()

        # Construct the topic
        topic = f"carid/{carid}/{emergency_type}/{situation}/{location}"

        # Get the message
        message = input("Enter the message (e.g., Ambulance heading to Hospital A): ").strip()

        # Get severity input
        print("Select severity for the event:")
        print("1: Critical")
        print("2: High")
        print("3: Moderate")
        severity = SEVERITY_MAP.get(input("Select severity (1/2/3): ").strip(), "moderate")

        # Publish the event
        publish_event(topic, message, severity)

        # Ask if the user wants to publish another event
        another = input("Do you want to publish another event? (yes/no): ").strip().lower()
        if another != "yes":
            print("Exiting publisher...")
            break

    except KeyboardInterrupt:
        print("Exiting publisher...")
        break
