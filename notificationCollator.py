#!/opt/notificationCollator/env/bin/python3

"""
Notification Collator Service

This service integrates multiple notification sources (Signal, Home Assistant, SMS, Email)
and displays them on an LED sign via MQTT. It also monitors calendar events and TODOs.

Requirements:
- signal-cli must be installed and paired with your phone
- Start signal-cli in dbus daemon mode: signal-cli -u <+phone> daemon
- Environment variables must be configured in .env file
"""

import os
import sys
import time
import json
import pickle
import asyncio
import logging
from datetime import datetime
from typing import Dict, List, Tuple, Optional, Any

import asyncws
import requests
from dotenv import load_dotenv
import paho.mqtt.client as mqtt
from gi.repository import GLib
from pydbus import SystemBus

# Load environment variables
load_dotenv()

# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
formatter = logging.Formatter(
    fmt="%(asctime)s %(name)s.%(levelname)s: %(message)s",
    datefmt="%Y.%m.%d %H:%M:%S"
)
handler = logging.StreamHandler(stream=sys.stdout)
handler.setFormatter(formatter)
logger.addHandler(handler)


class Config:
    """Configuration class for environment variables and constants"""
    
    # Home Assistant Configuration
    HASS_TOKEN = os.environ.get('HASS_TOKEN')
    HASS_HOST = os.environ.get('HASS_HOST')
    HASS_PORT = int(os.environ.get('HASS_PORT', 8123))
    
    # MQTT Configuration
    MQTT_BROKER = os.environ.get('MQTT_BROKER')
    MQTT_USER = os.environ.get('MQTT_USER')
    MQTT_PASSWORD = os.environ.get('MQTT_PASSWORD')
    
    # Calendar Scraper Configuration
    CAL_SCRAPER_HOST = os.environ.get('CAL_SCRAPER_HOST')
    
    # Monitored Home Assistant Entities
    MONITORED_ENTITIES = [
        "sensor.sm_s901u1_last_notification",
        "sensor.sm_s901u1_active_notification_count",
        "sensor.sm_s901u1_last_removed_notification"
    ]
    
    # File paths
    NICK_PICKLE_FILE = 'nickPickle.dat'
    
    # Display timing
    BASE_MESSAGE_DISPLAY_TIME = 6
    EXTRA_TIME_PER_CHAR = 0.75
    MESSAGE_LENGTH_THRESHOLD = 16


class SignalHandler:
    """Handles Signal messaging integration via DBus"""
    
    def __init__(self, message_queue: List[Tuple]):
        self.message_queue = message_queue
        self.nick_map = self._load_nick_map()
        self.bus = SystemBus()
        self.glibloop = GLib.MainLoop()
        self.glibcontext = self.glibloop.get_context()
        self.signal = self.bus.get("org.asamk.Signal", "/org/asamk/Signal/_17814134149")
        self.signal.onMessageReceived = self.receive_message
    
    def _load_nick_map(self) -> Dict[str, str]:
        """Load nickname mapping from pickle file"""
        try:
            with open(Config.NICK_PICKLE_FILE, 'rb+') as filehandler:
                return pickle.load(filehandler)
        except (EOFError, FileNotFoundError):
            return {}
        except Exception as e:
            logger.error(f"Error loading nick map: {e}")
            return {}
    
    def _save_nick_map(self) -> None:
        """Save nickname mapping to pickle file"""
        try:
            with open(Config.NICK_PICKLE_FILE, 'wb+') as filehandler:
                pickle.dump(self.nick_map, filehandler)
        except Exception as e:
            logger.error(f"Error saving nick map: {e}")
    
    def _format_nickname(self, name: str) -> str:
        """Format a name into a valid nickname"""
        return name.replace(' ', '_').replace(':', '')
    
    def receive_message(self, timestamp: int, source: str, group_id: str, 
                       message: str, attachments: List) -> None:
        """
        Process incoming Signal messages
        
        Args:
            timestamp: Message timestamp
            source: Sender's phone number
            group_id: Group ID if message is from a group
            message: Message content
            attachments: List of attachments
        """
        try:
            logger.info(f"Signal message received from {source}")
            
            nick_map_updated = False
            
            # Get sender nickname
            sending_user_name = self.signal.getContactName(source)
            if sending_user_name:
                from_nick = self._format_nickname(sending_user_name)
                if from_nick not in self.nick_map:
                    self.nick_map[from_nick] = source
                    nick_map_updated = True
            else:
                from_nick = source
            
            # Handle group messages
            if group_id:
                group_name = self.signal.getGroupName(group_id)
                group_name = 'GRP_' + self._format_nickname(group_name)
                if group_name not in self.nick_map:
                    self.nick_map[group_name] = group_id
                    nick_map_updated = True
                message = f"{from_nick}- {message}"
                sender_name = group_name
            else:
                sender_name = from_nick if from_nick else source
            
            # Save nick map if updated
            if nick_map_updated:
                self._save_nick_map()
            
            # Add to message queue
            sender_name = sender_name.replace("_", " ")
            self.message_queue.append((datetime.now(), "Signal", sender_name, message))
            
            if attachments:
                logger.info("Message contains attachments")
                
        except Exception as e:
            logger.error(f"Error processing Signal message: {e}")
    
    async def monitor(self) -> None:
        """Monitor Signal DBus for incoming messages"""
        while True:
            await asyncio.sleep(0.1)
            # False is needed to keep the thread from getting trapped
            self.glibcontext.iteration(False)


class HomeAssistantMonitor:
    """Monitors Home Assistant WebSocket for notifications"""
    
    def __init__(self, message_queue: List[Tuple]):
        self.message_queue = message_queue
        self.websocket = None
    
    async def connect(self) -> None:
        """Establish WebSocket connection to Home Assistant"""
        try:
            self.websocket = await asyncws.connect(
                f'ws://{Config.HASS_HOST}:{Config.HASS_PORT}/api/websocket'
            )
            
            # Authenticate
            await self.websocket.send(json.dumps({
                'type': 'auth',
                'access_token': Config.HASS_TOKEN
            }))
            
            # Subscribe to state changes
            await self.websocket.send(json.dumps({
                'id': 1,
                'type': 'subscribe_events',
                'event_type': 'state_changed'
            }))
            
            logger.info("Connected to Home Assistant WebSocket")
            
        except Exception as e:
            logger.error(f"Failed to connect to Home Assistant: {e}")
            await asyncio.sleep(300)
            raise
    
    def _process_signal_notification(self, attributes: Dict) -> Optional[Tuple]:
        """Process Signal notifications from Home Assistant"""
        # Filter out status notifications
        if (attributes.get('android.title') == 'Signal' and 
            attributes.get('android.text') == 'Checking for messages…'):
            return None
        
        return (
            datetime.now(),
            "Signal",
            attributes.get('android.title', 'Unknown'),
            attributes.get('android.text', '')
        )
    
    def _process_sms_notification(self, attributes: Dict) -> Optional[Tuple]:
        """Process SMS notifications from Home Assistant"""
        sender = None
        message_content = None
        
        # Handle different SMS notification formats
        if 'android.messages' in attributes:
            text_message = attributes['android.messages'].pop().split(", ")
            if len(text_message) >= 4:
                sender = text_message[2][7:]
                message_content = text_message[3][5:]
        
        elif 'android.textLines' in attributes and attributes['android.textLines']:
            text_message = attributes['android.textLines'][0].split("  ")
            if len(text_message) >= 2:
                sender = text_message[0]
                message_content = text_message[1]
        
        if sender and message_content:
            return (datetime.now(), "SMS", sender, message_content)
        return None
    
    def _process_email_notification(self, attributes: Dict, email_type: str) -> Tuple:
        """Process email notifications (Outlook/Fastmail)"""
        title = attributes.get('android.title', 'No Subject')
        return (datetime.now(), email_type, title, title)
    
    async def monitor(self) -> None:
        """Monitor Home Assistant WebSocket for notifications"""
        await self.connect()
        
        while True:
            message = await self.websocket.recv()
            
            if message is None:
                logger.warning("WebSocket connection lost, attempting restart")
                await asyncio.sleep(10)
                sys.exit()
            
            if "event" not in message:
                continue
            
            try:
                data = json.loads(message)['event']['data']
                entity_id = data.get('entity_id')
                
                if entity_id not in Config.MONITORED_ENTITIES:
                    continue
                
                if 'new_state' not in data or 'attributes' not in data['new_state']:
                    continue
                
                attributes = data['new_state']['attributes']
                package = attributes.get('package')
                
                notification = None
                
                # Process different notification types
                if package == "org.thoughtcrime.securesms":
                    # Signal notifications (legacy handler)
                    logger.info("Signal notification detected (legacy handler)")
                    continue
                    
                elif package == "com.google.android.apps.messaging":
                    # SMS notifications
                    notification = self._process_sms_notification(attributes)
                    
                elif package == "com.microsoft.office.outlook":
                    # Outlook notifications
                    notification = self._process_email_notification(attributes, "Outlook")
                    
                elif package == "com.fastmail.app":
                    # Fastmail notifications
                    notification = self._process_email_notification(attributes, "Fastmail")
                    
                elif package == "com.samsung.android.calendar":
                    # Calendar notifications
                    logger.info(f"Calendar event: {attributes}")
                    
                else:
                    logger.info(f"Notification from unknown package: {package}")
                
                if notification:
                    self.message_queue.append(notification)
                    logger.info(f"Added notification to queue: {notification[1]} from {notification[2]}")
                    
            except Exception as e:
                logger.error(f"Error processing Home Assistant message: {e}")


class LEDSignController:
    """Controls the LED sign via MQTT"""
    
    def __init__(self, mqtt_client: mqtt.Client):
        self.client = mqtt_client
        self.current_mode = "bigClock"
    
    def set_mode(self, mode: str) -> None:
        """Set the LED sign display mode"""
        self.client.publish("ledSign/mode", mode)
        self.current_mode = mode
    
    def display_message(self, message_type: str, sender: str, text: str) -> None:
        """Display a message on the LED sign"""
        self.client.publish("ledSign/message/type", message_type)
        self.client.publish("ledSign/message/sender", sender)
        self.client.publish("ledSign/message/text", text)
        self.set_mode("message")
    
    def calculate_display_time(self, message: str) -> float:
        """Calculate how long to display a message based on its length"""
        display_time = Config.BASE_MESSAGE_DISPLAY_TIME
        
        if len(message) > Config.MESSAGE_LENGTH_THRESHOLD:
            extra_chars = len(message) - Config.MESSAGE_LENGTH_THRESHOLD
            display_time += extra_chars * Config.EXTRA_TIME_PER_CHAR
        
        return display_time


class CalendarManager:
    """Manages calendar events and TODOs"""
    
    def __init__(self, mqtt_client: mqtt.Client, led_controller: LEDSignController):
        self.client = mqtt_client
        self.led_controller = led_controller
        self.next_event = {}
    
    def _parse_calendar_response(self, response_text: str) -> List[str]:
        """Parse calendar response and remove timestamp"""
        lines = response_text.split("\n")
        # Remove last updated timestamp
        return lines[1:] if len(lines) > 1 else []
    
    async def update_todos(self) -> None:
        """Fetch and publish TODO items"""
        todo_sources = [
            ("IBMLaptop_TODO", "nextTODO/work"),
            ("TO-DO_TODO", "nextTODO/personal")
        ]
        
        for source, topic in todo_sources:
            try:
                headers = {"mode": "getCalendar", "calSource": source}
                result = requests.get(Config.CAL_SCRAPER_HOST, headers=headers)
                todo_list = self._parse_calendar_response(result.text)
                
                if len(todo_list) > 1:
                    # Get next todo and trim extra info
                    todo = todo_list[1].split(',')[0]
                    self.client.publish(topic, todo)
                    
            except Exception as e:
                logger.error(f"Error updating TODOs from {source}: {e}")
    
    def reset_next_event(self) -> None:
        """Reset next event data"""
        self.next_event = {}
        event_fields = [
            ("eventStart", "nextEvent/timeStamp", None),
            ("eventSubject", "nextEvent/subject", None),
            ("eventOrganizer", "nextEvent/organizer", None),
            ("eventAttendees", "nextEvent/attendees", None),
            ("isExternal", "nextEvent/isExternal", False)
        ]
        
        for field, topic, default_value in event_fields:
            self.next_event[field] = default_value
            self.client.publish(topic, default_value)
        
        logger.info("Next event reset")
    
    async def update_schedule(self) -> None:
        """Fetch and process calendar schedule"""
        # Skip update if in countdown mode and event hasn't passed
        if (self.led_controller.current_mode == "eventCountdown" and 
            self.next_event.get("eventStart")):
            
            event_time = datetime.fromtimestamp(self.next_event['eventStart'])
            if datetime.now() < event_time or (datetime.now() - event_time).seconds < 120:
                return
        
        try:
            headers = {"mode": "getCalendar", "calSource": "IBMLaptop"}
            result = requests.get(Config.CAL_SCRAPER_HOST, headers=headers)
            schedule = self._parse_calendar_response(result.text)
            
            # Find next valid event
            event = None
            event_start = None
            
            while len(schedule) > 1:
                schedule = schedule[1:]
                event_data = schedule[0].split(",")
                
                # Parse event timestamp
                event_start = datetime.utcfromtimestamp(float(event_data[0]))
                
                # Check if event is in the future and not tentative
                if event_start > datetime.now():
                    if not event_data[2][:5].startswith("~~"):
                        event = event_data
                        break
                    
                event = None
                event_start = None
            
            if event_start and event:
                self._process_event(event, event_start)
            else:
                self.reset_next_event()
                
        except Exception as e:
            logger.error(f"Error updating schedule: {e}")
    
    def _process_event(self, event: List[str], event_start: datetime) -> None:
        """Process and publish calendar event details"""
        self.next_event['eventStart'] = event_start.timestamp()
        self.client.publish("nextEvent/timeStamp", self.next_event['eventStart'])
        
        self.next_event['eventSubject'] = event[2].strip()
        self.client.publish("nextEvent/subject", self.next_event['eventSubject'])
        
        self.next_event['eventOrganizer'] = event[3].strip()
        self.client.publish("nextEvent/organizer", self.next_event['eventOrganizer'])
        
        # Process attendees if available
        if len(event) > 4:
            attendees = event[4].strip()
            if len(attendees) > 200:
                attendees = attendees[:200] + "....."
            self.next_event['eventAttendees'] = attendees
            self.client.publish("nextEvent/attendees", attendees)
        
        # Check for external attendees
        is_external = len(event) > 5 and "EXTATTENDEES" in event[5]
        self.next_event['isExternal'] = is_external
        self.client.publish("nextEvent/isExternal", is_external)
    
    async def monitor_schedule(self) -> None:
        """Monitor schedule for upcoming events and trigger alerts"""
        while True:
            await asyncio.sleep(0.5)
            
            if not self.next_event.get('eventStart'):
                continue
            
            try:
                event_time = datetime.fromtimestamp(self.next_event['eventStart'])
                seconds_until = (event_time - datetime.now()).seconds
                
                # Alert for events starting in less than 5 minutes
                if seconds_until < 301:
                    if (self.led_controller.current_mode != "eventCountdown" or 
                        seconds_until % 10 == 0):
                        
                        logger.info(f"Event alert: {self.next_event.get('eventSubject', 'Unknown')}")
                        self.led_controller.set_mode("eventCountdown")
                        
                        # Activate beacon light for external meetings
                        if self.next_event.get('isExternal'):
                            self.client.publish("beaconLight/activate", True)
                        else:
                            self.client.publish("beaconLight/activate", False)
                
                # Reset after event has passed
                elif datetime.now() > event_time and (datetime.now() - event_time).seconds > 120:
                    if self.led_controller.current_mode == "eventCountdown":
                        logger.info("Event passed, resetting display")
                        self.led_controller.set_mode("bigClock")
                    self.reset_next_event()
                
                # Turn off beacon light 3 seconds before event
                if (event_time - datetime.now()).seconds < 3 or datetime.now() > event_time:
                    self.client.publish("beaconLight/activate", False)
                    
            except Exception as e:
                logger.error(f"Error monitoring schedule: {e}")


class MessageProcessor:
    """Processes and sends messages to the LED sign"""
    
    def __init__(self, message_queue: List[Tuple], led_controller: LEDSignController):
        self.message_queue = message_queue
        self.led_controller = led_controller
    
    async def process_messages(self) -> None:
        """Process queued messages and send to LED sign"""
        logger.info("Message processor started")
        active_queue = False
        
        while True:
            if not self.message_queue:
                # Reset to clock mode if queue is empty
                if active_queue:
                    self.led_controller.set_mode("bigClock")
                    active_queue = False
                await asyncio.sleep(0.01)
                continue
            
            active_queue = True
            
            try:
                # Get next message from queue
                timestamp, msg_type, sender, text = self.message_queue.pop(0)
                
                logger.info(f"Displaying message: {msg_type} from {sender}")
                
                # Display message on LED sign
                self.led_controller.display_message(msg_type, sender, text)
                
                # Calculate and wait for display time
                display_time = self.led_controller.calculate_display_time(text)
                await asyncio.sleep(display_time)
                
            except Exception as e:
                logger.error(f"Error processing message: {e}")


class NotificationCollator:
    """Main application class that coordinates all components"""
    
    def __init__(self):
        self.message_queue = []
        self.mqtt_client = self._setup_mqtt()
        self.led_controller = LEDSignController(self.mqtt_client)
        self.signal_handler = SignalHandler(self.message_queue)
        self.hass_monitor = HomeAssistantMonitor(self.message_queue)
        self.calendar_manager = CalendarManager(self.mqtt_client, self.led_controller)
        self.message_processor = MessageProcessor(self.message_queue, self.led_controller)
    
    def _setup_mqtt(self) -> mqtt.Client:
        """Setup MQTT client with callbacks"""
        client = mqtt.Client("HASS_messageprocessor")
        client.username_pw_set(Config.MQTT_USER, password=Config.MQTT_PASSWORD)
        
        client.on_connect = self._on_mqtt_connect
        client.on_disconnect = self._on_mqtt_disconnect
        client.on_connect_fail = self._on_mqtt_connect_fail
        
        return client
    
    def _on_mqtt_connect(self, client, userdata, flags, rc):
        """MQTT connection callback"""
        logger.info(f"Connected to MQTT broker with result code {rc}")
    
    def _on_mqtt_disconnect(self, client, userdata, rc):
        """MQTT disconnection callback"""
        logger.error(f"Disconnected from MQTT broker with code {rc}")
        time.sleep(5)
        logger.info("Restarting application...")
        os.execv(sys.executable, [sys.executable, __file__] + sys.argv)
    
    def _on_mqtt_connect_fail(self, client, userdata):
        """MQTT connection failure callback"""
        logger.error("Failed to connect to MQTT broker")
        time.sleep(5)
        logger.info("Restarting application...")
        os.execv(sys.executable, [sys.executable, __file__] + sys.argv)
    
    async def run(self):
        """Run the notification collator"""
        logger.info("Starting Notification Collator")
        
        # Connect to MQTT
        self.mqtt_client.connect(Config.MQTT_BROKER, 1883)
        self.mqtt_client.loop_start()
        
        # Reset LED sign to clock mode
        self.led_controller.set_mode("bigClock")
        
        # Create async tasks
        tasks = [
            asyncio.create_task(self.hass_monitor.monitor()),
            asyncio.create_task(self.signal_handler.monitor()),
            asyncio.create_task(self._update_todos_loop()),
            asyncio.create_task(self._update_schedule_loop()),
            asyncio.create_task(self.calendar_manager.monitor_schedule()),
            asyncio.create_task(self.message_processor.process_messages())
        ]
        
        # Wait for all tasks
        await asyncio.gather(*tasks)
    
    async def _update_todos_loop(self):
        """Periodic TODO update loop"""
        while True:
            await asyncio.sleep(5)
            await self.calendar_manager.update_todos()
    
    async def _update_schedule_loop(self):
        """Periodic schedule update loop"""
        while True:
            await asyncio.sleep(5)
            await self.calendar_manager.update_schedule()


def main():
    """Main entry point"""
    logger.info("Notification Collator starting...")
    
    try:
        collator = NotificationCollator()
        asyncio.run(collator.run())
    except ConnectionRefusedError:
        logger.error("Connection refused error")
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        raise


if __name__ == "__main__":
    main()