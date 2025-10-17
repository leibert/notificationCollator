#!/opt/notificationCollator/env/bin/python3

"""
Notification Collator Service

This service integrates multiple notification sources (Signal, Calendar)
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

import requests
from dotenv import load_dotenv
import paho.mqtt.client as mqtt
from paho.mqtt.enums import CallbackAPIVersion
from dbus_next.aio.message_bus import MessageBus
from dbus_next.constants import BusType

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
    
    # MQTT Configuration
    MQTT_BROKER = os.environ.get('MQTT_BROKER')
    MQTT_USER = os.environ.get('MQTT_USER')
    MQTT_PASSWORD = os.environ.get('MQTT_PASSWORD')
    
    # Calendar Scraper Configuration
    CAL_SCRAPER_HOST = os.environ.get('CAL_SCRAPER_HOST')
    
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
        self.bus = None
        self.signal_interface = None
    
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

    async def _initialize_dbus(self) -> None:
        """Initialize DBus connection to Signal"""
        try:
            # Connect to system bus
            self.bus = await MessageBus(bus_type=BusType.SYSTEM).connect()

            # Get Signal service introspection
            introspection = await self.bus.introspect('org.asamk.Signal', '/org/asamk/Signal/_17814134149')
            proxy_object = self.bus.get_proxy_object('org.asamk.Signal', '/org/asamk/Signal/_17814134149', introspection)

            # Get the Signal interface
            self.signal_interface = proxy_object.get_interface('org.asamk.Signal')

            # Subscribe to MessageReceived signal
            # Signal signature: MessageReceived(x:timestamp, s:source, ay:groupId, s:message, as:attachments)
            self.signal_interface.on_message_received(self._on_message_received)

            logger.info("Signal DBus connection established")

        except Exception as e:
            logger.error(f"Error initializing DBus connection: {e}")
            raise

    def _on_message_received(self, timestamp: int, source: str, group_id: bytes,
                            message: str, attachments: List) -> None:
        """
        Callback for incoming Signal messages

        Args:
            timestamp: Message timestamp
            source: Sender's phone number
            group_id: Group ID if message is from a group (bytes array)
            message: Message content
            attachments: List of attachments
        """
        asyncio.create_task(self._process_message(timestamp, source, group_id, message, attachments))

    async def _process_message(self, timestamp: int, source: str, group_id: bytes,
                               message: str, attachments: List) -> None:
        """
        Process incoming Signal messages asynchronously

        Args:
            timestamp: Message timestamp
            source: Sender's phone number
            group_id: Group ID if message is from a group (bytes array)
            message: Message content
            attachments: List of attachments
        """
        try:
            logger.info(f"Signal message received from {source}")

            nick_map_updated = False

            # Get sender nickname
            try:
                # type: ignore - call_get_contact_name is dynamically generated from DBus introspection
                sending_user_name = await self.signal_interface.call_get_contact_name(source)  # type: ignore

                if sending_user_name:
                    from_nick = self._format_nickname(sending_user_name)
                    if from_nick not in self.nick_map:
                        self.nick_map[from_nick] = source
                        nick_map_updated = True
                else:
                    from_nick = source
            except Exception:
                from_nick = source

            # Handle group messages
            if group_id and len(group_id) > 0:
                try:
                    # type: ignore - call_get_group_name is dynamically generated from DBus introspection
                    group_name = await self.signal_interface.call_get_group_name(group_id)  # type: ignore

                    group_name = 'GRP_' + self._format_nickname(group_name)
                    group_id_str = group_id.hex() if isinstance(group_id, bytes) else str(group_id)
                    if group_name not in self.nick_map:
                        self.nick_map[group_name] = group_id_str
                        nick_map_updated = True
                    message = f"{from_nick}- {message}"
                    sender_name = group_name
                except Exception:
                    sender_name = from_nick if from_nick else source
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
        # Initialize DBus connection
        await self._initialize_dbus()

        # Keep the monitor running
        while True:
            await asyncio.sleep(1)


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
        if not Config.CAL_SCRAPER_HOST:
            return

        try:
            headers = {"mode": "getCalendar", "calSource": "TO-DO_TODO"}
            result = requests.get(Config.CAL_SCRAPER_HOST, headers=headers)
            todo_list = self._parse_calendar_response(result.text)

            if len(todo_list) > 1:
                # Get next todo and trim extra info
                todo = todo_list[1].split(',')[0]
                self.client.publish("nextTODO/personal", todo)

        except Exception as e:
            logger.error(f"Error updating personal TODO: {e}")
    
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
        if not Config.CAL_SCRAPER_HOST:
            return
            
        # Skip update if in countdown mode and event hasn't passed
        if (self.led_controller.current_mode == "eventCountdown" and 
            self.next_event.get("eventStart")):
            
            event_time = datetime.fromtimestamp(self.next_event['eventStart'])
            if datetime.now() < event_time or (datetime.now() - event_time).seconds < 120:
                return
        
        try:
            headers = {"mode": "getCalendar", "calSource": "calendarEvents"}
            result = requests.get(Config.CAL_SCRAPER_HOST, headers=headers)
            schedule = self._parse_calendar_response(result.text)
            
            # Find next valid event
            event = None
            event_start = None
            
            while len(schedule) > 1:
                # schedule = schedule[1:]
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
        is_external = len(event) > 5 and "EXTATTENDEES" in event[6]
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
        self.calendar_manager = CalendarManager(self.mqtt_client, self.led_controller)
        self.message_processor = MessageProcessor(self.message_queue, self.led_controller)
    
    def _setup_mqtt(self) -> mqtt.Client:
        """Setup MQTT client with callbacks"""
        client = mqtt.Client(client_id="HASS_messageprocessor", callback_api_version=CallbackAPIVersion.VERSION1)
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
    
    # Check critical environment variables (MQTT is required)
    critical_vars = {
        'MQTT_BROKER': Config.MQTT_BROKER,
        'MQTT_USER': Config.MQTT_USER,
        'MQTT_PASSWORD': Config.MQTT_PASSWORD
    }
    
    missing_critical = [var for var, value in critical_vars.items() if not value]
    
    if missing_critical:
        logger.error(f"Missing critical environment variables: {', '.join(missing_critical)}")
        logger.error("Please ensure MQTT variables are set in your .env file")
        sys.exit(1)
    
    # Check optional environment variables
    if not Config.CAL_SCRAPER_HOST:
        logger.warning("CAL_SCRAPER_HOST not set - calendar features will be disabled")
    
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