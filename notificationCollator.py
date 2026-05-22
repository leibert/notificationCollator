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
import traceback
from datetime import datetime
from typing import Dict, List, Tuple, Optional, Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import requests
from requests.exceptions import RequestException
from dotenv import load_dotenv
import paho.mqtt.client as mqtt
from paho.mqtt.enums import CallbackAPIVersion
from dbus_next.aio.message_bus import MessageBus
from dbus_next.constants import BusType

# Load environment variables
load_dotenv()

# Configure logging
logger = logging.getLogger(__name__)
log_level = getattr(logging, os.environ.get('LOG_LEVEL', 'INFO').upper(), logging.INFO)
logger.setLevel(log_level)
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
    TIMEZONE = os.environ.get('TIMEZONE', 'UTC')
    LOG_LEVEL = os.environ.get('LOG_LEVEL', 'INFO').upper()
    
    # File paths
    NICK_PICKLE_FILE = 'nickPickle.dat'
    
    # Display timing
    BASE_MESSAGE_DISPLAY_TIME = 6
    EXTRA_TIME_PER_CHAR = 0.75
    MESSAGE_LENGTH_THRESHOLD = 16
    
    # Connection timeouts and retries
    HTTP_REQUEST_TIMEOUT = 10  # seconds
    DBUS_RECONNECT_DELAY = 5  # seconds
    DBUS_MAX_RETRIES = 3
    MQTT_RECONNECT_DELAY = 5  # seconds
    TASK_MONITORING_INTERVAL = 30  # seconds

    @classmethod
    def get_timezone(cls):
        try:
            return ZoneInfo(cls.TIMEZONE)
        except ZoneInfoNotFoundError:
            logger.warning(f"Invalid TIMEZONE '{cls.TIMEZONE}', falling back to UTC")
            return ZoneInfo('UTC')


class SignalHandler:
    """Handles Signal messaging integration via DBus"""

    def __init__(self, message_queue: asyncio.Queue[Tuple[datetime, str, str, str]]):
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

    async def _find_signal_interface_path(self, service_name: str = 'org.asamk.Signal', root_path: str = '/org/asamk/Signal') -> Optional[str]:
        """Discover the active Signal DBus object path for the org.asamk.Signal interface."""
        candidate_paths = [root_path]

        if self.bus is None:
            raise RuntimeError('DBus bus connection is not initialized')

        try:
            root_introspection = await self.bus.introspect(service_name, root_path)
            child_names = [node.name for node in getattr(root_introspection, 'nodes', [])]
            logger.debug(f"Signal root introspection found child nodes: {child_names}")
            for node in getattr(root_introspection, 'nodes', []):
                candidate_paths.append(f"{root_path}/{node.name}")
        except Exception as e:
            logger.debug(f"Unable to introspect Signal root path {root_path}: {e}")

        for path in candidate_paths:
            try:
                introspection = await self.bus.introspect(service_name, path)
                interface_names = [iface.name for iface in getattr(introspection, 'interfaces', [])]
                logger.debug(f"Introspected {path}, interfaces={interface_names}")

                if 'org.asamk.Signal' in interface_names:
                    logger.info(f"Discovered Signal interface at {path}")
                    return path
            except Exception as e:
                logger.debug(f"No org.asamk.Signal interface at {path}: {e}")

        logger.error(f"Could not find org.asamk.Signal interface under {root_path}")
        return None

    async def _initialize_dbus(self) -> bool:
        """Initialize DBus connection to Signal with retry logic.
        
        Returns:
            bool: True if connection successful, False if failed after all retries
        """
        for attempt in range(Config.DBUS_MAX_RETRIES):
            for bus_name, bus_type in (('SYSTEM', BusType.SYSTEM), ('SESSION', BusType.SESSION)):
                try:
                    self.bus = await MessageBus(bus_type=bus_type).connect()
                    assert self.bus is not None
                    logger.info(f"DBus {bus_name} bus connection established")
                except Exception as e:
                    logger.error(f"DBus {bus_name} bus connection failed: {e}")
                    logger.debug(f"Full traceback: {traceback.format_exc()}")
                    continue

                try:
                    object_path = await self._find_signal_interface_path()
                    if not object_path:
                        raise RuntimeError('Unable to locate org.asamk.Signal object path')

                    introspection = await self.bus.introspect('org.asamk.Signal', object_path)
                    proxy_object = self.bus.get_proxy_object('org.asamk.Signal', object_path, introspection)
                    self.signal_interface = proxy_object.get_interface('org.asamk.Signal')
                    await self._register_signal_handlers()

                    logger.info(f"Signal DBus interface connected and monitoring for messages on {bus_name} bus at {object_path}")
                    return True

                except Exception as e:
                    logger.error(f"Failed to connect Signal interface on {bus_name} bus: {e}")
                    logger.debug(f"Attempted object path: {locals().get('object_path', 'unknown')}")
                    logger.debug(f"Full traceback: {traceback.format_exc()}")
                finally:
                    if self.bus and getattr(self.bus, 'connected', False) and not self.signal_interface:
                        try:
                            self.bus.disconnect()
                        except Exception:
                            pass

            if attempt < Config.DBUS_MAX_RETRIES - 1:
                logger.info(f"Retrying Signal DBus initialization in {Config.DBUS_RECONNECT_DELAY}s (attempt {attempt + 1}/{Config.DBUS_MAX_RETRIES})")
                await asyncio.sleep(Config.DBUS_RECONNECT_DELAY)

        logger.critical("Failed to initialize Signal DBus connection after all retries")
        return False

    async def _register_signal_handlers(self) -> None:
        """Subscribe to Signal receive events and register incoming message callbacks."""
        if hasattr(self.signal_interface, 'call_subscribe_receive'):
            try:
                await self.signal_interface.call_subscribe_receive()  # type: ignore
                logger.info("Subscribed to Signal receive events")
            except Exception as e:
                logger.warning(f"Failed to subscribe to Signal receive events: {e}")
        else:
            logger.warning("Signal interface does not support subscribeReceive; relying on passive message events")

        # Create wrapper callbacks that match the exact signal arity expected
        def make_wrapper_5():
            def cb(a, b, c, d, e):
                task = asyncio.create_task(self._process_signal_event(a, b, c, d, e))
                task.add_done_callback(self._on_message_task_done)
            return cb

        def make_wrapper_6():
            def cb(a, b, c, d, e, f):
                task = asyncio.create_task(self._process_signal_event(a, b, c, d, e, f))
                task.add_done_callback(self._on_message_task_done)
            return cb

        handlers = []
        for signal_name in [
            'on_message_received',
            'on_message_received_v2',
            'on_sync_message_received',
            'on_sync_message_received_v2',
        ]:
            signal_slot = getattr(self.signal_interface, signal_name, None)
            if callable(signal_slot):
                # choose wrapper by whether signal contains 'sync' (these have 6 args)
                if 'sync' in signal_name:
                    signal_slot(make_wrapper_6())
                else:
                    signal_slot(make_wrapper_5())
                handlers.append(signal_name)

        if not handlers:
            raise AttributeError('Signal interface does not expose any incoming message events')

        logger.info(f"Registered Signal callbacks for: {', '.join(handlers)}")

    def _on_signal_event(self, *args) -> None:
        """Generic callback for incoming Signal DBus events."""
        logger.debug("Signal raw event received with %d args", len(args))
        logger.debug("Signal raw args: %s", args)
        task = asyncio.create_task(self._process_signal_event(*args))
        task.add_done_callback(self._on_message_task_done)

    def _parse_signal_args(self, args: Tuple[Any, ...]) -> Optional[Tuple[int, str, Any, str, Any]]:
        """Parse generic Signal event arguments into the expected message tuple."""
        if not args:
            return None
        timestamp = args[0]

        # Collect typed candidates
        str_items = [a for a in args[1:] if isinstance(a, str)]
        bytes_items = [a for a in args[1:] if isinstance(a, (bytes, bytearray))]
        list_like = [a for a in args[1:] if isinstance(a, (list, tuple)) and all(isinstance(x, int) for x in a)]
        attachments_items = [a for a in args[1:] if isinstance(a, (list, dict))]

        source = str_items[0] if len(str_items) >= 1 else None
        # message is typically the last string argument
        message = str_items[-1] if len(str_items) >= 1 else ''

        # Prefer explicit bytes group id, else a list-of-ints converted to bytes
        if bytes_items:
            group_id = bytes_items[0]
        elif list_like:
            try:
                group_id = bytes(list_like[0])
            except Exception:
                group_id = list_like[0]
        else:
            group_id = b''

        attachments = attachments_items[0] if attachments_items else []

        if source is None or message == '':
            return None

        return timestamp, source, group_id, message, attachments

    async def _process_signal_event(self, *args) -> None:
        parsed = self._parse_signal_args(args)
        if parsed is None:
            logger.warning("Received unrecognized Signal event args: %s", args)
            return

        timestamp, source, group_id, message, attachments = parsed
        await self._process_message(timestamp, source, group_id, message, attachments)
    
    def _on_message_task_done(self, task: asyncio.Task) -> None:
        """Callback for Signal message processing task completion"""
        try:
            # This will raise if the task failed
            task.result()
        except asyncio.CancelledError:
            logger.debug("Signal message processing task was cancelled")
        except Exception as e:
            logger.error(f"Signal message processing task failed: {e}")
            logger.debug(f"Full traceback: {traceback.format_exc()}")

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
            logger.info(f"Signal message received from {source}, length={len(message)}, attachments={len(attachments) if attachments else 0}")

            nick_map_updated = False
            from_nick = source

            # Get sender nickname
            try:
                # type: ignore - call_get_contact_name is dynamically generated from DBus introspection
                sending_user_name = await self.signal_interface.call_get_contact_name(source)  # type: ignore

                if sending_user_name:
                    from_nick = self._format_nickname(sending_user_name)
                    if from_nick not in self.nick_map:
                        self.nick_map[from_nick] = source
                        nick_map_updated = True
                        logger.debug(f"Stored new nick mapping: {from_nick} -> {source}")
                else:
                    logger.debug(f"No contact name found for {source}")
            except Exception as e:
                logger.warning(f"Failed to get contact name for {source}: {e}")
                from_nick = source

            # Handle group messages
            sender_name = from_nick
            if group_id and len(group_id) > 0:
                try:
                    # type: ignore - call_get_group_name is dynamically generated from DBus introspection
                    group_name = await self.signal_interface.call_get_group_name(group_id)  # type: ignore

                    group_name = 'GRP_' + self._format_nickname(group_name)
                    group_id_str = group_id.hex() if isinstance(group_id, bytes) else str(group_id)
                    if group_name not in self.nick_map:
                        self.nick_map[group_name] = group_id_str
                        nick_map_updated = True
                        logger.debug(f"Stored new group mapping: {group_name} -> {group_id_str}")
                    message = f"{from_nick}- {message}"
                    sender_name = group_name
                except Exception as e:
                    logger.warning(f"Failed to get group name for {group_id.hex() if isinstance(group_id, bytes) else group_id}: {e}")
                    sender_name = from_nick if from_nick else source

            # Save nick map if updated
            if nick_map_updated:
                try:
                    self._save_nick_map()
                    logger.debug("Nick map saved to disk")
                except Exception as e:
                    logger.error(f"Failed to save nick map: {e}")

            # Add to message queue
            sender_name = sender_name.replace("_", " ")
            await self.message_queue.put((datetime.now(), "Signal", sender_name, message))
            logger.info(f"Message queued: {sender_name}")

            if attachments:
                logger.debug(f"Message contains {len(attachments)} attachments")

        except Exception as e:
            logger.error(f"Error processing Signal message: {e}")
            logger.debug(f"Full traceback: {traceback.format_exc()}")

    async def monitor(self) -> None:
        """Monitor Signal DBus for incoming messages with graceful degradation"""
        while True:
            # Initialize DBus connection
            success = await self._initialize_dbus()
            
            if success:
                logger.info("Signal handler monitoring started")
                # Keep the monitor running
                try:
                    while True:
                        await asyncio.sleep(1)
                except Exception as e:
                    logger.error(f"Signal monitor encountered error: {e}")
                    logger.debug(f"Full traceback: {traceback.format_exc()}")
                    logger.info("Attempting to reconnect to Signal D-Bus...")
            else:
                logger.warning(f"Signal D-Bus unavailable, retrying in {Config.DBUS_RECONNECT_DELAY}s...")
                await asyncio.sleep(Config.DBUS_RECONNECT_DELAY)


class LEDSignController:
    """Controls the LED sign via MQTT.

    This class wraps the raw MQTT client to centralise publish error
    handling and to keep track of the current display mode.
    """
    
    def __init__(self, mqtt_client: mqtt.Client, connected_event: asyncio.Event):
        self.client = mqtt_client
        self.current_mode = "bigClock"
        self.connected_event = connected_event
    
    def _safe_publish(self, topic: str, payload: Any, qos: int = 0) -> None:
        """Publish to MQTT with exception handling and QoS support.

        paho-mqtt usually does not raise, but network issues or invalid
        payloads could generate exceptions; log them rather than crash.
        
        Args:
            topic: MQTT topic to publish to
            payload: Message payload
            qos: Quality of Service level (0=at-most-once, 1=at-least-once, 2=exactly-once)
        """
        try:
            if not self.connected_event.is_set():
                logger.warning(f"MQTT not connected, queuing publish to '{topic}'")
            logger.debug(f"Publishing to MQTT topic '{topic}' (QoS={qos}): {payload}")
            result = self.client.publish(topic, payload, qos=qos)
            logger.debug(f"Successfully published to MQTT topic '{topic}' (mid={result.mid})")
        except Exception as e:
            logger.error(f"Failed to publish {topic}: {e}")
            logger.debug(f"Payload was: {payload}")
    
    def set_mode(self, mode: str) -> None:
        """Set the LED sign display mode"""
        self._safe_publish("ledSign/mode", mode, qos=1)
        self.current_mode = mode
    
    def display_message(self, message_type: str, sender: str, text: str) -> None:
        """Display a message on the LED sign"""
        logger.info(f"Displaying message: type={message_type}, sender={sender}, text={text[:50]}...")
        self._safe_publish("ledSign/message/type", message_type, qos=1)
        self._safe_publish("ledSign/message/sender", sender, qos=1)
        self._safe_publish("ledSign/message/text", text, qos=1)
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
    
    def __init__(self, mqtt_client: mqtt.Client, led_controller: LEDSignController, connected_event: asyncio.Event):
        self.client = mqtt_client
        self.led_controller = led_controller
        self.connected_event = connected_event
        self.next_event = {}
        # maintain TODO list and index for rotation
        self.todo_list: List[str] = []
        self.todo_index: int = 0

    def _safe_publish(self, topic: str, payload: Any, qos: int = 0) -> None:
        """Publish with logging on failure and QoS support"""
        try:
            if not self.connected_event.is_set():
                logger.warning(f"MQTT not connected, queuing publish to '{topic}'")
            self.client.publish(topic, payload, qos=qos)
        except Exception as e:
            logger.error(f"Failed to publish {topic}: {e}")
            logger.debug(f"Payload was: {payload}")
    
    def _parse_calendar_response(self, response_text: str) -> List[str]:
        """Parse calendar response and remove timestamp"""
        lines = response_text.split("\n")
        # Remove last updated timestamp
        return lines[1:] if len(lines) > 1 else []

    def _now(self) -> datetime:
        return datetime.now(Config.get_timezone())

    def _from_timestamp(self, timestamp: float) -> datetime:
        return datetime.fromtimestamp(float(timestamp), Config.get_timezone())
    
    async def update_todos(self) -> None:
        """Fetch and publish TODO items"""
        if not Config.CAL_SCRAPER_HOST:
            return

        try:
            headers = {"mode": "getCalendar", "calSource": "calendar_TODO"}
            result = requests.get(Config.CAL_SCRAPER_HOST, headers=headers, timeout=Config.HTTP_REQUEST_TIMEOUT)
            result.raise_for_status()
            todo_list = self._parse_calendar_response(result.text)

            # store for rotation (skip first timestamp line)
            self.todo_list = todo_list[1:] if len(todo_list) > 1 else []
            self.todo_index = 0

            if self.todo_list:
                todo = self.todo_list[self.todo_index].split(',')[0]
                try:
                    self.client.publish("nextTODO/personal", todo)
                except Exception as e:
                    logger.error(f"Failed to publish TODO to MQTT: {e}")
                logger.debug(f"Published TODO: {todo}")
            else:
                logger.debug("No TODOs available after fetch")

        except RequestException as e:
            logger.warning(f"Failed to fetch TODOs from calendar scraper: {e}")
        except Exception as e:
            logger.error(f"Error updating personal TODO: {e}")
            logger.debug(f"Full traceback: {traceback.format_exc()}")
    
    def reset_next_event(self) -> None:
        """Reset next event data and publish default values.

        All published topics are wrapped in try/except because the MQTT
        connection may be temporarily unavailable when events expire.
        """
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
            try:
                self._safe_publish(topic, default_value, qos=1)
            except Exception as e:
                logger.error(f"Error publishing reset field {field} to {topic}: {e}")
        
        logger.info("Next event reset")

    def handle_todo_select(self, command: str) -> None:
        """Rotate through TODO list in response to MQTT commands

        Supported commands:
        - "next": move to next item
        - "prev": move to previous item

        The published MQTT topic is `nextTODO/personal`.
        """
        if not self.todo_list:
            logger.debug("Received todo select command but no todos are loaded")
            return

        cmd = command.strip().lower()
        if cmd == "next":
            self.todo_index = (self.todo_index + 1) % len(self.todo_list)
        elif cmd == "prev":
            self.todo_index = (self.todo_index - 1) % len(self.todo_list)
        else:
            logger.warning(f"Unsupported todo select command: {command}")
            return

        todo = self.todo_list[self.todo_index].split(',')[0]
        self.client.publish("nextTODO/personal", todo)
        logger.info(f"Rotated todo to '{cmd}': {todo}")
    
    async def update_schedule(self) -> None:
        """Fetch and process calendar schedule"""
        if not Config.CAL_SCRAPER_HOST:
            return
            
        # Skip update if in countdown mode and event hasn't passed
        if (self.led_controller.current_mode == "eventCountdown" and 
            self.next_event.get("eventStart")):
            
            try:
                event_time = self._from_timestamp(self.next_event['eventStart'])
                if self._now() < event_time or (self._now() - event_time).total_seconds() < 120:
                    return
            except (KeyError, ValueError, TypeError) as e:
                logger.warning(f"Invalid event start time in next_event: {e}")
        
        try:
            headers = {"mode": "getCalendar", "calSource": "calendarEvents"}
            result = requests.get(Config.CAL_SCRAPER_HOST, headers=headers, timeout=Config.HTTP_REQUEST_TIMEOUT)
            result.raise_for_status()
            schedule = self._parse_calendar_response(result.text)
            
            # Find next valid event
            event = None
            event_start = None
            
            while len(schedule) > 1:
                # schedule = schedule[1:]
                event_data = schedule[0].split(",")
                
                # Parse event timestamp using configured timezone
                event_start = self._from_timestamp(float(event_data[0]))
                
                # Check if event is in the future and not tentative
                if event_start > self._now():
                    if not event_data[2][:5].startswith("~~"):
                        event = event_data
                        break
                    
                event = None
                event_start = None
            
            if event_start and event:
                self._process_event(event, event_start)
            else:
                self.reset_next_event()
                
        except RequestException as e:
            logger.warning(f"Failed to fetch schedule from calendar scraper: {e}")
        except Exception as e:
            logger.error(f"Error updating schedule: {e}")
            logger.debug(f"Full traceback: {traceback.format_exc()}")
    
    def _process_event(self, event: List[str], event_start: datetime) -> None:
        """Process and publish calendar event details"""
        try:
            self.next_event['eventStart'] = event_start.timestamp()
            self._safe_publish("nextEvent/timeStamp", self.next_event['eventStart'], qos=1)
            
            self.next_event['eventSubject'] = event[2].strip() if len(event) > 2 else "Unknown"
            self._safe_publish("nextEvent/subject", self.next_event['eventSubject'], qos=1)
            
            self.next_event['eventOrganizer'] = event[3].strip() if len(event) > 3 else "Unknown"
            self._safe_publish("nextEvent/organizer", self.next_event['eventOrganizer'], qos=1)
            
            # Process attendees if available
            if len(event) > 4:
                attendees = event[4].strip()
                if len(attendees) > 200:
                    attendees = attendees[:200] + "....."
                self.next_event['eventAttendees'] = attendees
                self._safe_publish("nextEvent/attendees", attendees, qos=1)
            
            # Check for external attendees
            is_external = len(event) > 5 and "EXTATTENDEES" in event[5]
            self.next_event['isExternal'] = is_external
            self._safe_publish("nextEvent/isExternal", is_external, qos=1)
            
            logger.info(f"Event published: {self.next_event['eventSubject']} at {event_start}")
        except (IndexError, ValueError, TypeError) as e:
            logger.error(f"Error parsing event data: {e}, event data: {event}")
            logger.debug(f"Full traceback: {traceback.format_exc()}")
            self.reset_next_event()
    
    async def monitor_schedule(self) -> None:
        """Monitor schedule for upcoming events and trigger alerts"""
        while True:
            await asyncio.sleep(60)
            
            if not self.next_event.get('eventStart'):
                continue
            
            try:
                event_time = self._from_timestamp(self.next_event['eventStart'])
                seconds_until = (event_time - self._now()).total_seconds()
                
                # Alert for events starting in less than 5 minutes
                if seconds_until < 301:
                    logger.info(f"Event countdown: {seconds_until}s until {self.next_event.get('eventSubject', 'Unknown')}")
                    self.led_controller.set_mode("eventCountdown")
                    
                    # Publish event details
                    self._safe_publish("nextEvent/timeStamp", self.next_event.get('eventStart'))
                    self._safe_publish("nextEvent/subject", self.next_event.get('eventSubject', 'Unknown'))
                    self._safe_publish("nextEvent/organizer", self.next_event.get('eventOrganizer', 'Unknown'))
                    self._safe_publish("nextEvent/attendees", self.next_event.get('eventAttendees', ''))
                    self._safe_publish("nextEvent/isExternal", self.next_event.get('isExternal', False))
                    
                    # Activate beacon light for external meetings
                    if self.next_event.get('isExternal'):
                        self._safe_publish("beaconLight/activate", True)
                    else:
                        self._safe_publish("beaconLight/activate", False)
                
                # Reset after event has passed
                elif self._now() > event_time and (self._now() - event_time).total_seconds() > 120:
                    if self.led_controller.current_mode == "eventCountdown":
                        logger.info("Event passed, resetting display")
                        self.led_controller.set_mode("bigClock")
                    self.reset_next_event()
                
                # Turn off beacon light 3 seconds before event
                if (event_time - self._now()).total_seconds() < 3 or self._now() > event_time:
                    self._safe_publish("beaconLight/activate", False)
                    
            except (KeyError, ValueError, TypeError) as e:
                logger.warning(f"Invalid event data in monitor_schedule: {e}")
                self.reset_next_event()
            except Exception as e:
                logger.error(f"Error monitoring schedule: {e}")
                logger.debug(f"Full traceback: {traceback.format_exc()}")


class MessageProcessor:
    """Processes and sends messages to the LED sign"""
    
    def __init__(self, message_queue: asyncio.Queue[Tuple[datetime, str, str, str]], led_controller: LEDSignController):
        self.message_queue = message_queue
        self.led_controller = led_controller
    
    async def process_messages(self) -> None:
        """Process queued messages and send to LED sign"""
        logger.info("Message processor started")
        active_queue = False
        
        while True:
            timestamp, msg_type, sender, text = await self.message_queue.get()
            
            if not active_queue:
                active_queue = True
            logger.info(f"Processing message: {msg_type} from {sender} ({len(text)} chars)")
            logger.debug(f"Message queue size after get: {self.message_queue.qsize()}")
            
            try:
                # Display message on LED sign
                self.led_controller.display_message(msg_type, sender, text)
                
                # Calculate and wait for display time
                display_time = self.led_controller.calculate_display_time(text)
                logger.debug(f"Message display time: {display_time:.1f}s")
                await asyncio.sleep(display_time)
            except Exception as e:
                logger.error(f"Error processing message: {e}")
                logger.debug(f"Full traceback: {traceback.format_exc()}")
            finally:
                self.message_queue.task_done()
                if self.message_queue.empty():
                    self.led_controller.set_mode("bigClock")
                    active_queue = False


class NotificationCollator:
    """Main application class that coordinates all components"""
    
    def __init__(self):
        self.message_queue: asyncio.Queue[Tuple[datetime, str, str, str]] = asyncio.Queue()
        self.mqtt_connected = asyncio.Event()
        self.mqtt_client = self._setup_mqtt()
        self.led_controller = LEDSignController(self.mqtt_client, self.mqtt_connected)
        self.signal_handler = SignalHandler(self.message_queue)
        self.calendar_manager = CalendarManager(self.mqtt_client, self.led_controller, self.mqtt_connected)
        self.message_processor = MessageProcessor(self.message_queue, self.led_controller)
    
    def _setup_mqtt(self) -> mqtt.Client:
        """Setup MQTT client with callbacks"""
        client = mqtt.Client(client_id="HASS_messageprocessor", callback_api_version=CallbackAPIVersion.VERSION1)
        client.username_pw_set(Config.MQTT_USER, password=Config.MQTT_PASSWORD)
        
        client.on_connect = self._on_mqtt_connect
        client.on_disconnect = self._on_mqtt_disconnect
        client.on_connect_fail = self._on_mqtt_connect_fail
        # register general on_message handler (will route internally)
        client.on_message = self._on_mqtt_message
        
        return client
    
    def _on_mqtt_connect(self, client, userdata, flags, rc):
        """MQTT connection callback"""
        if rc == 0:
            logger.info("Successfully connected to MQTT broker")
            self.mqtt_connected.set()
        else:
            logger.error(f"Connected to MQTT broker with error code {rc}")
            self.mqtt_connected.clear()
    
    def _on_mqtt_disconnect(self, client, userdata, rc):
        """MQTT disconnection callback"""
        self.mqtt_connected.clear()
        if rc != 0:
            logger.error(f"Unexpected disconnect from MQTT broker with code {rc}")
        else:
            logger.info(f"Gracefully disconnected from MQTT broker")
        logger.info(f"MQTT client will attempt automatic reconnection (unexpected={rc != 0})")
    
    def _on_mqtt_connect_fail(self, client, userdata):
        """MQTT connection failure callback"""
        self.mqtt_connected.clear()
        logger.error("Failed to connect to MQTT broker, will retry...")

    def _on_mqtt_message(self, client, userdata, msg):
        """General MQTT message handler for subscribed commands"""
        topic = msg.topic
        payload = msg.payload.decode('utf-8', errors='ignore')
        logger.debug(f"Received MQTT message on {topic}: {payload}")

        if topic == "nextTODO/select":
            self.calendar_manager.handle_todo_select(payload)
        else:
            logger.debug(f"No handler for topic {topic}")
    
    async def run(self):
        """Run the notification collator"""
        logger.info("Starting Notification Collator")
        
        try:
            # Connect to MQTT
            logger.info(f"Connecting to MQTT broker at {Config.MQTT_BROKER}:1883")
            try:
                if not Config.MQTT_BROKER:
                    raise RuntimeError('MQTT broker host is not configured')
                self.mqtt_client.connect(Config.MQTT_BROKER, 1883)
                self.mqtt_client.loop_start()
                logger.info("MQTT client loop started")
            except Exception as e:
                logger.error(f"MQTT connection attempt failed: {e}")
                # continue, other components may still operate

            # subscribe to command topics (safe even if not connected immediately)
            try:
                self.mqtt_client.subscribe("nextTODO/select")
                logger.info("Subscribed to nextTODO/select for todo rotation commands")
            except Exception as e:
                logger.error(f"Unable to subscribe to nextTODO/select: {e}")
            
            # Create task to verify and retry subscriptions on reconnect
            asyncio.create_task(self._verify_mqtt_subscriptions())
            
            # Reset LED sign to clock mode
            self.led_controller.set_mode("bigClock")
            
            # Create async tasks
            tasks = {
                "signal_monitor": asyncio.create_task(self.signal_handler.monitor()),
                "todos_loop": asyncio.create_task(self._update_todos_loop()),
                "schedule_loop": asyncio.create_task(self._update_schedule_loop()),
                "schedule_monitor": asyncio.create_task(self.calendar_manager.monitor_schedule()),
                "message_processor": asyncio.create_task(self.message_processor.process_messages())
            }
            
            logger.info(f"Started {len(tasks)} async tasks: {list(tasks.keys())}")
            
            # Monitor tasks for failures
            asyncio.create_task(self._monitor_tasks(tasks))
            
            # Wait for all tasks
            await asyncio.gather(*tasks.values())
        
        except KeyboardInterrupt:
            logger.info("Shutting down on keyboard interrupt")
        except Exception as e:
            logger.error(f"Fatal error in main loop: {e}")
            logger.debug(f"Full traceback: {traceback.format_exc()}")
            raise
    
    async def _monitor_tasks(self, tasks: Dict[str, asyncio.Task]) -> None:
        """Monitor async tasks and log if any fail"""
        while True:
            await asyncio.sleep(Config.TASK_MONITORING_INTERVAL)
            for name, task in tasks.items():
                if task.done():
                    try:
                        task.result()  # This will raise if the task failed
                    except Exception as e:
                        logger.error(f"Task '{name}' has failed: {e}")
                        exception = task.exception()
                        logger.debug(f"Task exception: {exception if exception else 'N/A'}")
    
    async def _update_todos_loop(self):
        """Periodic TODO update loop"""
        while True:
            await asyncio.sleep(5)
            try:
                await self.calendar_manager.update_todos()
            except Exception as e:
                logger.error(f"Exception in todos update loop: {e}")
                logger.debug(f"Full traceback: {traceback.format_exc()}")
    
    async def _update_schedule_loop(self):
        """Periodic schedule update loop"""
        while True:
            await asyncio.sleep(5)
            try:
                await self.calendar_manager.update_schedule()
            except Exception as e:
                logger.error(f"Exception in schedule update loop: {e}")
                logger.debug(f"Full traceback: {traceback.format_exc()}")
    
    async def _verify_mqtt_subscriptions(self) -> None:
        """Verify MQTT subscriptions and retry if needed"""
        while True:
            await asyncio.sleep(30)
            try:
                # Wait for connection if not already connected
                if not self.mqtt_connected.is_set():
                    logger.debug("Waiting for MQTT connection before verifying subscriptions...")
                    await asyncio.wait_for(self.mqtt_connected.wait(), timeout=60)
                
                # Resubscribe to ensure we have subscriptions
                self.mqtt_client.subscribe("nextTODO/select")
                logger.debug("MQTT subscriptions verified")
            except asyncio.TimeoutError:
                logger.warning("Timeout waiting for MQTT connection")
            except Exception as e:
                logger.warning(f"Error verifying MQTT subscriptions: {e}")


def main():
    """Main entry point"""
    logger.info("="*80)
    logger.info("Notification Collator starting...")
    logger.info("="*80)
    
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
    
    logger.info(f"Configuration loaded:")
    logger.info(f"  MQTT_BROKER: {Config.MQTT_BROKER}")
    logger.info(f"  CAL_SCRAPER_HOST: {Config.CAL_SCRAPER_HOST or 'disabled'}")
    logger.info(f"  TIMEZONE: {Config.TIMEZONE}")
    logger.info(f"  HTTP_TIMEOUT: {Config.HTTP_REQUEST_TIMEOUT}s")
    logger.info(f"  DBUS_RETRIES: {Config.DBUS_MAX_RETRIES}")
    
    try:
        collator = NotificationCollator()
        asyncio.run(collator.run())
    except ConnectionRefusedError as e:
        logger.error(f"Connection refused: {e}")
        logger.debug(f"Full traceback: {traceback.format_exc()}")
        sys.exit(1)
    except KeyboardInterrupt:
        logger.info("Shutting down...")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Unexpected fatal error: {e}")
        logger.debug(f"Full traceback: {traceback.format_exc()}")
        sys.exit(1)


if __name__ == "__main__":
    main()