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
import pickle
import asyncio
import logging
import traceback
import base64
import textwrap
from datetime import datetime

from typing import Callable, Dict, List, Tuple, Optional, Any
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
    TODOIST_API_TOKEN = os.environ.get('TODOIST_API_TOKEN')
    PRINT_HOST = os.environ.get('PRINT_HOST')
    PRINT_USER = os.environ.get('PRINT_USER')
    PRINT_PASSWORD = os.environ.get('PRINT_PASSWORD')



    # Signal Configuration
    # Set to your Signal account phone number (e.g. +15551234567) to suppress
    # your own outgoing messages from being displayed on the LED sign.
    # This is a secondary guard; sync message handlers are already excluded.
    SIGNAL_OWN_NUMBER = os.environ.get('SIGNAL_OWN_NUMBER', '').strip()

    # File paths
    NICK_PICKLE_FILE = 'nickPickle.dat'

    # Display timing
    BASE_MESSAGE_DISPLAY_TIME = 6
    EXTRA_TIME_PER_CHAR = 0.75
    MESSAGE_LENGTH_THRESHOLD = 16

    # Connection timeouts and retries
    HTTP_REQUEST_TIMEOUT = 10       # seconds
    DBUS_RECONNECT_DELAY = 5        # seconds between DBus reconnect attempts
    DBUS_MAX_RETRIES = 3
    DBUS_HEALTH_CHECK_INTERVAL = 5  # seconds between bus liveness polls
    MQTT_RECONNECT_DELAY = 5        # seconds

    # Task supervision
    TASK_MONITORING_INTERVAL = 10   # seconds between supervisor checks
    TASK_RESTART_DELAY = 30         # seconds to wait before restarting a failed task

    # Scheduling intervals (seconds)
    TODO_UPDATE_INTERVAL = 300      # 5 minutes
    SCHEDULE_UPDATE_INTERVAL = 60   # 1 minute
    SCHEDULE_MONITOR_INTERVAL = 60  # 1 minute between countdown checks
    HEARTBEAT_INTERVAL = 300        # 5 minutes

    @classmethod
    def get_timezone(cls):
        try:
            return ZoneInfo(cls.TIMEZONE)
        except ZoneInfoNotFoundError:
            logger.warning(f"Invalid TIMEZONE '{cls.TIMEZONE}', falling back to UTC")
            return ZoneInfo('UTC')


# ---------------------------------------------------------------------------
# Signal Handler
# ---------------------------------------------------------------------------

class SignalHandler:
    """Handles Signal messaging integration via DBus"""

    def __init__(self, message_queue: asyncio.Queue[Tuple[datetime, str, str, str]]):
        self.message_queue = message_queue
        self.nick_map = self._load_nick_map()
        self.bus = None
        self.signal_interface = None
        self.last_message_time: Optional[datetime] = None
        # Stored after _initialize_dbus succeeds; used for callback arity detection
        self._signal_introspection = None

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

    async def _find_signal_interface_path(
        self,
        service_name: str = 'org.asamk.Signal',
        root_path: str = '/org/asamk/Signal',
    ) -> Optional[str]:
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
                    self._signal_introspection = introspection  # saved for arity detection
                    proxy_object = self.bus.get_proxy_object('org.asamk.Signal', object_path, introspection)
                    self.signal_interface = proxy_object.get_interface('org.asamk.Signal')
                    await self._register_signal_handlers()

                    logger.info(
                        f"Signal DBus interface connected and monitoring for messages "
                        f"on {bus_name} bus at {object_path}"
                    )
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
                logger.info(
                    f"Retrying Signal DBus initialization in {Config.DBUS_RECONNECT_DELAY}s "
                    f"(attempt {attempt + 1}/{Config.DBUS_MAX_RETRIES})"
                )
                await asyncio.sleep(Config.DBUS_RECONNECT_DELAY)

        logger.critical("Failed to initialize Signal DBus connection after all retries")
        return False

    async def _register_signal_handlers(self) -> None:
        """Subscribe to Signal receive events and register incoming message callbacks.

        dbus_next validates callback arity at registration time using inspect —
        it rejects *args functions.  We introspect each signal's type signature
        to discover the exact parameter count, then use _make_dbus_callback() to
        generate a wrapper with precisely that many positional parameters.

        Sync message signals are intentionally excluded: signal-cli fires them
        for messages the local account *sends*, which should not appear on the sign.
        """
        if hasattr(self.signal_interface, 'call_subscribe_receive'):
            try:
                await self.signal_interface.call_subscribe_receive()  # type: ignore
                logger.info("Subscribed to Signal receive events")
            except Exception as e:
                logger.warning(f"Failed to subscribe to Signal receive events: {e}")
        else:
            logger.warning(
                "Signal interface does not support subscribeReceive; "
                "relying on passive message events"
            )

        # Build arity map from introspection so each callback has the exact
        # number of positional parameters dbus_next expects.
        arity_map = self._build_signal_arity_map()
        logger.debug(f"Signal arity map from introspection: {arity_map}")

        handlers = []
        for signal_name in [
            'on_message_received',
            'on_message_received_v2',
            # NOTE: on_sync_message_received / on_sync_message_received_v2 are
            # intentionally excluded.  Signal-CLI fires sync events for messages
            # the local account *sends* — registering them would echo your own
            # outgoing messages onto the LED sign.
        ]:
            signal_slot = getattr(self.signal_interface, signal_name, None)
            if not callable(signal_slot):
                continue

            n_args = arity_map.get(signal_name)
            if n_args is None:
                logger.warning(
                    f"Could not determine arity for '{signal_name}' from introspection; "
                    f"falling back to 5 parameters (standard MessageReceived signature)"
                )
                n_args = 5

            cb = self._make_dbus_callback(n_args, signal_name)
            signal_slot(cb)
            handlers.append(f"{signal_name}({n_args} args)")

        if not handlers:
            raise AttributeError('Signal interface does not expose any incoming message events')

        logger.info(f"Registered Signal callbacks for: {', '.join(handlers)}")

    # ------------------------------------------------------------------
    # Callback arity helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _pascal_to_snake(name: str) -> str:
        """Convert PascalCase to snake_case (e.g. MessageReceived -> message_received)."""
        result = []
        for i, ch in enumerate(name):
            if ch.isupper() and i > 0:
                result.append('_')
            result.append(ch.lower())
        return ''.join(result)

    def _build_signal_arity_map(self) -> Dict[str, int]:
        """Return a map of dbus_next on_xxx property name -> D-Bus arg count.

        Reads signal definitions from the introspection data captured during
        _initialize_dbus.  Returns an empty dict if introspection is unavailable.
        """
        arity_map: Dict[str, int] = {}
        if self._signal_introspection is None:
            logger.warning("No introspection data available for arity detection")
            return arity_map

        for iface in getattr(self._signal_introspection, 'interfaces', []):
            if getattr(iface, 'name', '') != 'org.asamk.Signal':
                continue
            for sig in getattr(iface, 'signals', []):
                prop_name = 'on_' + self._pascal_to_snake(sig.name)
                n = len(getattr(sig, 'args', []))
                arity_map[prop_name] = n
                logger.debug(f"  introspected: {sig.name} -> {prop_name} ({n} args)")

        return arity_map

    def _make_dbus_callback(self, n_args: int, signal_name: str):
        """Generate a callback with exactly n_args positional parameters.

        dbus_next uses inspect.getfullargspec to validate that the registered
        callback has the correct number of positional parameters.  A *args
        function does not satisfy this check.  We use exec() to produce a
        function with a precise, statically-known signature at call time.
        """
        param_names = ', '.join(f'_p{i}' for i in range(n_args))
        globs: Dict[str, Any] = {
            '_asyncio': asyncio,
            '_logger': logger,
            '_process': self._process_signal_event,
            '_done_cb': self._on_message_task_done,
            '_sname': signal_name,
            '_n': n_args,
        }
        exec(
            f"def _cb({param_names}):\n"
            f"    _logger.debug('[%s] D-Bus event: {n_args} args', _sname)\n"
            f"    _t = _asyncio.create_task(_process({param_names}))\n"
            f"    _t.add_done_callback(_done_cb)\n",
            globs,
        )
        return globs['_cb']


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
        list_like = [
            a for a in args[1:]
            if isinstance(a, (list, tuple)) and all(isinstance(x, int) for x in a)
        ]
        attachments_items = [a for a in args[1:] if isinstance(a, (list, dict))]

        source = str_items[0] if len(str_items) >= 1 else None

        # Message is typically the last string argument; when there is only one string
        # it serves as both source and message which is ambiguous — log and return None.
        if len(str_items) >= 2:
            message = str_items[-1]
        elif len(str_items) == 1:
            # Only one string; use it as message and let source stay as-is
            message = str_items[0]
        else:
            message = ''

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
            logger.debug(
                f"_parse_signal_args: insufficient data — "
                f"source={source!r}, message={message!r}, raw_args={args}"
            )
            return None

        return timestamp, source, group_id, message, attachments

    async def _process_signal_event(self, *args) -> None:
        logger.debug(f"_process_signal_event called with {len(args)} args")
        parsed = self._parse_signal_args(args)
        if parsed is None:
            logger.warning("Received unrecognized Signal event args: %s", args)
            return

        timestamp, source, group_id, message, attachments = parsed
        await self._process_message(timestamp, source, group_id, message, attachments)

    def _on_message_task_done(self, task: asyncio.Task) -> None:
        """Callback for Signal message processing task completion"""
        try:
            task.result()
        except asyncio.CancelledError:
            logger.debug("Signal message processing task was cancelled")
        except Exception as e:
            logger.error(f"Signal message processing task failed: {e}")
            logger.debug(f"Full traceback: {traceback.format_exc()}")

    async def _process_message(
        self,
        timestamp: int,
        source: str,
        group_id: bytes,
        message: str,
        attachments: List,
    ) -> None:
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
            # Secondary self-filter: drop if source matches our own number.
            # (Primary guard is that sync message handlers are not registered.)
            if Config.SIGNAL_OWN_NUMBER and source == Config.SIGNAL_OWN_NUMBER:
                logger.debug(f"Ignoring message from own account ({source})")
                return

            logger.info(
                f"Signal message received from {source}, "
                f"length={len(message)}, "
                f"attachments={len(attachments) if attachments else 0}"
            )

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
                    gid_repr = group_id.hex() if isinstance(group_id, bytes) else group_id
                    logger.warning(f"Failed to get group name for {gid_repr}: {e}")
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
            self.last_message_time = datetime.now()
            logger.info(f"Message queued from: {sender_name!r}")

            if attachments:
                logger.debug(f"Message contains {len(attachments)} attachments")

        except Exception as e:
            logger.error(f"Error processing Signal message: {e}")
            logger.debug(f"Full traceback: {traceback.format_exc()}")

    async def monitor(self) -> None:
        """Monitor Signal DBus for incoming messages with graceful degradation.

        After a successful connection, polls `bus.connected` every
        DBUS_HEALTH_CHECK_INTERVAL seconds. When the bus drops (e.g. signal-cli
        restarts), the liveness loop exits and a full reconnect is attempted.
        """
        while True:
            # Reset interface references so stale objects are never reused
            self.signal_interface = None
            self.bus = None

            success = await self._initialize_dbus()

            if success:
                logger.info(
                    "Signal handler monitoring started — "
                    "polling D-Bus liveness every "
                    f"{Config.DBUS_HEALTH_CHECK_INTERVAL}s"
                )
                try:
                    # Block here as long as the bus stays connected
                    while getattr(self.bus, 'connected', False):
                        await asyncio.sleep(Config.DBUS_HEALTH_CHECK_INTERVAL)
                    logger.warning(
                        "D-Bus bus.connected is False — connection dropped, "
                        "triggering reconnect"
                    )
                except Exception as e:
                    logger.error(f"Error during D-Bus liveness poll: {e}")
                    logger.debug(f"Full traceback: {traceback.format_exc()}")
            else:
                logger.warning(
                    f"Signal D-Bus unavailable, retrying in {Config.DBUS_RECONNECT_DELAY}s..."
                )

            # Clean up before reconnecting
            if self.bus:
                try:
                    self.bus.disconnect()
                except Exception:
                    pass
                self.bus = None
            self.signal_interface = None

            await asyncio.sleep(Config.DBUS_RECONNECT_DELAY)


# ---------------------------------------------------------------------------
# LED Sign Controller
# ---------------------------------------------------------------------------

class LEDSignController:
    """Controls the LED sign via MQTT.

    This class wraps the raw MQTT client to centralise publish error
    handling and to keep track of the current display mode.
    """

    def __init__(self, mqtt_client: mqtt.Client, connected_event: asyncio.Event):
        self.client = mqtt_client
        self.current_mode = "bigClock"
        self.connected_event = connected_event
        self.paused_event = asyncio.Event()
        self.paused_event.set()


    def _safe_publish(self, topic: str, payload: Any, qos: int = 0) -> bool:
        """Publish to MQTT with exception handling and QoS support.

        Returns:
            bool: True if publish was attempted while connected, False otherwise.
        """
        try:
            if not self.connected_event.is_set():
                logger.warning(
                    f"MQTT not connected — skipping publish to '{topic}' "
                    f"(payload={payload!r})"
                )
                return False
            logger.debug(f"Publishing to MQTT topic '{topic}' (QoS={qos}): {payload}")
            result = self.client.publish(topic, payload, qos=qos)
            logger.debug(f"Successfully published to MQTT topic '{topic}' (mid={result.mid})")
            return True
        except Exception as e:
            logger.error(f"Failed to publish {topic}: {e}")
            logger.debug(f"Payload was: {payload}")
            return False

    def set_mode(self, mode: str) -> None:
        """Set the LED sign display mode"""
        self._safe_publish("ledSign/mode", mode, qos=1)
        self.current_mode = mode

    def display_message(self, message_type: str, sender: str, text: str) -> None:
        """Display a message on the LED sign"""
        logger.info(
            f"Displaying message: type={message_type}, sender={sender!r}, "
            f"text={text[:50]!r}{'...' if len(text) > 50 else ''}"
        )
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


# ---------------------------------------------------------------------------
# Calendar Manager
# ---------------------------------------------------------------------------

class CalendarManager:
    """Manages calendar events and TODOs"""

    def __init__(
        self,
        mqtt_client: mqtt.Client,
        led_controller: LEDSignController,
        connected_event: asyncio.Event,
    ):
        self.client = mqtt_client
        self.led_controller = led_controller
        self.connected_event = connected_event
        self.next_event: Dict[str, Any] = {}
        # maintain TODO list and index for rotation
        self.todo_list: List[str] = []
        self.todo_index: int = 0
        self.last_schedule_update: Optional[datetime] = None
        self.last_todo_update: Optional[datetime] = None

    def _safe_publish(self, topic: str, payload: Any, qos: int = 0) -> bool:
        """Publish with logging on failure and QoS support"""
        try:
            if not self.connected_event.is_set():
                logger.warning(f"MQTT not connected — skipping publish to '{topic}'")
                return False
            self.client.publish(topic, payload, qos=qos)
            return True
        except Exception as e:
            logger.error(f"Failed to publish {topic}: {e}")
            logger.debug(f"Payload was: {payload}")
            return False

    def _parse_calendar_response(self, response_text: str) -> List[str]:
        """Parse calendar response and remove the leading timestamp line."""
        lines = response_text.split("\n")
        # Index 0 is the "last updated" timestamp — strip it
        return lines[1:] if len(lines) > 1 else []

    def _now(self) -> datetime:
        return datetime.now(Config.get_timezone())

    def _from_timestamp(self, timestamp: float) -> datetime:
        return datetime.fromtimestamp(float(timestamp), Config.get_timezone())

    async def _http_get(self, headers: dict) -> Optional[requests.Response]:
        """Execute a blocking requests.get in a thread pool to avoid blocking the event loop.

        Uses asyncio.to_thread so the HTTP call never stalls other async tasks.
        """
        if not Config.CAL_SCRAPER_HOST:
            return None
        try:
            result = await asyncio.to_thread(
                requests.get,
                Config.CAL_SCRAPER_HOST,
                headers=headers,
                timeout=Config.HTTP_REQUEST_TIMEOUT,
            )
            result.raise_for_status()
            return result
        except RequestException as e:
            logger.warning(f"HTTP GET failed (headers={headers}): {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error during HTTP GET: {e}")
            logger.debug(f"Full traceback: {traceback.format_exc()}")
            return None

    async def update_todos(self) -> None:
        """Fetch and publish TODO items"""
        if not Config.CAL_SCRAPER_HOST:
            return

        try:
            logger.debug("Fetching TODOs from calendar scraper...")
            result = await self._http_get({"mode": "getCalendar", "calSource": "calendar_TODO"})
            if result is None:
                return

            # _parse_calendar_response already strips the leading timestamp line;
            # do NOT apply a second [1:] slice here (that was a bug that dropped the
            # first real TODO item on every refresh).
            self.todo_list = self._parse_calendar_response(result.text)
            self.todo_index = 0
            self.last_todo_update = datetime.now()

            logger.debug(f"Fetched {len(self.todo_list)} TODO items")

            if self.todo_list:
                todo = self.todo_list[self.todo_index].split(',')
                try:
                    todoist_id = todo[0].strip() if len(todo) > 0 else ""
                    title = todo[1].strip() if len(todo) > 1 else ""
                    description = todo[2].strip() if len(todo) > 2 else ""

                    self.client.publish("nextTODO/personal/todoistID", todoist_id)
                    self.client.publish("nextTODO/personal", title)
                    self.client.publish("nextTODO/personal/title", title)
                    self.client.publish("nextTODO/personal/description", description)
                    
                except Exception as e:
                    logger.error(f"Failed to publish TODO to MQTT: {e}")
                logger.debug(f"Published TODO: {todo!r}")

            else:
                logger.debug("No TODOs available after fetch")

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
            ("eventStart",    "nextEvent/timeStamp", None),
            ("eventSubject",  "nextEvent/subject",   None),
            ("eventOrganizer","nextEvent/organizer",  None),
            ("eventAttendees","nextEvent/attendees",  None),
            ("isExternal",    "nextEvent/isExternal", False),
        ]

        for field, topic, default_value in event_fields:
            self.next_event[field] = default_value
            try:
                self._safe_publish(topic, default_value, qos=1)
            except Exception as e:
                logger.error(f"Error publishing reset field {field} to {topic}: {e}")

        logger.info("Next event reset")

    def handle_todo_select(self, command: str) -> None:
        """Rotate through TODO list in response to MQTT commands.

        Supported commands:
        - "next": move to next item
        - "prev": move to previous item

        The published MQTT topic is ``nextTODO/personal``.
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
            logger.warning(f"Unsupported todo select command: {command!r}")
            return

        todo = self.todo_list[self.todo_index].split(',')
        todoist_id = todo[0].strip() if len(todo) > 0 else ""
        title = todo[1].strip() if len(todo) > 1 else ""
        description = todo[2].strip() if len(todo) > 2 else ""

        self.client.publish("nextTODO/personal/todoistID", todoist_id)
        self.client.publish("nextTODO/personal", title)
        self.client.publish("nextTODO/personal/title", title)
        self.client.publish("nextTODO/personal/description", description)
        if self.led_controller.paused_event.is_set():
            self.client.publish("ledSign/mode", "showTodo")
        else:
            logger.info("Sign is paused — skipping publishing ledSign/mode showTodo")
        
        logger.info(f"Rotated todo ({cmd}): {todo!r}")



    async def update_schedule(self) -> None:
        """Fetch and process calendar schedule."""
        if not Config.CAL_SCRAPER_HOST:
            return

        # Skip update if in countdown mode and event hasn't passed yet
        if (self.led_controller.current_mode == "eventCountdown"
                and self.next_event.get("eventStart")):
            try:
                event_time = self._from_timestamp(self.next_event['eventStart'])
                if (self._now() < event_time
                        or (self._now() - event_time).total_seconds() < 120):
                    logger.debug("Skipping schedule update — event countdown active")
                    return
            except (KeyError, ValueError, TypeError) as e:
                logger.warning(f"Invalid event start time in next_event: {e}")

        try:
            logger.debug("Fetching schedule from calendar scraper...")
            result = await self._http_get({"mode": "getCalendar", "calSource": "calendarEvents"})
            if result is None:
                return

            schedule = self._parse_calendar_response(result.text)
            self.last_schedule_update = datetime.now()
            logger.debug(f"Fetched {len(schedule)} raw schedule entries")

            # Iterate through entries to find the first upcoming non-tentative event.
            # IMPORTANT: always advance schedule = schedule[1:] to prevent an infinite
            # loop when the first entry is in the past.
            event = None
            event_start = None

            while schedule:
                raw_entry = schedule[0]
                schedule = schedule[1:]  # advance unconditionally — never loop on same entry

                if not raw_entry or not raw_entry.strip():
                    logger.debug("Skipping empty schedule entry")
                    continue

                event_data = raw_entry.split(",")

                if not event_data[0].strip():
                    logger.debug(f"Skipping entry with blank timestamp: {raw_entry!r}")
                    continue

                try:
                    event_start = self._from_timestamp(float(event_data[0]))
                except (ValueError, TypeError) as e:
                    logger.warning(
                        f"Bad timestamp in schedule entry, skipping: "
                        f"{event_data[0]!r} — {e}"
                    )
                    event_start = None
                    continue

                if event_start > self._now():
                    subject_prefix = event_data[2][:2] if len(event_data) > 2 else ''
                    if not subject_prefix.startswith("~~"):
                        event = event_data
                        evt_subj = repr(event_data[2]) if len(event_data) > 2 else '?'
                        logger.debug(f"Found next event: {evt_subj} at {event_start}")
                        break
                    else:
                        evt_subj = repr(event_data[2]) if len(event_data) > 2 else '?'
                        logger.debug(
                            f"Skipping tentative event at {event_start}: {evt_subj}"
                        )
                else:
                    logger.debug(f"Skipping past event at {event_start}")

                event = None
                event_start = None

            if event_start and event:
                self._process_event(event, event_start)
            else:
                logger.debug("No upcoming events found — resetting next_event")
                self.reset_next_event()

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

            logger.info(f"Event published: {self.next_event['eventSubject']!r} at {event_start}")
        except (IndexError, ValueError, TypeError) as e:
            logger.error(f"Error parsing event data: {e}, event data: {event}")
            logger.debug(f"Full traceback: {traceback.format_exc()}")
            self.reset_next_event()

    async def monitor_schedule(self) -> None:
        """Monitor schedule for upcoming events and trigger countdown alerts.

        Sleep is at the END of the loop so the first check runs immediately on startup
        (important if an event is imminent when the service restarts).
        """
        logger.info("Schedule monitor started")
        while True:
            if not self.next_event.get('eventStart'):
                logger.debug("monitor_schedule: no active event")
                await asyncio.sleep(Config.SCHEDULE_MONITOR_INTERVAL)
                continue

            try:
                event_time = self._from_timestamp(self.next_event['eventStart'])
                seconds_until = (event_time - self._now()).total_seconds()
                subject = self.next_event.get('eventSubject', 'Unknown')

                logger.debug(f"monitor_schedule: {seconds_until:.0f}s until {subject!r}")

                # Alert for events starting in less than 5 minutes
                if seconds_until < 301:
                    logger.info(f"Event countdown: {seconds_until:.0f}s until {subject!r}")
                    self.led_controller.set_mode("eventCountdown")

                    # Publish event details
                    self._safe_publish("nextEvent/timeStamp", self.next_event.get('eventStart'))
                    self._safe_publish("nextEvent/subject", subject)
                    self._safe_publish("nextEvent/organizer", self.next_event.get('eventOrganizer', 'Unknown'))
                    self._safe_publish("nextEvent/attendees", self.next_event.get('eventAttendees', ''))
                    self._safe_publish("nextEvent/isExternal", self.next_event.get('isExternal', False))

                    # Activate beacon light for external meetings
                    is_external = self.next_event.get('isExternal', False)
                    self._safe_publish("beaconLight/activate", is_external)

                # Reset after event has passed (> 2 minutes past)
                elif self._now() > event_time and (self._now() - event_time).total_seconds() > 120:
                    if self.led_controller.current_mode == "eventCountdown":
                        logger.info("Event passed — resetting display to bigClock")
                        self.led_controller.set_mode("bigClock")
                    self.reset_next_event()

                # Turn off beacon light when event is about to start or already started
                if (event_time - self._now()).total_seconds() < 3 or self._now() > event_time:
                    self._safe_publish("beaconLight/activate", False)

            except (KeyError, ValueError, TypeError) as e:
                logger.warning(f"Invalid event data in monitor_schedule: {e}")
                self.reset_next_event()
            except Exception as e:
                logger.error(f"Error monitoring schedule: {e}")
                logger.debug(f"Full traceback: {traceback.format_exc()}")

            # Sleep at the end so first iteration runs immediately on startup
            await asyncio.sleep(Config.SCHEDULE_MONITOR_INTERVAL)


# ---------------------------------------------------------------------------
# Message Processor
# ---------------------------------------------------------------------------

class MessageProcessor:
    """Processes and sends messages to the LED sign"""

    def __init__(
        self,
        message_queue: asyncio.Queue[Tuple[datetime, str, str, str]],
        led_controller: LEDSignController,
    ):
        self.message_queue = message_queue
        self.led_controller = led_controller
        self.processed_count: int = 0

    async def process_messages(self) -> None:
        """Process queued messages and send to LED sign"""
        logger.info("Message processor started")
        active_queue = False

        while True:
            await self.led_controller.paused_event.wait()
            timestamp, msg_type, sender, text = await self.message_queue.get()
            if not self.led_controller.paused_event.is_set():
                logger.info("Message processor paused while message was in queue; waiting to display...")
                await self.led_controller.paused_event.wait()


            if not active_queue:
                active_queue = True
            logger.info(f"Processing message: {msg_type} from {sender!r} ({len(text)} chars)")
            logger.debug(f"Message queue depth after get: {self.message_queue.qsize()}")

            try:
                # Display message on LED sign
                self.led_controller.display_message(msg_type, sender, text)

                # Calculate and wait for display time
                display_time = self.led_controller.calculate_display_time(text)
                logger.debug(f"Message display time: {display_time:.1f}s")
                await asyncio.sleep(display_time)
                self.processed_count += 1
                logger.debug(f"Messages processed so far: {self.processed_count}")
            except Exception as e:
                logger.error(f"Error processing message: {e}")
                logger.debug(f"Full traceback: {traceback.format_exc()}")
            finally:
                self.message_queue.task_done()
                if self.message_queue.empty():
                    self.led_controller.set_mode("bigClock")
                    active_queue = False


# ---------------------------------------------------------------------------
# Main Application
# ---------------------------------------------------------------------------

class NotificationCollator:
    """Main application class that coordinates all components"""

    def __init__(self):
        self.loop: Optional[asyncio.AbstractEventLoop] = None
        self.message_queue: asyncio.Queue[Tuple[datetime, str, str, str]] = asyncio.Queue()
        self.mqtt_connected = asyncio.Event()
        self.mqtt_client = self._setup_mqtt()
        self.led_controller = LEDSignController(self.mqtt_client, self.mqtt_connected)
        self.signal_handler = SignalHandler(self.message_queue)
        self.calendar_manager = CalendarManager(
            self.mqtt_client, self.led_controller, self.mqtt_connected
        )
        self.message_processor = MessageProcessor(self.message_queue, self.led_controller)
        self._task_restart_counts: Dict[str, int] = {}
        self.latest_elapsed_time: str = "0"


    # -------------------------------------------------------------------------
    # MQTT setup
    # -------------------------------------------------------------------------

    def _setup_mqtt(self) -> mqtt.Client:
        """Setup MQTT client with callbacks"""
        client = mqtt.Client(
            client_id="HASS_messageprocessor",
            callback_api_version=CallbackAPIVersion.VERSION1,
        )
        client.username_pw_set(Config.MQTT_USER, password=Config.MQTT_PASSWORD)

        client.on_connect = self._on_mqtt_connect
        client.on_disconnect = self._on_mqtt_disconnect
        client.on_connect_fail = self._on_mqtt_connect_fail
        client.on_message = self._on_mqtt_message

        return client

    def _set_mqtt_connected(self, value: bool) -> None:
        """Helper to thread-safely set or clear the mqtt_connected event."""
        if self.loop and self.loop.is_running():
            if value:
                self.loop.call_soon_threadsafe(self.mqtt_connected.set)
            else:
                self.loop.call_soon_threadsafe(self.mqtt_connected.clear)
        else:
            if value:
                self.mqtt_connected.set()
            else:
                self.mqtt_connected.clear()

    def _on_mqtt_connect(self, client, userdata, flags, rc):
        """MQTT connection callback.

        Subscriptions are registered here (not just at startup) so they are
        automatically restored after any broker reconnect.
        """
        if rc == 0:
            logger.info("Successfully connected to MQTT broker")
            self._set_mqtt_connected(True)
            # Re-subscribe on every connect — covers both initial connect and reconnects
            try:
                topics = [
                    ("nextTODO/select", 0),
                    ("nextTODO/select/start", 0),
                    ("nextTODO/select/stop", 0),
                    ("nextTODO/select/completed", 0),
                    ("nextTODO/personal/elapsed", 0),
                ]
                client.subscribe(topics)
                logger.info("Subscribed to nextTODO/select and related topics")
            except Exception as e:
                logger.error(f"Failed to subscribe on connect: {e}")

        else:
            logger.error(f"Connected to MQTT broker with error code {rc}")
            self._set_mqtt_connected(False)

    def _on_mqtt_disconnect(self, client, userdata, rc):
        """MQTT disconnection callback"""
        self._set_mqtt_connected(False)
        if rc != 0:
            logger.error(f"Unexpected disconnect from MQTT broker (rc={rc})")
        else:
            logger.info("Gracefully disconnected from MQTT broker")
        logger.info(f"MQTT client will attempt automatic reconnection (unexpected={rc != 0})")

    def _on_mqtt_connect_fail(self, client, userdata):
        """MQTT connection failure callback"""
        self._set_mqtt_connected(False)
        logger.error("Failed to connect to MQTT broker, will retry...")

    def _get_current_todo_info(self) -> Tuple[Optional[str], Optional[str], Optional[str]]:
        mgr = self.calendar_manager
        if not mgr.todo_list or mgr.todo_index >= len(mgr.todo_list):
            return None, None, None
        todo_line = mgr.todo_list[mgr.todo_index]
        todo_parts = todo_line.split(',')
        
        todoist_id = todo_parts[0].strip() if len(todo_parts) > 0 else None
        title = todo_parts[1].strip() if len(todo_parts) > 1 else None
        notes = todo_parts[3].strip() if len(todo_parts) > 3 else None
        return todoist_id, title, notes

    def _get_current_todoist_id(self) -> Optional[str]:
        return self._get_current_todo_info()[0]


    async def _add_todoist_comment(self, task_id: str, content: str) -> bool:
        if not Config.TODOIST_API_TOKEN:
            logger.error("TODOIST_API_TOKEN is not set in environment")
            return False
            
        headers = {
            "Authorization": f"Bearer {Config.TODOIST_API_TOKEN}",
            "Content-Type": "application/json"
        }
        data = {
            "task_id": task_id,
            "content": content
        }
        
        def do_post():
            response = requests.post("https://api.todoist.com/api/v1/comments", headers=headers, json=data, timeout=10)
            response.raise_for_status()
            return response.json()
            
        try:
            await asyncio.to_thread(do_post)
            logger.info(f"Successfully added comment to Todoist task {task_id}")
            return True
        except Exception as e:
            logger.error(f"Error adding comment to Todoist task {task_id}: {e}")
            return False

    async def _complete_todoist_task(self, task_id: str) -> bool:
        if not Config.TODOIST_API_TOKEN:
            logger.error("TODOIST_API_TOKEN is not set in environment")
            return False
            
        headers = {
            "Authorization": f"Bearer {Config.TODOIST_API_TOKEN}"
        }
        
        def do_post():
            response = requests.post(f"https://api.todoist.com/api/v1/tasks/{task_id}/close", headers=headers, timeout=10)
            response.raise_for_status()
            return response.status_code
            
        try:
            await asyncio.to_thread(do_post)
            logger.info(f"Successfully completed Todoist task {task_id}")
            return True
        except Exception as e:
            logger.error(f"Error completing Todoist task {task_id}: {e}")
            return False


    async def _handle_todoist_stop(self, todoist_id: str) -> None:
        elapsed = getattr(self, "latest_elapsed_time", "0")
        current_time_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        comment_content = f"Time spent: {elapsed} (recorded on {current_time_str})"
        
        await self._add_todoist_comment(todoist_id, comment_content)

    async def _handle_todoist_completed(self, todoist_id: str) -> None:
        elapsed = getattr(self, "latest_elapsed_time", "0")
        current_time_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        comment_content = f"Time spent: {elapsed} (recorded on {current_time_str})"
        
        await self._add_todoist_comment(todoist_id, comment_content)
        await self._complete_todoist_task(todoist_id)

    async def _send_devterm_print_command(self) -> None:
        host = Config.DEVTERM_HOST
        user = Config.DEVTERM_USER
        password = Config.DEVTERM_PASSWORD

        if not host or not user or not password:
            logger.warning("DevTerm SSH credentials not fully configured in environment")
            return

        todoist_id, title, notes = self._get_current_todo_info()
        if not title:
            logger.warning("No currently selected todo title to print")
            return

        # Formatting for a 2.25" (58mm) wide thermal paper strip
        # When double-width mode is enabled, line width is halved to 16 characters.
        wrapped_title = textwrap.fill(title, width=16)
        
        if notes and notes.lower() != 'none':
            wrapped_notes = textwrap.fill(notes, width=32)
        else:
            wrapped_notes = "No notes"
            
        time_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # ESC/POS commands
        ESC_BIG = "\x1b\x21\x30"    # Double-height + double-width
        ESC_NORMAL = "\x1b\x21\x00" # Standard font size

        # Construct the print content with inline ESC/POS commands
        print_content = (
            "================================\n"
            f"{ESC_BIG}{wrapped_title}\n{ESC_NORMAL}"
            "--------------------------------\n"
            f"{wrapped_notes}\n"
            "--------------------------------\n"
            f"Printed: {time_str}\n"
            "================================\n"
            "\n" * 6  # Feed spaces so we can tear it off cleanly
        )


        # Base64 encode the print content to safely transmit it without escaping bugs
        encoded_content = base64.b64encode(print_content.encode('utf-8')).decode('utf-8')

        # Shell command to execute on the remote machine
        remote_cmd = f"echo '{encoded_content}' | base64 -d > /tmp/DEVTERM_PRINTER_IN"

        cmd = [
            "sshpass", "-p", password,
            "ssh", "-o", "StrictHostKeyChecking=no", f"{user}@{host}",
            remote_cmd
        ]

        try:
            logger.info(f"Sending DevTerm print command to {user}@{host}...")
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await process.communicate()
            if process.returncode == 0:
                logger.info("Successfully sent print command to DevTerm")
            else:
                logger.error(f"Failed to send print command to DevTerm (code {process.returncode}): {stderr.decode().strip()}")
        except Exception as e:
            logger.error(f"Exception while sending SSH command: {e}")


    def _handle_start(self) -> None:
        logger.info("Pausing text messages/interrupts to the sign.")
        self.led_controller.paused_event.clear()
        if self.loop and self.loop.is_running():
            asyncio.run_coroutine_threadsafe(self._send_devterm_print_command(), self.loop)
        else:
            logger.error("Cannot send DevTerm command: asyncio event loop is not running")


    def _handle_stop(self) -> None:
        logger.info("Resuming all messages to MQTT, adding comment to Todoist.")
        self.led_controller.paused_event.set()
        todoist_id = self._get_current_todoist_id()
        if todoist_id:
            if self.loop and self.loop.is_running():
                asyncio.run_coroutine_threadsafe(self._handle_todoist_stop(todoist_id), self.loop)
            else:
                logger.error("Cannot handle Todoist stop: asyncio event loop is not running")
        else:
            logger.warning("No currently selected todoist ID found to add comment to.")

    def _handle_completed(self) -> None:
        logger.info("Resuming all messages to MQTT, adding comment and completing Todoist task.")
        self.led_controller.paused_event.set()
        todoist_id = self._get_current_todoist_id()
        if todoist_id:
            if self.loop and self.loop.is_running():
                asyncio.run_coroutine_threadsafe(self._handle_todoist_completed(todoist_id), self.loop)
            else:
                logger.error("Cannot handle Todoist completed: asyncio event loop is not running")
        else:
            logger.warning("No currently selected todoist ID found to complete.")


    def _on_mqtt_message(self, client, userdata, msg):
        """General MQTT message handler for subscribed commands"""
        topic = msg.topic
        payload = msg.payload.decode('utf-8', errors='ignore')
        logger.debug(f"Received MQTT message on {topic}: {payload!r}")

        if topic == "nextTODO/select":
            cmd = payload.strip().lower()
            if cmd == "start":
                self._handle_start()
            elif cmd == "stop":
                self._handle_stop()
            elif cmd == "completed":
                self._handle_completed()
            else:
                self.calendar_manager.handle_todo_select(payload)
        elif topic == "nextTODO/select/start":
            self._handle_start()
        elif topic == "nextTODO/select/stop":
            self._handle_stop()
        elif topic == "nextTODO/select/completed":
            self._handle_completed()
        elif topic == "nextTODO/personal/elapsed":
            self.latest_elapsed_time = payload
            logger.debug(f"Updated latest elapsed time: {payload!r}")
        else:
            logger.debug(f"No handler for topic {topic!r}")




    # -------------------------------------------------------------------------
    # Task supervision
    # -------------------------------------------------------------------------

    def _build_task_factories(self) -> Dict[str, Callable]:
        """Return a mapping of task names to their coroutine factory functions."""
        return {
            "signal_monitor":   self.signal_handler.monitor,
            "todos_loop":       self._update_todos_loop,
            "schedule_loop":    self._update_schedule_loop,
            "schedule_monitor": self.calendar_manager.monitor_schedule,
            "message_processor":self.message_processor.process_messages,
            "heartbeat":        self._heartbeat_loop,
            "mqtt_monitor":     self._mqtt_monitor_loop,
        }

    async def _supervised_run(self) -> None:
        """Run all tasks and automatically restart any that die.

        Each failed task is given a TASK_RESTART_DELAY cooldown before it is
        restarted, preventing tight crash loops from flooding the log and CPU.
        The supervisor itself runs indefinitely.
        """
        task_factories = self._build_task_factories()
        self._task_restart_counts = {name: 0 for name in task_factories}

        tasks: Dict[str, asyncio.Task] = {
            name: asyncio.create_task(factory(), name=name)
            for name, factory in task_factories.items()
        }
        logger.info(f"Supervisor started {len(tasks)} tasks: {list(tasks.keys())}")

        while True:
            await asyncio.sleep(Config.TASK_MONITORING_INTERVAL)

            for name in list(tasks.keys()):
                task = tasks[name]
                if not task.done():
                    continue  # still running — nothing to do

                # Task has exited — diagnose and schedule restart
                exc: Optional[BaseException] = None
                try:
                    exc = task.exception()
                except asyncio.CancelledError:
                    logger.info(f"[supervisor] Task '{name}' was cancelled")
                except Exception as inner:
                    logger.error(f"[supervisor] Task '{name}' raised during result check: {inner}")

                if exc is not None:
                    tb = "".join(
                        traceback.format_exception(type(exc), exc, exc.__traceback__)
                    )
                    logger.error(
                        f"[supervisor] Task '{name}' died with "
                        f"{type(exc).__name__}: {exc}\nTraceback:\n{tb}"
                    )
                else:
                    logger.warning(f"[supervisor] Task '{name}' exited without exception")

                self._task_restart_counts[name] += 1
                count = self._task_restart_counts[name]
                logger.info(
                    f"[supervisor] Scheduling restart of '{name}' "
                    f"in {Config.TASK_RESTART_DELAY}s (restart #{count})"
                )

                # Remove from the active dict while restarting; the restart
                # coroutine will put it back once the cooldown has elapsed.
                del tasks[name]
                asyncio.create_task(
                    self._restart_task_after_delay(name, task_factories[name], tasks),
                    name=f"_restart_{name}",
                )

    async def _restart_task_after_delay(
        self,
        name: str,
        factory: Callable,
        tasks: Dict[str, asyncio.Task],
    ) -> None:
        """Wait for the restart cooldown then re-create the named task."""
        await asyncio.sleep(Config.TASK_RESTART_DELAY)
        logger.info(f"[supervisor] Restarting task '{name}'")
        tasks[name] = asyncio.create_task(factory(), name=name)

    # -------------------------------------------------------------------------
    # Periodic loops
    # -------------------------------------------------------------------------

    async def _update_todos_loop(self) -> None:
        """Periodic TODO update loop.  Runs the update first, then sleeps."""
        logger.info("TODO update loop started")
        while True:
            try:
                await self.calendar_manager.update_todos()
            except Exception as e:
                logger.error(f"Exception in todos update loop: {e}")
                logger.debug(f"Full traceback: {traceback.format_exc()}")
            await asyncio.sleep(Config.TODO_UPDATE_INTERVAL)

    async def _update_schedule_loop(self) -> None:
        """Periodic schedule update loop.  Runs the update first, then sleeps."""
        logger.info("Schedule update loop started")
        while True:
            try:
                await self.calendar_manager.update_schedule()
            except Exception as e:
                logger.error(f"Exception in schedule update loop: {e}")
                logger.debug(f"Full traceback: {traceback.format_exc()}")
            await asyncio.sleep(Config.SCHEDULE_UPDATE_INTERVAL)

    async def _heartbeat_loop(self) -> None:
        """Emit a periodic status summary for observability.

        Log line prefix is ``[HEARTBEAT]`` so it can be grepped easily.
        """
        logger.info("Heartbeat loop started")
        while True:
            await asyncio.sleep(Config.HEARTBEAT_INTERVAL)
            try:
                next_evt_ts = self.calendar_manager.next_event.get('eventStart')
                next_evt_subject = self.calendar_manager.next_event.get('eventSubject', 'none')
                if next_evt_ts:
                    try:
                        nxt = self.calendar_manager._from_timestamp(next_evt_ts)
                        secs = (nxt - self.calendar_manager._now()).total_seconds()
                        next_evt_str = f"{next_evt_subject!r} in {secs:.0f}s"
                    except Exception:
                        next_evt_str = f"{next_evt_subject!r} (ts error)"
                else:
                    next_evt_str = "none"

                last_signal = (
                    self.signal_handler.last_message_time.strftime("%H:%M:%S")
                    if self.signal_handler.last_message_time
                    else "never"
                )
                last_sched = (
                    self.calendar_manager.last_schedule_update.strftime("%H:%M:%S")
                    if self.calendar_manager.last_schedule_update
                    else "never"
                )
                last_todo = (
                    self.calendar_manager.last_todo_update.strftime("%H:%M:%S")
                    if self.calendar_manager.last_todo_update
                    else "never"
                )
                dbus_ok = getattr(self.signal_handler.bus, 'connected', False)

                logger.info(
                    f"[HEARTBEAT] "
                    f"mqtt={'OK' if self.mqtt_connected.is_set() else 'DISCONNECTED'} | "
                    f"dbus={'OK' if dbus_ok else 'DISCONNECTED'} | "
                    f"led={self.led_controller.current_mode} | "
                    f"queue={self.message_queue.qsize()} | "
                    f"msgs_processed={self.message_processor.processed_count} | "
                    f"next_event={next_evt_str} | "
                    f"todos={len(self.calendar_manager.todo_list)} | "
                    f"last_signal={last_signal} | "
                    f"last_sched_update={last_sched} | "
                    f"last_todo_update={last_todo} | "
                    f"task_restarts={self._task_restart_counts}"
                )
            except Exception as e:
                logger.error(f"Error in heartbeat: {e}")

    async def _mqtt_monitor_loop(self) -> None:
        """Monitor MQTT connection and force reconnection if it stays disconnected."""
        logger.info("MQTT monitor loop started")
        while True:
            await asyncio.sleep(Config.MQTT_RECONNECT_DELAY)

            # Check connection status using the client's internal status
            is_connected = False
            try:
                is_connected = self.mqtt_client.is_connected()
            except Exception as e:
                logger.error(f"Error checking MQTT connection status: {e}")

            if not is_connected:
                # If the library thinks it's not connected, ensure our event is cleared
                if self.mqtt_connected.is_set():
                    logger.warning("MQTT client reporting disconnected but event was set. Clearing event.")
                    self._set_mqtt_connected(False)

                logger.warning("MQTT monitor detected client is disconnected. Attempting manual reconnect...")
                try:
                    # Run the blocking reconnect call in a thread pool so we don't block the asyncio event loop
                    await asyncio.to_thread(self.mqtt_client.reconnect)
                    logger.info("MQTT manual reconnect call initiated successfully")
                except Exception as e:
                    logger.error(f"MQTT manual reconnect attempt failed: {e}")
            else:
                # Connected! Ensure the event is set
                if not self.mqtt_connected.is_set():
                    logger.info("MQTT client is connected but event was not set. Setting event.")
                    self._set_mqtt_connected(True)

    # -------------------------------------------------------------------------
    # Application entry point
    # -------------------------------------------------------------------------

    async def run(self) -> None:
        """Run the notification collator"""
        self.loop = asyncio.get_running_loop()
        logger.info("Starting Notification Collator")

        try:
            # Connect to MQTT
            logger.info(f"Connecting to MQTT broker at {Config.MQTT_BROKER}:1883")
            try:
                if not Config.MQTT_BROKER:
                    raise RuntimeError('MQTT broker host is not configured')
                self.mqtt_client.reconnect_delay_set(min_delay=1, max_delay=30)
                self.mqtt_client.connect_async(Config.MQTT_BROKER, 1883)
                self.mqtt_client.loop_start()
                logger.info("MQTT client loop started (connecting asynchronously)")
            except Exception as e:
                logger.error(f"MQTT connection setup failed: {e}")
                # Continue — DBus and other components can still operate

            # Reset LED sign to clock mode
            self.led_controller.set_mode("bigClock")

            # Run all tasks under the supervisor (never returns unless a fatal error escapes)
            await self._supervised_run()

        except KeyboardInterrupt:
            logger.info("Shutting down on keyboard interrupt")
        except Exception as e:
            logger.error(f"Fatal error in main loop: {e}")
            logger.debug(f"Full traceback: {traceback.format_exc()}")
            raise


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    """Main entry point"""
    logger.info("=" * 80)
    logger.info("Notification Collator starting...")
    logger.info("=" * 80)

    # Check critical environment variables (MQTT is required)
    critical_vars = {
        'MQTT_BROKER':   Config.MQTT_BROKER,
        'MQTT_USER':     Config.MQTT_USER,
        'MQTT_PASSWORD': Config.MQTT_PASSWORD,
    }

    missing_critical = [var for var, value in critical_vars.items() if not value]
    if missing_critical:
        logger.error(f"Missing critical environment variables: {', '.join(missing_critical)}")
        logger.error("Please ensure MQTT variables are set in your .env file")
        sys.exit(1)

    # Check optional environment variables
    if not Config.CAL_SCRAPER_HOST:
        logger.warning("CAL_SCRAPER_HOST not set — calendar features will be disabled")

    logger.info("Configuration loaded:")
    logger.info(f"  MQTT_BROKER:           {Config.MQTT_BROKER}")
    logger.info(f"  CAL_SCRAPER_HOST:      {Config.CAL_SCRAPER_HOST or 'disabled'}")
    logger.info(f"  TIMEZONE:              {Config.TIMEZONE}")
    logger.info(f"  HTTP_TIMEOUT:          {Config.HTTP_REQUEST_TIMEOUT}s")
    logger.info(f"  DBUS_RETRIES:          {Config.DBUS_MAX_RETRIES}")
    logger.info(f"  DBUS_HEALTH_CHECK:     {Config.DBUS_HEALTH_CHECK_INTERVAL}s")
    logger.info(f"  TASK_RESTART_DELAY:    {Config.TASK_RESTART_DELAY}s")
    logger.info(f"  TODO_UPDATE_INTERVAL:  {Config.TODO_UPDATE_INTERVAL}s")
    logger.info(f"  SCHED_UPDATE_INTERVAL: {Config.SCHEDULE_UPDATE_INTERVAL}s")
    logger.info(f"  HEARTBEAT_INTERVAL:    {Config.HEARTBEAT_INTERVAL}s")

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