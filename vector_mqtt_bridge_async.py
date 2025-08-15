import configparser
import json
import logging
import os
import time
import pickle
import datetime
import asyncio
import inspect
import signal
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, Any, Tuple, Callable, Optional, List, Union
from enum import Enum
import ssl
import colorsys

import paho.mqtt.client as mqtt
from anki_vector import Robot
from anki_vector.events import Events
from anki_vector.util import degrees, distance_mm, speed_mmps
import anki_vector.messaging.protocol as protocol
import anki_vector.animation
from anki_vector import exceptions as vec_exc

# === ROBUST LOGGING SETUP ===
from logging.handlers import RotatingFileHandler

LOG_LEVEL = os.getenv("VECTOR_BRIDGE_LOG_LEVEL", "INFO").upper()

logger = logging.getLogger("VectorBridge")
logger.setLevel(LOG_LEVEL)

fmt = logging.Formatter('%(asctime)s - %(levelname)s - %(name)s - %(message)s')

fh = RotatingFileHandler("vector_bridge.log", maxBytes=1_000_000, backupCount=5)  # ~1MB x 5
fh.setFormatter(fmt)
fh.setLevel(LOG_LEVEL)

sh = logging.StreamHandler()
sh.setFormatter(fmt)
sh.setLevel(LOG_LEVEL)

logger.handlers.clear()
logger.addHandler(fh)
logger.addHandler(sh)

# === COMMAND PRIORITY ENUM ===
class CommandPriority(Enum):
    HIGH = 0    # Critical commands (shutdown, emergency stop)
    MEDIUM = 1  # Normal commands
    LOW = 2     # Background tasks, non-essential commands


# === MAIN BRIDGE CLASS ===
class AsyncVectorMQTTBridge:

# --- Initialization and Core Lifecycle ---

    def __init__(self, config):
        """Initialize Vector MQTT Bridge with config, topics, settings, state, MQTT client, and handlers."""
        # -------------------------------------------------
        # A) Config handle and core identity
        # -------------------------------------------------
        self.VERSION = "0.3.0"
        self.config = config

        # Vector identity
        self.VECTOR_SERIAL = config.get('Vector', 'serial')
        self.VECTOR_IP = config.get('Vector', 'ip')
        self.VECTOR_NAME = config.get('Vector', 'name')
        self.VECTOR_CERT_PATH = os.path.expanduser(
            f"~/.anki_vector/Vector-{self.VECTOR_NAME}-{self.VECTOR_SERIAL}.cert"
        )

        # Files
        self.ANIMATIONS_FILE = config.get('Files', 'animation_data_pkl')
        self.PERSISTENCE_FILE = config.get('Files', 'state_json', fallback='bridge_state.json')

        # MQTT broker
        self.MQTT_BROKER_HOST = config.get('MQTT', 'host')
        self.MQTT_BROKER_PORT = config.getint('MQTT', 'port')
        self.MQTT_BROKER_USERNAME = config.get('MQTT', 'username', fallback=None)
        self.MQTT_BROKER_PASSWORD = config.get('MQTT', 'password', fallback=None)

        # Topic base (recommend `vector/<serial>` if you add more robots in the future)
        self.TOPIC_BASE = config.get('MQTT', 'topic_base', fallback=f"vector/{self.VECTOR_SERIAL}")

        # -------------------------------------------------
        # B) Global settings and timeouts
        # -------------------------------------------------
        # Core behavior
        self.AUTO_CONNECT = config.getboolean('Settings', 'auto_connect', fallback=True)
        self.AUTO_RECONNECT = config.getboolean('Settings', 'auto_reconnect', fallback=True)
        self.STATE_PERSISTENCE = config.getboolean('Settings', 'state_persistence', fallback=True)

        # Cadence
        self.COMMAND_TIMEOUT = config.getint('Settings', 'command_timeout', fallback=10)
        self.TELEMETRY_INTERVAL = config.getint('Settings', 'telemetry_interval', fallback=30)
        self.HEARTBEAT_INTERVAL = config.getint('Settings', 'heartbeat_interval', fallback=10)

        # Telemetry/events
        self.TELEMETRY_LEVEL = config.get('Settings', 'telemetry_level', fallback='full')
        self.EVENT_THROTTLING = config.getboolean('Settings', 'event_throttling', fallback=True)
        self.EVENT_QOS = config.getint('Settings', 'event_qos', fallback=0)
        self.TELEMETRY_QOS = config.getint('Settings', 'telemetry_qos', fallback=0)
        self.CMD_COMPLETE_QOS = config.getint('Settings', 'command_complete_qos', fallback=0)

        # Behavior publishing mode
        self.BEHAVIOR_PUB_MODE = config.get('Settings', 'behavior_pub_mode', fallback='explicit').lower()

        # Camera settings
        self.CAMERA_FRAME_WAIT = config.getint('Settings', 'camera_frame_wait_sec', fallback=7)
        self.CAMERA_MAX_WIDTH = config.getint('Settings', 'camera_max_width', fallback=0)
        self.CAMERA_JPEG_QUALITY = config.getint('Settings', 'camera_jpeg_quality', fallback=85)

        # Charger robustness
        self.CHARGER_RETRIES = config.getint('Settings', 'charger_retries', fallback=2)
        self.CHARGER_RETRY_DELAY = config.getint('Settings', 'charger_retry_delay', fallback=3)

        # Queues/backpressure
        self.QUEUE_MAX = config.getint('Settings', 'queue_max', fallback=200)

        # Reconnect
        self.MAX_RECONNECT_ATTEMPTS = config.getint('Settings', 'max_reconnect_attempts', fallback=3)
        self.RECONNECT_DELAY = config.getint('Settings', 'reconnect_delay', fallback=5)

        # Home Assistant
        self.HA_DISCOVERY = config.getboolean('HomeAssistant', 'discovery', fallback=True)
        self.HA_DISCOVERY_PREFIX = config.get('HomeAssistant', 'discovery_prefix', fallback='homeassistant')

        # Per-command timeouts
        self.COMMAND_TIMEOUTS = {
            'default': config.getint('Timeouts', 'default', fallback=10),
            'drive': config.getint('Timeouts', 'drive', fallback=15),
            'animation': config.getint('Timeouts', 'animation', fallback=20),
            'charger': config.getint('Timeouts', 'charger', fallback=30),
            'takephoto': config.getint('Timeouts', 'takephoto', fallback=20),
            'look_at_me': config.getint('Timeouts', 'look_at_me', fallback=25),
        }

        # -------------------------------------------------
        # C) Topic map (commands, status, events)
        # -------------------------------------------------
        self.COMMAND_BASE = f"{self.TOPIC_BASE}/command"
        self.STATUS_BASE = f"{self.TOPIC_BASE}/status"
        self.EVENT_BASE = f"{self.TOPIC_BASE}/event"

        # Command topics
        self.COMMAND_TOPIC_LIFECYCLE = f"{self.COMMAND_BASE}/lifecycle"
        self.COMMAND_TOPIC_BEHAVIOR = f"{self.COMMAND_BASE}/behavior"
        self.COMMAND_TOPIC_SAY = f"{self.COMMAND_BASE}/say"
        self.COMMAND_TOPIC_DRIVE = f"{self.COMMAND_BASE}/drive"
        self.COMMAND_TOPIC_ROTATE = f"{self.COMMAND_BASE}/rotate"
        self.COMMAND_TOPIC_LIFT = f"{self.COMMAND_BASE}/lift"
        self.COMMAND_TOPIC_HEAD = f"{self.COMMAND_BASE}/head"
        self.COMMAND_TOPIC_EYECOLOR = f"{self.COMMAND_BASE}/eye_color"
        self.COMMAND_TOPIC_CHARGER = f"{self.COMMAND_BASE}/charger"
        self.COMMAND_TOPIC_TAKEPHOTO = f"{self.COMMAND_BASE}/takephoto"
        self.COMMAND_TOPIC_LOOK_AT_ME = f"{self.COMMAND_BASE}/look_at_me"
        self.COMMAND_TOPIC_GETSTATUS = f"{self.COMMAND_BASE}/getstatus"
        self.COMMAND_TOPIC_SLEEP = f"{self.COMMAND_BASE}/sleep"
        self.COMMAND_TOPIC_SHUTUP = f"{self.COMMAND_BASE}/shutup"
        self.COMMAND_TOPIC_DRIVE_CONTINUOUS = f"{self.COMMAND_BASE}/drive_continuous"
        self.COMMAND_TOPIC_ROTATE_CONTINUOUS = f"{self.COMMAND_BASE}/rotate_continuous"
        self.COMMAND_TOPIC_STOP_MOVEMENT = f"{self.COMMAND_BASE}/stop_movement"
        self.COMMAND_TOPIC_ANIMATION = f"{self.COMMAND_BASE}/animation"
        self.COMMAND_TOPIC_TRIGGER = f"{self.COMMAND_BASE}/trigger"
        self.COMMAND_TOPIC_FISTBUMP = f"{self.COMMAND_BASE}/fistbump"
        self.COMMAND_TOPIC_CUBE_LIGHTS = f"{self.COMMAND_BASE}/cube_lights"
        # Optional/extra
        self.COMMAND_TOPIC_SEQUENCE = f"{self.COMMAND_BASE}/sequence"
        self.COMMAND_TOPIC_MAPPING = f"{self.COMMAND_BASE}/mapping"
        self.COMMAND_TOPIC_SET_TRIGGER = f"{self.COMMAND_BASE}/set_trigger"
        self.COMMAND_TOPIC_MACRO = f"{self.COMMAND_BASE}/macro"
        self.COMMAND_TOPIC_NODERED_STATUS = f"{self.COMMAND_BASE}/nodered_status"

        # Status topics
        self.STATUS_TOPIC_COMMAND_COMPLETE = f"{self.STATUS_BASE}/command_complete"
        self.STATUS_TOPIC_BATTERY = f"{self.STATUS_BASE}/battery"
        self.STATUS_TOPIC_LIFECYCLE = f"{self.STATUS_BASE}/lifecycle"
        self.STATUS_TOPIC_BEHAVIOR = f"{self.STATUS_BASE}/behavior"
        self.STATUS_TOPIC_TELEMETRY = f"{self.STATUS_BASE}/telemetry"
        self.STATUS_TOPIC_HEARTBEAT = f"{self.STATUS_BASE}/heartbeat"
        self.STATUS_TOPIC_BRIDGE_STATUS = f"{self.STATUS_BASE}/bridge_status"
        self.STATUS_TOPIC_ERROR_STATUS = f"{self.STATUS_BASE}/error_status"
        self.STATUS_TOPIC_ACTIVITY = f"{self.STATUS_BASE}/activity"

        # Event topics
        self.EVENT_TOPIC_FACE_DETECTED = f"{self.EVENT_BASE}/face_detected"
        self.EVENT_TOPIC_TOUCH = f"{self.EVENT_BASE}/touch"
        self.EVENT_TOPIC_VOICE_COMMAND = f"{self.EVENT_BASE}/voice_command"
        self.EVENT_TOPIC_CUBE_CONNECTION = f"{self.EVENT_BASE}/cube_connection"

        # -------------------------------------------------
        # D) Runtime state and flags
        # -------------------------------------------------
        self.robot = None
        self.is_vector_connected = False
        self.has_behavior_control = False

        # Tasks
        self.monitor_task = None
        self.heartbeat_task = None
        self.telemetry_task = None
        self.reconnect_task = None
        self.command_processor_task = None

        # Misc state
        self.last_battery_level = None
        self.last_event_times = {}          # throttling
        self.event_triggers = {}            # event→action
        self.robot_activity = {
            "is_active": False,
            "activity_type": None,
            "last_activity_change": 0,
            "last_published": 0,
            "pending_change": False
        }
        self._start_time = time.time()
        self._last_error = None
        self.last_error_message = None
        self.shutting_down = False
        
        # Thread pool for blocking SDK calls
        self.thread_pool = ThreadPoolExecutor(max_workers=4)
        
        # In-memory image (for MQTT/HTTP if you add it)
        self.latest_jpeg: Optional[bytes] = None
        self.latest_jpeg_ts: float = 0.0

        # Face detection helpers (event-driven look_at_me)
        self._face_seen_event = asyncio.Event()
        self._last_seen_face = None

        # Locks & queues
        self.sdk_lock = asyncio.Lock()
        self.command_queues = {
            CommandPriority.HIGH: asyncio.Queue(maxsize=self.QUEUE_MAX),
            CommandPriority.MEDIUM: asyncio.Queue(maxsize=self.QUEUE_MAX),
            CommandPriority.LOW: asyncio.Queue(maxsize=int(self.QUEUE_MAX / 2)),
        }

        # -------------------------------------------------
        # E) MQTT client wiring (callbacks set here; connect in run())
        # -------------------------------------------------
        self.mqtt_client = mqtt.Client()
        self.mqtt_client.reconnect_delay_set(min_delay=1, max_delay=30)
        self.mqtt_client.max_queued_messages_set(1000)
        self.mqtt_client.max_inflight_messages_set(20)
        self.mqtt_client.on_connect = self.on_mqtt_connect
        self.mqtt_client.on_message = self.on_mqtt_message
        self.mqtt_client.on_disconnect = self.on_mqtt_disconnect

        # -------------------------------------------------
        # F) Command handler registry
        # -------------------------------------------------
        self.command_handlers = {
            "say": VectorCommands.handle_say,
            "drive": VectorCommands.handle_drive,
            "rotate": VectorCommands.handle_rotate,
            "lift": VectorCommands.handle_lift,
            "head": VectorCommands.handle_head,
            "eye_color": VectorCommands.handle_eye_color,
            "charger": VectorCommands.handle_charger,
            "look_at_me": VectorCommands.handle_look_at_me,
            "sleep": VectorCommands.handle_sleep,
            "shutup": VectorCommands.handle_shutup,
            "takephoto": VectorCommands.handle_takephoto,
            "getstatus": VectorCommands.handle_getstatus,
            "animation": VectorCommands.handle_animation,
            "trigger": VectorCommands.handle_trigger,
            "cube_lights": VectorCommands.handle_cube_lights,
            "drive_continuous": VectorCommands.handle_drive_continuous,
            "rotate_continuous": VectorCommands.handle_rotate_continuous,
            "stop_movement": VectorCommands.handle_stop_movement,
            "sequence": VectorCommands.handle_sequence,
            "mapping": VectorCommands.handle_mapping,
            "set_trigger": VectorCommands.handle_set_trigger,
            "macro": VectorCommands.handle_macro,
            "nodered_status": VectorCommands.handle_nodered_status
        }

        # -------------------------------------------------
        # G) State persistence (load after everything initialized)
        # -------------------------------------------------
        if self.STATE_PERSISTENCE:
            self.load_state()
        
    async def run(self):
        """Starts the MQTT client and runs the main async loop."""
        logger.info("--- Vector MQTT Bridge Starting ---")
        
        # Store the event loop reference
        self.loop = asyncio.get_running_loop()
        
        # Set up signal handlers for graceful shutdown
        for sig in (signal.SIGINT, signal.SIGTERM):
            self.loop.add_signal_handler(
                sig, lambda: asyncio.create_task(self.shutdown()))
        
        if self.MQTT_BROKER_USERNAME:
            self.mqtt_client.username_pw_set(self.MQTT_BROKER_USERNAME, self.MQTT_BROKER_PASSWORD)

        # Optional TLS. Set MQTT.tls=true in config.ini to enable
        if self.config.getboolean('MQTT', 'tls', fallback=False):
            # If you have a custom CA, set MQTT.ca_cert=/path/ca.crt in config.ini
            ca_cert = self.config.get('MQTT', 'ca_cert', fallback=None)
            if ca_cert and os.path.exists(ca_cert):
                self.mqtt_client.tls_set(ca_certs=ca_cert, tls_version=ssl.PROTOCOL_TLS_CLIENT)
            else:
                # Use system CAs
                self.mqtt_client.tls_set(tls_version=ssl.PROTOCOL_TLS_CLIENT)

        # Single Last Will (bridge availability JSON)
        bridge_offline = json.dumps({
            "status": "offline",
            "timestamp": datetime.datetime.now().isoformat(),
            "uptime": 0
        })
        self.mqtt_client.will_set(self.STATUS_TOPIC_BRIDGE_STATUS, payload=bridge_offline, qos=1, retain=True)

        try:
            logger.info(f"Connecting to MQTT Broker at {self.MQTT_BROKER_HOST}...")
            self.mqtt_client.connect(self.MQTT_BROKER_HOST, self.MQTT_BROKER_PORT, 60)
            self.mqtt_client.loop_start()  # Start MQTT in a separate thread
            
            # Publish initial bridge status
            self.publish_bridge_status("online")
            self.publish_error_status(force_clear=True)  # Clear any previous errors
            
            # Start the command processor
            self.command_processor_task = asyncio.create_task(self.command_processor())
            
            # Start the heartbeat task
            self.heartbeat_task = asyncio.create_task(self.send_heartbeat())
            
            # Keep the main task running
            while not self.shutting_down:
                await asyncio.sleep(1)
                
        except Exception as e:
            logger.critical(f"FATAL: Could not connect to MQTT Broker: {e}")
            self.publish_error_status(f"MQTT connection failed: {e}", "connection", True)
        finally:
            await self.shutdown()

    async def shutdown(self):
        """Gracefully shuts down the bridge."""
        if self.shutting_down:
            logger.warning("Shutdown already in progress")
            return

        self.shutting_down = True
        logger.info("--- Vector MQTT Bridge Shutting Down ---")

        # Publish shutdown status first
        self.publish_bridge_status("shutdown")

        # Save state if enabled
        if self.STATE_PERSISTENCE:
            self.save_state()

        # Cancel all running tasks
        tasks = []
        for task_attr in ['command_processor_task', 'monitor_task', 'heartbeat_task',
                          'telemetry_task', 'reconnect_task', 'continuous_movement_task']:
            task = getattr(self, task_attr, None)
            if task:
                if not task.done():
                    task.cancel()
                tasks.append(task)

        # Wait for all tasks to complete
        if tasks:
            try:
                await asyncio.wait([t for t in tasks if t is not None], timeout=2.0, return_when=asyncio.ALL_COMPLETED)
            except Exception:
                pass

        # Disconnect from Vector
        if self.is_vector_connected:
            try:
                await self.disconnect_from_vector()
            except Exception as e:
                logger.error(f"Error disconnecting Vector during shutdown: {e}")

        # Final MQTT messages and disconnect
        try:
            self.mqtt_client.publish(self.STATUS_TOPIC_LIFECYCLE, "offline", qos=1, retain=True)
            self.publish_bridge_status("offline")
            self.mqtt_client.loop_stop()
            self.mqtt_client.disconnect()
        except Exception as e:
            logger.error(f"Error during MQTT shutdown: {e}")

        # Shut down thread pool
        try:
            self.thread_pool.shutdown(wait=False)
        except Exception as e:
            logger.error(f"Error shutting down thread pool: {e}")

        logger.info("Shutdown complete")

# --- Vector Connection Management ---

    async def connect_to_vector(self):
        """Establishes connection with the Vector robot using ThreadPoolExecutor."""
        logger.info("Attempting to connect to Vector...")
        try:
            # Clear any previous connection errors
            # self.publish_error_status(force_clear=True)
            
            # Step 1: Create robot instance with synchronous Robot class
            self.robot = Robot(
                serial=self.VECTOR_SERIAL,
                behavior_control_level=None,
                cache_animation_lists=False
            )

            # Step 2: Connect in a thread pool to avoid blocking the event loop
            await self.loop.run_in_executor(self.thread_pool, self.robot.connect)
            
            # Step 3: Perform the "Mock Injection" from your saved data file
            logger.info("Injecting animation data from local animation data file...")
            try:
                # Load the data from your pickle file
                with open(self.ANIMATIONS_FILE, 'rb') as f:
                    loaded_data = pickle.load(f)

                # --- The Surgery: We manually recreate the SDK's internal state ---
                # 1. Set the private lookup dictionaries
                self.robot.anim._anim_trigger_dict = loaded_data['triggers']
                self.robot.anim._anim_dict = loaded_data['animations']
                
                # 2. Also set the public lists for completeness
                self.robot.anim.anim_trigger_list = list(loaded_data['triggers'].keys())
                self.robot.anim.anim_list = list(loaded_data['animations'].keys())
                
                logger.info(f"--> Manual data injected successfully ({len(self.robot.anim.anim_trigger_list)} triggers, {len(self.robot.anim.anim_list)} animations).")
            except Exception as e:
                logger.warning(f"--> WARNING: Could not inject animation data. Is '{self.ANIMATIONS_FILE}' present? Error: {e}")
                      
            # Step 4: Finalize the connection process
            self.is_vector_connected = True            
            self.has_behavior_control = False  # Start in autonomous mode
            
            self.publish_error_status(force_clear=True)            
            logger.info("Successfully connected to Vector. He is currently AUTONOMOUS.")
            
            self.mqtt_client.publish(self.STATUS_TOPIC_LIFECYCLE, "online_robot", qos=1, retain=True)
            self.mqtt_client.publish(self.STATUS_TOPIC_BEHAVIOR, "autonomous", qos=1, retain=True)

            # Subscribe to the events we want to receive
            logger.info("Subscribing to Vector events...")
            self.robot.events.subscribe(
                self.on_user_intent,
                Events.user_intent
            )
            self.robot.events.subscribe(
                self.on_face_detected,
                Events.robot_observed_face
            )
            self.robot.events.subscribe(
                self.on_robot_state,
                Events.robot_state
            )
            
            # Subscribe to cube events if available
            try:
                self.robot.events.subscribe(self.on_cube_tapped, Events.cube_tapped)
                logger.info("Subscribed to cube_tapped")
            except Exception as e:
                logger.debug(f"cube_tapped events not available: {e}")

            try:
                self.robot.events.subscribe(self.on_cube_connected, Events.cube_connected)
                self.robot.events.subscribe(self.on_cube_disconnected, Events.cube_disconnected)
                logger.info("Subscribed to cube connection events")
            except Exception as e:
                logger.debug(f"cube connection events not available: {e}")
            
            logger.info("Subscription successful. Ready for MQTT commands.")
            
            # Start the status monitoring task
            self.monitor_task = asyncio.create_task(self.monitor_vector_status())
            
            # Start the telemetry task
            self.telemetry_task = asyncio.create_task(self.send_telemetry())
            
            # Cancel any pending reconnect task
            if self.reconnect_task and not self.reconnect_task.done():
                self.reconnect_task.cancel()
                self.reconnect_task = None

        except Exception as e:
            logger.error(f"ERROR during connection: {e}")
            self.is_vector_connected = False
            self.mqtt_client.publish(self.STATUS_TOPIC_LIFECYCLE, "offline_robot", qos=1, retain=True)
            
            # Publish more detailed error info
            # error_details = {
                # "error": str(e),
                # "timestamp": datetime.datetime.now().isoformat(),
                # "retry_scheduled": self.AUTO_RECONNECT
            # }
            # self.mqtt_client.publish(f"{self.STATUS_BASE}/error", json.dumps(error_details), qos=1)
            self.publish_error_status(
                error_message=f"Connection failed: Unable to connect to Vector",
                error_type="connection",
                critical=True
            )
            
            # Schedule reconnection if enabled
            if self.AUTO_RECONNECT and not self.shutting_down:
                # Only schedule reconnection if not already reconnecting
                if not self.reconnect_task or self.reconnect_task.done():
                    max_attempts = self.config.getint('Settings', 'max_reconnect_attempts', fallback=3)
                    delay = self.config.getint('Settings', 'reconnect_delay', fallback=5)
                    self.reconnect_task = asyncio.create_task(self.reconnect_vector(max_attempts, delay))

    async def disconnect_from_vector(self):
        """Disconnects from the Vector robot."""
        logger.info("Disconnecting from Vector...")
        
        # Cancel monitoring tasks
        tasks = [self.monitor_task, self.telemetry_task]
        for task in tasks:
            if task:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass  # Expected
        
        # Best-effort unsubscribe (SDK usually clears on disconnect, but be safe)
        if self.robot:
            try:
                self.robot.events.unsubscribe(self.on_user_intent, Events.user_intent)
                self.robot.events.unsubscribe(self.on_face_detected, Events.robot_observed_face)
                self.robot.events.unsubscribe(self.on_robot_state, Events.robot_state)

                self.robot.events.unsubscribe(self.on_cube_tapped, Events.cube_tapped)
                self.robot.events.unsubscribe(self.on_cube_connected, Events.cube_connected)
                self.robot.events.unsubscribe(self.on_cube_disconnected, Events.cube_disconnected)
            except Exception:
                pass
 
        # Clean up camera resources
        await self.cleanup_camera()
        
        # Disconnect from the robot using thread pool to avoid blocking
        if self.robot:
            try:
                await self.loop.run_in_executor(self.thread_pool, self.robot.disconnect)
            except Exception as e:
                logger.error(f"Error during disconnect: {e}")
            
        self.is_vector_connected = False
        self.robot = None
        self.mqtt_client.publish(self.STATUS_TOPIC_LIFECYCLE, "offline_robot", qos=1, retain=True)
        logger.info("Disconnected from Vector.")

    async def reconnect_vector(self, max_attempts=3, delay=5):
        """Attempts to reconnect to Vector after connection failure with exponential backoff."""
        base_delay = delay
        for attempt in range(1, max_attempts + 1):
            if self.shutting_down:
                logger.info("Reconnection cancelled - shutdown in progress")
                return False

            logger.info(f"Attempting to reconnect to Vector (attempt {attempt}/{max_attempts})...")
            try:
                await self.connect_to_vector()
                if self.is_vector_connected:
                    logger.info("Reconnection successful")
                    return True
            except Exception as e:
                logger.error(f"Reconnection attempt {attempt} failed: {e}")

            if attempt < max_attempts and not self.shutting_down:
                sleep_for = min(base_delay * (2 ** (attempt - 1)), 60)
                logger.info(f"Waiting {sleep_for} seconds before next reconnection attempt...")
                await asyncio.sleep(sleep_for)

        if not self.is_vector_connected:
            logger.error(f"Failed to reconnect after {max_attempts} attempts")
            self.mqtt_client.publish(self.STATUS_TOPIC_LIFECYCLE, "offline_reconnect_failed", qos=1, retain=True)
            self.publish_error_status(
                error_message=f"Vector connection failed after {max_attempts} attempts",
                error_type="connection",
                critical=True
            )
            self.reconnect_task = None

        return False

    async def cleanup_camera(self):
        """Cleans up camera resources."""
        if self.robot and hasattr(self.robot, 'camera'):
            try:
                await self.loop.run_in_executor(self.thread_pool, self.robot.camera.close_camera_feed)
                logger.info("Camera feed closed successfully")
            except Exception as e:
                logger.error(f"Error closing camera feed: {e}")

    async def cleanup_camera_with_timeout(self, timeout: float = 3.0):
        """Close camera feed without blocking the command forever."""
        try:
            await asyncio.wait_for(self.cleanup_camera(), timeout=timeout)
        except asyncio.TimeoutError:
            logger.warning(f"Camera cleanup exceeded {timeout}s; continuing in background.")

# --- MQTT Interface ---

    def on_mqtt_connect(self, client, userdata, flags, rc):
        """Callback for when the client connects to the MQTT broker."""
        if rc == 0:
            logger.info("Connected to MQTT Broker successfully.")
            client.publish(self.STATUS_TOPIC_LIFECYCLE, "online_bridge", qos=1, retain=True)
                      
            client.subscribe(f"{self.COMMAND_BASE}/#", 1)
            logger.info("Subscribed to all command topics via wildcard.")
            
            # Publish HA discovery once
            try:
                self.publish_ha_discovery()
            except Exception as e:
                logger.warning(f"HA discovery error: {e}")            
            
            # Auto-connect to Vector if enabled
            if self.AUTO_CONNECT and not self.is_vector_connected:
                logger.info("Auto-connect enabled, initiating connection to Vector...")
                # We need to run this in the event loop
                asyncio.run_coroutine_threadsafe(self.connect_to_vector(), self.loop)
        else:
            logger.error(f"Failed to connect to MQTT, return code {rc}")

    def on_mqtt_disconnect(self, client, userdata, rc):
        """Callback for when the client disconnects from the MQTT broker."""
        if rc != 0:
            logger.warning(f"Unexpected MQTT disconnection (code {rc}). Will auto-reconnect.")

    def on_mqtt_message(self, client, userdata, msg):
        """Callback for when a message is received from a subscribed topic."""
        original_payload = msg.payload.decode()
        logger.debug(f"MQTT Message Received - Topic: {msg.topic}, Payload: '{original_payload}'")

        # Extract command name from topic
        parts = msg.topic.split('/')
        cmd = parts[-1].strip().lower()

        # Aliases: map common names to canonical commands and payload transforms
        def _default_photo(_p: str) -> str:
            return _p.strip() or "/root/.anki_vector/vector_photos/ha_latest.jpg"

        aliases = {
            "dock": ("charger", "on"),
            "charge": ("charger", "on"),
            "go_to_charger": ("charger", "on"),
            "undock": ("charger", "off"),
            "off_charger": ("charger", "off"),
            "sleep_now": ("sleep", "now"),
            "quiet": ("shutup", "now"),
            "shut_up": ("shutup", "now"),
            "stop": ("stop_movement", ""),
            "halt": ("stop_movement", ""),
            "photo": ("takephoto", _default_photo),
            "say_text": ("say", None),
            "scene": ("macro", None),
            "macro": ("macro", None),
            "status": ("nodered_status", None)
        }

        canonical_cmd = cmd
        new_payload = original_payload
        if cmd not in self.command_handlers and cmd in aliases:
            canonical_cmd, transform = aliases[cmd]
            if callable(transform):
                try:
                    new_payload = transform(original_payload)
                except Exception:
                    new_payload = original_payload
            elif isinstance(transform, str):
                new_payload = transform
            # rebuild topic with canonical command
            topic = f"{self.COMMAND_BASE}/{canonical_cmd}"
        else:
            topic = msg.topic

        # Determine priority
        priority = CommandPriority.MEDIUM
        if topic in [self.COMMAND_TOPIC_LIFECYCLE, self.COMMAND_TOPIC_BEHAVIOR]:
            priority = CommandPriority.HIGH
        elif topic in [self.COMMAND_TOPIC_GETSTATUS]:
            priority = CommandPriority.LOW

        # Backpressure: non-blocking put; drop low-priority if full
        try:
            self.command_queues[priority].put_nowait((topic, new_payload, new_payload.lower(), priority))
        except asyncio.QueueFull:
            if priority == CommandPriority.LOW:
                logger.warning("Dropping low-priority command: queue full")
            else:
                self.publish_error_status("Command queue full; high/medium priority backlogged", "processor", False)

    def publish_bridge_status(self, status="online"):
        """Publishes the bridge operational status."""
        try:
            status_data = {
                "status": status,
                "timestamp": datetime.datetime.now().isoformat(),
                "uptime": time.time() - self._start_time if hasattr(self, '_start_time') else 0,
                "version": self.VERSION
            }
            
            self.mqtt_client.publish(
                self.STATUS_TOPIC_BRIDGE_STATUS,
                json.dumps(status_data),
                qos=1,
                retain=True
            )
            logger.info(f"Bridge status published: {status}")
        except Exception as e:
            logger.error(f"Failed to publish bridge status: {e}")

    def publish_error_status(self, error_message=None, error_type=None, critical=False, force_clear=False):
        """Publishes bridge error status.
        
        Args:
            error_message: The error message to publish
            error_type: Type of error (e.g., 'connection', 'command', 'internal')
            critical: Whether this is a critical error requiring attention
            force_clear: Whether to force clear the error status even without a new error
        """
        try:
            timestamp = datetime.datetime.now().isoformat()
            
            # Debug info
            logger.debug(f"Error status call: error='{error_message}', last='{self.last_error_message}', force_clear={force_clear}")
            
            # Don't publish null errors if we don't already have a real error
            # unless force_clear is True
            if error_message is None and self.last_error_message is None and not force_clear:
                logger.debug("Skipping null error with no previous error")
                return
                
            # Only update if this is a new error or explicitly clearing a resolved error
            if error_message != self.last_error_message or force_clear:
                if error_message:
                    error_data = {
                        "timestamp": timestamp,
                        "error": error_message,
                        "type": error_type or "general",
                        "critical": critical,
                        "has_error": True
                    }
                    
                    # Log appropriately
                    log_level = logger.error if critical else logger.warning
                    log_level(f"Bridge error published: {error_message}")
                    
                    # Store this as the last error message
                    self.last_error_message = error_message
                else:
                    # Only clear if force_clear or we have a last error to clear
                    if force_clear or self.last_error_message is not None:
                        error_data = {
                            "timestamp": timestamp,
                            "error": None,
                            "type": None,
                            "critical": False,
                            "has_error": False
                        }
                        
                        logger.info("Bridge error status cleared")
                        self.last_error_message = None
                    else:
                        # No need to publish anything
                        return
                    
                # Publish with retain=True for persistence
                self.mqtt_client.publish(
                    self.STATUS_TOPIC_ERROR_STATUS,
                    json.dumps(error_data),
                    qos=1,
                    retain=True
                )
        except Exception as e:
            logger.error(f"Failed to publish error status: {e}")

    def publish_cmd_complete(self, data: dict):
        """Publish command completion with configured QoS."""
        try:
            self.mqtt_client.publish(self.STATUS_TOPIC_COMMAND_COMPLETE, json.dumps(data), qos=self.CMD_COMPLETE_QOS)
        except Exception as e:
            logger.error(f"Failed to publish command_complete: {e}")

    def publish_event(self, event_type, data, qos=None):
        """Publishes an event with standardized structure (QoS from config by default)."""
        event_data = {
            "timestamp": datetime.datetime.now().isoformat(),
            "type": event_type
        }
        event_data.update(data)

        topic_attr = f"EVENT_TOPIC_{event_type.upper()}"
        if hasattr(self, topic_attr):
            topic = getattr(self, topic_attr)
        else:
            topic = f"{self.EVENT_BASE}/{event_type.lower()}"

        effective_qos = self.EVENT_QOS if qos is None else qos
        self.mqtt_client.publish(topic, json.dumps(event_data), qos=effective_qos)
        logger.debug(f"Published event: {event_type}")

        # Triggers (already centralized here, so don't duplicate elsewhere)
        if event_type in self.event_triggers:
            action = self.event_triggers[event_type]
            logger.info(f"Executing trigger for {event_type}")
            topic = f"{self.COMMAND_BASE}/{action['command']}"
            asyncio.run_coroutine_threadsafe(self.execute_command(topic, action['payload']), self.loop)

# --- Home Assistant / Node-Red ---

    def publish_ha_discovery(self):
        """Publish HA discovery for battery, availability, on_charger, and MQTT camera."""
        if not self.HA_DISCOVERY:
            return
        try:
            serial = self.VECTOR_SERIAL
            dev_id = f"vector_{serial}"
            dev = {
                "identifiers": [dev_id],
                "manufacturer": "Anki",
                "model": "Vector",
                "name": self.VECTOR_NAME or f"Vector {serial}",
            }

            # Battery (map 1->10%, 2->50%, 3->90%)
            batt_cfg = {
                "name": f"{self.VECTOR_NAME} Battery",
                "unique_id": f"{dev_id}_battery",
                "state_topic": self.STATUS_TOPIC_BATTERY,
                "value_template": "{{ {'1':10,'2':50,'3':90}[value|string] if (value|string) in ['1','2','3'] else 0 }}",
                "unit_of_measurement": "%",
                "device_class": "battery",
                "state_class": "measurement",
                "availability": [{
                    "topic": self.STATUS_TOPIC_BRIDGE_STATUS,
                    "value_template": "{{ value_json.status }}",
                    "payload_available": "online",
                    "payload_not_available": "offline"
                }],
                "device": dev,
            }
            self.mqtt_client.publish(
                f"{self.HA_DISCOVERY_PREFIX}/sensor/{dev_id}/battery/config",
                json.dumps(batt_cfg),
                qos=1, retain=True
            )

            # On charger (from telemetry JSON)
            onchg_cfg = {
                "name": f"{self.VECTOR_NAME} On Charger",
                "unique_id": f"{dev_id}_on_charger",
                "state_topic": self.STATUS_TOPIC_TELEMETRY,
                "value_template": "{{ 'ON' if value_json.battery.is_on_charger | default(false) else 'OFF' }}",
                "payload_on": "ON",
                "payload_off": "OFF",
                "device_class": "power",
                "availability": [{
                    "topic": self.STATUS_TOPIC_BRIDGE_STATUS,
                    "value_template": "{{ value_json.status }}",
                    "payload_available": "online",
                    "payload_not_available": "offline"
                }],
                "device": dev,
            }
            self.mqtt_client.publish(
                f"{self.HA_DISCOVERY_PREFIX}/binary_sensor/{dev_id}/on_charger/config",
                json.dumps(onchg_cfg),
                qos=1, retain=True
            )

            # Bridge availability (binary sensor)
            avail_cfg = {
                "name": f"{self.VECTOR_NAME} Bridge",
                "unique_id": f"{dev_id}_availability",
                "state_topic": self.STATUS_TOPIC_BRIDGE_STATUS,
                "value_template": "{{ 'ON' if value_json.status == 'online' else 'OFF' }}",
                "payload_on": "ON",
                "payload_off": "OFF",
                "device_class": "connectivity",
                "availability": [{
                    "topic": self.STATUS_TOPIC_BRIDGE_STATUS,
                    "value_template": "{{ value_json.status }}",
                    "payload_available": "online",
                    "payload_not_available": "offline"
                }],
                "device": dev,
            }
            self.mqtt_client.publish(
                f"{self.HA_DISCOVERY_PREFIX}/binary_sensor/{dev_id}/availability/config",
                json.dumps(avail_cfg),
                qos=1, retain=True
            )

            # MQTT Camera for latest photo
            cam_cfg = {
                "name": f"{self.VECTOR_NAME} Photo",
                "unique_id": f"{dev_id}_photo",
                "topic": f"{self.STATUS_BASE}/photo_b64",
                "image_encoding": "b64",
                "availability": [{
                    "topic": self.STATUS_TOPIC_BRIDGE_STATUS,
                    "value_template": "{{ value_json.status }}",
                    "payload_available": "online",
                    "payload_not_available": "offline"
                }],
                "device": dev,
            }
            self.mqtt_client.publish(
                f"{self.HA_DISCOVERY_PREFIX}/camera/{dev_id}/photo/config",
                json.dumps(cam_cfg),
                qos=1, retain=True
            )
            
            default_photo = "/root/.anki_vector/vector_photos/vector_latest.jpg"
            buttons = [
                {"key": "look_at_me", "name": f"{self.VECTOR_NAME} Look At Me", "topic": self.COMMAND_TOPIC_LOOK_AT_ME, "payload": "start"},
                {"key": "sleep", "name": f"{self.VECTOR_NAME} Sleep", "topic": self.COMMAND_TOPIC_SLEEP, "payload": "now"},
                {"key": "shutup", "name": f"{self.VECTOR_NAME} Shutup", "topic": self.COMMAND_TOPIC_SHUTUP, "payload": "now"},
                {"key": "takephoto", "name": f"{self.VECTOR_NAME} Take Photo", "topic": self.COMMAND_TOPIC_TAKEPHOTO, "payload": default_photo},
            ]

            for b in buttons:
                btn = {
                    "name": b["name"],
                    "unique_id": f"{dev_id}_button_{b['key']}",
                    "command_topic": b["topic"],
                    "payload_press": b["payload"],
                    "availability": [{
                        "topic": self.STATUS_TOPIC_BRIDGE_STATUS,
                        "value_template": "{{ value_json.status }}",
                        "payload_available": "online",
                        "payload_not_available": "offline"
                    }],
                    "device": dev
                }
                self.mqtt_client.publish(
                    f"{self.HA_DISCOVERY_PREFIX}/button/{dev_id}/{b['key']}/config",
                    json.dumps(btn), qos=1, retain=True
                )

            logger.info("Published Home Assistant discovery configs")
        except Exception as e:
            logger.error(f"HA discovery publish failed: {e}")

# --- Command Processing ---

    async def command_processor(self):
        """Async task that processes commands from the queues based on priority."""
        while not self.shutting_down:
            try:
                # Check high priority queue first
                if not self.command_queues[CommandPriority.HIGH].empty():
                    queue = self.command_queues[CommandPriority.HIGH]
                # Then medium priority queue
                elif not self.command_queues[CommandPriority.MEDIUM].empty():
                    queue = self.command_queues[CommandPriority.MEDIUM]
                # Finally low priority queue
                elif not self.command_queues[CommandPriority.LOW].empty():
                    queue = self.command_queues[CommandPriority.LOW]
                # If all queues are empty, wait for a short time and check again
                else:
                    await asyncio.sleep(0.1)
                    continue
                
                # Get the next command from the selected queue
                topic, original_payload, command_payload, priority = await queue.get()
                
                # Process lifecycle commands immediately
                if topic == self.COMMAND_TOPIC_LIFECYCLE:
                    if command_payload == "connect" and not self.is_vector_connected:
                        await self.connect_to_vector()
                    elif command_payload == "disconnect" and self.is_vector_connected:
                        await self.disconnect_from_vector()
                    elif command_payload == "reconnect" and self.is_vector_connected:
                        await self.disconnect_from_vector()
                        await asyncio.sleep(1)  # Small delay before reconnecting
                        await self.connect_to_vector()
                    queue.task_done()
                    continue
                
                # Skip other commands if Vector is not connected
                if not self.is_vector_connected or not self.robot:
                    logger.warning("Ignoring command: Vector is not connected.")
                    queue.task_done()
                    continue
                
                # Process behavior control commands
                if topic == self.COMMAND_TOPIC_BEHAVIOR:
                    if command_payload == "acquire" and not self.has_behavior_control:
                        logger.info("Attempting to acquire behavior control...")
                        await self.loop.run_in_executor(
                            self.thread_pool, 
                            lambda: self.robot.conn.request_control()
                        )
                        self.has_behavior_control = True
                        self.mqtt_client.publish(
                            self.STATUS_TOPIC_BEHAVIOR, 
                            "sdk_control", 
                            qos=1, 
                            retain=True
                        )
                        logger.info("Behavior control ACQUIRED. Ready for commands.")

                    elif command_payload == "release" and self.has_behavior_control:
                        logger.info("Attempting to release behavior control...")
                        await self.loop.run_in_executor(
                            self.thread_pool, 
                            lambda: self.robot.conn.release_control()
                        )
                        self.has_behavior_control = False
                        self.mqtt_client.publish(
                            self.STATUS_TOPIC_BEHAVIOR, 
                            "autonomous", 
                            qos=1, 
                            retain=True
                        )
                        logger.info("Behavior control RELEASED. Vector is autonomous.")
                    queue.task_done()
                    continue
                
                # Special handling for fistbump
                if topic == self.COMMAND_TOPIC_FISTBUMP:
                    asyncio.create_task(self.fistbump())
                    queue.task_done()
                    continue
                
                # Process standard commands
                await self.execute_command(topic, original_payload)
                queue.task_done()
                
            except asyncio.CancelledError:
                # Exit gracefully if the task is cancelled
                logger.info("Command processor task cancelled")
                break
            except Exception as e:
                logger.error(f"Error in command processor: {e}")
                self.publish_error_status(
                    error_message=f"Command processor error: {e}",
                    error_type="processor",
                    critical=False
                )
                # Make sure to mark the task as done even if there's an error
                if 'queue' in locals():
                    queue.task_done()

    async def execute_command(self, topic, payload):
        logger.debug(f"--- New Command Received --- Topic: {topic}, Payload: {payload}")
        command_name = topic.split('/')[-1]
        handler = self.command_handlers.get(command_name)

        if not handler:
            return self.handle_command_error(command_name, "Unknown command", 0, 0)

        timeout = self.COMMAND_TIMEOUTS.get(command_name, self.COMMAND_TIMEOUTS.get('default', 10))
        max_retries = 2 if command_name not in ['animation', 'trigger'] else 1

        cmd_id = self._extract_cmd_id(payload)
        t0 = time.time()

        for attempt in range(max_retries + 1):
            try:
                if attempt > 0:
                    logger.info(f"Retry attempt {attempt} for command {command_name}")

                success = await asyncio.wait_for(handler(self, payload), timeout=int(timeout))

                if success:
                    self.publish_cmd_complete({
                        "id": cmd_id,
                        "command": command_name,
                        "status": "success",
                        "duration_ms": int((time.time() - t0) * 1000)
                    })
                    if self._last_error and self._last_error.startswith(f"Command '{command_name}'"):
                        self.publish_error_status(force_clear=True)
                    break

                elif attempt == max_retries:
                    return self.handle_command_error(
                        command_name, "Command handler returned failure", attempt, max_retries, cmd_id=cmd_id
                    )

            except asyncio.TimeoutError:
                logger.error(f"Command timed out ({timeout}s)")
                if attempt == max_retries:
                    return self.handle_command_error(
                        command_name, f"Timed out after {timeout}s", attempt, max_retries, cmd_id=cmd_id
                    )
            except Exception as e:
                if attempt == max_retries:
                    return self.handle_command_error(command_name, e, attempt, max_retries, cmd_id=cmd_id)

        logger.info("--- Command Finished ---")

    async def _execute_with_control(self, action_function, timeout=None):
        if timeout is None:
            timeout = self.COMMAND_TIMEOUT

        async with self.sdk_lock:
            control_acquired_here = False
            try:
                if not self.has_behavior_control:
                    await self.loop.run_in_executor(self.thread_pool, lambda: self.robot.conn.request_control())
                    control_acquired_here = True
                    if self.BEHAVIOR_PUB_MODE == 'transient':
                        self.has_behavior_control = True
                        self.mqtt_client.publish(self.STATUS_TOPIC_BEHAVIOR, "sdk_control", qos=1, retain=True)
                    await asyncio.sleep(0.3)

                await asyncio.wait_for(self.loop.run_in_executor(self.thread_pool, action_function), timeout=timeout)
                logger.info("  - Action finished successfully.")
                return True

            except asyncio.TimeoutError:
                logger.error(f"Action timed out after {timeout} seconds")
                return False
            except Exception as e:
                # If VectorControlException exists and matches, treat as control lost
                vce = getattr(vec_exc, "VectorControlException", None)
                if vce and isinstance(e, vce):
                    if self.has_behavior_control:
                        self.has_behavior_control = False
                        self.mqtt_client.publish(self.STATUS_TOPIC_BEHAVIOR, "autonomous", qos=1, retain=True)
                        logger.warning("Behavior control lost; switched to autonomous")
                    raise
                # Otherwise, generic failure
                logger.error(f"ERROR during controlled execution: {e}")
                return False
            finally:
                if control_acquired_here:
                    try:
                        await self.loop.run_in_executor(self.thread_pool, lambda: self.robot.conn.release_control())
                    except Exception as e:
                        logger.error(f"Error releasing control: {e}")
                    finally:
                        if self.BEHAVIOR_PUB_MODE == 'transient':
                            self.has_behavior_control = False
                            self.mqtt_client.publish(self.STATUS_TOPIC_BEHAVIOR, "autonomous", qos=1, retain=True)

    def handle_command_error(self, command_name, error, attempt=0, max_retries=0, cmd_id=None):
        error_msg = f"Command '{command_name}' failed: {error}"
        logger.error(error_msg)

        data = {
            "command": command_name,
            "status": "failure",
            "error": str(error),
            "attempts": attempt + 1,
            "max_retries": max_retries
        }
        if cmd_id is not None:
            data["id"] = cmd_id

        self.publish_cmd_complete(data)

        if attempt >= max_retries:
            self.publish_error_status(error_msg, "command", False)
            self._last_error = error_msg

        return False

    def _extract_cmd_id(self, payload: str) -> Optional[str]:
        try:
            p = payload.strip()
            if p.startswith("{"):
                data = json.loads(p)
                cid = data.get("id")
                if isinstance(cid, (str, int)):
                    return str(cid)
        except Exception:
            pass
        return None

# --- Status Monitoring ---

    async def monitor_vector_status(self):
        """Async task to periodically check and publish Vector's status."""
        try:
            while not self.shutting_down:
                if not self.is_vector_connected or not self.robot:
                    await asyncio.sleep(5)
                    continue

                try:
                    battery_state = await self.loop.run_in_executor(self.thread_pool, self.robot.get_battery_state)

                    if battery_state:
                        level = getattr(battery_state, 'battery_level', None)  # 1..3
                        if level is not None and level != self.last_battery_level:
                            logger.info(f"Battery level changed: {level}")
                            # Publish retained simple battery level (1..3)
                            self.mqtt_client.publish(
                                self.STATUS_TOPIC_BATTERY,
                                str(level),
                                qos=1,
                                retain=True
                            )
                            self.last_battery_level = level

                            # Low battery warning if level == 1
                            if level == 1:
                                logger.warning("Vector battery low (level 1)")
                                self.mqtt_client.publish(f"{self.STATUS_TOPIC_BATTERY}/warning", "low_battery", qos=1)

                except Exception as e:
                    logger.error(f"Error in monitor status check: {e}")

                # Activity publishing debounce
                if self.robot_activity["pending_change"]:
                    current_time = time.time()
                    min_interval = self.config.getint('Settings', 'activity_min_interval', fallback=5)
                    if current_time - self.robot_activity["last_published"] >= min_interval:
                        self._publish_activity_state()

                await asyncio.sleep(5)

        except asyncio.CancelledError:
            logger.info("Monitor task cancelled.")
            raise

    def _publish_activity_state(self):
        """Publishes the current activity state."""
        activity_data = {
            "is_active": self.robot_activity["is_active"],
            "activity_type": self.robot_activity["activity_type"]
        }
        
        # Use the standardized event publisher
        self.publish_event("activity", activity_data, 
                         qos=self.config.getint('Settings', 'activity_qos', fallback=0))
        
        self.robot_activity["last_published"] = time.time()
        self.robot_activity["pending_change"] = False
        logger.debug(f"Published activity state: {activity_data['is_active']}")

    async def send_heartbeat(self):
        """Sends periodic heartbeat messages to indicate bridge is alive."""
        try:
            while not self.shutting_down:
                heartbeat_data = {
                    "timestamp": datetime.datetime.now().isoformat(),
                    "bridge_uptime": time.time() - self._start_time if hasattr(self, '_start_time') else 0,
                    "vector_connected": self.is_vector_connected,
                    "behavior_control": self.has_behavior_control
                }
                
                self.mqtt_client.publish(
                    self.STATUS_TOPIC_HEARTBEAT,
                    json.dumps(heartbeat_data),
                    qos=0  # Use QoS 0 for heartbeat to minimize overhead
                )
                
                await asyncio.sleep(self.HEARTBEAT_INTERVAL)
                
        except asyncio.CancelledError:
            logger.info("Heartbeat task cancelled.")
            raise

    async def send_telemetry(self):
        """Sends detailed telemetry data about Vector's state."""
        try:
            # Skip if disabled
            if hasattr(self, 'TELEMETRY_LEVEL') and self.TELEMETRY_LEVEL.lower() == 'off':
                logger.info("Telemetry is disabled")
                return

            await asyncio.sleep(5)  # Initial delay

            while not self.shutting_down:
                if self.latest_jpeg and (time.time() - self.latest_jpeg_ts) > 300:
                    self.latest_jpeg = None
                if not self.is_vector_connected or not self.robot:
                    await asyncio.sleep(self.TELEMETRY_INTERVAL)
                    continue

                try:
                    battery_state = await self.loop.run_in_executor(self.thread_pool, self.robot.get_battery_state)

                    def safe_get(obj, attr, default=None):
                        return getattr(obj, attr, default)

                    # Battery: level is 1..3
                    level = safe_get(battery_state, 'battery_level')
                    label = {1: "low", 2: "medium", 3: "high"}.get(level, None)

                    telemetry_data = {
                        "timestamp": datetime.datetime.now().isoformat(),
                        "battery": {
                            "level": level,
                            "label": label,
                            "is_charging": safe_get(battery_state, 'is_charging') if battery_state else None,
                            "volts": safe_get(battery_state, 'battery_volts') if battery_state else None,
                        }
                    }

                    if not hasattr(self, 'TELEMETRY_LEVEL') or self.TELEMETRY_LEVEL.lower() == 'full':
                        # Charger status from robot.status if available
                        if hasattr(self.robot.status, 'is_on_charger'):
                            telemetry_data["battery"]["is_on_charger"] = self.robot.status.is_on_charger

                        telemetry_data["status"] = {
                            "is_moving": safe_get(self.robot.status, 'is_moving', False),
                            "is_picked_up": safe_get(self.robot.status, 'is_picked_up', False),
                            "is_on_charger": safe_get(self.robot.status, 'is_on_charger', False),
                            "is_being_held": safe_get(self.robot.status, 'is_being_held', False),
                            "is_cliff_detected": safe_get(self.robot.status, 'is_cliff_detected', False),
                            "is_falling": safe_get(self.robot.status, 'is_falling', False),
                            "is_animating": safe_get(self.robot.status, 'is_animating', False),
                            "is_button_pressed": safe_get(self.robot.status, 'is_button_pressed', False)
                        }

                        if hasattr(self.robot, 'pose') and hasattr(self.robot.pose, 'position'):
                            telemetry_data["position"] = {
                                "x": self.robot.pose.position.x,
                                "y": self.robot.pose.position.y,
                                "z": self.robot.pose.position.z
                            }
                            if hasattr(self.robot.pose, 'head_angle'):
                                telemetry_data["head_angle_deg"] = self.robot.pose.head_angle.degrees
                            if hasattr(self.robot.pose, 'lift_height'):
                                telemetry_data["lift_height_mm"] = self.robot.pose.lift_height.distance_mm

                        if hasattr(self.robot, 'accel'):
                            telemetry_data["accelerometer"] = {
                                "x": self.robot.accel.x,
                                "y": self.robot.accel.y,
                                "z": self.robot.accel.z
                            }

                        telemetry_data["behavior_control"] = {
                            "has_control": self.has_behavior_control
                        }

                        if hasattr(self.robot, 'world'):
                            try:
                                face_count = len(list(self.robot.world.visible_faces))
                                has_cube = self.robot.world.connected_light_cube is not None
                                telemetry_data["world"] = {
                                    "visible_faces": face_count,
                                    "has_cube": has_cube
                                }
                            except Exception as e:
                                logger.debug(f"Error getting world data: {e}")

                    # Remove empty dicts
                    for key in list(telemetry_data.keys()):
                        if isinstance(telemetry_data[key], dict) and not telemetry_data[key]:
                            del telemetry_data[key]

                    self.mqtt_client.publish(self.STATUS_TOPIC_TELEMETRY, json.dumps(telemetry_data), qos=self.TELEMETRY_QOS)

                except Exception as e:
                    logger.error(f"Error sending telemetry: {e}")
                    if hasattr(self, 'publish_error_status'):
                        self.publish_error_status(f"Telemetry error: {e}", "telemetry", False)

                await asyncio.sleep(self.TELEMETRY_INTERVAL)

        except asyncio.CancelledError:
            logger.info("Telemetry task cancelled.")
            raise

# --- Vector Events ---

    def on_face_detected(self, robot, event_type, event, **kwargs):
        """Publishes recognized face names to MQTT."""
        # Throttle events to max 1 per second per face
        current_time = time.time()
        face_id = event.face_id
        if (face_id in self.last_event_times and 
            current_time - self.last_event_times[face_id] < 1.0 and
            self.EVENT_THROTTLING):
            return  # Skip this event
                
        self.last_event_times[face_id] = current_time  
        
        if event.name:
            logger.info(f"Detected face: {event.name}")
            
            # Use the standardized event publisher instead of direct MQTT publishing
            self.publish_event("face_detected", {
                "name": event.name,
                "face_id": event.face_id,
                "expression": event.expression if hasattr(event, 'expression') else None
            })
            # Remember the latest face object if we can resolve it
            try:
                face_obj = None
                for f in list(self.robot.world.visible_faces):
                    if getattr(f, 'face_id', None) == event.face_id:
                        face_obj = f
                        break
                self._last_seen_face = face_obj
                self._face_seen_event.set()
            except Exception:
                pass            

    def on_robot_state(self, robot, event_type, event, **kwargs):
        """Handles robot state events with traffic control."""
        # Skip if activity monitoring is disabled
        if not self.config.getboolean('Settings', 'activity_monitoring', fallback=True):
            return
                
        current_time = time.time()
        
        # Monitor for cube connection changes
        if hasattr(event, 'cube_battery'):
            # Existing cube code...
            pass
        
        # Check for animation/speaking activity (improved detection)
        activity_detected = False
        activity_type = None
        
        # Use multiple detection methods without relying on is_moving()
        if hasattr(robot, 'audio') and hasattr(robot.audio, 'is_playing') and robot.audio.is_playing:
            activity_detected = True
            activity_type = "audio"
        # Check if head or lift are moving by comparing current positions to stored positions
        elif hasattr(robot, 'pose'):
            head_angle = robot.pose.head_angle.radians if hasattr(robot.pose, 'head_angle') else None
            lift_height = robot.pose.lift_height.distance_mm if hasattr(robot.pose, 'lift_height') else None
            
            # Compare with last known values (initialize if not set)
            if not hasattr(self, '_last_head_angle'):
                self._last_head_angle = head_angle
                self._last_lift_height = lift_height
            elif (head_angle is not None and self._last_head_angle is not None and 
                  abs(head_angle - self._last_head_angle) > 0.01):
                activity_detected = True
                activity_type = "head_moving"
            elif (lift_height is not None and self._last_lift_height is not None and 
                  abs(lift_height - self._last_lift_height) > 0.5):
                activity_detected = True
                activity_type = "lift_moving"
                
            # Update last known values
            self._last_head_angle = head_angle
            self._last_lift_height = lift_height
        
        # Only process state if activity changed
        if activity_detected != self.robot_activity["is_active"]:
            self.robot_activity["is_active"] = activity_detected
            self.robot_activity["activity_type"] = activity_type
            self.robot_activity["last_activity_change"] = current_time
            self.robot_activity["pending_change"] = True
            
            # Only publish immediately if it's been long enough since last update
            min_interval = self.config.getint('Settings', 'activity_min_interval', fallback=5)
            if current_time - self.robot_activity["last_published"] >= min_interval:
                self._publish_activity_state()

    def on_user_intent(self, robot, event_type, event, **kwargs):
        """Handles voice commands detected by Vector."""
        logger.info("--- SDK Voice Intent Received ---")
        payload_str = event.json_data
        
        try:
            intent_data = json.loads(payload_str)
            intent_type = intent_data.get("type")
            
            # Publish the intent using the standardized event publisher
            self.publish_event("voice_command", {
                "intent_type": intent_type,
                "raw_data": intent_data
            })

            # Check if this is a command we want to override
            CUSTOM_COMMANDS_TO_OVERRIDE = [
                "imperative_gotosleep",
                "imperative_praise",
                "imperative_weather",
                "imperative_findcube"
            ]
            
            if intent_type in CUSTOM_COMMANDS_TO_OVERRIDE:
                logger.info(f"'{intent_type}' is a custom override command. Taking control!")
                # Create a task to handle this asynchronously
                asyncio.create_task(self._handle_voice_override(intent_type))
            else:
                # If it's a standard command, let Vector handle it
                logger.info(f"'{intent_type}' is a standard command. Letting Vector handle it.")
        except json.JSONDecodeError:
            logger.error(f"Invalid intent JSON: {payload_str}")
            self.publish_event("voice_command_error", {"error": "Invalid JSON", "raw_data": payload_str})
        except Exception as e:
            logger.error(f"Error processing intent: {e}")
            self.publish_event("voice_command_error", {"error": str(e), "raw_data": payload_str})
            
        logger.info("--- End SDK Voice Intent ---")

    async def _handle_voice_override(self, intent_type):
        """Handles overridden voice commands asynchronously."""
        async with self.sdk_lock:
            try:
                # Map of intent types to custom responses
                custom_responses = {
                    "imperative_gotosleep": {
                        "text": "I do not want to sleep right now.",
                        "animation": "anim_keepaway_getout_01"
                    },
                    "imperative_praise": {
                        "text": "Thank you very much!",
                        "animation": "anim_fistbump_success_01"
                    },
                    "imperative_weather": {
                        "text": "I'll check the weather for you via MQTT.",
                        "animation": "anim_rtpickup_loop_09",
                        "mqtt_event": "request_weather"
                    },
                    "imperative_findcube": {
                        "text": "Let me see if I can find my cube.",
                        "animation": "anim_lookinplaceforfaces_keepalive_long",
                        "mqtt_event": "request_find_cube"
                    }
                }
                
                # Get the custom response or use default
                response = custom_responses.get(intent_type, {
                    "text": "I'm not sure how to respond to that command.",
                    "animation": "anim_feedback_acknowledgement_01"
                })
                
                # Acquire control
                await self.loop.run_in_executor(
                    self.thread_pool,
                    lambda: self.robot.conn.request_control()
                )
                self.has_behavior_control = True
                
                # Execute the custom action
                if "text" in response:
                    await self.loop.run_in_executor(
                        self.thread_pool,
                        lambda: self.robot.behavior.say_text(response["text"])
                    )
                
                if "animation" in response:
                    try:
                        await self.loop.run_in_executor(
                            self.thread_pool,
                            lambda: self.robot.anim.play_animation(response["animation"])
                        )
                    except Exception as e:
                        logger.warning(f"Failed to play animation {response['animation']}: {e}")
                        # Fallback to a simpler animation
                        try:
                            await self.loop.run_in_executor(
                                self.thread_pool,
                                lambda: self.robot.anim.play_animation("anim_neutral_eyes_01")
                            )
                        except:
                            pass
                
                # Publish any requested MQTT events
                if "mqtt_event" in response:
                    self.mqtt_client.publish(
                        f"{self.EVENT_BASE}/custom/{response['mqtt_event']}", 
                        payload=intent_type, 
                        qos=1
                    )
                    
            except Exception as e:
                logger.error(f"Error handling voice override: {e}")
            finally:
                # Release control
                logger.info("Custom action finished. Releasing control.")
                await self.loop.run_in_executor(
                    self.thread_pool,
                    lambda: self.robot.conn.release_control()
                )
                self.has_behavior_control = False

# --- Vector Movement ---

    async def _continuous_drive(self, direction, speed):
        """Continuously drives Vector in the specified direction."""
        try:
            # Get behavior control if needed
            control_acquired_here = False
            if not self.has_behavior_control:
                await self.loop.run_in_executor(
                    self.thread_pool,
                    lambda: self.robot.conn.request_control()
                )
                control_acquired_here = True
            
            # Set wheel speeds based on direction
            wheel_speed = speed if direction == 'forward' else -speed
            
            # Continue moving until task is cancelled
            while True:
                await self.loop.run_in_executor(
                    self.thread_pool,
                    lambda: self.robot.motors.set_wheel_motors(wheel_speed, wheel_speed)
                )
                await asyncio.sleep(0.1)  # Small delay to prevent flooding Vector with commands
        
        except asyncio.CancelledError:
            # Stop motors when cancelled
            await self.loop.run_in_executor(
                self.thread_pool,
                lambda: self.robot.motors.set_wheel_motors(0, 0)
            )
            raise
        
        finally:
            # Always stop motors and release control if we acquired it
            try:
                await self.loop.run_in_executor(
                    self.thread_pool,
                    lambda: self.robot.motors.set_wheel_motors(0, 0)
                )
                
                if control_acquired_here:
                    await self.loop.run_in_executor(
                        self.thread_pool,
                        lambda: self.robot.conn.release_control()
                    )
            except Exception as e:
                logger.error(f"Error stopping motors: {e}")

    async def _continuous_rotate(self, direction, speed):
        """Continuously rotates Vector in the specified direction."""
        try:
            # Get behavior control if needed
            control_acquired_here = False
            if not self.has_behavior_control:
                await self.loop.run_in_executor(
                    self.thread_pool,
                    lambda: self.robot.conn.request_control()
                )
                control_acquired_here = True
            
            # Set wheel speeds based on direction
            if direction == 'left':
                left_wheel = -speed
                right_wheel = speed
            else:
                left_wheel = speed
                right_wheel = -speed
            
            # Continue rotating until task is cancelled
            while True:
                await self.loop.run_in_executor(
                    self.thread_pool,
                    lambda: self.robot.motors.set_wheel_motors(left_wheel, right_wheel)
                )
                await asyncio.sleep(0.1)  # Small delay to prevent flooding Vector with commands
        
        except asyncio.CancelledError:
            # Stop motors when cancelled
            await self.loop.run_in_executor(
                self.thread_pool,
                lambda: self.robot.motors.set_wheel_motors(0, 0)
            )
            raise
        
        finally:
            # Always stop motors and release control if we acquired it
            try:
                await self.loop.run_in_executor(
                    self.thread_pool,
                    lambda: self.robot.motors.set_wheel_motors(0, 0)
                )
                
                if control_acquired_here:
                    await self.loop.run_in_executor(
                        self.thread_pool,
                        lambda: self.robot.conn.release_control()
                    )
            except Exception as e:
                logger.error(f"Error stopping motors: {e}")

    async def _continuous_movement(self, movement_type, direction, speed):
        """Generic continuous movement function."""
        try:
            # Acquire control if needed
            control_acquired_here = False
            if not self.has_behavior_control:
                await self.loop.run_in_executor(
                    self.thread_pool,
                    lambda: self.robot.conn.request_control()
                )
                control_acquired_here = True
            
            # Set wheel speeds based on movement type and direction
            if movement_type == "drive":
                left_wheel = right_wheel = speed if direction == 'forward' else -speed
            else:  # rotate
                if direction == 'left':
                    left_wheel, right_wheel = -speed, speed
                else:
                    left_wheel, right_wheel = speed, -speed
            
            # Continue movement until task is cancelled
            while True:
                await self.loop.run_in_executor(
                    self.thread_pool,
                    lambda: self.robot.motors.set_wheel_motors(left_wheel, right_wheel)
                )
                await asyncio.sleep(0.1)
        
        except asyncio.CancelledError:
            # Stop motors when cancelled
            await self.loop.run_in_executor(
                self.thread_pool,
                lambda: self.robot.motors.set_wheel_motors(0, 0)
            )
            raise
        
        finally:
            # Cleanup
            try:
                await self.loop.run_in_executor(
                    self.thread_pool,
                    lambda: self.robot.motors.set_wheel_motors(0, 0)
                )
                
                if control_acquired_here:
                    await self.loop.run_in_executor(
                        self.thread_pool,
                        lambda: self.robot.conn.release_control()
                    )
            except Exception as e:
                logger.error(f"Error stopping motors: {e}")

    async def fistbump(self):
        """Performs an interactive fistbump sequence."""
        
        async with self.sdk_lock:
            
            logger.info("Starting interactive fistbump sequence...")            
            control_was_granted = False
            try:
                if not self.has_behavior_control:
                    # 1. Request control
                    logger.info("Requesting control...")
                    await self.loop.run_in_executor(
                        self.thread_pool,
                        lambda: self.robot.conn.request_control()
                    )
                    await asyncio.sleep(0.5)
                    control_was_granted = True
                
                # Add a wait to ensure any speaking has completed
                await asyncio.sleep(0.2)
            
                # 2. Raise the lift and wait for the user
                logger.info("Raising lift...")
                await self.loop.run_in_executor(
                    self.thread_pool, 
                    lambda: self.robot.anim.play_animation_trigger('FistBumpRequestOnce')
                )
                
                # 3. Start polling the touch sensor
                last_x = self.robot.accel.x
                last_y = self.robot.accel.y
                max_noise_x = 0
                max_noise_y = 0
                
                # Calibration phase
                calibration_start = time.time()
                while time.time() - calibration_start < 1.0:  # Calibrate for 1 full second
                    current_accel = self.robot.accel
                    delta_x = abs(current_accel.x - last_x)
                    delta_y = abs(current_accel.y - last_y)
                    if delta_x > max_noise_x: max_noise_x = delta_x
                    if delta_y > max_noise_y: max_noise_y = delta_y
                    last_x = current_accel.x
                    last_y = current_accel.y
                    await asyncio.sleep(0.05)
                    
                logger.info(f"Calibration complete. Max noise floor is X:{max_noise_x:.2f}, Y:{max_noise_y:.2f}")

                # 4. Start polling for a jolt
                logger.info("Now polling for a jolt for 10 seconds...")
                start_time = time.time()
                fistbump_success = False
                
                while time.time() - start_time < 10 and not self.shutting_down:
                    current_accel = self.robot.accel
                    delta_x = abs(current_accel.x - last_x)
                    delta_y = abs(current_accel.y - last_y)
                    last_x = current_accel.x
                    last_y = current_accel.y
                    
                    # The threshold for detecting a tap
                    jolt_threshold = 350
                    if delta_x > (max_noise_x + jolt_threshold) or delta_y > (max_noise_y + jolt_threshold):
                        logger.info(f"Jolt detected! (Delta X: {delta_x:.2f}, Delta Y: {delta_y:.2f}). Success.")
                        fistbump_success = True
                        break
                    
                    await asyncio.sleep(0.05)  

                # 5. Perform the appropriate follow-up action
                if fistbump_success:
                    await self.loop.run_in_executor(
                        self.thread_pool,
                        lambda: self.robot.anim.play_animation('anim_fistbump_success_01')
                    )
                    self.publish_cmd_complete({"command": "fistbump", "status": "success"})
                else:
                    await self.loop.run_in_executor(
                        self.thread_pool,
                        lambda: self.robot.anim.play_animation('anim_fistbump_fail_01')
                    )
                    logger.info("No touch detected within the time limit.")
                    self.publish_cmd_complete({"command": "fistbump", "status": "timeout"})

            except Exception as e:
                logger.error(f"ERROR in fistbump: {e}")
                self.publish_cmd_complete({"command": "fistbump", "status": "failure", "error": str(e)})
                
            finally:
                # Always release control if we acquired it
                if control_was_granted:
                    logger.info("Releasing control.")
                    try:
                        await self.loop.run_in_executor(
                            self.thread_pool,
                            lambda: self.robot.conn.release_control()
                        )
                    except Exception as e:
                        logger.error(f"Error releasing control: {e}")
                
                logger.info("Fistbump sequence finished.")

    def _movement_allowed(self) -> Tuple[bool, str]:
        try:
            s = self.robot.status
            if getattr(s, "is_picked_up", False):
                return False, "Robot is picked up"
            if getattr(s, "is_cliff_detected", False):
                return False, "Cliff detected"
            return True, ""
        except Exception:
            return True, ""

# --- State Persistence ---

    def save_state(self):
        """Saves the current state of the bridge."""
        state = {
            "timestamp": datetime.datetime.now().isoformat(),
            "is_vector_connected": self.is_vector_connected,
            "has_behavior_control": self.has_behavior_control,
            "last_battery_level": self.last_battery_level,
        }
        try:
            with open(self.PERSISTENCE_FILE, 'w') as f:
                json.dump(state, f)
            logger.info("Bridge state saved successfully")
        except Exception as e:
            logger.error(f"Error saving bridge state: {e}")

    def load_state(self):
        """Loads the saved state of the bridge."""
        try:
            with open(self.PERSISTENCE_FILE, 'r') as f:
                state = json.load(f)
            logger.info("Previous bridge state loaded successfully")
            
            # Only load non-critical state variables
            self.last_battery_level = state.get('last_battery_level')
            
            # Record bridge start time for uptime calculation
            self._start_time = time.time()
            
            return state
        except (FileNotFoundError, json.JSONDecodeError):
            logger.info("No previous state found or invalid state file")
            self._start_time = time.time()
            return {}
        except Exception as e:
            logger.error(f"Error loading bridge state: {e}")
            self._start_time = time.time()
            return {}

# --- Cube events ---

    def on_cube_tapped(self, robot, event_type, event, **kwargs):
        self.publish_event("cube_tap", {
            "object_id": getattr(event, 'object_id', None),
            "tap_count": getattr(event, 'tap_count', 1),
        })

    def on_cube_connected(self, robot, event_type, event, **kwargs):
        self.publish_event("cube_connection", {
            "state": "connected",
            "object_id": getattr(event, 'object_id', None),
        })

    def on_cube_disconnected(self, robot, event_type, event, **kwargs):
        self.publish_event("cube_connection", {
            "state": "disconnected",
            "object_id": getattr(event, 'object_id', None),
        })


# === COMMAND HANDLERS ===
class VectorCommands:

    @staticmethod
    async def handle_say(bridge: 'AsyncVectorMQTTBridge', payload: str) -> bool:
        """JSON: {"text":"...","style":"whisper|excited|sad|thinking","voice_pitch":0} or plain text."""
        text = payload
        style = None
        voice_pitch = None
        if payload.strip().startswith("{"):
            data = json.loads(payload)
            text = data.get("text", "")
            style = (data.get("style") or "").lower() or None
            voice_pitch = data.get("voice_pitch", None)

        if not text.strip():
            raise ValueError("Empty text not allowed")

        style_map = {
            "whisper": {"pre": "anim_reacttocliff_edge_01"},
            "excited": {"pre": "anim_announce_hey_01", "post": "anim_bbv_pose_sleepy_01"},
            "sad": {"pre": "anim_mtpraise_sad_01"},
            "thinking": {"pre": "anim_onboarding_reacttoface_01"},
        }
        sty = style_map.get(style, {})

        # say_text signature (voice_pitch optional)
        try:
            sig = inspect.signature(bridge.robot.behavior.say_text)
            supports_pitch = 'voice_pitch' in sig.parameters
        except Exception:
            supports_pitch = False

        async def do_pre():
            if sty.get("pre") and sty["pre"] in getattr(bridge.robot.anim, "anim_list", []):
                await bridge.loop.run_in_executor(bridge.thread_pool, lambda: bridge.robot.anim.play_animation(sty["pre"]))

        async def do_post():
            if sty.get("post") and sty["post"] in getattr(bridge.robot.anim, "anim_list", []):
                await bridge.loop.run_in_executor(bridge.thread_pool, lambda: bridge.robot.anim.play_animation(sty["post"]))

        await do_pre()
        if voice_pitch is not None and supports_pitch:
            await bridge._execute_with_control(lambda: bridge.robot.behavior.say_text(text, voice_pitch=voice_pitch))
        else:
            await bridge._execute_with_control(lambda: bridge.robot.behavior.say_text(text))
        await do_post()

        bridge.publish_event("speech", {"text": text, "action": "say", "style": style})
        return True

    @staticmethod
    async def handle_drive(bridge: 'AsyncVectorMQTTBridge', payload: str) -> bool:
        if payload.strip().startswith("{"):
            data = json.loads(payload)
            if "distance" not in data or "speed" not in data:
                raise ValueError("Missing 'distance' or 'speed'")
            distance = int(data["distance"])
            speed = int(data["speed"])
        else:
            parts = payload.split(',')
            if len(parts) != 2:
                raise ValueError("Expected 'distance,speed'")
            distance, speed = [int(p) for p in parts]

        ok, reason = bridge._movement_allowed() if hasattr(bridge, "_movement_allowed") else (True, "")
        if not ok:
            raise RuntimeError(f"Movement blocked: {reason}")

        await bridge._execute_with_control(lambda: bridge.robot.behavior.drive_straight(distance_mm(distance), speed_mmps(speed)))
        bridge.publish_event("movement", {"action": "drive", "distance": distance, "speed": speed})
        return True

    @staticmethod
    async def handle_rotate(bridge: 'AsyncVectorMQTTBridge', payload: str) -> bool:
        if payload.strip().startswith("{"):
            data = json.loads(payload)
            angle = float(data.get("angle"))
            speed = float(data.get("speed", 45.0))
        else:
            parts = payload.split(',')
            if len(parts) not in [1, 2]:
                raise ValueError("Expected 'angle[,speed]'")
            angle = float(parts[0])
            speed = float(parts[1]) if len(parts) > 1 else 45.0

        if not -360.0 <= angle <= 360.0:
            raise ValueError("Angle must be between -360 and 360 degrees")
        if not 5.0 <= speed <= 180.0:
            raise ValueError("Speed must be between 5 and 180 degrees/second")

        ok, reason = bridge._movement_allowed() if hasattr(bridge, "_movement_allowed") else (True, "")
        if not ok:
            raise RuntimeError(f"Movement blocked: {reason}")

        await bridge._execute_with_control(lambda: bridge.robot.behavior.turn_in_place(degrees(angle), speed=degrees(speed)))
        bridge.publish_event("movement", {"action": "rotate", "angle": angle, "speed": speed})
        return True

    @staticmethod
    async def handle_lift(bridge: 'AsyncVectorMQTTBridge', payload: str) -> bool:
        try:
            height = float(payload)
        except ValueError:
            raise ValueError("Height must be a number")
        if not 0.0 <= height <= 1.0:
            raise ValueError("Height must be between 0.0 and 1.0")

        await bridge._execute_with_control(lambda: bridge.robot.behavior.set_lift_height(height))
        bridge.publish_event("movement", {"action": "lift", "height": height})
        return True

    @staticmethod
    async def handle_head(bridge: 'AsyncVectorMQTTBridge', payload: str) -> bool:
        try:
            angle = float(payload)
        except ValueError:
            raise ValueError("Angle must be a number")
        if not -22.0 <= angle <= 45.0:
            raise ValueError("Angle must be between -22.0 and 45.0 degrees")

        await bridge._execute_with_control(lambda: bridge.robot.behavior.set_head_angle(degrees(angle)))
        bridge.publish_event("movement", {"action": "head", "angle": angle})
        return True

    @staticmethod
    async def handle_eye_color(bridge: 'AsyncVectorMQTTBridge', payload: str) -> bool:
        parts = [p.strip() for p in payload.split(',')]
        if len(parts) == 2:
            h = float(parts[0]); s = float(parts[1])
            if h > 1.0:
                h = (h % 360.0) / 360.0
            h = max(0.0, min(1.0, h))
            s = max(0.0, min(1.0, s))
            hue, sat = h, s
        elif len(parts) == 3:
            r, g, b = [int(x) for x in parts]
            if not all(0 <= c <= 255 for c in (r, g, b)):
                raise ValueError("RGB values must be 0-255")
            h, s, v = colorsys.rgb_to_hsv(r / 255.0, g / 255.0, b / 255.0)
            hue, sat = h, s
        else:
            raise ValueError("Expected 'h,s' or 'r,g,b'")

        await bridge._execute_with_control(lambda: bridge.robot.behavior.set_eye_color(hue, sat))
        bridge.publish_event("appearance", {"action": "eye_color", "hue": hue, "saturation": sat})
        return True

    @staticmethod
    async def handle_charger(bridge: 'AsyncVectorMQTTBridge', payload: str) -> bool:
        cmd = payload.strip().lower()
        if cmd not in ("on", "off"):
            raise ValueError("Expected 'on' or 'off'")
        charger_timeout = bridge.COMMAND_TIMEOUTS.get('charger', 30)

        if cmd == "off":
            await bridge._execute_with_control(lambda: bridge.robot.behavior.drive_off_charger(), timeout=charger_timeout)
            bridge.publish_event("movement", {"action": "charger", "direction": "off"})
            return True

        # Dock with retries
        for attempt in range(1, bridge.CHARGER_RETRIES + 2):
            try:
                await bridge._execute_with_control(lambda: bridge.robot.behavior.drive_on_charger(), timeout=charger_timeout)
                # Wait for status reflect
                docked = False
                for _ in range(12):
                    if getattr(bridge.robot.status, "is_on_charger", False):
                        docked = True
                        break
                    await asyncio.sleep(0.5)
                if docked:
                    bridge.publish_event("movement", {"action": "charger", "direction": "on", "attempt": attempt})
                    return True
            except Exception:
                pass
            # Nudge and retry
            await bridge._execute_with_control(lambda: bridge.robot.behavior.turn_in_place(degrees(30), speed=degrees(90)))
            await asyncio.sleep(bridge.CHARGER_RETRY_DELAY)

        raise RuntimeError("Failed to dock with charger after retries")

    @staticmethod
    async def handle_takephoto(bridge: 'AsyncVectorMQTTBridge', payload: str) -> bool:
        success = False
        target_path = (payload or "").strip()
        if not target_path:
            raise RuntimeError("Missing file path")

        await bridge.loop.run_in_executor(bridge.thread_pool, bridge.robot.camera.init_camera_feed)

        image = None
        start = time.time()
        while time.time() - start < bridge.CAMERA_FRAME_WAIT:
            try:
                img = bridge.robot.camera.latest_image
                if img and getattr(img, "raw_image", None) is not None:
                    _ = img.raw_image.size
                    image = img
                    break
            except Exception:
                await asyncio.sleep(0.1)
                continue
            await asyncio.sleep(0.05)

        if not image:
            raise RuntimeError("No image available from camera")

        directory = os.path.dirname(target_path)
        if directory and not os.path.exists(directory):
            try:
                os.makedirs(directory, exist_ok=True)
            except PermissionError:
                raise RuntimeError(f"Permission denied creating directory: {directory}")
            except Exception as e:
                raise RuntimeError(f"Could not create directory: {e}")

        try:
            image.raw_image.save(target_path, "JPEG")
            logger.info(f"Photo saved to {target_path}")
        except PermissionError:
            raise RuntimeError(f"Permission denied writing file: {target_path}")
        except Exception as e:
            raise RuntimeError(f"Error saving image: {e}")

        try:
            import io, base64
            img = image.raw_image
            # Optional resize
            if getattr(bridge, "CAMERA_MAX_WIDTH", 0) and img.width > bridge.CAMERA_MAX_WIDTH:
                new_h = int(img.height * (bridge.CAMERA_MAX_WIDTH / img.width))
                img = img.resize((bridge.CAMERA_MAX_WIDTH, new_h))
            quality = getattr(bridge, "CAMERA_JPEG_QUALITY", 85)
            buf = io.BytesIO()
            img.save(buf, format="JPEG", quality=quality)
            jpeg_bytes = buf.getvalue()
            bridge.latest_jpeg = jpeg_bytes
            bridge.latest_jpeg_ts = time.time()
            jpeg_b64 = base64.b64encode(jpeg_bytes).decode("ascii")
            bridge.mqtt_client.publish(f"{bridge.STATUS_BASE}/photo_b64", jpeg_b64, qos=bridge.EVENT_QOS, retain=False)
        except Exception as e:
            logger.debug(f"Failed to publish photo to MQTT: {e}")

        bridge.publish_event("camera", {
            "action": "photo_taken",
            "path": target_path,
            "size": [img.width, img.height]  # use the (possibly resized) image
        })
        success = True
        if success:
            asyncio.create_task(bridge.cleanup_camera_with_timeout(3.0))
        return True

    @staticmethod
    async def handle_getstatus(bridge: 'AsyncVectorMQTTBridge', payload: str) -> bool:
        battery_state = await bridge.loop.run_in_executor(bridge.thread_pool, bridge.robot.get_battery_state)

        def safe_get(obj, attr, default=None):
            return getattr(obj, attr, default)

        status_data = {
            "timestamp": datetime.datetime.now().isoformat(),
            "battery": {
                "level": safe_get(battery_state, 'battery_level') if battery_state else None,
                "volts": safe_get(battery_state, 'battery_volts') if battery_state else None,
                "is_charging": safe_get(battery_state, 'is_charging') if battery_state else None,
            }
        }
        status_data["status"] = {
            "is_moving": safe_get(bridge.robot.status, 'is_moving', False),
            "is_picked_up": safe_get(bridge.robot.status, 'is_picked_up', False),
            "is_on_charger": safe_get(bridge.robot.status, 'is_on_charger', False),
            "is_being_held": safe_get(bridge.robot.status, 'is_being_held', False),
            "is_cliff_detected": safe_get(bridge.robot.status, 'is_cliff_detected', False),
            "is_falling": safe_get(bridge.robot.status, 'is_falling', False),
            "is_animating": safe_get(bridge.robot.status, 'is_animating', False),
            "is_button_pressed": safe_get(bridge.robot.status, 'is_button_pressed', False)
        }
        if hasattr(bridge.robot, 'accel'):
            status_data["accelerometer"] = {"x": bridge.robot.accel.x, "y": bridge.robot.accel.y, "z": bridge.robot.accel.z}
        if hasattr(bridge.robot, 'pose') and hasattr(bridge.robot.pose, 'position'):
            status_data["position"] = {"x": bridge.robot.pose.position.x, "y": bridge.robot.pose.position.y, "z": bridge.robot.pose.position.z}
            if hasattr(bridge.robot.pose, 'head_angle'):
                status_data["head_angle_deg"] = bridge.robot.pose.head_angle.degrees
            if hasattr(bridge.robot.pose, 'lift_height'):
                status_data["lift_height_mm"] = bridge.robot.pose.lift_height.distance_mm

        bridge.publish_cmd_complete({"command": "getstatus", "status": "success", "data": status_data})
        bridge.mqtt_client.publish(bridge.STATUS_TOPIC_TELEMETRY, json.dumps(status_data), qos=bridge.TELEMETRY_QOS)
        logger.info("Published robot status")
        return True

    @staticmethod
    async def handle_animation(bridge: 'AsyncVectorMQTTBridge', payload: str) -> bool:
        if not payload or payload not in bridge.robot.anim.anim_list:
            raise ValueError(f"Animation '{payload}' not found")
        await bridge._execute_with_control(lambda: bridge.robot.anim.play_animation(payload))
        bridge.publish_event("animation", {"name": payload, "type": "animation"})
        return True

    @staticmethod
    async def handle_trigger(bridge: 'AsyncVectorMQTTBridge', payload: str) -> bool:
        if not payload or payload not in bridge.robot.anim.anim_trigger_list:
            raise ValueError(f"Animation trigger '{payload}' not found")
        await bridge._execute_with_control(lambda: bridge.robot.anim.play_animation_trigger(payload))
        bridge.publish_event("animation", {"name": payload, "type": "trigger"})
        return True

    @staticmethod
    async def handle_cube_lights(bridge: 'AsyncVectorMQTTBridge', payload: str) -> bool:
        parts = payload.split(',')
        if len(parts) != 4:
            raise ValueError("Expected 'object_id,r,g,b'")
        try:
            obj_id = int(parts[0]); r, g, b = [int(p) for p in parts[1:4]]
        except ValueError:
            raise ValueError("Values must be integers")
        if not all(0 <= c <= 255 for c in (r, g, b)):
            raise ValueError("RGB values must be 0-255")

        try:
            await bridge._execute_with_control(lambda: bridge.robot.world.connect_cube())
        except Exception:
            pass

        light = protocol.LightControlRequest.Light(
            on_color=protocol.Color(rgb=(r, g, b)),
            off_color=protocol.Color(rgb=(0, 0, 0)),
            on_period_ms=500, off_period_ms=500,
            transition_on_period_ms=100, transition_off_period_ms=100
        )
        await bridge.loop.run_in_executor(
            bridge.thread_pool,
            lambda: bridge.robot.conn.control_request(protocol.LightControlRequest(object_id=obj_id, lights=[light]))
        )
        bridge.publish_event("cube", {"action": "lights", "object_id": obj_id, "rgb": [r, g, b]})
        return True

    @staticmethod
    async def handle_sequence(bridge: 'AsyncVectorMQTTBridge', payload: str) -> bool:
        data = json.loads(payload)
        if not isinstance(data, list):
            raise ValueError("Expected a JSON array of command objects")
        for cmd in data:
            command = cmd.get("command")
            cmd_payload = cmd.get("payload", "")
            delay = float(cmd.get("delay", 0))
            handler = bridge.command_handlers.get(command)
            if not handler:
                raise ValueError(f"Unknown command in sequence: {command}")
            await handler(bridge, cmd_payload)
            if delay > 0:
                await asyncio.sleep(delay)
        return True

    @staticmethod
    async def handle_mapping(bridge: 'AsyncVectorMQTTBridge', payload: str) -> bool:
        if not hasattr(bridge.robot, 'nav_map'):
            raise RuntimeError("Mapping not supported on this SDK")
        command = payload.lower().strip()
        if command == "start":
            if not hasattr(bridge.robot.behavior, 'start_mapping'):
                raise RuntimeError("start_mapping not supported on this SDK")
            await bridge._execute_with_control(lambda: bridge.robot.behavior.start_mapping())
        elif command == "publish":
            nav_map = await bridge.loop.run_in_executor(bridge.thread_pool, bridge.robot.nav_map.get_nav_map)
            simplified_map = {
                "timestamp": datetime.datetime.now().isoformat(),
                "origin": {"x": nav_map.origin.x, "y": nav_map.origin.y},
                "cells": [{"x": c.x, "y": c.y, "type": c.cell_type} for c in nav_map.cells]
            }
            bridge.mqtt_client.publish(f"{bridge.STATUS_BASE}/map", json.dumps(simplified_map), qos=1)
        else:
            raise ValueError("mapping expects 'start' or 'publish'")
        return True

    @staticmethod
    async def handle_set_trigger(bridge: 'AsyncVectorMQTTBridge', payload: str) -> bool:
        trigger_data = json.loads(payload)
        event_type = trigger_data.get("event")
        action = trigger_data.get("action")
        if not event_type or not action or not isinstance(action, dict) or "command" not in action:
            raise ValueError("Invalid trigger: expect {'event': '...', 'action': {'command':'...', 'payload': '...'}}")
        bridge.event_triggers[event_type] = action
        logger.info(f"Registered trigger for {event_type}")
        return True

    @staticmethod
    async def handle_look_at_me(bridge: 'AsyncVectorMQTTBridge', payload: str) -> bool:
        control_acquired = False
        try:
            if not bridge.has_behavior_control:
                await bridge.loop.run_in_executor(bridge.thread_pool, lambda: bridge.robot.conn.request_control())
                control_acquired = True

            await bridge.loop.run_in_executor(bridge.thread_pool, lambda: bridge.robot.behavior.set_head_angle(degrees(35.0)))
            await asyncio.sleep(0.4)

            try:
                bridge._face_seen_event.clear()
            except Exception:
                pass
            bridge._last_seen_face = None

            found = None
            steps = 12; step_deg = 30.0; speed_deg = 120.0
            for _ in range(steps):
                await bridge.loop.run_in_executor(bridge.thread_pool, lambda: bridge.robot.behavior.turn_in_place(degrees(step_deg), speed=degrees(speed_deg)))
                await asyncio.sleep(0.15)
                try:
                    bridge._face_seen_event.clear()
                    await asyncio.wait_for(bridge._face_seen_event.wait(), timeout=0.6)
                    if bridge._last_seen_face:
                        found = bridge._last_seen_face
                        break
                except asyncio.TimeoutError:
                    pass

            if found:
                await bridge.loop.run_in_executor(bridge.thread_pool, lambda: bridge.robot.behavior.turn_towards_face(found))
                try:
                    await bridge.loop.run_in_executor(bridge.thread_pool, lambda: bridge.robot.anim.play_animation("anim_greeting_happy_01"))
                except Exception:
                    pass
                face_name = getattr(found, "name", "") or "unknown"
                bridge.publish_cmd_complete({"command": "look_at_me", "status": "success", "face": face_name})
            else:
                bridge.publish_cmd_complete({"command": "look_at_me", "status": "no_face_found"})
            return True
        finally:
            if control_acquired:
                try:
                    await bridge.loop.run_in_executor(bridge.thread_pool, lambda: bridge.robot.conn.release_control())
                except Exception:
                    pass

    @staticmethod
    async def handle_sleep(bridge: 'AsyncVectorMQTTBridge', payload: str) -> bool:
        try:
            try:
                await VectorCommands.handle_charger(bridge, "on")
            except Exception:
                pass
            await bridge._execute_with_control(lambda: bridge.robot.behavior.set_lift_height(0.0))
            await bridge._execute_with_control(lambda: bridge.robot.behavior.set_head_angle(degrees(-20.0)))
            try:
                if "anim_gotosleep_sleeploop_01" in getattr(bridge.robot.anim, "anim_list", []):
                    await bridge._execute_with_control(lambda: bridge.robot.anim.play_animation("anim_gotosleep_sleeploop_01"))
            except Exception:
                pass
            bridge.publish_event("state", {"action": "sleep"})
            bridge.publish_cmd_complete({"command": "sleep", "status": "success"})
            return True
        except Exception:
            raise

    @staticmethod
    async def handle_shutup(bridge: 'AsyncVectorMQTTBridge', payload: str) -> bool:
        if hasattr(bridge, 'continuous_movement_task') and bridge.continuous_movement_task:
            bridge.continuous_movement_task.cancel()
            bridge.continuous_movement_task = None
        await bridge._execute_with_control(lambda: bridge.robot.motors.set_wheel_motors(0, 0))
        try:
            await bridge._execute_with_control(lambda: bridge.robot.anim.play_animation("anim_neutral_eyes_01"))
        except Exception:
            pass
        bridge.publish_event("state", {"action": "shutup"})
        bridge.publish_cmd_complete({"command": "shutup", "status": "success"})
        return True

    @staticmethod
    async def handle_drive_continuous(bridge: 'AsyncVectorMQTTBridge', payload: str) -> bool:
        parts = payload.split(',')
        if len(parts) < 2:
            raise ValueError("Expected 'direction,speed'")
        direction = parts[0].lower()
        if direction not in ['forward', 'backward']:
            raise ValueError("Direction must be 'forward' or 'backward'")
        try:
            speed = float(parts[1])
        except ValueError:
            raise ValueError("Speed must be a number")
        if speed <= 0:
            raise ValueError("Speed must be positive")
        if hasattr(bridge, 'continuous_movement_task') and bridge.continuous_movement_task:
            bridge.continuous_movement_task.cancel()
        bridge.continuous_movement_task = asyncio.create_task(bridge._continuous_drive(direction, speed))
        bridge.publish_event("movement", {"action": "continuous_drive", "direction": direction, "speed": speed})
        return True

    @staticmethod
    async def handle_rotate_continuous(bridge: 'AsyncVectorMQTTBridge', payload: str) -> bool:
        parts = payload.split(',')
        if len(parts) < 2:
            raise ValueError("Expected 'direction,speed'")
        direction = parts[0].lower()
        if direction not in ['left', 'right']:
            raise ValueError("Direction must be 'left' or 'right'")
        try:
            speed = float(parts[1])
        except ValueError:
            raise ValueError("Speed must be a number")
        if speed <= 0:
            raise ValueError("Speed must be positive")
        if hasattr(bridge, 'continuous_movement_task') and bridge.continuous_movement_task:
            bridge.continuous_movement_task.cancel()
        bridge.continuous_movement_task = asyncio.create_task(bridge._continuous_rotate(direction, speed))
        bridge.publish_event("movement", {"action": "continuous_rotate", "direction": direction, "speed": speed})
        return True

    @staticmethod
    async def handle_stop_movement(bridge: 'AsyncVectorMQTTBridge', payload: str) -> bool:
        if hasattr(bridge, 'continuous_movement_task') and bridge.continuous_movement_task:
            bridge.continuous_movement_task.cancel()
            bridge.continuous_movement_task = None
        await bridge._execute_with_control(lambda: bridge.robot.motors.set_wheel_motors(0, 0))
        bridge.publish_event("movement", {"action": "stop"})
        return True

    @staticmethod
    async def handle_macro(bridge, payload):
        """Execute predefined command sequences."""
        macros = {
            "greeting": [
                {"command": "say", "payload": "Hello there!"},
                {"command": "animation", "payload": "anim_greeting_happy_01", "delay": 2}
            ],
            "bedtime": [
                {"command": "say", "payload": "Time for bed!"},
                {"command": "charger", "payload": "on", "delay": 2},
                {"command": "sleep", "payload": "", "delay": 0}
            ],
            "wake": [
                {"command": "charger", "payload": "off"},
                {"command": "say", "payload": "Good morning!", "delay": 1}
            ]
        }
        
        if payload in macros:
            return await VectorCommands.handle_sequence(bridge, json.dumps(macros[payload]))
        else:
            return bridge.handle_command_error("macro", f"Unknown macro: {payload}")

    @staticmethod
    async def handle_nodered_status(bridge: 'AsyncVectorMQTTBridge', payload: str) -> bool:
        """Send detailed status for Node-RED flows."""
        status = {
            "vector": {
                "connected": bridge.is_vector_connected,
                "battery": bridge.last_battery_level,
                "on_charger": bridge.robot.status.is_on_charger if bridge.robot else None,
                "has_control": bridge.has_behavior_control
            },
            "mqtt": {
                "connected": bridge.mqtt_client.is_connected(),
                "uptime": time.time() - bridge._start_time
            },
            "animations": {
                "count": len(bridge.robot.anim.anim_list) if bridge.robot else 0,
                "triggers": len(bridge.robot.anim.anim_trigger_list) if bridge.robot else 0
            },
            "version": getattr(bridge, "VERSION", None)
        }
        bridge.mqtt_client.publish(f"{bridge.STATUS_BASE}/nodered", json.dumps(status), qos=1)
        return True


# === main() ===
async def main():
    """Main entry point for the application."""
    config_file = 'config.ini'
    
    # Check if config file exists, if not, create a default one
    if not os.path.exists(config_file):
        logger.info(f"Config file {config_file} not found, creating default...")
        default_config = configparser.ConfigParser()
        
        default_config['Vector'] = {
            'serial': 'your_vector_serial',
            'ip': 'your_vector_ip',
            'name': 'your_vector_name'
        }
        
        default_config['MQTT'] = {
            'host': 'localhost',
            'port': '1883',
            'username': '',
            'password': '',
            'topic_base': 'vector'
        }
        
        default_config['Files'] = {
            'animation_data_pkl': 'animation_data.pkl',
            'state_json': 'bridge_state.json'
        }
        
        default_config['Settings'] = {
            'auto_connect': 'true',
            'auto_reconnect': 'true',
            'max_reconnect_attempts': '3',
            'reconnect_delay': '5',
            'command_timeout': '10',
            'telemetry_interval': '30',
            'heartbeat_interval': '10',
            'state_persistence': 'true',
            'activity_monitoring': 'true',
            'activity_min_interval': '5',
            'activity_qos': '0'
        }
        
        with open(config_file, 'w') as f:
            default_config.write(f)
        
        logger.info("Default config file created. Please edit with your Vector's details.")
        return
    
    config = configparser.ConfigParser()
    config.read(config_file)
    
    # Check if Vector settings are configured
    vector_section = config['Vector']
    if 'serial' not in vector_section or vector_section['serial'] == 'your_vector_serial':
        logger.error("Vector not configured! Please edit config.ini with your Vector's details.")
        return
    
    bridge = AsyncVectorMQTTBridge(config)
    await bridge.run()

if __name__ == '__main__':
    asyncio.run(main())