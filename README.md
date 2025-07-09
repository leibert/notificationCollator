# Notification Collator

A Python service that collects notifications from multiple sources (Signal messenger, calendar events) and displays them on an LED sign via MQTT. It also monitors calendar events and TODOs, providing alerts for upcoming meetings.

## Features

- **Signal Message Integration**: Receives Signal messages via DBus and displays them on the LED sign
- **Calendar Integration**: Monitors calendar events and provides countdown alerts for upcoming meetings
- **TODO Management**: Tracks work and personal TODOs from calendar sources
- **LED Sign Control**: Displays messages and alerts on an LED sign via MQTT
- **Smart Meeting Alerts**: 
  - 5-minute countdown warnings for upcoming events
  - External meeting detection with beacon light activation
  - Automatic return to clock mode after events

## Prerequisites

### System Requirements

- Python 3.7+
- Linux system with DBus support
- [signal-cli](https://github.com/AsamK/signal-cli) installed and configured
- MQTT broker (e.g., Mosquitto)
- LED sign that accepts MQTT commands
- Calendar scraper service (optional, for calendar features)

### System Dependencies

For PyGObject support, install system packages:

**Ubuntu/Debian:**
```bash
sudo apt-get install python3-gi python3-gi-cairo gir1.2-gtk-3.0
```

**Fedora:**
```bash
sudo dnf install python3-gobject gtk3
```

**Arch Linux:**
```bash
sudo pacman -S python-gobject gtk3
```

## Installation

1. **Clone the repository:**
```bash
git clone https://github.com/yourusername/notification-collator.git
cd notification-collator
```

2. **Create a virtual environment:**
```bash
python3 -m venv env
source env/bin/activate  # On Windows: env\Scripts\activate
```

3. **Install Python dependencies:**
```bash
pip install -r requirements.txt
```

4. **Set up signal-cli:**
- Install signal-cli following instructions at https://github.com/AsamK/signal-cli
- Link with your phone: `signal-cli link -n "Notification Collator"`
- Start daemon: `signal-cli -u +YOUR_PHONE daemon`

5. **Configure environment variables:**

Create a `.env` file in the project root:
```env
# Required - MQTT Configuration
MQTT_BROKER=192.168.1.100
MQTT_USER=your_mqtt_username
MQTT_PASSWORD=your_mqtt_password

# Optional - Calendar Integration
CAL_SCRAPER_HOST=http://your-calendar-scraper:8080
```

## Usage

### Running the Service

1. **Start signal-cli daemon (in a separate terminal):**
```bash
signal-cli -u +YOUR_PHONE_NUMBER daemon
```

2. **Run the notification collator:**
```bash
python notificationCollator.py
```

### Running as a System Service

Create a systemd service file `/etc/systemd/system/notification-collator.service`:

```ini
[Unit]
Description=Notification Collator Service
After=network.target

[Service]
Type=simple
User=your_username
WorkingDirectory=/path/to/notification-collator
Environment="PATH=/path/to/notification-collator/env/bin"
ExecStart=/path/to/notification-collator/env/bin/python notificationCollator.py
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

Enable and start the service:
```bash
sudo systemctl enable notification-collator
sudo systemctl start notification-collator
```

## MQTT Topics

The service publishes to the following MQTT topics:

### LED Sign Control
- `ledSign/mode` - Display mode (bigClock, message, eventCountdown)
- `ledSign/message/type` - Message type (Signal, SMS, etc.)
- `ledSign/message/sender` - Message sender name
- `ledSign/message/text` - Message content

### Calendar Events
- `nextEvent/timeStamp` - Unix timestamp of next event
- `nextEvent/subject` - Event subject
- `nextEvent/organizer` - Event organizer
- `nextEvent/attendees` - Event attendees
- `nextEvent/isExternal` - Boolean for external meetings

### TODOs
- `nextTODO/work` - Next work TODO item
- `nextTODO/personal` - Next personal TODO item

### Other
- `beaconLight/activate` - Activate beacon for external meetings

## Architecture

```
┌─────────────────┐     ┌──────────────┐     ┌───────────┐
│   Signal CLI    │────▶│              │     │           │
│    (DBus)       │     │              │     │ LED Sign  │
└─────────────────┘     │ Notification │     │           │
                        │   Collator    │────▶│  (MQTT)   │
┌─────────────────┐     │              │     │           │
│ Calendar Scraper│────▶│              │     └───────────┘
│     (HTTP)      │     │              │
└─────────────────┘     └──────────────┘
```

## Configuration

### Message Display Timing

The display duration for messages is calculated based on length:
- Base time: 6 seconds
- Additional time: 0.75 seconds per character over 16 characters

### Calendar Alerts

- Events trigger countdown alerts 5 minutes (300 seconds) before start
- External meetings activate a beacon light
- Display returns to clock mode 2 minutes after event start

## Troubleshooting

### Common Issues

1. **"MQTT_BROKER environment variable is not set"**
   - Ensure `.env` file exists and contains MQTT configuration
   - Check that python-dotenv is installed

2. **Signal messages not received**
   - Verify signal-cli daemon is running
   - Check DBus path matches your signal-cli configuration
   - Update the DBus path in `SignalHandler.__init__` if needed

3. **Calendar features not working**
   - Verify CAL_SCRAPER_HOST is set and accessible
   - Check calendar scraper service is running

### Debug Mode

To enable more verbose logging, modify the logging level in the code:
```python
logger.setLevel(logging.DEBUG)
```

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments

- [signal-cli](https://github.com/AsamK/signal-cli) for Signal messaging integration
- [paho-mqtt](https://pypi.org/project/paho-mqtt/) for MQTT communication
- The Python community for excellent libraries and documentation
