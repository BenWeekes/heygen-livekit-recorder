# HeyGen LiveKit Recorder

A Python tool for recording HeyGen avatar sessions using the LiveKit API. This tool allows you to stream audio to a HeyGen avatar and record the synchronized audio and video output.

## Setup

### Environment Setup

1. **Create a Python virtual environment** (Python 3.10 recommended):

```bash
# Create a directory for your project
mkdir heygen-livekit-recorder
cd heygen-livekit-recorder

# Create a virtual environment
python3.10 -m venv livekit-env-py310

# Activate the virtual environment
# On macOS/Linux:
source livekit-env-py310/bin/activate
# On Windows:
# livekit-env-py310\Scripts\activate
```

2. **Install the required packages**:

```bash
pip install --upgrade pip
pip install numpy opencv-python requests websocket-client livekit-client
```

### Required Dependencies

- **System Requirements**:
  - FFmpeg: Required for audio conversion

  ```bash
  # macOS (with Homebrew)
  brew install ffmpeg

  # Ubuntu/Debian
  sudo apt-get install ffmpeg

  # Windows
  # Download from https://ffmpeg.org/download.html#build-windows
  ```

## Usage

1. **Save the script** to a file named `heygen_livekit_pubsub.py`

2. **Prepare an input audio file** (WAV format is recommended)

3. **Run the script** with your HeyGen API key:

```bash
python heygen_livekit_pubsub.py --api-key "YOUR_HEYGEN_API_KEY" --wav-file "input.wav"
```

### Command-Line Options

```bash
python heygen_livekit_pubsub.py --api-key "YOUR_HEYGEN_API_KEY" \
                               --avatar-name "AVATAR_NAME" \
                               --output-dir "OUTPUT_DIRECTORY" \
                               --wav-file "PATH_TO_WAV_FILE"
```

- `--api-key`: Your HeyGen API key (required)
- `--avatar-name`: Name of the avatar to use (default: "Wayne_20240711")
- `--output-dir`: Directory to save recordings (default: "recordings")
- `--wav-file`: Path to the WAV file to send (default: "input.wav")

## Output Files

The script will save the following files in the output directory:

- PNG frames from the video feed in a timestamped directory (e.g., `recordings/Wayne_20240711_video_20250304_100533/`)
- WAV audio recording with timestamped filename (e.g., `recordings/Wayne_20240711_audio_20250304_100533.wav`)

## How It Works

1. The script authenticates with the HeyGen API
2. Creates and starts a streaming session with a specified avatar
3. Connects to the LiveKit room to receive audio/video streams
4. Sends audio data from the provided WAV file to the avatar via WebSocket
5. Records the avatar's audio and video responses
6. Automatically exits after the recording duration (default: 20 seconds)

## Troubleshooting

- If you encounter audio issues, ensure your WAV file is valid and compatible (16kHz mono is recommended)
- For WebSocket connection issues, check your internet connection and HeyGen API key validity
- If the program doesn't exit properly, check if FFmpeg is installed correctly
- If audio quality is poor, try using a higher quality input file

## Requirements

- Python 3.10 or higher
- FFmpeg
- Python modules:
  - numpy
  - opencv-python
  - requests
  - websocket-client
  - livekit-client

