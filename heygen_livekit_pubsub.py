import asyncio
import logging
import os
import argparse
import cv2
import numpy as np
import requests
import wave
import struct
import json
import ssl
import websocket
import time
import threading
import tempfile
import subprocess
import base64
import uuid
from datetime import datetime
from signal import SIGINT, SIGTERM

from livekit import rtc

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("HeyGenRecorder")

# Disable verbose websocket logging
logging.getLogger('websocket').setLevel(logging.WARNING)

# Global set to track async tasks
tasks = set()

def convert_float32_to_s16_pcm(float32_data):
    """Convert float32 audio data to 16-bit PCM"""
    # Scale to int16 range and clip
    int16_data = np.clip(float32_data * 32767, -32768, 32767).astype(np.int16)
    return int16_data

def ensure_wav_format(wav_file_path, output_sample_rate=16000, output_channels=1):
    """Ensure WAV file is in the correct format (16kHz mono)"""
    # Create a temporary file for the converted audio
    temp_file = tempfile.NamedTemporaryFile(suffix='.wav', delete=False)
    temp_file_path = temp_file.name
    temp_file.close()
    
    try:
        # Use ffmpeg to convert the WAV file
        cmd = [
            'ffmpeg',
            '-y',  # Overwrite output file if it exists
            '-i', wav_file_path,
            '-ar', str(output_sample_rate),  # Set sample rate
            '-ac', str(output_channels),     # Set channels
            '-f', 'wav',                     # Force WAV format
            temp_file_path
        ]
        
        subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        logger.info(f"Converted WAV file to {output_sample_rate}Hz {output_channels}-channel format")
        
        return temp_file_path
    except subprocess.CalledProcessError as e:
        logger.error(f"Error converting WAV file: {e}")
        if os.path.exists(temp_file_path):
            os.unlink(temp_file_path)
        return wav_file_path  # Return original if conversion fails

class HeyGenRecorder:
    def __init__(self, api_key, avatar_name="Wayne_20240711", output_dir="recordings", wav_file="input.wav"):
        self.api_key = api_key
        self.avatar_name = avatar_name
        self.output_dir = output_dir
        self.wav_file = wav_file
        self.session_id = None
        self.token = None
        self.ws = None
        self.formatted_wav_file = None
        self.realtime_endpoint = None
        self.stop_event = asyncio.Event()
        
        # Create output directory if it doesn't exist
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
            
    def get_heygen_token(self):
        """Get authentication token from HeyGen API"""
        logger.info("Hi! Getting HeyGen authentication token...")
        
        url = "https://api.heygen.com/v1/streaming.create_token"
        headers = {
            "x-api-key": self.api_key
        }
        
        response = requests.post(url, headers=headers, data="")
        response_data = response.json()
        
        if response.status_code != 200 or response_data.get("error"):
            logger.error(f"Failed to get HeyGen token: {response_data}")
            raise Exception(f"Failed to get HeyGen token: {response_data}")
            
        token = response_data.get("data", {}).get("token")
        if not token:
            logger.error("No token found in response")
            raise Exception("No token found in response")
            
        logger.info("Successfully obtained HeyGen token")
        self.token = token
        return token
        
    def create_streaming_session(self, token):
        """Create a streaming session with HeyGen API"""
        logger.info("Creating streaming session with HeyGen...")
        
        url = "https://api.heygen.com/v1/streaming.new"
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}"
        }
        
        payload = {
            "avatar_name": self.avatar_name,
            "quality": "low",
            "voice": {},
            "version": "v2",
            "video_encoding": "H264",
            "source": "sdk",
            "disable_idle_timeout": True
        }
        
        response = requests.post(url, headers=headers, json=payload)
        response_data = response.json()
        
        if response.status_code != 200 or response_data.get("code") != 100:
            logger.error(f"Failed to create streaming session: {response_data}")
            raise Exception(f"Failed to create streaming session: {response_data}")
            
        session_data = response_data.get("data", {})
        livekit_token = session_data.get("access_token")
        livekit_url = session_data.get("url")
        self.session_id = session_data.get("session_id")
        
        # Get the realtime_endpoint for sending audio
        self.realtime_endpoint = session_data.get("realtime_endpoint")
        if not self.realtime_endpoint:
            logger.warning("No realtime_endpoint found in response - this is needed for server-to-server audio streaming")
        else:
            logger.info(f"Found realtime_endpoint: {self.realtime_endpoint}")
        
        if not livekit_token or not livekit_url:
            logger.error("Missing LiveKit credentials in response")
            raise Exception("Missing LiveKit credentials in response")
            
        logger.info(f"Successfully created streaming session with ID: {self.session_id}")
        return livekit_url, livekit_token
        
    def start_streaming_session(self, token):
        """Start the streaming session with HeyGen API"""
        logger.info(f"Starting streaming session with ID: {self.session_id}...")
        
        url = "https://api.heygen.com/v1/streaming.start"
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}"
        }
        
        payload = {
            "session_id": self.session_id
        }
        
        response = requests.post(url, headers=headers, json=payload)
        response_data = response.json()
        
        if response.status_code != 200 or response_data.get("code") != 100:
            logger.error(f"Failed to start streaming session: {response_data}")
            raise Exception(f"Failed to start streaming session: {response_data}")
            
        logger.info(f"Successfully started streaming session with ID: {self.session_id}")

    def connect_to_websocket(self):
        """Connect to HeyGen WebSocket for streaming audio input"""
        if not self.realtime_endpoint:
            logger.error("No realtime_endpoint found - cannot connect WebSocket")
            return None
            
        logger.info(f"Connecting to HeyGen realtime WebSocket: {self.realtime_endpoint}")
        
        try:
            # Setup WebSocket connection with timeouts
            websocket.enableTrace(False)  # Disable tracing to reduce log clutter
            self.ws = websocket.create_connection(
                self.realtime_endpoint,
                timeout=5  # 5 second timeout
            )
            logger.info("Real-time WebSocket connection established")
            
            # Set the socket to non-blocking mode
            self.ws.sock.setblocking(False)
            
            # Try to get initial message with timeout
            try:
                # Make socket temporarily blocking with timeout for receive
                self.ws.sock.setblocking(True)
                self.ws.sock.settimeout(2)
                init_message = self.ws.recv()
                logger.info(f"Initial WebSocket message: {init_message[:200]}")
            except websocket.WebSocketTimeoutException:
                logger.info("No initial message received (timeout)")
            except Exception as e:
                logger.warning(f"Error receiving initial message: {e}")
            finally:
                # Return to non-blocking mode
                self.ws.sock.setblocking(False)
            
            return self.ws
            
        except Exception as e:
            logger.error(f"Error connecting to WebSocket: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return None

    def prepare_wav_file(self):
        """Ensure the WAV file is in the correct format for HeyGen (16kHz mono)"""
        self.formatted_wav_file = ensure_wav_format(self.wav_file)
        return self.formatted_wav_file

    def send_audio_to_websocket(self):
        """Send audio data from WAV file to HeyGen WebSocket with proper events"""
        if not self.ws:
            logger.error("WebSocket connection not established")
            return
            
        # Ensure WAV file is in correct format (16kHz mono)
        if not self.formatted_wav_file:
            self.prepare_wav_file()
        
        wav_file_path = self.formatted_wav_file
        logger.info(f"Sending audio from {wav_file_path} to WebSocket")
        
        try:
            with wave.open(wav_file_path, 'rb') as wav_file:
                sample_rate = wav_file.getframerate()
                num_channels = wav_file.getnchannels()
                sample_width = wav_file.getsampwidth()
                
                logger.info(f"WAV file info: {sample_rate}Hz, {num_channels} channels, {sample_width} bytes per sample")
                
                # Process the audio in chunks and send with proper format
                chunk_size = int(sample_rate * 0.1)  # 100ms chunks
                
                # Read all audio data
                wav_file.rewind()
                all_audio_data = wav_file.readframes(wav_file.getnframes())
                
                # Convert to 24kHz if needed
                if sample_rate != 24000:
                    logger.info(f"Converting audio from {sample_rate}Hz to 24000Hz for WebSocket")
                    
                    # Create a temporary file for conversion
                    with tempfile.NamedTemporaryFile(suffix='.wav', delete=False) as temp_file:
                        temp_path = temp_file.name
                        
                    # Use ffmpeg to convert to 24kHz
                    cmd = [
                        'ffmpeg',
                        '-y',  # Overwrite output file if it exists
                        '-f', 's16le',  # Force signed 16-bit little-endian format
                        '-ar', str(sample_rate),  # Input sample rate
                        '-ac', str(num_channels),  # Input channels
                        '-i', '-',  # Read from stdin
                        '-ar', '24000',  # Output sample rate
                        '-ac', '1',  # Output mono
                        '-f', 's16le',  # Output format
                        temp_path
                    ]
                    
                    try:
                        process = subprocess.Popen(
                            cmd, 
                            stdin=subprocess.PIPE, 
                            stdout=subprocess.PIPE, 
                            stderr=subprocess.PIPE
                        )
                        stdout_data, stderr_data = process.communicate(input=all_audio_data)
                        
                        # Read the converted audio
                        with open(temp_path, 'rb') as f:
                            converted_audio = f.read()
                            
                        # Update variables for the converted audio
                        all_audio_data = converted_audio
                        sample_rate = 24000
                        num_channels = 1
                        
                        logger.info(f"Successfully converted audio to 24kHz (new size: {len(all_audio_data)} bytes)")
                    except Exception as e:
                        logger.error(f"Error converting audio to 24kHz: {str(e)}")
                        import traceback
                        logger.error(traceback.format_exc())
                    finally:
                        # Clean up temp file
                        if os.path.exists(temp_path):
                            os.unlink(temp_path)
                
                # Send audio in chunks
                total_chunks = len(all_audio_data) // (2 * num_channels)  # 2 bytes per sample (16-bit)
                chunk_bytes = chunk_size * 2 * num_channels  # bytes per chunk
                
                logger.info(f"Sending audio in {total_chunks // chunk_size + 1} chunks of {chunk_size} samples each")
                
                # Check WebSocket connection before starting
                try:
                    # Make sure socket is ready for writing
                    self.ws.sock.setblocking(True)
                    self.ws.ping()
                    logger.info("WebSocket connection confirmed active")
                except Exception as e:
                    logger.error(f"WebSocket connection error before sending: {str(e)}")
                    import traceback
                    logger.error(traceback.format_exc())
                    return
                finally:
                    self.ws.sock.setblocking(False)
                
                # Setup for tracking response messages
                def ws_receiver():
                    """Background thread to read WebSocket responses"""
                    try:
                        self.ws.sock.setblocking(True)
                        self.ws.sock.settimeout(1.0)  # 1 second timeout
                        while not self.stop_event.is_set():
                            try:
                                msg = self.ws.recv()
                                logger.info(f"Received WebSocket message: {msg[:200]}...")
                            except websocket.WebSocketTimeoutException:
                                continue  # Just try again
                            except Exception as e:
                                if not self.stop_event.is_set():
                                    logger.warning(f"Error in WebSocket receiver: {e}")
                                break
                    except Exception as e:
                        logger.error(f"WebSocket receiver thread error: {e}")
                
                # Start WebSocket receiver thread
                receiver_thread = threading.Thread(target=ws_receiver)
                receiver_thread.daemon = True
                receiver_thread.start()
                
                # Send chunks with small delay between them
                for i in range(0, len(all_audio_data), chunk_bytes):
                    # Extract chunk
                    chunk = all_audio_data[i:i + chunk_bytes]
                    
                    if not chunk:
                        break
                    
                    # Generate unique UUID for this chunk's event_id
                    chunk_event_id = str(uuid.uuid4())
                    
                    # Create agent.speak event - exactly as in the sample
                    audio_base64 = base64.b64encode(chunk).decode('utf-8')
                    audio_event = {
                        "type": "agent.speak",
                        #"type": "agent.audio_buffer_append",                        
                        "audio": audio_base64,
                        "event_id": chunk_event_id
                    }
                    
                    # Log message details for debugging
                    if i % (chunk_bytes * 10) == 0:
                        logger.info(f"Sending chunk {i//chunk_bytes} with UUID event_id: {chunk_event_id}")
                        logger.info(f"Audio data sample (first 20 chars): {audio_base64[:20]}...")
                    
                    # Add error handling for send operation
                    retry_count = 0
                    max_retries = 3
                    success = False
                    
                    while retry_count < max_retries and not success:
                        try:
                            # Set socket to blocking mode for reliable sending
                            self.ws.sock.setblocking(True)
                            self.ws.send(json.dumps(audio_event))
                            success = True
                        except ssl.SSLWantWriteError:
                            # Socket buffer is full, wait and retry
                            retry_count += 1
                            logger.warning(f"Socket buffer full, retrying send (attempt {retry_count}/{max_retries})")
                            time.sleep(0.05 * retry_count)  # Exponential backoff
                        except websocket.WebSocketConnectionClosedException:
                            logger.error("WebSocket connection closed during send")
                            return
                        except Exception as e:
                            logger.error(f"Error sending chunk {i//chunk_bytes}: {str(e)}")
                            import traceback
                            logger.error(traceback.format_exc())
                            return
                        finally:
                            # Restore non-blocking mode
                            self.ws.sock.setblocking(False)
                    
                    if not success:
                        logger.error(f"Failed to send chunk {i//chunk_bytes} after {max_retries} attempts")
                        return
                    
                    # Add a larger delay between chunks to prevent buffer overflow and ensure proper processing
                    time.sleep(0.03)  # 30ms delay - increased from original 10ms
                    
                    # Log progress occasionally
                    if i % (chunk_bytes * 10) == 0:
                        logger.info(f"Sent {i//chunk_bytes} chunks of audio data")
                
                # Wait longer before sending the end event
                logger.info("Finished sending all audio chunks, waiting before sending end event...")
                time.sleep(0.5)  # 500ms delay before sending the end event
                
                # Store the last event ID for the end event
                last_event_id = str(uuid.uuid4())
                
                # Send agent.speak_end event with a fresh UUID
                end_event = {
                    "type": "agent.speak_end",
                    #"type": "agent.audio_buffer_commit",                    
                    "event_id": last_event_id
                }
                
                logger.info(f"Sending agent.speak_end with UUID event_id: {last_event_id}")
                
                try:
                    # Set socket to blocking mode for reliable sending
                    self.ws.sock.setblocking(True)
                    self.ws.send(json.dumps(end_event))
                    logger.info(f"Sent agent.speak_end event with UUID event_id: {last_event_id}")
                except Exception as e:
                    logger.error(f"Error sending agent.speak_end: {str(e)}")
                    import traceback
                    logger.error(traceback.format_exc())
                finally:
                    self.ws.sock.setblocking(False)
                
                # Wait for additional time to receive responses and ensure processing completes
                logger.info("Waiting for responses after sending all audio...")
                time.sleep(3)  # Wait 3 seconds for additional responses
                
                # Signal the receiver thread to stop
                self.stop_event.set()
                
                # Wait for receiver thread to finish
                receiver_thread.join(timeout=1.0)
                
                logger.info("Audio sending completed")
                
        except Exception as e:
            logger.error(f"Error in audio sending process: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            
        finally:
            # Clean up temporary file if it was created
            if self.formatted_wav_file != self.wav_file and os.path.exists(self.formatted_wav_file):
                try:
                    os.unlink(self.formatted_wav_file)
                    logger.info(f"Cleaned up temporary WAV file {self.formatted_wav_file}")
                except Exception as e:
                    logger.warning(f"Failed to clean up temporary WAV file: {e}")

    async def record(self, livekit_url, livekit_token):
        """Main recording function"""
        # Create a new Room instance
        room = rtc.Room()
        
        # Track start time to implement auto-exit after recording_duration seconds
        start_time = time.time()
        recording_duration = 20  # seconds
        
        # Define track_subscribed callback
        @room.on("track_subscribed")
        def on_track_subscribed(track, publication, participant):
            # Access global tasks
            global tasks
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            if track.kind == rtc.TrackKind.KIND_VIDEO:
                logger.info(f"Subscribed to video track: {track.name} from {participant.identity}")
                output_path = os.path.join(self.output_dir, f"{self.avatar_name}_video_{timestamp}.mp4")
                
                # Create a video stream and start processing frames
                video_stream = rtc.VideoStream(track, format=rtc.VideoBufferType.RGB24)
                
                # Create a task for processing video frames
                task = asyncio.create_task(self.process_video_frames(video_stream, output_path))
                tasks.add(task)
                task.add_done_callback(tasks.remove)
                
            elif track.kind == rtc.TrackKind.KIND_AUDIO:
                logger.info(f"Subscribed to audio track: {track.name} from {participant.identity}")
                output_path = os.path.join(self.output_dir, f"{self.avatar_name}_audio_{timestamp}.wav")
                
                # Create an audio stream and start processing frames
                audio_stream = rtc.AudioStream(track)
                
                # Create a task for processing audio frames
                task = asyncio.create_task(self.process_audio_frames(audio_stream, output_path))
                tasks.add(task)
                task.add_done_callback(tasks.remove)
        
        # Track receive event handler
        @room.on("data_received")
        def on_data_received(data, participant=None):
            try:
                # Try to decode as string first
                if isinstance(data, bytes):
                    message = data.decode('utf-8')
                    logger.info(f"Received data from room: {message[:200]}...")
                else:
                    logger.info(f"Received non-string data from room: {type(data)}")
            except Exception as e:
                logger.error(f"Error decoding data: {e}")
        
        # Track signaling message event handler
        @room.on("message")
        def on_message(message):
            logger.info(f"Received signaling message: {message[:200]}...")

        try:
            # Connect to the LiveKit room
            await room.connect(livekit_url, livekit_token)
            logger.info(f"Connected to room: {room.name}")
            
            # Prepare the WAV file (convert to 16kHz mono if needed)
            self.prepare_wav_file()
            
            # Connect to the HeyGen WebSocket for audio input
            self.connect_to_websocket()
            
            # Give the connections time to establish
            await asyncio.sleep(1)
            
            # Reset the stop event
            self.stop_event.clear()
            
            # Create a separate thread for sending audio to the WebSocket
            audio_thread = threading.Thread(target=self.send_audio_to_websocket)
            audio_thread.daemon = True
            audio_thread.start()
            
            # Wait for recording_duration seconds or until manually stopped
            while time.time() - start_time < recording_duration and not self.stop_event.is_set():
                # Log remaining time occasionally
                elapsed = time.time() - start_time
                remaining = recording_duration - elapsed
                if int(elapsed) % 5 == 0 and int(elapsed) > 0:
                    logger.info(f"Recording in progress: {int(elapsed)}s elapsed, {int(remaining)}s remaining until auto-exit")
                await asyncio.sleep(1)
                
            logger.info(f"Auto-exiting after {recording_duration} seconds of recording")
            
        except Exception as e:
            logger.error(f"Error during recording: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
        finally:
            # Set the stop event to signal threads to close
            self.stop_event.set()
            
            # Make sure the audio thread has time to clean up
            time.sleep(0.5)
            
            # Cancel any remaining tasks
            for task in list(tasks):
                task.cancel()
            
            # Disconnect from the room and close WebSocket
            if self.ws:
                try:
                    self.ws.close()
                    logger.info("WebSocket connection closed")
                except Exception as e:
                    logger.warning(f"Error closing WebSocket: {e}")
                    
            await room.disconnect()
            logger.info("Disconnected from room")

    async def process_video_frames(self, video_stream, output_path):
        """Process and save video frames as PNG images every second"""
        # Create a directory for the PNG files based on the output path
        png_dir = output_path.replace('.mp4', '')
        if not os.path.exists(png_dir):
            os.makedirs(png_dir)
            
        logger.info(f"Saving PNG frames to directory: {png_dir}")
        
        frame_count = 0
        last_save_time = 0
        
        try:
            async for frame_event in video_stream:
                buffer = frame_event.frame
                
                # Convert buffer to numpy array
                arr = np.frombuffer(buffer.data, dtype=np.uint8)
                arr = arr.reshape((buffer.height, buffer.width, 3))
                
                # Convert from RGB to BGR for OpenCV
                arr = cv2.cvtColor(arr, cv2.COLOR_RGB2BGR)
                
                # Get current time in seconds
                current_time = int(datetime.now().timestamp())
                
                # Save a frame every second
                if current_time > last_save_time:
                    # Create a filename with timestamp
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    png_path = os.path.join(png_dir, f"frame_{timestamp}.png")
                    
                    # Save the frame as PNG
                    cv2.imwrite(png_path, arr)
                    logger.info(f"Saved frame to: {png_path}")
                    
                    # Update last save time
                    last_save_time = current_time
                
                frame_count += 1
                
                # Check if we should stop
                if self.stop_event.is_set():
                    logger.info("Stop event received, stopping video processing")
                    break
                
        except asyncio.CancelledError:
            logger.info("Video processing cancelled")
            raise
        except Exception as e:
            logger.error(f"Error processing video frames: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
        finally:
            logger.info(f"Saved {frame_count} frames in total")
            
    async def process_audio_frames(self, audio_stream, output_path):
        """Process and save audio frames to a WAV file"""
        logger.info(f"Starting audio recording to: {output_path}")
        wav_file = None
        sample_rate = 48000  # Standard WebRTC audio sample rate
        channels = 1         # Mono audio
        
        try:
            # Open WAV file for writing
            wav_file = wave.open(output_path, 'wb')
            wav_file.setnchannels(channels)
            wav_file.setsampwidth(2)  # 16-bit PCM
            wav_file.setframerate(sample_rate)
            
            sample_count = 0
            
            async for frame_event in audio_stream:
                # Get audio data
                audio_frame = frame_event.frame
                
                # Convert audio samples to int16 and write to WAV file
                for sample in audio_frame.data:
                    # Ensure the sample is within the valid range for int16
                    clamped_sample = max(min(sample, 32767), -32768)
                    # Convert to bytes and write to the WAV file
                    wav_file.writeframes(struct.pack('<h', int(clamped_sample)))
                    sample_count += 1
                
                if sample_count % 48000 == 0:  # Log every second of audio
                    seconds = sample_count // 48000
                    logger.info(f"Recorded {seconds} seconds of audio")
                
                # Check if we should stop
                if self.stop_event.is_set():
                    logger.info("Stop event received, stopping audio processing")
                    break
                
        except asyncio.CancelledError:
            logger.info("Audio processing cancelled")
            raise
        except Exception as e:
            logger.error(f"Error processing audio frames: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
        finally:
            # Close the WAV file
            if wav_file:
                wav_file.close()
                logger.info(f"Audio recording saved to: {output_path}")
                logger.info(f"Recorded {sample_count / 48000:.2f} seconds of audio")

async def main():
    parser = argparse.ArgumentParser(description='Record HeyGen avatar streams and publish audio')
    parser.add_argument('--api-key', required=True, help='HeyGen API key')
    parser.add_argument('--avatar-name', default='Wayne_20240711', help='Avatar name to use')
    parser.add_argument('--output-dir', default='recordings', help='Output directory for recordings')
    parser.add_argument('--wav-file', default='input.wav', help='WAV file to send to HeyGen')
    
    args = parser.parse_args()
    
    try:
        # Create recorder instance
        recorder = HeyGenRecorder(args.api_key, args.avatar_name, args.output_dir, args.wav_file)
        
        # Get HeyGen token
        heygen_token = recorder.get_heygen_token()
        
        # Create streaming session
        livekit_url, livekit_token = recorder.create_streaming_session(heygen_token)
        
        time.sleep(3)

        # Start streaming session
        recorder.start_streaming_session(heygen_token)
        
        # Start recording with timeout
        try:
            await asyncio.wait_for(recorder.record(livekit_url, livekit_token), timeout=25)
        except asyncio.TimeoutError:
            logger.info("Recording timeout reached, forcing shutdown")
    finally:
        # Signal to the event loop that we're done
        loop = asyncio.get_event_loop()
        loop.stop()
        
        # Force exit after a delay to allow cleanup
        logger.info("Main function completed, stopping event loop and scheduling exit")
        loop.call_later(2, lambda: os._exit(0))

async def graceful_shutdown():
    """Gracefully shut down all tasks and the event loop"""
    logger.info("Initiating graceful shutdown...")
    
    # Cancel all tasks except the current one
    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    for task in tasks:
        task.cancel()
    
    # Wait for all tasks to complete with a timeout
    if tasks:
        await asyncio.wait(tasks, timeout=5)
    
    # Stop the event loop
    asyncio.get_event_loop().stop()
    logger.info("Graceful shutdown completed")

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    
    # Handle graceful shutdown
    for signal in [SIGINT, SIGTERM]:
        loop.add_signal_handler(signal, lambda: asyncio.create_task(graceful_shutdown()))
    
    try:
        main_task = loop.create_task(main())
        loop.run_until_complete(main_task)
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
        # Force exit after a short delay if interrupted
        time.sleep(1)
        os._exit(0)