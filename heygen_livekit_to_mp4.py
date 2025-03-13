#!/usr/bin/env python3
import sys
import os
import asyncio
import argparse
import logging
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
import cv2
from datetime import datetime
from signal import SIGINT, SIGTERM

from livekit import rtc

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    stream=sys.stdout  # Explicitly log to stdout
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

def get_wav_duration(wav_file_path):
    """Get the duration of a WAV file in seconds"""
    try:
        with wave.open(wav_file_path, 'rb') as wav_file:
            frames = wav_file.getnframes()
            rate = wav_file.getframerate()
            duration = frames / float(rate)
            return duration
    except Exception as e:
        logger.error(f"Error getting WAV duration: {e}")
        return 20  # Default to 20 seconds if unable to determine

class HeyGenDualRecorder:
    def __init__(self, api_key, avatar_name="Wayne_20240711", output_dir="recordings", wav_file="input.wav"):
        print(f"Initializing HeyGenDualRecorder with avatar={avatar_name}, wav={wav_file}")
        sys.stdout.flush()
        
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
        self.audio_sent_event = asyncio.Event()  # New event to track when audio sending is complete
        
        # Video and audio file paths
        self.video_file_path = None
        self.audio_file_path = None
        self.wav_output_path = None  # Path for the original WAV file
        
        # Frame rate and dimensions for video
        self.frame_width = 720
        self.frame_height = 1280
        self.fps = 30
        
        # Audio parameters
        self.audio_sample_rate = 48000
        self.audio_channels = 1
        
        # Recording duration based on input WAV file
        self.recording_duration = 20  # default, will be updated with actual WAV duration
        self.input_wav_duration = None  # Will be set after analyzing the input file
        
        # Create output directory if it doesn't exist
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
            print(f"Created output directory: {output_dir}")
        
        print("HeyGenDualRecorder initialized")
        sys.stdout.flush()
            
    def get_heygen_token(self):
        """Get authentication token from HeyGen API"""
        print("Getting HeyGen authentication token...")
        sys.stdout.flush()
        logger.info("Getting HeyGen authentication token...")
        
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
        print("Creating streaming session with HeyGen...")
        sys.stdout.flush()
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
        print(f"Starting streaming session with ID: {self.session_id}...")
        sys.stdout.flush()
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
        
        # Get the duration of the input WAV file
        self.input_wav_duration = get_wav_duration(self.formatted_wav_file)
        logger.info(f"Input WAV file duration: {self.input_wav_duration:.2f} seconds")
        
        # Set recording duration to match input WAV file plus a buffer
        self.recording_duration = self.input_wav_duration + 5  # Add 5 seconds buffer
        logger.info(f"Setting recording duration to {self.recording_duration:.2f} seconds")
        
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
                # chunk_size = int(sample_rate * 0.1)  # 100ms chunks
                chunk_size = int(sample_rate * 0.5)  # 500ms chunks (5x larger than before)
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
                            time.sleep(0.01 * retry_count)  # Exponential backoff
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
                    
                    
                    time.sleep(0.001)  # 1ms delay 
                    
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
                
                # Signal that audio sending is complete
                self.audio_sent_event.set()
                logger.info("Audio sending completed and signaled with event")
                
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
        """Main recording function with dual output (WAV and MP4)"""
        print("Starting dual-format recording function...")
        sys.stdout.flush()
        logger.info("Starting dual-format recording function...")
        
        # Create a new Room instance
        room = rtc.Room()
        
        # Track start time to implement auto-exit after recording_duration seconds
        start_time = time.time()
        
        # Generate a unique timestamp for this recording
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Create output paths for both WAV and MP4
        mp4_output_path = os.path.join(self.output_dir, f"{self.avatar_name}_{timestamp}.mp4")
        self.wav_output_path = os.path.join(self.output_dir, f"{self.avatar_name}_{timestamp}_audio.wav")
        
        # Create temporary file for video
        video_file = tempfile.NamedTemporaryFile(suffix='.yuv', delete=False)
        self.video_file_path = video_file.name
        video_file.close()
        
        logger.info(f"Created output paths: mp4={mp4_output_path}, wav={self.wav_output_path}")
        logger.info(f"Created temporary video file: {self.video_file_path}")
        
        # Define track_subscribed callback
        @room.on("track_subscribed")
        def on_track_subscribed(track, publication, participant):
            # Access global tasks
            global tasks
            
            if track.kind == rtc.TrackKind.KIND_VIDEO:
                logger.info(f"Subscribed to video track: {track.name} from {participant.identity}")
                
                # Create a video stream
                video_stream = rtc.VideoStream(track, format=rtc.VideoBufferType.RGB24)
                
                # Create a task for processing video frames
                task = asyncio.create_task(self.process_video_frames(video_stream))
                tasks.add(task)
                task.add_done_callback(tasks.remove)
                
            elif track.kind == rtc.TrackKind.KIND_AUDIO:
                logger.info(f"Subscribed to audio track: {track.name} from {participant.identity}")
                
                # Create an audio stream
                audio_stream = rtc.AudioStream(track)
                
                # Create a task for processing audio frames
                task = asyncio.create_task(self.process_audio_frames(audio_stream))
                tasks.add(task)
                task.add_done_callback(tasks.remove)
        
        try:
            # Reset the audio sent event
            self.audio_sent_event.clear()
            
            # Connect to the LiveKit room
            await room.connect(livekit_url, livekit_token)
            logger.info(f"Connected to room: {room.name}")
            
            # Prepare the WAV file (convert to 16kHz mono if needed) and get its duration
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
            
            # Wait for the longer of: recording_duration seconds OR until the audio sending is complete plus buffer
            while (time.time() - start_time < self.recording_duration) and not self.stop_event.is_set():
                # Log remaining time occasionally
                elapsed = time.time() - start_time
                remaining = self.recording_duration - elapsed
                if int(elapsed) % 5 == 0 and int(elapsed) > 0:
                    logger.info(f"Recording in progress: {int(elapsed)}s elapsed, {int(remaining):.1f}s remaining until auto-exit")
                
                # Check if audio sending is complete and add additional buffer
                if self.audio_sent_event.is_set():
                    logger.info("Audio sending is complete, recording for additional buffer time")
                    # Continue recording for 3 more seconds to capture all output
                    await asyncio.sleep(3)
                    # If we're past 80% of the original duration, just exit
                    if elapsed > (self.recording_duration * 0.8):
                        logger.info("Reached sufficient duration after audio completion, exiting recording")
                        break
                
                await asyncio.sleep(1)
                
            logger.info(f"Auto-exiting after {time.time() - start_time:.2f} seconds of recording")
            
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
            
            # Combine video and saved WAV audio into MP4 using FFmpeg
            if os.path.exists(self.video_file_path) and os.path.exists(self.wav_output_path):
                logger.info(f"Combining video and WAV audio into MP4: {mp4_output_path}")
                
                try:
                    # Add detailed metadata logging before FFmpeg
                    logger.info(f"Video dimensions: {self.frame_width}x{self.frame_height}")
                    logger.info(f"Video file size: {os.path.getsize(self.video_file_path)} bytes")
                    logger.info(f"Audio file size: {os.path.getsize(self.wav_output_path)} bytes")

                    # Get audio duration from the WAV file
                    with wave.open(self.wav_output_path, 'rb') as wav_file:
                        audio_frames = wav_file.getnframes()
                        audio_rate = wav_file.getframerate()
                        audio_duration = audio_frames / float(audio_rate)
                        logger.info(f"Audio duration from WAV file: {audio_duration:.2f} seconds")

                    # Estimate video duration based on file size
                    frame_size = self.frame_width * self.frame_height * 3  # RGB24 = 3 bytes per pixel
                    total_frames = os.path.getsize(self.video_file_path) / frame_size
                    video_duration = total_frames / self.fps
                    logger.info(f"Estimated video duration: {video_duration:.2f} seconds ({total_frames:.1f} frames at {self.fps} fps)")

                    # Use the longer of the two durations
                    target_duration = max(audio_duration, video_duration)
                    logger.info(f"Target output duration: {target_duration:.2f} seconds")
                    
                    # Updated FFmpeg command with explicit duration
                    cmd = [
                        'ffmpeg',
                        '-y',  # Overwrite output file if it exists
                        
                        # Video input with explicit duration
                        '-f', 'rawvideo',
                        '-vcodec', 'rawvideo',
                        '-pix_fmt', 'rgb24',
                        '-s', f'{self.frame_width}x{self.frame_height}',
                        '-r', str(self.fps),
                        '-i', self.video_file_path,
                        
                        # Audio input 
                        '-i', self.wav_output_path,
                        
                        # Map both streams
                        '-map', '0:v',  # First input's video
                        '-map', '1:a',  # Second input's audio
                        
                        # Video codec settings
                        '-c:v', 'libx264',
                        '-preset', 'medium',
                        '-crf', '23',
                        '-pix_fmt', 'yuv420p',
                        
                        # Audio codec settings
                        '-c:a', 'aac',
                        '-b:a', '256k', 
                        
                        # Ensure entire duration is processed
                        '-avoid_negative_ts', 'make_zero',
                        '-max_interleave_delta', '0',
                        
                        # Ensure proper sync
                        '-vsync', 'cfr',
                        '-async', '1',
                        
                        # Don't use -shortest, allow full duration
                        mp4_output_path
                    ]
                    
                    logger.info(f"Running FFmpeg command: {' '.join(cmd)}")
                    
                    result = subprocess.run(
                        cmd,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                        text=True
                    )
                    
                    if result.returncode == 0:
                        logger.info(f"Successfully created MP4 file: {mp4_output_path}")
                    else:
                        logger.error(f"FFmpeg error (return code {result.returncode}): {result.stderr}")
                except Exception as e:
                    logger.error(f"Error creating MP4 file: {e}")
                    import traceback
                    logger.error(traceback.format_exc())
            
            # Clean up temporary video file
            try:
                if self.video_file_path and os.path.exists(self.video_file_path):
                    os.unlink(self.video_file_path)
                    logger.info(f"Cleaned up temporary file: {self.video_file_path}")
            except Exception as e:
                logger.warning(f"Error cleaning up temporary files: {e}")
            
            # Disconnect from the room and close WebSocket
            if self.ws:
                try:
                    self.ws.close()
                    logger.info("WebSocket connection closed")
                except Exception as e:
                    logger.warning(f"Error closing WebSocket: {e}")
                    
            await room.disconnect()
            logger.info("Disconnected from room")

    async def process_video_frames(self, video_stream):
        """Process video frames and save to temporary file"""
        print("Starting video frame processing...")
        sys.stdout.flush()
        logger.info("Starting video frame processing...")
        
        frame_count = 0
        first_frame = True
        
        try:
            # Open the video file for writing
            with open(self.video_file_path, 'wb') as video_file:
                async for frame_event in video_stream:
                    buffer = frame_event.frame
                    
                    # Convert buffer to numpy array
                    arr = np.frombuffer(buffer.data, dtype=np.uint8)
                    arr = arr.reshape((buffer.height, buffer.width, 3))
                    
                    # Set frame dimensions on first frame
                    if first_frame:
                        self.frame_height, self.frame_width = arr.shape[0], arr.shape[1]
                        logger.info(f"Video frame dimensions: {self.frame_width}x{self.frame_height}")
                        first_frame = False
                    
                    # Write raw frame data to file
                    video_file.write(arr.tobytes())
                    
                    frame_count += 1
                    
                    # Log progress occasionally
                    if frame_count % 30 == 0:  # Log once per second at 30fps
                        logger.info(f"Processed {frame_count} video frames")
                    
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
            logger.info(f"Processed {frame_count} video frames in total")

    async def process_audio_frames(self, audio_stream):
        """Process audio frames using the original sample-by-sample method"""
        print("Starting audio frame processing using original method...")
        sys.stdout.flush()
        logger.info("Starting audio frame processing using original method...")
        
        sample_rate = 48000  # Standard WebRTC audio sample rate
        channels = 1         # Mono audio
        sample_count = 0
        
        try:
            # Open WAV file for writing
            wav_file = wave.open(self.wav_output_path, 'wb')
            wav_file.setnchannels(channels)
            wav_file.setsampwidth(2)  # 16-bit PCM
            wav_file.setframerate(sample_rate)
            
            try:
                async for frame_event in audio_stream:
                    # Get audio data
                    audio_frame = frame_event.frame
                    
                    if audio_frame and audio_frame.data:
                        # Process each sample individually - exactly as in the original script
                        for sample in audio_frame.data:
                            # Ensure the sample is within the valid range for int16
                            clamped_sample = max(min(sample, 32767), -32768)
                            # Convert to bytes and write to the WAV file
                            wav_file.writeframes(struct.pack('<h', int(clamped_sample)))
                            sample_count += 1
                    
                    # Log progress occasionally (every ~1 second)
                    if sample_count % sample_rate < 100 and sample_count > 0:
                        seconds = sample_count // sample_rate
                        logger.info(f"Processed {seconds} seconds of audio")
                    
                    # Check if we should stop
                    if self.stop_event.is_set():
                        logger.info("Stop event received, stopping audio processing")
                        break
            finally:
                # Close the WAV file
                wav_file.close()
                logger.info(f"Closed WAV file: {self.wav_output_path}")
        
        except asyncio.CancelledError:
            logger.info("Audio processing cancelled")
            raise
        except Exception as e:
            logger.error(f"Error processing audio frames: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
        finally:
            logger.info(f"Processed {sample_count / sample_rate:.2f} seconds of audio")


async def main():
    print("Starting HeyGen Dual Output Recorder...")
    sys.stdout.flush()
    
    parser = argparse.ArgumentParser(description='Record HeyGen avatar streams to both WAV and MP4')
    parser.add_argument('--api-key', required=True, help='HeyGen API key')
    parser.add_argument('--avatar-name', default='Wayne_20240711', help='Avatar name to use')
    parser.add_argument('--output-dir', default='recordings', help='Output directory for recordings')
    parser.add_argument('--wav-file', default='input.wav', help='WAV file to send to HeyGen')
    parser.add_argument('--debug', action='store_true', help='Enable debug mode with extra logging')
    
    args = parser.parse_args()
    
    # Configure more verbose logging if debug mode is enabled
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
        print(f"Debug mode enabled, set logging level to DEBUG")
        sys.stdout.flush()
    
    # Check if FFmpeg is available
    print("Checking FFmpeg availability...")
    sys.stdout.flush()
    try:
        subprocess.run(['ffmpeg', '-version'], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        print("FFmpeg is available!")
        sys.stdout.flush()
    except (subprocess.SubprocessError, FileNotFoundError):
        print("ERROR: FFmpeg is not installed or not found in PATH. Please install FFmpeg.")
        sys.stdout.flush()
        return 1
    
    # Check if the WAV file exists
    if not os.path.exists(args.wav_file):
        print(f"ERROR: WAV file not found: {args.wav_file}")
        sys.stdout.flush()
        return 1
    
    print(f"Using WAV file: {args.wav_file}")
    print(f"Avatar name: {args.avatar_name}")
    print(f"Output directory: {args.output_dir}")
    
    # Get input WAV duration
    input_duration = get_wav_duration(args.wav_file)
    print(f"Input WAV duration: {input_duration:.2f} seconds")
    print(f"Recording duration will match input audio plus buffer time")
    sys.stdout.flush()
    
    try:
        # Create recorder instance
        print("Creating HeyGenDualRecorder instance...")
        sys.stdout.flush()
        
        recorder = HeyGenDualRecorder(args.api_key, args.avatar_name, args.output_dir, args.wav_file)
        
        # Get HeyGen token
        print("Getting HeyGen authentication token...")
        sys.stdout.flush()
        try:
            heygen_token = recorder.get_heygen_token()
            print("Successfully got HeyGen token.")
            sys.stdout.flush()
        except Exception as e:
            print(f"ERROR getting HeyGen token: {e}")
            sys.stdout.flush()
            raise
        
        # Create streaming session
        print("Creating streaming session...")
        sys.stdout.flush()
        try:
            livekit_url, livekit_token = recorder.create_streaming_session(heygen_token)
            print(f"Successfully created streaming session. LiveKit URL: {livekit_url}")
            sys.stdout.flush()
        except Exception as e:
            print(f"ERROR creating streaming session: {e}")
            sys.stdout.flush()
            raise
        
        # Wait for session initialization
        print("Waiting for session initialization...")
        sys.stdout.flush()
        time.sleep(3)

        # Start streaming session
        print("Starting streaming session...")
        sys.stdout.flush()
        try:
            recorder.start_streaming_session(heygen_token)
            print("Successfully started streaming session.")
            sys.stdout.flush()
        except Exception as e:
            print(f"ERROR starting streaming session: {e}")
            sys.stdout.flush()
            raise
        
        # Start recording with timeout based on input audio plus buffer
        total_duration = input_duration + 10  # Input duration plus 10 second buffer
        print(f"Starting dual-format recording for up to {total_duration:.1f} seconds...")
        sys.stdout.flush()
        try:
            await asyncio.wait_for(recorder.record(livekit_url, livekit_token), timeout=total_duration)
            print("Recording completed successfully.")
            sys.stdout.flush()
        except asyncio.TimeoutError:
            print("Recording timeout reached, forcing shutdown")
            sys.stdout.flush()
        except Exception as e:
            print(f"ERROR during recording: {e}")
            sys.stdout.flush()
            import traceback
            print(traceback.format_exc())
            sys.stdout.flush()
    except Exception as e:
        print(f"ERROR in main function: {e}")
        sys.stdout.flush()
        import traceback
        print(traceback.format_exc())
        sys.stdout.flush()
        return 1
    finally:
        print("Cleaning up and exiting...")
        sys.stdout.flush()
        # Signal to the event loop that we're done
        loop = asyncio.get_event_loop()
        loop.stop()
        
        # Force exit after a delay to allow cleanup
        print("Main function completed, stopping event loop and scheduling exit")
        sys.stdout.flush()
        loop.call_later(2, lambda: os._exit(0))
    
    return 0


async def graceful_shutdown():
    """Gracefully shut down all tasks and the event loop"""
    print("Initiating graceful shutdown...")
    sys.stdout.flush()
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
    print("Starting HeyGen Dual Output Recorder script...")
    sys.stdout.flush()
    
    try:
        # Check for required dependencies
        print("Checking for FFmpeg...")
        sys.stdout.flush()
        try:
            subprocess.run(['ffmpeg', '-version'], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            print("FFmpeg found!")
            sys.stdout.flush()
        except (subprocess.SubprocessError, FileNotFoundError):
            print("ERROR: FFmpeg is not installed or not found in PATH. Please install FFmpeg.")
            sys.stdout.flush()
            sys.exit(1)
            
        print("Setting up asyncio event loop...")
        sys.stdout.flush()
        loop = asyncio.get_event_loop()
        
        # Handle graceful shutdown
        for signal in [SIGINT, SIGTERM]:
            loop.add_signal_handler(signal, lambda: asyncio.create_task(graceful_shutdown()))
        
        print("Creating main task...")
        sys.stdout.flush()
        main_task = loop.create_task(main())
        print("Running event loop...")
        sys.stdout.flush()
        loop.run_until_complete(main_task)
    except KeyboardInterrupt:
        print("Interrupted by user")
        sys.stdout.flush()
        # Force exit after a short delay if interrupted
        time.sleep(1)
        os._exit(0)
    except Exception as e:
        print(f"CRITICAL ERROR: {str(e)}")
        sys.stdout.flush()
        import traceback
        print(traceback.format_exc())
        sys.stdout.flush()
        os._exit(1)