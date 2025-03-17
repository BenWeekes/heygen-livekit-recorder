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
import ctypes  # needed for GStreamer buffer operations
from datetime import datetime
from signal import SIGINT, SIGTERM

from livekit import rtc

# Attempt to import GStreamer GI if user wants --enable-gs
GST_AVAILABLE = False
try:
    import gi
    gi.require_version('Gst', '1.0')
    from gi.repository import Gst
    GST_AVAILABLE = True
except ImportError:
    pass

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    stream=sys.stdout
)
logger = logging.getLogger("HeyGenRecorder")
logging.getLogger('websocket').setLevel(logging.WARNING)

# A global set to track running async tasks in the event loop
tasks = set()

def ensure_wav_format(wav_file_path, output_sample_rate=16000, output_channels=1):
    """Ensure a WAV file is in the correct (16kHz, mono) format. Returns path to a possibly converted file."""
    temp_file = tempfile.NamedTemporaryFile(suffix='.wav', delete=False)
    temp_file_path = temp_file.name
    temp_file.close()
    
    try:
        cmd = [
            'ffmpeg',
            '-y',
            '-i', wav_file_path,
            '-ar', str(output_sample_rate),
            '-ac', str(output_channels),
            '-f', 'wav',
            temp_file_path
        ]
        subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        logger.info(f"Converted WAV to {output_sample_rate}Hz mono => {temp_file_path}")
        return temp_file_path
    except subprocess.CalledProcessError as e:
        logger.error(f"Error converting WAV: {e}")
        if os.path.exists(temp_file_path):
            os.unlink(temp_file_path)
        return wav_file_path

def get_wav_duration(path):
    """Return the duration (in seconds) of a WAV file."""
    try:
        with wave.open(path, 'rb') as w:
            frames = w.getnframes()
            rate = w.getframerate()
            return frames / float(rate)
    except Exception as e:
        logger.error(f"Error reading WAV duration: {e}")
        return 20.0  # fallback


class HeyGenDualRecorder:
    def __init__(
        self,
        api_key,
        avatar_name="Wayne_20240711",
        output_dir="recordings",
        wav_file="input.wav",
        mp4_file=None,
        enable_gs=False,
        appid=None,
        channel=None
    ):
        logger.info(
            f"Initializing with avatar={avatar_name}, wav={wav_file}, "
            f"mp4={mp4_file}, enable_gs={enable_gs}"
        )
        
        self.api_key = api_key
        self.avatar_name = avatar_name
        self.output_dir = output_dir
        self.wav_file = wav_file
        self.mp4_file = mp4_file           # if provided => we’ll create an MP4 at end
        self.enable_gs = enable_gs
        self.gs_appid = appid
        self.gs_channel = channel
        
        self.session_id = None
        self.token = None
        self.ws = None
        self.formatted_wav_file = None
        self.realtime_endpoint = None
        
        self.stop_event = asyncio.Event()
        self.audio_sent_event = asyncio.Event()
        
        # Output paths
        self.video_file_path = None
        self.wav_output_path = None
        
        # Default (may be overridden by first actual frame)
        self.frame_width = 720
        self.frame_height = 1280
        self.fps = 30
        
        # If we do local audio saving
        self.audio_sample_rate = 48000
        self.audio_channels = 1
        
        # We'll choose how long to record based on the input WAV plus buffer
        self.recording_duration = 20
        self.input_wav_duration = None
        
        # Make sure output dir exists
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        
        # If using GStreamer, we check availability
        if self.enable_gs:
            if not GST_AVAILABLE:
                raise RuntimeError("GStreamer not available; cannot enable --enable-gs")
            if not self.gs_appid or not self.gs_channel:
                raise RuntimeError("--enable-gs requires both --appid and --channel")
            # Initialize GStreamer
            Gst.init(None)
            logger.info("GStreamer initialized for audio/video pipelines")
            
        # GStreamer pipelines, appsrc references
        self.gs_video_pipeline = None
        self.gs_audio_pipeline = None
        self.gs_video_appsrc = None
        self.gs_audio_appsrc = None
        self.gs_video_running = False
        self.gs_audio_running = False

    # -------------------------------------------------------------------------
    #                 HELPER METHODS FROM YOUR SECOND SCRIPT
    # -------------------------------------------------------------------------
    def get_heygen_token(self):
        """
        Get authentication token from the HeyGen API.
        """
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
        """
        Create a streaming session with HeyGen.
        Returns (livekit_url, livekit_token) or raises exception on failure.
        """
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
        
        # For realtime audio sending
        self.realtime_endpoint = session_data.get("realtime_endpoint")
        if not self.realtime_endpoint:
            logger.warning("No realtime_endpoint found in response.")
        else:
            logger.info(f"Realtime endpoint: {self.realtime_endpoint}")
        
        if not livekit_token or not livekit_url:
            logger.error("Missing LiveKit credentials in response")
            raise Exception("Missing LiveKit credentials in response")
            
        logger.info(f"Successfully created streaming session, session_id={self.session_id}")
        return livekit_url, livekit_token

    def start_streaming_session(self, token):
        """
        Start the streaming session with HeyGen API.
        """
        if not self.session_id:
            raise Exception("Session ID not set; cannot start streaming session.")
        
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
        """
        Connect to HeyGen's real-time WebSocket for server-to-server audio.
        """
        if not self.realtime_endpoint:
            logger.error("No realtime_endpoint found – cannot connect WebSocket.")
            return None
        
        logger.info(f"Connecting to HeyGen realtime WebSocket: {self.realtime_endpoint}")
        
        try:
            websocket.enableTrace(False)
            self.ws = websocket.create_connection(self.realtime_endpoint, timeout=5)
            logger.info("Real-time WebSocket connection established")
            
            # Temporarily blocking to grab initial message
            self.ws.sock.setblocking(True)
            self.ws.sock.settimeout(2)
            try:
                init_msg = self.ws.recv()
                logger.info(f"Initial WebSocket message: {init_msg[:200]}")
            except websocket.WebSocketTimeoutException:
                logger.info("No initial message received (timeout).")
            except Exception as e:
                logger.warning(f"Error receiving initial message: {e}")
            finally:
                # back to non-blocking
                self.ws.sock.setblocking(False)
            
            return self.ws
        
        except Exception as e:
            logger.error(f"Error connecting to WebSocket: {str(e)}", exc_info=True)
            return None

    # -------------------------------------------------------------------------
    #                 WAV PREPARATION AND AUDIO WEBSOCKET SENDER
    # -------------------------------------------------------------------------
    def prepare_wav_file(self):
        """Convert the input WAV to 16kHz mono, measure duration, set record length."""
        self.formatted_wav_file = ensure_wav_format(self.wav_file)
        self.input_wav_duration = get_wav_duration(self.formatted_wav_file)
        logger.info(f"input WAV duration = {self.input_wav_duration:.2f}s")
        
        # We'll record a bit longer than the input audio
        self.recording_duration = self.input_wav_duration + 5
        return self.formatted_wav_file

    def send_audio_to_websocket(self):
        """
        Sends audio data from the prepared WAV file in small chunks
        to the HeyGen WebSocket. Based on your second snippet's
        chunk-based approach.
        """
        if not self.ws:
            logger.error("WebSocket connection not established; cannot send audio.")
            return
        
        if not self.formatted_wav_file:
            self.prepare_wav_file()
        
        wav_file_path = self.formatted_wav_file
        logger.info(f"Sending audio from {wav_file_path} to WebSocket.")
        
        try:
            with wave.open(wav_file_path, 'rb') as wav_file:
                sample_rate = wav_file.getframerate()
                num_channels = wav_file.getnchannels()
                sample_width = wav_file.getsampwidth()
                
                logger.info(
                    f"WAV info: {sample_rate}Hz, {num_channels}ch, {sample_width}B/sample"
                )
                
                # Read entire audio
                wav_file.rewind()
                all_audio_data = wav_file.readframes(wav_file.getnframes())
                
                # The official docs expect 24kHz, 16-bit, mono for the agent.speak event.
                if sample_rate != 24000 or num_channels != 1:
                    logger.info(
                        f"Converting audio from {sample_rate}Hz/{num_channels}ch to 24000Hz/mono..."
                    )
                    
                    with tempfile.NamedTemporaryFile(suffix='.wav', delete=False) as temp_file:
                        temp_path = temp_file.name
                    
                    cmd = [
                        'ffmpeg',
                        '-y',
                        '-f', 's16le',  # force 16-bit PCM input
                        '-ar', str(sample_rate),
                        '-ac', str(num_channels),
                        '-i', '-',
                        '-ar', '24000',   # output sample rate
                        '-ac', '1',       # output channels
                        '-f', 's16le',    # output as raw PCM
                        temp_path
                    ]
                    
                    try:
                        p = subprocess.Popen(
                            cmd,
                            stdin=subprocess.PIPE,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE
                        )
                        stdout_data, stderr_data = p.communicate(input=all_audio_data)
                        
                        with open(temp_path, 'rb') as f:
                            converted_audio = f.read()
                        
                        all_audio_data = converted_audio
                        sample_rate = 24000
                        num_channels = 1
                        logger.info(
                            f"Successfully converted audio to 24kHz/mono, {len(all_audio_data)} bytes"
                        )
                    except Exception as e:
                        logger.error(f"Error converting audio to 24kHz mono: {e}", exc_info=True)
                    finally:
                        if os.path.exists(temp_path):
                            os.unlink(temp_path)
                
                # Now we chunk it. Each sample is 2 bytes (16-bit).
                bytes_per_sample = 2 * num_channels
                chunk_size_samples = int(sample_rate * 0.5)  # ~500ms chunk
                chunk_size_bytes = chunk_size_samples * bytes_per_sample
                total_samples = len(all_audio_data) // bytes_per_sample
                
                total_chunks = (total_samples // chunk_size_samples) + 1
                logger.info(f"Sending audio in up to {total_chunks} chunks (~500ms each)")
                
                # We'll read responses in a background thread
                def ws_receiver():
                    try:
                        self.ws.sock.setblocking(True)
                        self.ws.sock.settimeout(1.0)
                        while not self.stop_event.is_set():
                            try:
                                msg = self.ws.recv()
                                if msg:
                                    logger.info(f"[WebSocket response] {msg[:200]}...")
                            except websocket.WebSocketTimeoutException:
                                continue
                            except Exception as e:
                                if not self.stop_event.is_set():
                                    logger.warning(f"Error in WS receiver: {e}")
                                break
                    except Exception as e:
                        logger.error(f"WS receiver thread error: {e}")
                
                receiver_thread = threading.Thread(target=ws_receiver, daemon=True)
                receiver_thread.start()
                
                # Try a ping to confirm connection
                try:
                    self.ws.sock.setblocking(True)
                    self.ws.ping()
                except Exception as e:
                    logger.error(f"WebSocket ping error: {e}", exc_info=True)
                    return
                finally:
                    self.ws.sock.setblocking(False)
                
                # Send the data in chunks
                for i in range(0, len(all_audio_data), chunk_size_bytes):
                    chunk = all_audio_data[i: i + chunk_size_bytes]
                    if not chunk:
                        break
                    
                    chunk_event_id = str(uuid.uuid4())
                    
                    audio_base64 = base64.b64encode(chunk).decode('utf-8')
                    audio_event = {
                        "type": "agent.speak",
                        "audio": audio_base64,
                        "event_id": chunk_event_id
                    }
                    
                    success = False
                    attempts = 0
                    max_attempts = 3
                    
                    while attempts < max_attempts and not success:
                        attempts += 1
                        try:
                            self.ws.sock.setblocking(True)
                            self.ws.send(json.dumps(audio_event))
                            success = True
                        except ssl.SSLWantWriteError:
                            logger.warning("Socket buffer full; retrying...")
                            time.sleep(0.01 * attempts)
                        except websocket.WebSocketConnectionClosedException:
                            logger.error("WebSocket connection closed mid-send.")
                            return
                        except Exception as e:
                            logger.error(f"Error sending chunk: {e}", exc_info=True)
                            return
                        finally:
                            self.ws.sock.setblocking(False)
                    
                    if not success:
                        logger.error("Failed to send chunk after retries.")
                        return
                    
                    # Just a small sleep
                    time.sleep(0.001)
                
                # Wait a little before sending the speak_end event
                logger.info("All audio chunks sent. Waiting 0.5s before speak_end.")
                time.sleep(0.5)
                
                speak_end_event_id = str(uuid.uuid4())
                end_event = {
                    "type": "agent.speak_end",
                    "event_id": speak_end_event_id
                }
                
                try:
                    self.ws.sock.setblocking(True)
                    self.ws.send(json.dumps(end_event))
                    logger.info(f"Sent agent.speak_end event: {speak_end_event_id}")
                except Exception as e:
                    logger.error(f"Error sending speak_end event: {e}", exc_info=True)
                finally:
                    self.ws.sock.setblocking(False)
                
                logger.info("Waiting ~3s after sending speak_end for responses.")
                time.sleep(3)
                
                # Signal that audio sending is done
                self.audio_sent_event.set()
                
                receiver_thread.join(timeout=1.0)
                logger.info("Audio send complete.")
        
        except Exception as e:
            logger.error(f"Error in send_audio_to_websocket: {e}", exc_info=True)
        finally:
            # Cleanup temporary wave if it was a conversion
            if (self.formatted_wav_file != self.wav_file) and os.path.exists(self.formatted_wav_file):
                try:
                    os.unlink(self.formatted_wav_file)
                except:
                    pass

    # -------------------------------------------------------------------------
    #                 MAIN RECORD FUNCTION (VIDEO + AUDIO)
    # -------------------------------------------------------------------------
    async def record(self, livekit_url, livekit_token):
        logger.info("Starting record()")
        room = rtc.Room()
        start_time = time.time()
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # If the user wants a final MP4
        do_mp4 = (self.mp4_file is not None)
        
        # We'll store raw video in a temp file, and raw WAV in output_dir
        mp4_output_path = (
            os.path.join(self.output_dir, f"{self.avatar_name}_{timestamp}.mp4")
            if do_mp4 else None
        )
        self.wav_output_path = os.path.join(
            self.output_dir, f"{self.avatar_name}_{timestamp}_audio.wav"
        )
        
        video_file = tempfile.NamedTemporaryFile(suffix='.yuv', delete=False)
        self.video_file_path = video_file.name
        video_file.close()
        
        if self.enable_gs:
            logger.info("Will send decoded frames to GStreamer pipeline (no local YUV/WAV).")
        else:
            logger.info("Will save local raw YUV + WAV, then optionally mux to MP4.")
        
        @room.on("track_subscribed")
        def on_track_subscribed(track, publication, participant):
            global tasks
            if track.kind == rtc.TrackKind.KIND_VIDEO:
                logger.info(f"Got video track: {track.name}")
                video_stream = rtc.VideoStream(track, format=rtc.VideoBufferType.RGB24)
                task = asyncio.create_task(self.process_video_frames(video_stream))
                tasks.add(task)
                task.add_done_callback(tasks.remove)
            elif track.kind == rtc.TrackKind.KIND_AUDIO:
                logger.info(f"Got audio track: {track.name}")
                audio_stream = rtc.AudioStream(track)
                task = asyncio.create_task(self.process_audio_frames(audio_stream))
                tasks.add(task)
                task.add_done_callback(tasks.remove)
        
        try:
            self.audio_sent_event.clear()
            
            await room.connect(livekit_url, livekit_token)
            await asyncio.sleep(1)
            
            # Prepare WAV file
            self.prepare_wav_file()
            self.connect_to_websocket()
            self.stop_event.clear()
            
            # Kick off audio in a separate thread
            audio_thread = threading.Thread(target=self.send_audio_to_websocket, daemon=True)
            audio_thread.start()
            
            # Loop until we hit the recording_duration or we get a stop
            while (time.time() - start_time < self.recording_duration) and not self.stop_event.is_set():
                elapsed = time.time() - start_time
                remain = self.recording_duration - elapsed
                if int(elapsed) % 5 == 0 and int(elapsed) > 0:
                    logger.info(f"Recording: {int(elapsed)}s elapsed, {int(remain)}s remain")
                
                # If audio is done, let's wait a few seconds more, then break
                if self.audio_sent_event.is_set():
                    logger.info("Audio done => allow a few seconds flush, then stop")
                    await asyncio.sleep(3)
                    break
                
                await asyncio.sleep(1)
            
            logger.info("Exiting record() loop due to time or stop_event.")
        
        except Exception as e:
            logger.error(f"Error in record: {e}", exc_info=True)
        finally:
            self.stop_event.set()
            for t in list(tasks):
                t.cancel()
            
            # If local YUV + WAV were saved and user wants MP4, mux them
            if do_mp4 and not self.enable_gs:
                if os.path.exists(self.video_file_path) and os.path.exists(self.wav_output_path):
                    try:
                        logger.info(f"Muxing => {mp4_output_path}")
                        cmd = [
                            'ffmpeg',
                            '-y',
                            '-f', 'rawvideo',
                            '-vcodec', 'rawvideo',
                            '-pix_fmt', 'rgb24',
                            '-s', f'{self.frame_width}x{self.frame_height}',
                            '-r', str(self.fps),
                            '-i', self.video_file_path,
                            '-i', self.wav_output_path,
                            '-c:v', 'libx264',
                            '-preset', 'medium',
                            '-crf', '23',
                            '-pix_fmt', 'yuv420p',
                            '-c:a', 'aac',
                            '-b:a', '256k',
                            '-vsync', 'cfr',
                            '-async', '1',
                            mp4_output_path
                        ]
                        
                        r = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
                        if r.returncode == 0:
                            logger.info(f"Created MP4 => {mp4_output_path}")
                        else:
                            logger.error(f"FFmpeg error: {r.stderr}")
                    except Exception as e2:
                        logger.error(f"Cannot create MP4: {e2}", exc_info=True)
            
            # Clean up
            if self.video_file_path and os.path.exists(self.video_file_path):
                os.unlink(self.video_file_path)
            
            if self.ws:
                try:
                    self.ws.close()
                except:
                    pass
            
            await room.disconnect()
            logger.info("room disconnected")

    # -------------------------------------------------------------------------
    #                 VIDEO AND AUDIO FRAME PROCESSING
    # -------------------------------------------------------------------------
    async def process_video_frames(self, video_stream):
        logger.info("process_video_frames started")
        frame_count = 0
        first_frame = True
        
        if self.enable_gs:
            # GStreamer pipeline for video
            # For example: appsrc -> videoconvert -> x264enc -> agorasink
            pipeline_desc = f"""
            appsrc name=video_appsrc ! videoconvert ! x264enc key-int-max=60 tune=zerolatency ! agorasink appid={self.gs_appid} channel={self.gs_channel}
            """
            self.gs_video_pipeline = Gst.parse_launch(pipeline_desc)
            self.gs_video_appsrc = self.gs_video_pipeline.get_by_name("video_appsrc")
            
            caps = Gst.Caps.from_string(
                f"video/x-raw,format=RGB,width={self.frame_width},height={self.frame_height},framerate={self.fps}/1"
            )
            self.gs_video_appsrc.set_property("caps", caps)
            self.gs_video_appsrc.set_property("stream-type", 0)  # 0 => STREAM
            
            self.gs_video_pipeline.set_state(Gst.State.PLAYING)
            self.gs_video_running = True
        else:
            # We'll write raw frames to a local file
            pass
        
        try:
            if not self.enable_gs:
                # open once; append raw frames
                pass  # We'll open/append inside the loop or just open in "ab"
            
            async for frame_event in video_stream:
                buffer = frame_event.frame
                arr = np.frombuffer(buffer.data, dtype=np.uint8)
                arr = arr.reshape((buffer.height, buffer.width, 3))
                
                if first_frame:
                    self.frame_height, self.frame_width = arr.shape[:2]
                    logger.info(f"Video first frame => {self.frame_width}x{self.frame_height}")
                    first_frame = False
                
                frame_count += 1
                
                if self.enable_gs and self.gs_video_running:
                    gstbuf = Gst.Buffer.new_allocate(None, arr.size, None)
                    gstmap = gstbuf.map(Gst.MapFlags.WRITE)
                    ctypes.memmove(gstmap.data, arr.ctypes.data, arr.nbytes)
                    gstbuf.unmap(gstmap)
                    
                    # Timestamps
                    gstbuf.pts = gstbuf.dts = int(frame_count * Gst.SECOND / self.fps)
                    
                    ret = self.gs_video_appsrc.emit("push-buffer", gstbuf)
                    if ret != Gst.FlowReturn.OK:
                        logger.warning(f"Video push-buffer flow_return={ret}")
                else:
                    # Append to .yuv file
                    with open(self.video_file_path, 'ab') as vf:
                        vf.write(arr.tobytes())
                
                if frame_count % 30 == 0:
                    logger.info(f"Video frames: {frame_count}")
                
                if self.stop_event.is_set():
                    break
            
            logger.info(f"Finished video stream => {frame_count} frames")
        
        except asyncio.CancelledError:
            logger.info("Video processing cancelled")
        except Exception as e:
            logger.error(f"Video frames error: {e}", exc_info=True)
        finally:
            if self.enable_gs and self.gs_video_pipeline and self.gs_video_running:
                logger.info("Shutting down GStreamer video pipeline")
                self.gs_video_pipeline.set_state(Gst.State.NULL)
                self.gs_video_running = False

    async def process_audio_frames(self, audio_stream):
        logger.info("process_audio_frames started")
        sample_rate = 48000
        sample_count = 0
        
        if self.enable_gs:
            # GStreamer pipeline for audio
            pipeline_desc = f"""
            appsrc name=audio_appsrc format=time is-live=true ! audioconvert ! opusenc ! agorasink audio=true appid={self.gs_appid} channel={self.gs_channel}
            """
            self.gs_audio_pipeline = Gst.parse_launch(pipeline_desc)
            self.gs_audio_appsrc = self.gs_audio_pipeline.get_by_name("audio_appsrc")
            caps = Gst.Caps.from_string("audio/x-raw,format=S16LE,channels=1,rate=48000,layout=interleaved")
            self.gs_audio_appsrc.set_property("caps", caps)
            self.gs_audio_appsrc.set_property("stream-type", 0)
            self.gs_audio_pipeline.set_state(Gst.State.PLAYING)
            self.gs_audio_running = True
        else:
            # Local WAV
            wf = wave.open(self.wav_output_path, 'wb')
            wf.setnchannels(1)
            wf.setsampwidth(2)
            wf.setframerate(sample_rate)
            wf.close()
        
        try:
            async for frame_event in audio_stream:
                aframe = frame_event.frame
                if not aframe or not aframe.data:
                    continue
                
                if self.enable_gs and self.gs_audio_running:
                    # Convert to bytes
                    arr = np.array(aframe.data, dtype=np.int16)
                    raw_bytes = arr.tobytes()
                    
                    gstbuf = Gst.Buffer.new_allocate(None, len(raw_bytes), None)
                    gstmap = gstbuf.map(Gst.MapFlags.WRITE)
                    ctypes.memmove(gstmap.data, raw_bytes, len(raw_bytes))
                    gstbuf.unmap(gstmap)
                    
                    sample_count += len(arr)
                    pts_ns = int(sample_count * Gst.SECOND / sample_rate)
                    gstbuf.pts = pts_ns
                    gstbuf.dts = pts_ns
                    
                    ret = self.gs_audio_appsrc.emit("push-buffer", gstbuf)
                    if ret != Gst.FlowReturn.OK:
                        logger.warning(f"Audio push-buffer flow_return={ret}")
                    
                else:
                    # Append to local WAV
                    # We'll open for append each time to avoid concurrency issues
                    with wave.open(self.wav_output_path, 'ab') as wf:
                        for sample in aframe.data:
                            clamped = max(min(sample, 32767), -32768)
                            wf.writeframes(struct.pack('<h', int(clamped)))
                    
                    sample_count += len(aframe.data)
                
                if sample_count % sample_rate < 100:
                    sec = sample_count / sample_rate
                    logger.info(f"Audio ~{sec:.1f}s processed")
                
                if self.stop_event.is_set():
                    break
        
        except asyncio.CancelledError:
            logger.info("Audio processing cancelled")
        except Exception as e:
            logger.error(f"Audio frames error: {e}", exc_info=True)
        finally:
            if self.enable_gs:
                if self.gs_audio_pipeline and self.gs_audio_running:
                    logger.info("Shutting down GStreamer audio pipeline")
                    self.gs_audio_pipeline.set_state(Gst.State.NULL)
                    self.gs_audio_running = False
            else:
                # nothing special – local WAV is appended in the loop
                pass


# -------------------------------------------------------------------------
#                               MAIN ENTRY POINT
# -------------------------------------------------------------------------
async def main():
    parser = argparse.ArgumentParser(description='Record HeyGen avatar to WAV + MP4 or GStreamer.')
    parser.add_argument('--api-key', required=True, help='HeyGen API key')
    parser.add_argument('--avatar-name', default='Wayne_20240711', help='Avatar name')
    parser.add_argument('--output-dir', default='recordings', help='Directory for outputs')
    parser.add_argument('--wav-file', default='input.wav', help='Path to input WAV file')
    parser.add_argument('--mp4-file', default=None, help='Path to optional MP4 output')
    parser.add_argument('--enable-gs', action='store_true', help='Use GStreamer pipelines for audio/video')
    parser.add_argument('--appid', default=None, help='AppID for GStreamer sink')
    parser.add_argument('--channel', default=None, help='Channel for GStreamer sink')
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')
    args = parser.parse_args()

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Check FFmpeg
    try:
        subprocess.run(['ffmpeg', '-version'], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    except:
        print("FFmpeg not found. Install it or add to PATH.")
        return 1
    
    # If user wants GStreamer but not installed
    if args.enable_gs and not GST_AVAILABLE:
        print("ERROR: GStreamer python bindings not found, cannot proceed with --enable-gs.")
        return 1
    
    # Create the recorder
    recorder = HeyGenDualRecorder(
        api_key=args.api_key,
        avatar_name=args.avatar_name,
        output_dir=args.output_dir,
        wav_file=args.wav_file,
        mp4_file=args.mp4_file,
        enable_gs=args.enable_gs,
        appid=args.appid,
        channel=args.channel
    )
    
    # Acquire HeyGen token and create LiveKit session
    try:
        heygen_token = recorder.get_heygen_token()
        livekit_url, livekit_token = recorder.create_streaming_session(heygen_token)
        recorder.start_streaming_session(heygen_token)
    except Exception as e:
        logger.error(f"Could not initialize streaming session: {e}")
        return 1
    
    # figure out a maximum overall timeout in case it stalls
    # (input_wav_duration + buffer from the recorder itself) => but let's just do 300s
    try:
        await asyncio.wait_for(recorder.record(livekit_url, livekit_token), timeout=300)
        logger.info("Done recording!")
    except asyncio.TimeoutError:
        logger.warning("Timed out after 300s.")
    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)
    
    return 0


async def graceful_shutdown():
    logger.info("Graceful shutdown triggered")
    # Cancel all tasks except ourselves
    others = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    for t in others:
        t.cancel()
    await asyncio.gather(*others, return_exceptions=True)
    asyncio.get_event_loop().stop()


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    
    # Attach signal handlers for graceful shutdown
    loop.add_signal_handler(SIGINT, lambda: asyncio.create_task(graceful_shutdown()))
    loop.add_signal_handler(SIGTERM, lambda: asyncio.create_task(graceful_shutdown()))
    
    try:
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        pass
    finally:
        loop.run_until_complete(graceful_shutdown())
        loop.close()
