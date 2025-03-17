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

tasks = set()

def ensure_wav_format(wav_file_path, output_sample_rate=16000, output_channels=1):
    """Ensure WAV file is in correct format (16kHz mono). Returns new path or original if error."""
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
    try:
        with wave.open(path, 'rb') as w:
            frames = w.getnframes()
            rate = w.getframerate()
            return frames / float(rate)
    except:
        return 20.0

class HeyGenDualRecorder:
    def __init__(self, api_key, avatar_name="Wayne_20240711",
                 output_dir="recordings", wav_file="input.wav",
                 mp4_file=None, enable_gs=False, appid=None, channel=None):
        logger.info(f"Initializing with avatar={avatar_name}, wav={wav_file}, mp4={mp4_file}, enable_gs={enable_gs}")
        self.api_key = api_key
        self.avatar_name = avatar_name
        self.output_dir = output_dir
        self.wav_file = wav_file
        self.mp4_file = mp4_file           # If provided => we’ll create MP4 at the end
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
        
        self.video_file_path = None
        self.wav_output_path = None
        self.frame_width = 720
        self.frame_height = 1280
        self.fps = 30
        
        # If we do local audio
        self.audio_sample_rate = 48000
        self.audio_channels = 1
        
        self.recording_duration = 20
        self.input_wav_duration = None
        
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        
        # GStreamer pipelines for audio / video (if enabled)
        self.gs_audio_pipeline = None
        self.gs_video_pipeline = None
        self.gs_audio_appsrc = None
        self.gs_video_appsrc = None
        
        # We’ll keep a tiny buffer for audio frames
        self.gs_audio_running = False
        self.gs_video_running = False

        if self.enable_gs:
            if not GST_AVAILABLE:
                raise RuntimeError("GStreamer not available; cannot enable --enable-gs")
            if not self.gs_appid or not self.gs_channel:
                raise RuntimeError("--enable-gs requires both --appid and --channel as well")
            
            # Initialize GStreamer
            Gst.init(None)
            logger.info("GStreamer initialized for audio/video pipelines")
        
    def get_heygen_token(self):
        # ...
        # Same as original. For brevity, skipping code body here
        # ...
        pass

    def create_streaming_session(self, token):
        # ...
        pass

    def start_streaming_session(self, token):
        # ...
        pass

    def connect_to_websocket(self):
        # ...
        pass

    def prepare_wav_file(self):
        self.formatted_wav_file = ensure_wav_format(self.wav_file)
        self.input_wav_duration = get_wav_duration(self.formatted_wav_file)
        logger.info(f"input WAV duration = {self.input_wav_duration:.2f}s")
        self.recording_duration = self.input_wav_duration + 5
        return self.formatted_wav_file

    def send_audio_to_websocket(self):
        # ...
        pass

    async def record(self, livekit_url, livekit_token):
        logger.info("Starting record()")
        room = rtc.Room()
        start_time = time.time()
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # If user asked for mp4:
        do_mp4 = (self.mp4_file is not None)
        
        # We’ll store raw video in a temp file, and raw WAV in output_dir
        mp4_output_path = os.path.join(self.output_dir, f"{self.avatar_name}_{timestamp}.mp4") \
                          if do_mp4 else None
        self.wav_output_path = os.path.join(self.output_dir, f"{self.avatar_name}_{timestamp}_audio.wav")
        video_file = tempfile.NamedTemporaryFile(suffix='.yuv', delete=False)
        self.video_file_path = video_file.name
        video_file.close()
        
        if self.enable_gs:
            logger.info("Will send decoded frames to GStreamer pipelines instead of local YUV/WAV")
        else:
            logger.info("Will save local YUV + WAV, then optional MP4 mux")
        
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
            
            self.prepare_wav_file()
            self.connect_to_websocket()
            self.stop_event.clear()
            
            # Start thread for audio
            audio_thread = threading.Thread(target=self.send_audio_to_websocket)
            audio_thread.daemon = True
            audio_thread.start()
            
            while (time.time() - start_time < self.recording_duration) and not self.stop_event.is_set():
                elapsed = time.time() - start_time
                remain = self.recording_duration - elapsed
                if int(elapsed) % 5 == 0 and int(elapsed) > 0:
                    logger.info(f"Recording: {int(elapsed)}s elapsed, {int(remain)}s remain")
                
                if self.audio_sent_event.is_set():
                    logger.info("Audio done => keep going a few more sec for flush")
                    await asyncio.sleep(3)
                    break
                
                await asyncio.sleep(1)
            
            logger.info("Exiting record() loop due to time or stop_event")
        
        except Exception as e:
            logger.error(f"Error in record: {e}")
        finally:
            self.stop_event.set()
            for t in list(tasks):
                t.cancel()
            
            # If we saved local raw video + WAV, combine into MP4
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
                        logger.error(f"Cannot create MP4: {e2}")
            
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

    async def process_video_frames(self, video_stream):
        logger.info("process_video_frames started")
        frame_count = 0
        first_frame = True
        
        # If using GStreamer pipeline for video
        if self.enable_gs:
            # Build pipeline:
            # We feed raw RGB => 'jpegdec' is not correct for raw RGB. We might do 'videoconvert' -> 'jpegenc' -> 'jpegdec'?
            # For simplicity let’s assume we feed appsrc => we do 'videoconvert ! x264enc ...'
            # We'll do 'appsrc name=video_appsrc ! videoconvert ! x264enc key-int-max=60 tune=zerolatency ! agorasink appid=xxx channel=yyy'
            
            pipeline_desc = f"""
            appsrc name=video_appsrc ! videoconvert ! x264enc key-int-max=60 tune=zerolatency ! agorasink appid={self.gs_appid} channel={self.gs_channel}
            """
            self.gs_video_pipeline = Gst.parse_launch(pipeline_desc)
            self.gs_video_appsrc = self.gs_video_pipeline.get_by_name("video_appsrc")
            # Set caps:
            caps = Gst.Caps.from_string(f"video/x-raw,format=RGB,width={self.frame_width},height={self.frame_height},framerate={self.fps}/1")
            self.gs_video_appsrc.set_property("caps", caps)
            self.gs_video_appsrc.set_property("stream-type", 0)  # 0 => STREAM
            # Start pipeline
            self.gs_video_pipeline.set_state(Gst.State.PLAYING)
            self.gs_video_running = True
        
        else:
            # We'll do local approach => open self.video_file_path
            pass
        
        try:
            if not self.enable_gs:
                video_file = open(self.video_file_path, 'wb')
            
            async for frame_event in video_stream:
                buffer = frame_event.frame
                arr = np.frombuffer(buffer.data, dtype=np.uint8)
                arr = arr.reshape((buffer.height, buffer.width, 3))
                if first_frame:
                    self.frame_height, self.frame_width = arr.shape[0], arr.shape[1]
                    logger.info(f"Video first frame => {self.frame_width}x{self.frame_height}")
                    first_frame = False
                
                frame_count += 1
                
                if self.enable_gs and self.gs_video_running:
                    # Convert arr => GStreamer Buffer
                    gstbuf = Gst.Buffer.new_allocate(None, arr.size, None)
                    gstmap = gstbuf.map(Gst.MapFlags.WRITE)
                    ctypes.memmove(gstmap.data, arr.ctypes.data, arr.nbytes)
                    gstbuf.unmap(gstmap)
                    
                    # Provide a timestamp
                    gstbuf.pts = gstbuf.dts = int(frame_count * Gst.SECOND / self.fps)
                    
                    # Push
                    ret = self.gs_video_appsrc.emit("push-buffer", gstbuf)
                    if ret != Gst.FlowReturn.OK:
                        logger.warning(f"push-buffer flow_return={ret}")
                    
                else:
                    # Save raw data
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
            logger.error(f"Video frames error: {e}")
        finally:
            if not self.enable_gs:
                pass  # local file is closed in `finally` of record()
            else:
                if self.gs_video_pipeline and self.gs_video_running:
                    logger.info("Shutting down GStreamer video pipeline")
                    self.gs_video_pipeline.set_state(Gst.State.NULL)
                    self.gs_video_running = False

    async def process_audio_frames(self, audio_stream):
        logger.info("process_audio_frames started")
        sample_rate = 48000
        sample_count = 0
        
        if self.enable_gs:
            # 'appsrc name=audio_appsrc ! audioconvert ! opusenc ! agorasink audio=true appid=xxx channel=yyy'
            pipeline_desc = f"""
            appsrc name=audio_appsrc format=time is-live=true ! audioconvert ! opusenc ! agorasink audio=true appid={self.gs_appid} channel={self.gs_channel}
            """
            self.gs_audio_pipeline = Gst.parse_launch(pipeline_desc)
            self.gs_audio_appsrc = self.gs_audio_pipeline.get_by_name("audio_appsrc")
            # Caps for raw audio: 16-bit, mono, 48000
            caps = Gst.Caps.from_string("audio/x-raw,format=S16LE,channels=1,rate=48000,layout=interleaved")
            self.gs_audio_appsrc.set_property("caps", caps)
            self.gs_audio_appsrc.set_property("stream-type", 0)  # 0 => STREAM
            self.gs_audio_pipeline.set_state(Gst.State.PLAYING)
            self.gs_audio_running = True
        else:
            # We'll store to a local WAV
            wav_file = wave.open(self.wav_output_path, 'wb')
            wav_file.setnchannels(1)
            wav_file.setsampwidth(2)
            wav_file.setframerate(sample_rate)
        
        try:
            async for frame_event in audio_stream:
                aframe = frame_event.frame
                if not aframe or not aframe.data:
                    continue
                # Write each sample
                if not self.enable_gs:
                    # local WAV approach
                    with wave.open(self.wav_output_path, 'ab') as wf:
                        for sample in aframe.data:
                            clamped = max(min(sample, 32767), -32768)
                            wf.writeframes(struct.pack('<h', int(clamped)))
                    
                else:
                    # GStreamer approach
                    arr = np.array(aframe.data, dtype=np.int16)
                    # turn to bytes
                    raw_bytes = arr.tobytes()
                    
                    gstbuf = Gst.Buffer.new_allocate(None, len(raw_bytes), None)
                    gstmap = gstbuf.map(Gst.MapFlags.WRITE)
                    ctypes.memmove(gstmap.data, raw_bytes, len(raw_bytes))
                    gstbuf.unmap(gstmap)
                    
                    # Provide a PTS
                    sample_count += len(arr)
                    # approximate time
                    pts_ns = int(sample_count * Gst.SECOND / sample_rate)
                    gstbuf.pts = pts_ns
                    gstbuf.dts = pts_ns
                    
                    ret = self.gs_audio_appsrc.emit("push-buffer", gstbuf)
                    if ret != Gst.FlowReturn.OK:
                        logger.warning(f"Audio push-buffer flow_return={ret}")
                
                if sample_count % (sample_rate) < 100:
                    sec = sample_count / sample_rate
                    logger.info(f"Audio ~{sec:.1f}s processed")
                
                if self.stop_event.is_set():
                    break
        
        except asyncio.CancelledError:
            logger.info("Audio processing cancelled")
        except Exception as e:
            logger.error(f"Audio frames error: {e}")
        finally:
            if not self.enable_gs:
                pass
            else:
                if self.gs_audio_pipeline and self.gs_audio_running:
                    logger.info("Shutting down GStreamer audio pipeline")
                    self.gs_audio_pipeline.set_state(Gst.State.NULL)
                    self.gs_audio_running = False

async def main():
    parser = argparse.ArgumentParser(description='Record HeyGen avatar to WAV + MP4 or GStreamer.')
    parser.add_argument('--api-key', required=True)
    parser.add_argument('--avatar-name', default='Wayne_20240711')
    parser.add_argument('--output-dir', default='recordings')
    parser.add_argument('--wav-file', default='input.wav')
    parser.add_argument('--mp4-file', default=None, help='Path to MP4 output (if you want local MP4).')
    parser.add_argument('--enable-gs', action='store_true', help='Enable GStreamer pipelines for audio/video.')
    parser.add_argument('--appid', default=None, help='AppID for GStreamer sink.')
    parser.add_argument('--channel', default=None, help='Channel for GStreamer sink.')
    parser.add_argument('--debug', action='store_true')
    args = parser.parse_args()

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Check ffmpeg
    try:
        subprocess.run(['ffmpeg', '-version'], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    except:
        print("FFmpeg not found. Install it.")
        return 1
    
    # Possibly check GStreamer if enable_gs
    if args.enable_gs and not GST_AVAILABLE:
        print("ERROR: GStreamer python bindings not found, cannot proceed with --enable-gs.")
        return 1
    
    # Create instance
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
    
    # For brevity, we skip showing the full token + session creation code. Let's mock:
    heygen_token = "FAKE_TOKEN"
    livekit_url = "FAKE_URL"
    livekit_token = "FAKE_LK_TOKEN"
    
    # In real usage:
    # heygen_token = recorder.get_heygen_token()
    # livekit_url, livekit_token = recorder.create_streaming_session(heygen_token)
    # recorder.start_streaming_session(heygen_token)
    
    # Then do recorder.record
    try:
        await asyncio.wait_for(
            recorder.record(livekit_url, livekit_token),
            timeout=300  # Max 5 min
        )
        print("Done recording!")
    except asyncio.TimeoutError:
        print("Timed out.")
    except Exception as e:
        print(f"Error: {e}")

async def graceful_shutdown():
    logger.info("Graceful shutdown triggered")
    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    asyncio.get_event_loop().stop()

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.add_signal_handler(SIGINT, lambda: asyncio.create_task(graceful_shutdown()))
    loop.add_signal_handler(SIGTERM, lambda: asyncio.create_task(graceful_shutdown()))
    try:
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        pass
    finally:
        loop.run_until_complete(graceful_shutdown())
        loop.close()

