#!/usr/bin/env python3
import asyncio
import json
import logging
import requests
import sys
import os
import time
import traceback
import argparse
import subprocess
import wave
import uuid
import base64
import websocket
import threading
import ssl
from datetime import datetime

print("HELLO FROM MONKEY-PATCH SCRIPT!")

from aiortc import (
    RTCPeerConnection,
    RTCSessionDescription,
    RTCConfiguration,
    RTCIceServer,
    RTCRtpReceiver
)
# Import MediaRecorder from contrib.media (for optional MP4)
from aiortc.contrib.media import MediaRecorder
from aiortc.rtp import RtpPacket

#####################
# GStreamer imports (if using --enable-gs)
#####################

try:
    import gi
    gi.require_version('Gst', '1.0')
    from gi.repository import Gst
    GST_AVAILABLE = True
    print("GStreamer successfully imported!")
except ImportError as e:
    print(f"GStreamer import error: {e}")
    GST_AVAILABLE = False

#####################
# Reassemblers
#####################

class MinimalOpusReassembler:
    def depacketize(self, payload: bytes, marker: bool) -> bytes:
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(f"[OpusReassembler] len={len(payload)}, marker={marker}")
        if not payload:
            return b""
        return payload

class MinimalVp8Reassembler:
    def __init__(self):
        self._buffer = bytearray()
        self._has_start = False

    def depacketize(self, payload: bytes, marker: bool) -> bytes:
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(f"[Vp8Reassembler] len={len(payload)}, marker={marker}")

        if len(payload) < 1:
            return b""
        first_byte = payload[0]
        S = (first_byte & 0x10) != 0
        X = (first_byte & 0x80) != 0

        if S:
            self._buffer = bytearray()
            self._has_start = True
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug("[Vp8Reassembler] Start of new frame")

        if not self._has_start:
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug("[Vp8Reassembler] ignoring because no start bit yet")
            return b""

        idx = 1
        if X and len(payload) > 1:
            b2 = payload[idx]
            idx += 1
            I = (b2 & 0x80) != 0
            L = (b2 & 0x40) != 0
            T = (b2 & 0x20) != 0
            K = (b2 & 0x10) != 0

            if I and idx < len(payload):
                pic_id = payload[idx]
                idx += 1
                if (pic_id & 0x80) and idx < len(payload):
                    idx += 1
            if L and idx < len(payload):
                idx += 1
            if (T or K) and idx < len(payload):
                idx += 1

        if idx > len(payload):
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug("[Vp8Reassembler] Malformed packet, skipping.")
            return b""

        appended = len(payload) - idx
        self._buffer += payload[idx:]
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(f"[Vp8Reassembler] Appended {appended} bytes, total={len(self._buffer)}")

        if marker:
            frame = bytes(self._buffer)
            self._buffer.clear()
            self._has_start = False
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(f"[Vp8Reassembler] marker=1 => returning frame of {len(frame)} bytes")
            return frame
        return b""


#####################
# Global logging
#####################

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("HeyGenRecorder")
logging.getLogger('websocket').setLevel(logging.WARNING)

#####################
# GStreamer pipeline helpers
#####################
class GStreamerPusher:
    """
    Simple helper to create a GStreamer pipeline with appsrc -> agorasink,
    and push raw bytes in real-time. This is a minimal / naive approach.

    If you want advanced handling (caps, timestamps, etc.), you need a more robust setup.
    """

    def __init__(self, is_audio, appid, channel):
        if not GST_AVAILABLE:
            raise RuntimeError("GStreamer not available (gi import failed).")

        Gst.init(None)

        if is_audio:
            # E.g.: appsrc ! agorasink audio=true appid=xxx channel=xxx
            self.pipeline_str = f"appsrc name=appsrc0 is-live=true block=true " \
                                f"! agorasink audio=true appid={appid} channel={channel}"
        else:
            # E.g.: appsrc ! agorasink appid=xxx channel=xxx
            self.pipeline_str = f"appsrc name=appsrc0 is-live=true block=true " \
                                f"! agorasink appid={appid} channel={channel}"

        logger.info(f"[GStreamerPusher] Creating pipeline: {self.pipeline_str}")
        self.pipeline = Gst.parse_launch(self.pipeline_str)
        self.appsrc = self.pipeline.get_by_name("appsrc0")
        if not self.appsrc:
            raise RuntimeError("Failed to get appsrc from pipeline")

        self.pipeline.set_state(Gst.State.PLAYING)
        logger.info("[GStreamerPusher] Pipeline set to PLAYING")

    def push_data(self, data: bytes):
        """
        Push raw encoded frames to appsrc. For audio or video.
        """
        if not data:
            return
        buf = Gst.Buffer.new_allocate(None, len(data), None)
        buf.fill(0, data)
        # We do not set any timestamps here, so GStreamer runs in "live" push mode
        retval = self.appsrc.emit("push-buffer", buf)
        if retval != Gst.FlowReturn.OK:
            logger.warning(f"GStreamer push-buffer flow return={retval}")

    def stop(self):
        logger.info("[GStreamerPusher] Stopping pipeline")
        self.pipeline.set_state(Gst.State.NULL)


#####################
# Patch RTCRtpReceiver._handle_rtp_packet
#####################

orig_handle_rtp_packet = RTCRtpReceiver._handle_rtp_packet

def patched_handle_rtp_packet(self, packet: RtpPacket, **kwargs):
    # We'll log at DEBUG if user gave --debug
    if logger.isEnabledFor(logging.DEBUG):
        real_kind = getattr(self, "_RTCRtpReceiver__kind", None)
        logger.debug(f"_handle_rtp_packet: seq={packet.sequence_number}, kind={real_kind}")

    real_kind = getattr(self, "_RTCRtpReceiver__kind", None)

    # If user has enable_gs on, we do not write local files. Instead, push to GStreamer
    if g_recorder and g_recorder.enable_gs:
        # If "video", assume VP8
        if real_kind == "video":
            frame = g_recorder.vp8_reassembler.depacketize(packet.payload, packet.marker)
            if frame and g_recorder.gs_video:
                g_recorder.gs_video.push_data(frame)
        # If "audio", assume Opus
        elif real_kind == "audio":
            chunk = g_recorder.opus_reassembler.depacketize(packet.payload, packet.marker)
            if chunk and g_recorder.gs_audio:
                g_recorder.gs_audio.push_data(chunk)

    else:
        # old approach: write local raw files
        if real_kind == "video":
            frame = g_vp8_reassembler.depacketize(packet.payload, packet.marker)
            if frame and g_vp8_file:
                g_vp8_file.write(frame)
        elif real_kind == "audio":
            chunk = g_opus_reassembler.depacketize(packet.payload, packet.marker)
            if chunk and g_opus_file:
                g_opus_file.write(chunk)

    return orig_handle_rtp_packet(self, packet, **kwargs)

RTCRtpReceiver._handle_rtp_packet = patched_handle_rtp_packet


#####################
# HeyGenRecorder
#####################

# We'll store a global reference for the monkey patch to see the settings
g_recorder = None

class HeyGenRecorder:
    def __init__(self, api_key, avatar_name="Wayne_20240711",
                 output_dir="recordings", wav_file="input.wav",
                 mp4_file=None,
                 enable_gs=False,
                 appid=None,
                 channel=None):
        self.api_key = api_key
        self.avatar_name = avatar_name
        self.output_dir = output_dir
        self.wav_file = wav_file
        self.mp4_file = mp4_file  # path to optional MP4
        self.enable_gs = enable_gs
        self.appid = appid
        self.channel = channel

        self.session_id = None
        self.realtime_endpoint = None
        self.peer_connection = None
        self.connection_established = False
        self.stop_event = asyncio.Event()
        self.audio_sent_event = threading.Event()
        self.ws = None

        # For optional MP4
        self.mp4_recorder = None
        self.mp4_started = False

        # If user wants gstreamer, set up pusher references
        self.gs_audio = None
        self.gs_video = None

        # We create local reassemblers for gstreamer usage
        self.vp8_reassembler = MinimalVp8Reassembler()
        self.opus_reassembler = MinimalOpusReassembler()

        if not enable_gs:
            # normal raw file approach
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            if not os.path.exists(output_dir):
                os.makedirs(output_dir)

            self.vp8_raw_path = os.path.join(output_dir, f"{avatar_name}_{timestamp}.vp8.raw")
            self.opus_raw_path = os.path.join(output_dir, f"{avatar_name}_{timestamp}.opus.raw")

            logger.info(f"Will write raw VP8 to: {self.vp8_raw_path}")
            logger.info(f"Will write raw Opus to: {self.opus_raw_path}")

            # open the global files
            global g_vp8_file, g_opus_file
            g_vp8_file = open(self.vp8_raw_path, "wb")
            g_opus_file = open(self.opus_raw_path, "wb")
        else:
            logger.info("Will send frames to GStreamer pipelines instead of local .raw files")
            if not GST_AVAILABLE:
                raise RuntimeError("GStreamer not available; cannot enable --enable-gs")
            if not self.appid or not self.channel:
                raise ValueError("Must supply --appid and --channel if using --enable-gs")

            # Build two GStreamer pushers
            self.gs_video = GStreamerPusher(is_audio=False, appid=self.appid, channel=self.channel)
            self.gs_audio = GStreamerPusher(is_audio=True, appid=self.appid, channel=self.channel)

    async def create_streaming_session(self):
        logger.info("Creating streaming session with HeyGen (v1) ...")
        url = "https://api.heygen.com/v1/streaming.new"
        headers = {"Content-Type": "application/json", "X-Api-Key": self.api_key}
        payload = {
            "avatar_name": self.avatar_name,
            "quality": "high",
            "voice": {"voice_id": ""},
            "version": "v1"
        }
        response = requests.post(url, headers=headers, json=payload)
        if response.status_code != 200:
            raise Exception(f"Create session failed: HTTP {response.status_code}: {response.text}")

        data = response.json()
        if data.get("code") != 100:
            raise Exception(f"Failed to create streaming session: {data}")

        session_data = data["data"]
        self.session_id = session_data.get("session_id")
        self.realtime_endpoint = session_data.get("realtime_endpoint")

        logger.info(f"Session created, ID={self.session_id}, endpoint={self.realtime_endpoint}")
        return session_data

    async def setup_webrtc(self, session_data):
        sdp_str = session_data["sdp"]["sdp"]
        logger.info("---- Remote SDP (codec lines) ----")
        for line in sdp_str.splitlines():
            if line.startswith("a=rtpmap:"):
                logger.info(line)
        logger.info("----------------------------------")

        ice_servers = []
        for s in session_data.get("ice_servers2", []):
            ice_servers.append(
                RTCIceServer(
                    urls=s.get("urls", []),
                    username=s.get("username", ""),
                    credential=s.get("credential", "")
                )
            )
        config = RTCConfiguration(iceServers=ice_servers)
        self.peer_connection = RTCPeerConnection(config)

        @self.peer_connection.on("track")
        async def on_track(track):
            logger.info(f"Received {track.kind} track")

            # If user wants an MP4, we can route track to self.mp4_recorder
            if self.mp4_file and not self.mp4_recorder:
                logger.info(f"Creating MP4 recorder at {self.mp4_file}")
                self.mp4_recorder = MediaRecorder(self.mp4_file, format="mp4")
                await self.mp4_recorder.start()
                self.mp4_started = True

            if self.mp4_recorder and self.mp4_started:
                self.mp4_recorder.addTrack(track)

        @self.peer_connection.on("connectionstatechange")
        async def on_conn_state():
            state = self.peer_connection.connectionState
            logger.info(f"Connection state changed: {state}")
            if state == "connected":
                self.connection_established = True
            elif state in ("failed", "closed", "disconnected"):
                self.connection_established = False
                self.stop_event.set()

        @self.peer_connection.on("icecandidate")
        async def on_ice_candidate(candidate):
            if candidate:
                await self.send_ice_candidate(candidate)

        sdp = session_data["sdp"]
        rd = RTCSessionDescription(sdp=sdp["sdp"], type=sdp["type"])
        await self.peer_connection.setRemoteDescription(rd)

    async def start_streaming_session(self):
        answer = await self.peer_connection.createAnswer()
        await self.peer_connection.setLocalDescription(answer)

        for _ in range(10):
            if self.peer_connection.iceGatheringState == "complete":
                break
            await asyncio.sleep(0.5)

        url = "https://api.heygen.com/v1/streaming.start"
        headers = {"Content-Type": "application/json", "X-Api-Key": self.api_key}
        sdp_answer = {
            "type": self.peer_connection.localDescription.type,
            "sdp": self.peer_connection.localDescription.sdp,
        }
        payload = {"session_id": self.session_id, "sdp": sdp_answer}
        r = requests.post(url, headers=headers, json=payload)
        j = r.json()
        if r.status_code != 200 or j.get("code") != 100:
            raise Exception(f"Failed to start streaming session: {j}")

        logger.info("Streaming session started successfully")

    async def send_ice_candidate(self, candidate):
        try:
            c = {
                "candidate": candidate.candidate,
                "sdpMid": candidate.sdpMid,
                "sdpMLineIndex": candidate.sdpMLineIndex,
                "usernameFragment": candidate.usernameFragment,
            }
            url = "https://api.heygen.com/v1/streaming.ice"
            headers = {"Content-Type": "application/json", "X-Api-Key": self.api_key}
            payload = {"session_id": self.session_id, "candidate": c}
            rr = requests.post(url, headers=headers, json=payload)
            if rr.status_code != 200:
                logger.warning(f"Failed to send ICE candidate: {rr.status_code}")
        except Exception as e:
            logger.error(f"Error sending ICE candidate: {e}")

    def connect_to_websocket(self):
        if not self.realtime_endpoint:
            logger.info("No realtime_endpoint found; cannot send WAV data.")
            return
        try:
            websocket.enableTrace(False)
            self.ws = websocket.create_connection(self.realtime_endpoint, timeout=5)
            logger.info("Real-time WebSocket connection established")
            self.ws.sock.setblocking(True)
            self.ws.sock.settimeout(2)
            try:
                init_message = self.ws.recv()
                logger.info(f"Initial WebSocket message: {init_message}")
            except:
                pass
            self.ws.sock.setblocking(False)
        except Exception as e:
            logger.error(f"Error connecting to WebSocket: {e}")
            self.ws = None

    def send_audio_to_websocket(self):
        if not self.ws:
            logger.error("No WebSocket to send WAV data.")
            return
        if not self.wav_file or not os.path.exists(self.wav_file):
            logger.info("No WAV file or file not found.")
            return

        logger.info(f"Sending WAV from {self.wav_file} to WebSocket...")

        try:
            with wave.open(self.wav_file, 'rb') as wf:
                sr = wf.getframerate()
                ch = wf.getnchannels()
                sw = wf.getsampwidth()
                logger.info(f"WAV: {sr}Hz, {ch}ch, {sw} bytes/sample")

                frames = wf.readframes(wf.getnframes())
                chunk_size = int(sr * 0.5)
                sample_bytes = sw * ch
                chunk_bytes = chunk_size * sample_bytes

                idx = 0
                while idx < len(frames):
                    if self.stop_event.is_set():
                        break
                    chunk = frames[idx : idx+chunk_bytes]
                    idx += chunk_bytes
                    if not chunk:
                        break
                    b64 = base64.b64encode(chunk).decode('utf-8')
                    event_id = str(uuid.uuid4())
                    msg = {
                        "type": "agent.speak",
                        "audio": b64,
                        "event_id": event_id
                    }
                    for _ in range(3):
                        try:
                            self.ws.sock.setblocking(True)
                            self.ws.send(json.dumps(msg))
                            break
                        except ssl.SSLWantWriteError:
                            time.sleep(0.01)
                        except Exception as e:
                            logger.error(f"Error sending chunk: {e}")
                        finally:
                            self.ws.sock.setblocking(False)
                    time.sleep(0.01)
                time.sleep(0.5)
                end_msg = {"type": "agent.speak_end", "event_id": str(uuid.uuid4())}
                try:
                    self.ws.sock.setblocking(True)
                    self.ws.send(json.dumps(end_msg))
                    logger.info("Sent agent.speak_end")
                except Exception as e:
                    logger.error(f"Error sending speak_end: {e}")
                finally:
                    self.ws.sock.setblocking(False)
                time.sleep(2.0)
        except Exception as e:
            logger.error(f"Error sending WAV: {e}")
            logger.error(traceback.format_exc())
        finally:
            self.audio_sent_event.set()
            logger.info("Finished sending WAV")

    async def cleanup(self):
        logger.info("Cleanup: closing files / PC / WS ...")

        # Close MP4 recorder if used
        if self.mp4_recorder and self.mp4_started:
            logger.info("Stopping MP4 recorder")
            await self.mp4_recorder.stop()
            logger.info("MP4 recorder stopped")

        # If enable_gs, stop GStreamer
        if self.enable_gs and self.gs_audio:
            self.gs_audio.stop()
            self.gs_audio = None
        if self.enable_gs and self.gs_video:
            self.gs_video.stop()
            self.gs_video = None

        # close our global raw files if not using gstreamer
        if not self.enable_gs:
            global g_vp8_file, g_opus_file
            if g_vp8_file:
                g_vp8_file.close()
                g_vp8_file = None
                logger.info("Closed VP8 file")
            if g_opus_file:
                g_opus_file.close()
                g_opus_file = None
                logger.info("Closed Opus file")

        if self.peer_connection:
            await self.peer_connection.close()
            logger.info("Closed peer connection")

        if self.ws:
            try:
                self.ws.close()
            except:
                pass
            logger.info("Closed WebSocket")

        logger.info("Cleanup done")

    async def run(self):
        try:
            session_data = await self.create_streaming_session()
            await asyncio.sleep(1)
            await self.setup_webrtc(session_data)
            await self.start_streaming_session()

            # Wait for connection
            for i in range(60):
                if self.connection_established:
                    logger.info("WebRTC connected!")
                    break
                await asyncio.sleep(1)
            else:
                logger.error("Timed out waiting for connection.")
                return False

            logger.info("Recording now. Press Ctrl+C to stop.")
            self.stop_event.clear()

            if self.wav_file:
                self.connect_to_websocket()
                if self.ws:
                    thread = threading.Thread(target=self.send_audio_to_websocket)
                    thread.daemon = True
                    thread.start()

            while not self.stop_event.is_set():
                await asyncio.sleep(1)
                if not self.connection_established:
                    break
            return True

        except asyncio.CancelledError:
            logger.info("Recording cancelled.")
            raise
        except Exception as e:
            logger.error(f"Error in run(): {e}")
            logger.error(traceback.format_exc())
            return False
        finally:
            self.stop_event.set()
            await self.cleanup()


async def main():
    parser = argparse.ArgumentParser(
        description="Monkey-patch RTCRtpReceiver._handle_rtp_packet to dump raw VP8 & Opus. Optionally record MP4. Or send frames to GStreamer."
    )
    parser.add_argument("--api-key", required=True, help="HeyGen API key")
    parser.add_argument("--avatar-name", default="Wayne_20240711", help="Avatar name")
    parser.add_argument("--output-dir", default="recordings", help="Output directory")
    parser.add_argument("--wav-file", default="input.wav", help="Local WAV file for avatar (optional).")
    parser.add_argument("--mp4-file", default=None, help="Optional MP4 file path to record (re-encoded).")
    parser.add_argument("--debug", action="store_true", help="Enable debug logs")

    # GStreamer extra options
    parser.add_argument("--enable-gs", action="store_true", help="If set, use GStreamer pipelines instead of local raw files")
    parser.add_argument("--appid", default=None, help="Agora app ID for GStreamer pipeline")
    parser.add_argument("--channel", default=None, help="Agora channel for GStreamer pipeline")

    args = parser.parse_args()

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    try:
        subprocess.run(["ffmpeg", "-version"], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        logger.info("FFmpeg found.")
    except:
        logger.warning("FFmpeg not found, continuing anyway.")

    global g_recorder
    g_recorder = HeyGenRecorder(
        api_key=args.api_key,
        avatar_name=args.avatar_name,
        output_dir=args.output_dir,
        wav_file=args.wav_file,
        mp4_file=args.mp4_file,
        enable_gs=args.enable_gs,
        appid=args.appid,
        channel=args.channel
    )

    try:
        success = await g_recorder.run()
        if success:
            logger.info("Recording completed successfully.")
            return 0
        else:
            logger.error("Recording failed.")
            return 1
    except KeyboardInterrupt:
        logger.info("Interrupted by user.")
        g_recorder.stop_event.set()
        await asyncio.sleep(1)
        return 0
    except Exception as e:
        logger.error(f"Unhandled exception: {e}")
        return 1


if __name__ == "__main__":
    try:
        rc = asyncio.run(main())
        sys.exit(rc)
    except KeyboardInterrupt:
        sys.exit(0)
