import os
import sys
import time
import uuid
import tempfile
import shutil
import threading
from threading import Semaphore
import logging
from typing import Dict, Any
import concurrent.futures
from datetime import datetime

from fastapi import FastAPI, HTTPException, Request, BackgroundTasks
from fastapi.responses import JSONResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware

from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

import yt_dlp
import subprocess
import re
import platform

# ------------------- NEW/CHANGED CONFIG -------------------
#MAX_CONCURRENT_JOBS = max(1, os.cpu_count() // 2)
MAX_CONCURRENT_JOBS = 1
job_semaphore = Semaphore(MAX_CONCURRENT_JOBS)
cpu_cores = os.cpu_count() or 1
concurrent_fragments = min(max(cpu_cores * 2, 4), 20)
safe_threads = max(os.cpu_count() // 2, 1)

# Threadpool for metadata extraction
metadata_executor = concurrent.futures.ThreadPoolExecutor(max_workers=safe_threads)

# Lock for jobs dict
jobs_lock = threading.Lock()

# ====== Configuration ======
API_KEY = os.getenv("API_KEY", "devkey")
HOST = "127.0.0.1"
PORT = 8000
CLEANUP_SECONDS = 60 * 60  # 1 hour

# ====== Logging ======
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("downloader_server")

log_file_path = os.path.join(os.getcwd(), "yt_job.log")
file_handler = logging.FileHandler(log_file_path, mode='a', encoding='utf-8')
file_handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
log.addHandler(file_handler)


def fmt_dur(seconds):
    """Convert seconds to H:MM:SS format."""
    m, s = divmod(int(seconds), 60)
    h, m = divmod(m, 60)
    return f"{h:d}:{m:02d}:{s:02d}"


app = FastAPI(title="yt-dlp job server (dev)")

# Prevent command prompt window from appearing on Windows
CREATE_NO_WINDOW = 0x08000000 if platform.system() == "Windows" else 0

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Jobs store
jobs: Dict[str, Dict[str, Any]] = {}

# (existing ffmpeg detection)
def find_ffmpeg_executable() -> str | None:
    ff_name = "ffmpeg.exe" if os.name == "nt" else "ffmpeg"
    meipass = getattr(sys, "_MEIPASS", None)
    if meipass:
        candidate = os.path.join(meipass, ff_name)
        if os.path.exists(candidate):
            return candidate
    candidate = os.path.join(os.path.dirname(__file__), ff_name)
    if os.path.exists(candidate):
        return candidate
    return shutil.which("ffmpeg")

FFMPEG_PATH = find_ffmpeg_executable()
if FFMPEG_PATH:
    log.info("ffmpeg found at: %s", FFMPEG_PATH)
else:
    log.warning("ffmpeg not found; falling back to progressive downloads.")

def get_video_codec(file_path: str) -> str | None:
    if not FFMPEG_PATH:
        log.warning("ffprobe not found, skipping codec check")
        return None
    ffprobe_cmd = [
        "ffprobe",
        "-v", "error",
        "-select_streams", "v:0",
        "-show_entries", "stream=codec_name",
        "-of", "default=noprint_wrappers=1:nokey=1",
        file_path,
    ]
    try:
        result = subprocess.run(ffprobe_cmd, capture_output=True, text=True, check=True)
        codec = result.stdout.strip()
        return codec.lower() if codec else None
    except Exception as e:
        log.warning(f"Failed to get codec info for {file_path}: {e}")
        return None

def schedule_cleanup(job_id: str, delay: int = CLEANUP_SECONDS):
    def _cleanup():
        entry = None
        with jobs_lock:
            entry = jobs.pop(job_id, None)
        if entry:
            try:
                shutil.rmtree(entry.get("dir", ""), ignore_errors=True)
                log.info("Cleaned up job %s", job_id)
            except Exception:
                pass
    t = threading.Timer(delay, _cleanup)
    t.daemon = True
    t.start()

def yt_progress_hook(job_id):
    def hook(d):
        try:
            with jobs_lock:
                j = jobs.get(job_id)
            if not j or j.get("cancelled"):
                return
            status = d.get("status")
            # update within lock
            with jobs_lock:
                j = jobs.get(job_id)
                if not j:
                    return
                j["last_yt_status"] = status
                if status == "downloading":
                    total = d.get("total_bytes") or d.get("total_bytes_estimate")
                    downloaded = d.get("downloaded_bytes")
                    percent = int(downloaded * 100 / total) if total and downloaded else None
                    j["progress"] = {
                        "percent": percent,
                        "eta": d.get("eta"),
                        "speed": d.get("speed"),
                    }
                elif status == "finished":
                    j["progress"] = {"percent": 100}
                elif status == "error":
                    j["error"] = d.get("message", "unknown")
        except Exception as e:
            log.debug("Progress hook minor issue: %s", e)
    return hook

def format_size(bytes_value):
    if not bytes_value:
        return None
    for unit in ['B', 'KB', 'MB', 'GB']:
        if bytes_value < 1024:
            return f"{bytes_value:.1f} {unit}"
        bytes_value /= 1024
    return f"{bytes_value:.1f} TB"

# ------------------- NEW: metadata fetcher -------------------
def fetch_metadata(job_id: str, url: str):
    """
    Runs in the metadata threadpool. Updates jobs[job_id] with metadata fields:
    - meta_status: 'pending'|'running'|'done'|'error'
    - title
    - estimated_size (bytes)
    - estimated_size_human
    - requested_formats (if available)
    """
    with jobs_lock:
        # If job got cancelled or already has metadata, skip
        j = jobs.get(job_id)
        if not j or j.get("cancelled") or j.get("meta_status") == "done":
            return
        j["meta_status"] = "running"

    try:
        ydl_opts = {
            "quiet": True,
            "no_warnings": True,
            "skip_download": True,  # yt-dlp option; but we'll use download=False call below
        }
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=False)
        title = info.get("title")
        requested_formats = info.get("requested_formats")
        if requested_formats:
            video_size = requested_formats[0].get("filesize") or 0
            audio_size = requested_formats[1].get("filesize") or 0
            total_size = video_size + audio_size
        else:
            total_size = info.get("filesize") or info.get("filesize_approx") or 0

        with jobs_lock:
            j = jobs.get(job_id)
            if not j:
                return
            j["title"] = title
            j["requested_formats"] = requested_formats
            j["estimated_size"] = total_size or None
            j["estimated_size_human"] = format_size(total_size)
            j["meta_status"] = "done"
    except Exception as e:
        log.warning("Metadata fetch failed for %s: %s", job_id, e)
        with jobs_lock:
            j = jobs.get(job_id)
            if j:
                j["meta_status"] = "error"
                j["meta_error"] = str(e)

# Optional helper to queue metadata for existing jobs (e.g. left over after restart)
def queue_metadata_for_existing_jobs():
    with jobs_lock:
        for job_id, j in list(jobs.items()):
            if j.get("status") in ("pending", "running") and j.get("meta_status") not in ("running", "done"):
                metadata_executor.submit(fetch_metadata, job_id, j["url"])

# Fire off queue on import (safe: jobs is empty at fresh start, but useful if jobs loaded earlier)
threading.Thread(target=queue_metadata_for_existing_jobs, daemon=True).start()


def normalize_youtube_url(url: str) -> str:
    """
    Cleans YouTube links so that playlist URLs pointing to a single video
    are normalized to the direct video link — without touching Shorts, Live, or other formats.
    """
    try:
        parsed = urlparse(url)
        netloc = parsed.netloc.lower()
        path = parsed.path

        # Only handle YouTube domains
        if "youtube.com" not in netloc and "youtu.be" not in netloc:
            return url

        # ---- Handle short links ----
        if "youtu.be" in netloc:
            video_id = path.strip("/")
            return f"https://www.youtube.com/watch?v={video_id}"

        # ---- Leave Shorts, Live, Clip, Music URLs alone ----
        if any(seg in path for seg in ["/shorts/", "/live/", "/clip/", "/music/"]):
            return url

        # ---- For standard /watch?v=... URLs ----
        qs = parse_qs(parsed.query)
        if "v" in qs:
            # Keep only v=... , drop playlist-specific params
            new_qs = {"v": qs["v"][0]}
            new_query = urlencode(new_qs)
            new_url = parsed._replace(query=new_query)
            return urlunparse(new_url)

        # Otherwise, return unchanged
        return url

    except Exception:
        return url  # fail-safe


def resolve_single_video_url(url: str) -> str:
    normalized = normalize_youtube_url(url)
    # If normalization didn't remove playlist and we have no video ID, fallback
    if "playlist?list=" in normalized and "v=" not in normalized:
        try:
            ydl_opts = {'quiet': True, 'no_warnings': True, 'extract_flat': True}
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(url, download=False)
                if "entries" in info and info["entries"]:
                    return info["entries"][0].get("webpage_url") or normalized
        except Exception as e:
            log.warning(f"yt-dlp fallback failed: {e}")
    return normalized




# ------------------- run_job changes: avoid blocking on metadata -------------------
def run_job(job_id: str, url: str):
    start_time = time.time()
    # Acquire a slot for this job (waits if limit reached)
    with job_semaphore:
        with jobs_lock:
            jobs[job_id]["status"] = "running"
            jobs[job_id]["stage"] = "downloading"
        tempdir = tempfile.mkdtemp(prefix="ydl_")
        with jobs_lock:
            jobs[job_id]["dir"] = tempdir

        try:
            def is_cancelled():
                with jobs_lock:
                    return jobs.get(job_id, {}).get("cancelled", False)

            if is_cancelled():
                with jobs_lock:
                    jobs[job_id]["status"] = "cancelled"
                return

            # Build ydl_opts (unchanged)
            ydl_opts = {
                "outtmpl": os.path.join(tempdir, "%(title)s.%(ext)s"),
                "format": "best[ext=mp4][vcodec^=avc1]/bestvideo[ext=mp4][vcodec^=avc1]+bestaudio[ext=m4a]/best",
                "merge_output_format": "mp4",
                "noplaylist": True,
                "continuedl": True,
                "quiet": True,
                "no_warnings": True,
                "progress_hooks": [yt_progress_hook(job_id)],
                "concurrent_fragment_downloads": concurrent_fragments,
                "fragment_retries": 15,
                "retries": 10,
                "prefer_free_formats": False,
                "overwrites": True
            }

            if FFMPEG_PATH:
                ydl_opts["ffmpeg_location"] = FFMPEG_PATH
            else:
                with jobs_lock:
                    jobs[job_id]["warning"] = "ffmpeg not found; using fallback format"

            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                # If metadata is already available we don't need to pre-fetch anything.
                # We intentionally DO NOT block waiting for metadata; download proceeds whether or not metadata is ready.
                # Actual download (this will return the info dict after download completes)
                download_start = time.time()
                log.info("[Job %s] Starting yt-dlp download at %s", job_id,datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                info = ydl.extract_info(url, download=True)
                download_end = time.time()
                download_duration = download_end - download_start
                log.info("[Job %s] ✅ Download completed in %s seconds", job_id, fmt_dur(download_duration))
                print(f"[Job {job_id}] #### Download completed in {download_duration:.2f} seconds")
                # --- Detect which format rule matched ---
                # info may contain info['requested_downloads'] or info['requested_formats']
                chosen_format = None
                if "requested_formats" in info and info["requested_formats"]:
                    vfmt = info["requested_formats"][0]
                    afmt = info["requested_formats"][1] if len(info["requested_formats"]) > 1 else None
                    chosen_format = {
                        "video_format_id": vfmt.get("format_id"),
                        "video_ext": vfmt.get("ext"),
                        "video_codec": vfmt.get("vcodec"),
                        "audio_format_id": afmt.get("format_id") if afmt else None,
                        "audio_ext": afmt.get("ext") if afmt else None,
                        "audio_codec": afmt.get("acodec") if afmt else None,
                    }
                    log.info("[Job %s] Selected separate video/audio formats: %s", job_id, chosen_format)
                    print(f"[Job {job_id}] Selected separate video/audio formats:", chosen_format)
                else:
                    chosen_format = {
                        "format_id": info.get("format_id"),
                        "ext": info.get("ext"),
                        "vcodec": info.get("vcodec"),
                        "acodec": info.get("acodec"),
                    }
                    log.info("[Job %s] Selected single unified format: %s", job_id, chosen_format)
                    print(f"[Job {job_id}] Selected single unified format:", chosen_format)

                # --- Detect if merging happened ---
                was_merged = bool(info.get("requested_formats"))
                log.info("[Job %s] FFmpeg merge occurred: %s", job_id, was_merged)
                print(f"[Job {job_id}] FFmpeg merge occurred:", was_merged)

                # After download, we can set filename/path info
                final_path = ydl.prepare_filename(info)
                if not os.path.exists(final_path):
                    files = [f for f in os.listdir(tempdir) if os.path.isfile(os.path.join(tempdir, f))]
                    if not files:
                        raise RuntimeError("yt-dlp did not produce a file")
                    final_path = os.path.join(tempdir, files[0])
            
            
            # Check codec and re-encode if needed
            convert_duration = 0
            codec = get_video_codec(final_path)
            if codec and codec not in ("h264", "avc1"):
                convert_start = time.time()
                log.info("[Job %s] Starting FFmpeg conversion to H.264 at %s", job_id,datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                log.info("Re-encoding %s to H.264 for job %s", codec, job_id)
                log.info("[Job %s] Re-encoding required: original codec was %s", job_id, codec)
                print(f"[Job {job_id}] Re-encoding required: original codec was {codec}")
                with jobs_lock:
                    jobs[job_id]["stage"] = "converting"
                    jobs[job_id]["progress"] = {"percent": 100, "note": "Converting to H.264..."}

                converted_path = final_path + ".tmp.mp4"
                ffmpeg_cmd = [
                    FFMPEG_PATH or "ffmpeg",
                    "-threads", str(safe_threads),
                    "-i", final_path,
                    "-c:v", "libx264",
                    "-preset", "ultrafast",
                    "-crf", "22",
                    "-c:a", "copy",
                    converted_path
                ]
                
                log.info("[Job %s] FFmpeg command: %s", job_id, " ".join(ffmpeg_cmd))
                print(f"[Job {job_id}] Running FFmpeg conversion to H.264...")


                proc = subprocess.Popen(ffmpeg_cmd, creationflags=CREATE_NO_WINDOW)
                with jobs_lock:
                    jobs[job_id]["ffmpeg_proc"] = proc
                retcode = proc.wait()
                convert_end = time.time()

                convert_duration = convert_end - convert_start
                log.info("[Job %s] ✅ Conversion completed in %.2f seconds", job_id, convert_duration)
                print(f"[Job {job_id}] #### Conversion completed in {convert_duration:.2f} seconds")
                with jobs_lock:
                    jobs[job_id].pop("ffmpeg_proc", None)

                if retcode != 0:
                    raise RuntimeError("FFmpeg conversion failed.")

                os.replace(converted_path, final_path)
                with jobs_lock:
                    jobs[job_id]["converted"] = True
                    jobs[job_id]["original_codec"] = codec
                log.info("Conversion complete: %s", final_path)
            else:
                log.info("[Job %s] No re-encoding needed. Codec already %s", job_id, codec)
                print(f"[Job {job_id}] No re-encoding needed. Codec already {codec}")
                with jobs_lock:
                    jobs[job_id]["converted"] = False
                    jobs[job_id]["original_codec"] = codec or "unknown"
            total_end = time.time()
            total_duration = total_end - start_time

            log.info("[Job %s] 🏁 Total job duration: %s seconds (download: %s + convert: %s)",job_id, fmt_dur(total_duration), fmt_dur(download_duration), fmt_dur(convert_duration))

                     
            with jobs_lock:
                jobs[job_id].update({
                    "stage": "finished",
                    "status": "done",
                    "filename": os.path.basename(final_path),
                    "path": final_path,
                    "finished": time.time(),
                })
            
            log.info("Job %s finished successfully.\n", job_id)
            schedule_cleanup(job_id)

        except Exception as e:
            log.exception("Job %s error: %s", job_id, e)
            with jobs_lock:
                jobs[job_id].update({"status": "error", "error": str(e)})
            try:
                shutil.rmtree(tempdir, ignore_errors=True)
            except Exception:
                pass

        finally:
            log.info("Job %s completed/failed — slot released (available: %d/%d)",
                     job_id, job_semaphore._value, MAX_CONCURRENT_JOBS)

# ========== API ENDPOINTS ==========

@app.post("/start_job")
async def start_job(req: Request):
    key = req.headers.get("x-api-key")
    if key != API_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized")

    body = await req.json()
    url = body.get("url")
    if not url:
        raise HTTPException(status_code=400, detail="Missing url")
    url = resolve_single_video_url(url)

    job_id = str(uuid.uuid4())
    with jobs_lock:
        jobs[job_id] = {
            "status": "pending",
            "stage": "pending",
            "created": time.time(),
            "url": url,
            "progress": {"percent": 0},
            "cancelled": False,
            "meta_status": "pending",  # NEW: metadata lifecycle
        }

    # Submit metadata extraction immediately (decoupled)
    metadata_executor.submit(fetch_metadata, job_id, url)

    # Start the main download/convert thread
    t = threading.Thread(target=run_job, args=(job_id, url), daemon=True)
    with jobs_lock:
        jobs[job_id]["thread"] = t
    t.start()

    return JSONResponse({"job_id": job_id})

@app.post("/cancel_job/{job_id}")
async def cancel_job(job_id: str, request: Request):
    key = request.headers.get("x-api-key")
    if key != API_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized")

    with jobs_lock:
        job = jobs.get(job_id)
        if not job:
            raise HTTPException(status_code=404, detail="Job not found")
        job["cancelled"] = True
        job["status"] = "cancelled"

    # If FFmpeg is running, terminate it with fallback kill
    proc = job.get("ffmpeg_proc")
    if proc and proc.poll() is None:  # Still running
        try:
            proc.terminate()
            proc.wait(timeout=5)
            log.info("Terminated FFmpeg process for job %s", job_id)
        except subprocess.TimeoutExpired:
            proc.kill()
            log.warning("Force-killed FFmpeg process for job %s after timeout", job_id)
        except Exception as e:
            log.warning("Failed to terminate FFmpeg for job %s: %s", job_id, e)

    try:
        shutil.rmtree(job.get("dir", ""), ignore_errors=True)
    except Exception:
        pass

    log.info("Cancelled job %s", job_id)
    return JSONResponse({"status": "cancelled"})

@app.get("/job_status/{job_id}")
async def job_status(job_id: str, request: Request):
    key = request.headers.get("x-api-key")
    if key != API_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized")
    with jobs_lock:
        j = jobs.get(job_id)
        if not j:
            raise HTTPException(status_code=404, detail="Job not found")
        # return also meta_status fields
        payload = {
            "job_id": job_id,
            "status": j.get("status"),
            "stage": j.get("stage", "unknown"),
            "progress": j.get("progress"),
            "filename": j.get("filename"),
            "title": j.get("title"),
            "estimated_size": j.get("estimated_size"),
            "error": j.get("error"),
            "warning": j.get("warning"),
            "meta_status": j.get("meta_status"),
            "meta_error": j.get("meta_error"),
            "estimated_size_human": j.get("estimated_size_human"),
        }
    return JSONResponse(payload)

@app.get("/files/{job_id}")
async def serve_file(job_id: str, key: str = None, background_tasks: BackgroundTasks = None):
    if key != API_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized")

    with jobs_lock:
        j = jobs.get(job_id)
        if not j or j.get("status") != "done":
            raise HTTPException(status_code=404, detail="File not ready")
        path = j.get("path")
    if not path or not os.path.exists(path):
        raise HTTPException(status_code=404, detail="File missing")

    def _remove():
        time.sleep(300)
        try:
            with jobs_lock:
                jobs.pop(job_id, None)
            shutil.rmtree(j.get("dir", ""), ignore_errors=True)
            log.info(" 🏁 Served and cleaned job %s", job_id)
        except Exception:
            pass

    background_tasks.add_task(_remove)
    return FileResponse(path, media_type="application/octet-stream", filename=j.get("filename"))

@app.get("/status")
async def status(request: Request):
    key = request.headers.get("x-api-key")
    if key != API_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized")

    summary = []
    with jobs_lock:
        for job_id, job in jobs.items():
            summary.append({
                "job_id": job_id,
                "status": job.get("status"),
                "stage": job.get("stage", "unknown"),
                "title": job.get("title"),
                "progress": job.get("progress"),
                "filename": job.get("filename"),
                "cancelled": job.get("cancelled"),
                "meta_status": job.get("meta_status"),
            })
    return JSONResponse({"jobs": summary})


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host=HOST, port=PORT)
